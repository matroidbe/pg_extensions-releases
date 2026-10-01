//! CP-SAT scheduling surface for pg_ortools (feature `cpsat`).
//!
//! Exposes interval variables, cumulative resources, precedences, and a
//! minimise-wait objective as SQL, backed by the pure-Rust `ortools_core::cpsat`
//! engine (Pumpkin). Solving is synchronous via [`solve_cp_sync`]; the async
//! worker path is a later phase.
//!
//! Storage mirrors the engine model:
//! - `cp_intervals`   — operations `[start, start+duration)`
//! - `cp_resources`   — a station with an integer `capacity` (1 ⇒ disjunctive)
//! - `cp_demands`     — how much of a resource an interval consumes while running
//! - `cp_precedences` — `end[before] + gap <= start[after]`
//! - `cp_wait_objectives` — minimise `weight · (start[after] - end[before])`
//!
//! Constraints reference intervals/resources by name; the loader resolves names
//! to engine indices when building the model.

use std::collections::HashMap;

use ortools_core::cpsat::{Model, SolveLimits, SolveOutcome};
use pgrx::prelude::*;

use crate::error::PgOrtoolsError;

/// True when this backend has a pending cancel/terminate (statement_timeout,
/// `pg_cancel_backend`, `pg_terminate_backend`, or shutdown).
///
/// A plain volatile read of Postgres' interrupt flags. Crucially it does **not**
/// `longjmp` out of the CP solver's Rust stack the way `CHECK_FOR_INTERRUPTS`
/// would — that would skip destructors mid-search. Instead the search stops
/// cleanly, and the caller runs a real interrupt check at a safe point once the
/// solve has returned. This is what makes a synchronous `solve_cp_sync` respond
/// to `statement_timeout` and client cancel instead of pinning a core forever.
pub fn backend_interrupt_pending() -> bool {
    unsafe {
        std::ptr::addr_of!(pg_sys::QueryCancelPending).read_volatile() != 0
            || std::ptr::addr_of!(pg_sys::ProcDiePending).read_volatile() != 0
    }
}

// =============================================================================
// Schema (feature-gated: only created when the extension is built with `cpsat`)
// =============================================================================

pgrx::extension_sql!(
    r#"
CREATE TABLE IF NOT EXISTS pgortools.cp_intervals (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    duration INTEGER NOT NULL,
    earliest_start INTEGER NOT NULL DEFAULT 0,
    latest_end INTEGER NOT NULL,
    UNIQUE(problem_id, name)
);

CREATE TABLE IF NOT EXISTS pgortools.cp_resources (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    capacity INTEGER NOT NULL,
    UNIQUE(problem_id, name)
);

CREATE TABLE IF NOT EXISTS pgortools.cp_demands (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    resource_name TEXT NOT NULL,
    interval_name TEXT NOT NULL,
    demand INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS pgortools.cp_precedences (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    before_name TEXT NOT NULL,
    after_name TEXT NOT NULL,
    gap INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS pgortools.cp_wait_objectives (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    after_name TEXT NOT NULL,
    before_name TEXT NOT NULL,
    weight INTEGER NOT NULL DEFAULT 1
);
"#,
    name = "cpsat_bootstrap",
    requires = ["bootstrap"],
);

// =============================================================================
// Modelling functions
// =============================================================================

/// Add an interval variable (a scheduled operation) to a problem.
/// Occupies `[start, start + duration)`; `start` ranges over
/// `[earliest_start, latest_end - duration]`.
#[pg_extern]
fn add_interval_var(
    problem: &str,
    name: &str,
    duration: i32,
    earliest_start: i32,
    latest_end: i32,
) -> bool {
    run_for_problem(
        problem,
        r#"
        INSERT INTO pgortools.cp_intervals
            (problem_id, name, duration, earliest_start, latest_end)
        SELECT id, $2, $3, $4, $5 FROM pgortools.problems WHERE name = $1
        "#,
        &[
            problem.into(),
            name.into(),
            duration.into(),
            earliest_start.into(),
            latest_end.into(),
        ],
        "add interval",
    )
}

/// Declare a cumulative resource (a station). Concurrent demand may not exceed
/// `capacity`; `capacity = 1` makes it disjunctive (no overlap).
#[pg_extern]
fn add_resource(problem: &str, name: &str, capacity: i32) -> bool {
    run_for_problem(
        problem,
        r#"
        INSERT INTO pgortools.cp_resources (problem_id, name, capacity)
        SELECT id, $2, $3 FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), name.into(), capacity.into()],
        "add resource",
    )
}

/// Declare that `interval` consumes `demand` units of `resource` while running.
#[pg_extern]
fn add_demand(problem: &str, resource: &str, interval: &str, demand: default!(i32, 1)) -> bool {
    run_for_problem(
        problem,
        r#"
        INSERT INTO pgortools.cp_demands (problem_id, resource_name, interval_name, demand)
        SELECT id, $2, $3, $4 FROM pgortools.problems WHERE name = $1
        "#,
        &[
            problem.into(),
            resource.into(),
            interval.into(),
            demand.into(),
        ],
        "add demand",
    )
}

/// Add a precedence: `end[before] + gap <= start[after]`. Encodes FIFO ("serve
/// ticket-1 before ticket-3") and same-ticket ordering (grill before plate).
#[pg_extern]
fn add_precedence(problem: &str, before: &str, after: &str, gap: default!(i32, 0)) -> bool {
    run_for_problem(
        problem,
        r#"
        INSERT INTO pgortools.cp_precedences (problem_id, before_name, after_name, gap)
        SELECT id, $2, $3, $4 FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), before.into(), after.into(), gap.into()],
        "add precedence",
    )
}

/// Minimise `weight · (start[after] - end[before])` — how long `after` waits
/// after `before` finishes (e.g. a plated dish sitting after the grill).
#[pg_extern]
fn minimize_wait(problem: &str, after: &str, before: &str, weight: default!(i32, 1)) -> bool {
    run_for_problem(
        problem,
        r#"
        INSERT INTO pgortools.cp_wait_objectives (problem_id, after_name, before_name, weight)
        SELECT id, $2, $3, $4 FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), after.into(), before.into(), weight.into()],
        "add wait objective",
    )
}

/// Solve a scheduling problem synchronously with the CP engine. Returns a JSONB
/// document `{status, objective, intervals: {name: {start, end}}}` and stores it
/// so `get_solution` can retrieve it later.
///
/// Bounded and interruptible: the solve is capped at `pg_ortools.solver_time_limit`
/// seconds and polls for `statement_timeout` / query-cancel, so it can never pin a
/// core indefinitely proving optimality. On a cutoff it returns the best schedule
/// found so far (status `FEASIBLE`), or `UNKNOWN` if none was found yet.
#[pg_extern]
fn solve_cp_sync(problem: &str) -> pgrx::JsonB {
    let limits = crate::worker::cp_solve_limits();
    let json = match solve_and_store_cp(problem, &limits, &mut backend_interrupt_pending) {
        Ok(json) => json,
        Err(e) => pgrx::error!("{}", e),
    };
    // If a cancel/timeout tripped the solver's cooperative check above, raise it
    // now at a safe point (clean Rust stack) instead of returning as if nothing
    // happened.
    pgrx::check_for_interrupts!();
    pgrx::JsonB(json)
}

/// Solve a scheduling problem asynchronously via the background worker. Returns
/// a job_id to poll with `solve_status()`; the result lands in `get_solution()`.
///
/// `time_limit_seconds` caps the CP search; `NULL` (the default) uses
/// `pg_ortools.solver_time_limit`. The worker honours this limit for the CP solve
/// and can be interrupted mid-solve by `cancel_solve(job_id)`.
#[pg_extern]
fn solve_cp(problem: &str, time_limit_seconds: default!(Option<i32>, "NULL")) -> i64 {
    let config = crate::jobs::SolveJobConfig {
        time_limit_seconds,
        strategy: Some("cpsat".to_string()),
    };
    match crate::jobs::queue_solve_job(problem, &config) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Internals
// =============================================================================

/// Run a parameterised INSERT that requires the named problem to exist.
fn run_for_problem(
    problem: &str,
    sql: &str,
    args: &[pgrx::datum::DatumWithOid],
    what: &str,
) -> bool {
    match Spi::run_with_args(sql, args) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to {} for problem '{}': {}", what, problem, e),
    }
}

/// Load, solve (bounded by `limits`, interruptible via `cancelled`), store, and
/// return the result JSON. Shared by the synchronous [`solve_cp_sync`] and the
/// background-worker path.
///
/// `cancelled` is polled during the search: `solve_cp_sync` wires it to the
/// backend's interrupt flags; the worker wires it to the job's `cancelled` state.
pub fn solve_and_store_cp(
    problem: &str,
    limits: &SolveLimits,
    cancelled: &mut dyn FnMut() -> bool,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let result = build_cp_result(problem, limits, cancelled)?;
    store_cp_solution(problem, &result)?;
    Ok(result)
}

fn build_cp_result(
    problem: &str,
    limits: &SolveLimits,
    cancelled: &mut dyn FnMut() -> bool,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let (model, names) = load_cp_model(problem)?;
    let outcome = model.solve_with(limits, cancelled);
    Ok(outcome_to_json(&outcome, &model, &names))
}

/// Persist a CP result into `pgortools.solutions` so `get_solution` can return
/// it after an async solve. The full `{status, objective, intervals}` document
/// is stored as `variable_values`. Must run inside a transaction (SPI).
pub(crate) fn store_cp_solution(
    problem: &str,
    result: &serde_json::Value,
) -> Result<(), PgOrtoolsError> {
    let status = result["status"].as_str().unwrap_or("UNKNOWN");
    let objective = result["objective"].as_f64();
    let values = pgrx::JsonB(result.clone());

    Spi::run_with_args(
        r#"
        INSERT INTO pgortools.solutions
            (problem_id, status, objective_value, variable_values, solve_time_ms)
        SELECT p.id, $2, $3, $4, 0 FROM pgortools.problems p WHERE p.name = $1
        "#,
        &[
            problem.into(),
            status.into(),
            objective.into(),
            values.into(),
        ],
    )?;
    Ok(())
}

/// Load a problem's scheduling model from the `cp_*` tables. Returns the engine
/// model plus the index→interval-name mapping needed to decode the solution.
/// Must run inside a transaction (SPI); the returned `Model`/names are fully
/// owned, so the caller can drop the transaction and solve outside it.
pub(crate) fn load_cp_model(problem: &str) -> Result<(Model, Vec<String>), PgOrtoolsError> {
    let problem_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgortools.problems WHERE name = $1",
        &[problem.into()],
    )?
    .ok_or_else(|| PgOrtoolsError::ProblemNotFound(problem.to_string()))?;

    let mut model = Model::new();
    let mut name_to_idx: HashMap<String, usize> = HashMap::new();
    let mut idx_to_name: Vec<String> = Vec::new();

    // Intervals — ordered by id so indices are stable/deterministic.
    Spi::connect(|client| {
        let q = format!(
            "SELECT name::text, duration, earliest_start, latest_end \
             FROM pgortools.cp_intervals WHERE problem_id = {problem_id} ORDER BY id"
        );
        for row in client.select(&q, None, &[])? {
            let name: String = row.get(1)?.unwrap_or_default();
            let duration: i32 = row.get(2)?.unwrap_or(0);
            let earliest_start: i32 = row.get(3)?.unwrap_or(0);
            let latest_end: i32 = row.get(4)?.unwrap_or(0);
            let idx = model.add_interval(name.clone(), duration, earliest_start, latest_end);
            name_to_idx.insert(name.clone(), idx);
            idx_to_name.push(name);
        }
        Ok::<_, pgrx::spi::Error>(())
    })?;

    let idx = |n: &str| -> Result<usize, PgOrtoolsError> {
        name_to_idx
            .get(n)
            .copied()
            .ok_or_else(|| PgOrtoolsError::InvalidParameter(format!("unknown interval '{n}'")))
    };

    // Resources → one cumulative per resource, from its demand rows.
    let resources: Vec<(String, i32)> = Spi::connect(|client| {
        let q = format!(
            "SELECT name::text, capacity FROM pgortools.cp_resources \
             WHERE problem_id = {problem_id} ORDER BY id"
        );
        let mut out = Vec::new();
        for row in client.select(&q, None, &[])? {
            out.push((
                row.get::<String>(1)?.unwrap_or_default(),
                row.get(2)?.unwrap_or(1),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })?;

    for (resource, capacity) in resources {
        let members: Vec<(String, i32)> = Spi::connect(|client| {
            let q = format!(
                "SELECT interval_name::text, demand FROM pgortools.cp_demands \
                 WHERE problem_id = {problem_id} AND resource_name = $1 ORDER BY id"
            );
            let mut out = Vec::new();
            for row in client.select(&q, None, &[resource.as_str().into()])? {
                out.push((
                    row.get::<String>(1)?.unwrap_or_default(),
                    row.get(2)?.unwrap_or(1),
                ));
            }
            Ok::<_, pgrx::spi::Error>(out)
        })?;

        if members.is_empty() {
            continue; // a resource nobody uses constrains nothing
        }
        let mut intervals = Vec::with_capacity(members.len());
        let mut demands = Vec::with_capacity(members.len());
        for (interval, demand) in members {
            intervals.push(idx(&interval)?);
            demands.push(demand);
        }
        model.add_cumulative(intervals, demands, capacity);
    }

    // Precedences.
    let precedences: Vec<(String, String, i32)> = Spi::connect(|client| {
        let q = format!(
            "SELECT before_name::text, after_name::text, gap \
             FROM pgortools.cp_precedences WHERE problem_id = {problem_id} ORDER BY id"
        );
        let mut out = Vec::new();
        for row in client.select(&q, None, &[])? {
            out.push((
                row.get::<String>(1)?.unwrap_or_default(),
                row.get::<String>(2)?.unwrap_or_default(),
                row.get(3)?.unwrap_or(0),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })?;
    for (before, after, gap) in precedences {
        model.add_precedence(idx(&before)?, idx(&after)?, gap);
    }

    // Wait objective.
    let objectives: Vec<(String, String, i32)> = Spi::connect(|client| {
        let q = format!(
            "SELECT after_name::text, before_name::text, weight \
             FROM pgortools.cp_wait_objectives WHERE problem_id = {problem_id} ORDER BY id"
        );
        let mut out = Vec::new();
        for row in client.select(&q, None, &[])? {
            out.push((
                row.get::<String>(1)?.unwrap_or_default(),
                row.get::<String>(2)?.unwrap_or_default(),
                row.get(3)?.unwrap_or(1),
            ));
        }
        Ok::<_, pgrx::spi::Error>(out)
    })?;
    for (after, before, weight) in objectives {
        model.minimize_wait(idx(&after)?, idx(&before)?, weight);
    }

    Ok((model, idx_to_name))
}

pub(crate) fn outcome_to_json(
    outcome: &SolveOutcome,
    model: &Model,
    names: &[String],
) -> serde_json::Value {
    let (status, solution) = match outcome {
        SolveOutcome::Optimal(s) => ("OPTIMAL", Some(s)),
        SolveOutcome::Satisfiable(s) => ("FEASIBLE", Some(s)),
        SolveOutcome::Infeasible => ("INFEASIBLE", None),
        SolveOutcome::Unknown => ("UNKNOWN", None),
    };

    let mut intervals = serde_json::Map::new();
    let mut objective = serde_json::Value::Null;
    if let Some(sol) = solution {
        for (i, name) in names.iter().enumerate() {
            intervals.insert(
                name.clone(),
                serde_json::json!({ "start": sol.starts[i], "end": sol.end(model, i) }),
            );
        }
        if let Some(obj) = sol.objective {
            objective = serde_json::json!(obj);
        }
    }

    serde_json::json!({
        "status": status,
        "objective": objective,
        "intervals": serde_json::Value::Object(intervals),
    })
}

// =============================================================================
// Integration tests (synchronous solve — no background worker needed)
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    /// Helper: run a SQL statement, panicking on error.
    fn run(sql: &str) {
        Spi::run(sql).unwrap();
    }

    #[pg_test]
    fn cp_disjunctive_and_fifo() {
        run("SELECT pgortools.create_problem('cp_fifo')");
        run("SELECT pgortools.add_interval_var('cp_fifo', 't1_grill', 8, 0, 60)");
        run("SELECT pgortools.add_interval_var('cp_fifo', 't3_grill', 10, 0, 60)");
        run("SELECT pgortools.add_resource('cp_fifo', 'grill', 1)");
        run("SELECT pgortools.add_demand('cp_fifo', 'grill', 't1_grill', 1)");
        run("SELECT pgortools.add_demand('cp_fifo', 'grill', 't3_grill', 1)");
        run("SELECT pgortools.add_precedence('cp_fifo', 't1_grill', 't3_grill', 0)");

        let sol = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_cp_sync('cp_fifo')")
            .unwrap()
            .unwrap()
            .0;

        assert_eq!(sol["status"].as_str(), Some("FEASIBLE"));
        let t1_end = sol["intervals"]["t1_grill"]["end"].as_i64().unwrap();
        let t3_start = sol["intervals"]["t3_grill"]["start"].as_i64().unwrap();
        assert!(t1_end <= t3_start, "FIFO violated: {sol:?}");

        run("SELECT pgortools.drop_problem('cp_fifo')");
    }

    #[pg_test]
    fn cp_capacity_one_infeasible() {
        run("SELECT pgortools.create_problem('cp_infeasible')");
        // three dur-4 ops that cannot be sequenced disjointly within horizon 9
        for op in ["a", "b", "c"] {
            run(&format!(
                "SELECT pgortools.add_interval_var('cp_infeasible', '{op}', 4, 0, 9)"
            ));
            run(&format!(
                "SELECT pgortools.add_demand('cp_infeasible', 'station', '{op}', 1)"
            ));
        }
        run("SELECT pgortools.add_resource('cp_infeasible', 'station', 1)");

        let sol = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_cp_sync('cp_infeasible')")
            .unwrap()
            .unwrap()
            .0;
        assert_eq!(sol["status"].as_str(), Some("INFEASIBLE"));

        run("SELECT pgortools.drop_problem('cp_infeasible')");
    }

    #[pg_test]
    fn cp_minimize_wait_reaches_gap_floor() {
        run("SELECT pgortools.create_problem('cp_wait')");
        run("SELECT pgortools.add_interval_var('cp_wait', 'g', 2, 0, 30)");
        run("SELECT pgortools.add_interval_var('cp_wait', 'p', 2, 0, 30)");
        run("SELECT pgortools.add_precedence('cp_wait', 'g', 'p', 5)"); // >= 5 wait forced
        run("SELECT pgortools.minimize_wait('cp_wait', 'p', 'g', 1)");

        let sol = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_cp_sync('cp_wait')")
            .unwrap()
            .unwrap()
            .0;

        assert_eq!(sol["status"].as_str(), Some("OPTIMAL"));
        assert_eq!(sol["objective"].as_i64(), Some(5));

        run("SELECT pgortools.drop_problem('cp_wait')");
    }
}
