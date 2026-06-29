//! pg_ortools: Constraint optimization using HiGHS MIP/LP solver
//!
//! This extension brings constraint optimization capabilities directly into SQL,
//! with zero external dependencies. Problems are solved asynchronously via a
//! background worker, or synchronously with solve_sync().

use pgrx::prelude::*;

pub mod error;
mod jobs;
pub mod metaheuristic;
mod solver;
mod worker;

pub use error::PgOrtoolsError;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation (README.md) as a string.
/// This enables runtime discovery of extension capabilities via pg_mcp.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Extension Initialization
// =============================================================================

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    worker::register_gucs();

    if worker::is_worker_enabled() {
        worker::register_background_worker();
    }
}

// =============================================================================
// Bootstrap SQL - Creates schema and tables
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Schema is created by control file (schema = pgortools)

CREATE TABLE IF NOT EXISTS pgortools.problems (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    objective_type TEXT,
    objective_expr TEXT,
    status TEXT DEFAULT 'draft',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pgortools.variables (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    var_type TEXT DEFAULT 'int',
    domain_min BIGINT,
    domain_max BIGINT,
    pinned BOOLEAN DEFAULT false,
    UNIQUE(problem_id, name)
);

CREATE TABLE IF NOT EXISTS pgortools.constraints (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    name TEXT,
    expression TEXT,
    constraint_type TEXT,
    constraint_config JSONB
);

CREATE TABLE IF NOT EXISTS pgortools.solutions (
    id SERIAL PRIMARY KEY,
    problem_id INTEGER REFERENCES pgortools.problems(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    objective_value DOUBLE PRECISION,
    variable_values JSONB,
    solve_time_ms INTEGER,
    solved_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS pgortools.solve_jobs (
    id BIGSERIAL PRIMARY KEY,
    problem_name TEXT NOT NULL,
    config JSONB DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'solving', 'completed', 'failed', 'cancelled')),
    progress FLOAT DEFAULT 0.0,
    current_step TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    worker_pid INTEGER
);

CREATE INDEX IF NOT EXISTS idx_solve_jobs_state
    ON pgortools.solve_jobs(state) WHERE state = 'queued';
"#,
    name = "bootstrap",
    bootstrap
);

// =============================================================================
// Finalize SQL - Set permissions
// =============================================================================

pgrx::extension_sql!(
    r#"
GRANT USAGE ON SCHEMA pgortools TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgortools TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgortools TO PUBLIC;
"#,
    name = "permissions",
    finalize
);

// =============================================================================
// Problem Management Functions
// =============================================================================

/// Create a new optimization problem.
#[pg_extern]
fn create_problem(name: &str) -> i64 {
    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgortools.problems (name) VALUES ($1) RETURNING id",
        &[name.into()],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create problem: no ID returned"),
        Err(e) => pgrx::error!("Failed to create problem: {}", e),
    }
}

/// Drop an optimization problem and all its variables/constraints.
#[pg_extern]
fn drop_problem(name: &str) -> bool {
    match Spi::run_with_args(
        "DELETE FROM pgortools.problems WHERE name = $1",
        &[name.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to drop problem: {}", e),
    }
}

/// Add an integer variable to a problem.
#[pg_extern]
fn add_int_var(problem: &str, var_name: &str, domain_min: i64, domain_max: i64) -> bool {
    match Spi::run_with_args(
        r#"
        INSERT INTO pgortools.variables (problem_id, name, var_type, domain_min, domain_max)
        SELECT id, $2, 'int', $3, $4
        FROM pgortools.problems WHERE name = $1
        "#,
        &[
            problem.into(),
            var_name.into(),
            domain_min.into(),
            domain_max.into(),
        ],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to add variable: {}", e),
    }
}

/// Add a boolean variable to a problem.
#[pg_extern]
fn add_bool_var(problem: &str, var_name: &str) -> bool {
    match Spi::run_with_args(
        r#"
        INSERT INTO pgortools.variables (problem_id, name, var_type, domain_min, domain_max)
        SELECT id, $2, 'bool', 0, 1
        FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), var_name.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to add variable: {}", e),
    }
}

/// Add a constraint to a problem.
#[pg_extern]
fn add_constraint(problem: &str, expression: &str) -> bool {
    match Spi::run_with_args(
        r#"
        INSERT INTO pgortools.constraints (problem_id, expression)
        SELECT id, $2
        FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), expression.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to add constraint: {}", e),
    }
}

/// Set the objective to maximize.
#[pg_extern]
fn maximize(problem: &str, expression: &str) -> bool {
    match Spi::run_with_args(
        r#"
        UPDATE pgortools.problems
        SET objective_type = 'maximize', objective_expr = $2
        WHERE name = $1
        "#,
        &[problem.into(), expression.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to set objective: {}", e),
    }
}

/// Set the objective to minimize.
#[pg_extern]
fn minimize(problem: &str, expression: &str) -> bool {
    match Spi::run_with_args(
        r#"
        UPDATE pgortools.problems
        SET objective_type = 'minimize', objective_expr = $2
        WHERE name = $1
        "#,
        &[problem.into(), expression.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to set objective: {}", e),
    }
}

// =============================================================================
// Solve Functions
// =============================================================================

/// Solve a problem asynchronously via background worker.
/// Returns a job_id that can be polled with solve_status().
#[pg_extern]
fn solve(problem: &str) -> i64 {
    let config = jobs::SolveJobConfig::default();
    match jobs::queue_solve_job(problem, &config) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Solve a problem asynchronously with a specific strategy.
/// strategy: "mip", "hill_climbing", "tabu_search", "simulated_annealing",
///           "late_acceptance", "auto"
/// Returns a job_id that can be polled with solve_status().
#[pg_extern]
fn solve_with_strategy(
    problem: &str,
    strategy: &str,
    time_limit_seconds: default!(i32, 30),
) -> i64 {
    let config = jobs::SolveJobConfig {
        time_limit_seconds: Some(time_limit_seconds),
        strategy: Some(strategy.to_string()),
    };
    match jobs::queue_solve_job(problem, &config) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Solve a problem synchronously (blocks until complete).
/// Use this for quick problems or testing. For larger problems, use solve().
#[pg_extern]
fn solve_sync(problem: &str) -> pgrx::JsonB {
    match solver::solve_problem(problem, false) {
        Ok(solution) => pgrx::JsonB(solution),
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Solve a problem using greedy mode (returns first feasible solution, no optimization).
/// Much faster than solve_sync — useful for simulation and quick feasibility checks.
#[pg_extern]
fn solve_greedy(problem: &str) -> pgrx::JsonB {
    match solver::solve_problem(problem, true) {
        Ok(solution) => pgrx::JsonB(solution),
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Solve a problem using metaheuristic local search.
/// Returns a JSONB solution with the same format as solve_sync.
#[pg_extern]
fn solve_local(
    problem: &str,
    algorithm: default!(&str, "'late_acceptance'"),
    time_limit_seconds: default!(i32, 30),
) -> pgrx::JsonB {
    let algo = match metaheuristic::parse_algorithm(algorithm) {
        Ok(a) => a,
        Err(e) => pgrx::error!("{}", e),
    };
    let time_limit = std::time::Duration::from_secs(time_limit_seconds.max(1) as u64);
    match metaheuristic::solve_from_db(problem, &algo, time_limit) {
        Ok(solution) => pgrx::JsonB(solution),
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Automatically choose between MIP and local search based on problem size.
/// Small problems (< auto_threshold variables) use MIP, larger use local search.
#[pg_extern]
fn solve_auto(problem: &str, time_limit_seconds: default!(i32, 60)) -> pgrx::JsonB {
    // Count variables for this problem
    let var_count = Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*)::bigint FROM pgortools.variables v \
         JOIN pgortools.problems p ON v.problem_id = p.id \
         WHERE p.name = $1",
        &[problem.into()],
    );

    let count = match var_count {
        Ok(Some(c)) => c,
        Ok(None) => 0,
        Err(e) => pgrx::error!("Failed to count variables: {}", e),
    };

    let threshold = worker::get_auto_threshold() as i64;

    if count < threshold {
        // Small problem: use MIP
        match solver::solve_problem(problem, false) {
            Ok(solution) => pgrx::JsonB(solution),
            Err(e) => pgrx::error!("{}", e),
        }
    } else {
        // Large problem: use local search
        let algo_name = worker::get_default_algorithm();
        let algo = match metaheuristic::parse_algorithm(&algo_name) {
            Ok(a) => a,
            Err(e) => pgrx::error!("{}", e),
        };
        let time_limit = std::time::Duration::from_secs(time_limit_seconds.max(1) as u64);
        match metaheuristic::solve_from_db(problem, &algo, time_limit) {
            Ok(solution) => pgrx::JsonB(solution),
            Err(e) => pgrx::error!("{}", e),
        }
    }
}

/// Add a typed constraint to a problem (for metaheuristic solving).
/// constraint_type: capacity, group_balance, no_overlap, skill_match,
///                  minimize_field, minimize_cost, pin_current
/// config: JSONB with constraint parameters
#[pg_extern]
fn add_typed_constraint(problem: &str, constraint_type: &str, config: pgrx::JsonB) -> bool {
    if !metaheuristic::is_valid_constraint_type(constraint_type) {
        pgrx::error!(
            "Invalid constraint type: '{}'. Valid types: capacity, group_balance, \
             no_overlap, skill_match, minimize_field, minimize_cost, pin_current",
            constraint_type
        );
    }

    let config_str = config.0.to_string();

    match Spi::run_with_args(
        r#"
        INSERT INTO pgortools.constraints (problem_id, constraint_type, constraint_config)
        SELECT id, $2, $3::jsonb
        FROM pgortools.problems WHERE name = $1
        "#,
        &[problem.into(), constraint_type.into(), config_str.into()],
    ) {
        Ok(_) => true,
        Err(e) => pgrx::error!("Failed to add typed constraint: {}", e),
    }
}

/// Pin a variable so the solver doesn't move it.
#[pg_extern]
fn pin_variable(problem: &str, var_name: &str) -> bool {
    let updated = Spi::get_one_with_args::<i64>(
        r#"
        UPDATE pgortools.variables v
        SET pinned = true
        FROM pgortools.problems p
        WHERE v.problem_id = p.id AND p.name = $1 AND v.name = $2
        RETURNING v.id
        "#,
        &[problem.into(), var_name.into()],
    );

    match updated {
        Ok(Some(_)) => true,
        Ok(None) | Err(pgrx::spi::SpiError::InvalidPosition) => {
            pgrx::error!("Variable '{}' not found in problem '{}'", var_name, problem)
        }
        Err(e) => pgrx::error!("Failed to pin variable: {}", e),
    }
}

/// Unpin a variable so the solver can move it.
#[pg_extern]
fn unpin_variable(problem: &str, var_name: &str) -> bool {
    let updated = Spi::get_one_with_args::<i64>(
        r#"
        UPDATE pgortools.variables v
        SET pinned = false
        FROM pgortools.problems p
        WHERE v.problem_id = p.id AND p.name = $1 AND v.name = $2
        RETURNING v.id
        "#,
        &[problem.into(), var_name.into()],
    );

    match updated {
        Ok(Some(_)) => true,
        Ok(None) | Err(pgrx::spi::SpiError::InvalidPosition) => {
            pgrx::error!("Variable '{}' not found in problem '{}'", var_name, problem)
        }
        Err(e) => pgrx::error!("Failed to unpin variable: {}", e),
    }
}

/// Get the status of a solve job.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn solve_status(
    job_id: i64,
) -> TableIterator<
    'static,
    (
        name!(job_id, i64),
        name!(problem_name, String),
        name!(state, String),
        name!(progress, f64),
        name!(current_step, Option<String>),
        name!(error_message, Option<String>),
        name!(started_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(completed_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(elapsed_seconds, Option<f64>),
    ),
> {
    match jobs::get_job_status(job_id) {
        Ok(Some(status)) => TableIterator::once((
            status.job_id,
            status.problem_name,
            status.state,
            status.progress,
            status.current_step,
            status.error_message,
            status.started_at,
            status.completed_at,
            status.elapsed_seconds,
        )),
        Ok(None) => pgrx::error!("Solve job {} not found", job_id),
        Err(e) => pgrx::error!("Failed to get solve status: {}", e),
    }
}

/// Cancel a queued or running solve job.
#[pg_extern]
fn cancel_solve(job_id: i64) -> bool {
    match jobs::cancel_job(job_id) {
        Ok(cancelled) => cancelled,
        Err(e) => pgrx::error!("Failed to cancel solve job: {}", e),
    }
}

/// Get the solution for a problem as JSONB.
#[pg_extern]
fn get_solution(problem: &str) -> Option<pgrx::JsonB> {
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT variable_values
        FROM pgortools.solutions s
        JOIN pgortools.problems p ON s.problem_id = p.id
        WHERE p.name = $1
        ORDER BY s.solved_at DESC
        LIMIT 1
        "#,
        &[problem.into()],
    );

    match result {
        Ok(val) => val,
        Err(e) => pgrx::error!("Failed to get solution: {}", e),
    }
}

// =============================================================================
// Declarative Assignment API
// =============================================================================

/// Solve an assignment problem declaratively from tables.
/// Returns a job_id for async solving via background worker.
#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn solve_assignment(
    problem_name: &str,
    resources_table: &str,
    resource_name_col: &str,
    resource_group_col: &str,
    resource_cost_col: &str,
    targets_table: &str,
    target_name_col: &str,
    group_per_target: i32,
    objective: &str,
) -> i64 {
    if objective != "minimize" && objective != "maximize" {
        pgrx::error!("objective must be 'minimize' or 'maximize'");
    }

    // Drop existing problem if exists
    Spi::run_with_args(
        "DELETE FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )
    .ok();

    // Create the problem
    let problem_id = match Spi::get_one_with_args::<i64>(
        "INSERT INTO pgortools.problems (name) VALUES ($1) RETURNING id",
        &[problem_name.into()],
    ) {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to create problem: no ID returned"),
        Err(e) => pgrx::error!("Failed to create problem: {}", e),
    };

    // Get resources (name, group, cost)
    let resources_query = format!(
        "SELECT {}::text, {}::text, {}::bigint FROM {}",
        resource_name_col, resource_group_col, resource_cost_col, resources_table
    );
    let resources: Vec<(String, String, i64)> = Spi::connect(|client| {
        let mut result = Vec::new();
        let table = client.select(&resources_query, None, &[])?;
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let group: String = row.get(2)?.unwrap_or_default();
            let cost: i64 = row.get(3)?.unwrap_or(0);
            result.push((name, group, cost));
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_else(|e| pgrx::error!("Failed to read resources: {}", e));

    // Get targets (name)
    let targets_query = format!("SELECT {}::text FROM {}", target_name_col, targets_table);
    let targets: Vec<String> = Spi::connect(|client| {
        let mut result = Vec::new();
        let table = client.select(&targets_query, None, &[])?;
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            result.push(name);
        }
        Ok::<_, pgrx::spi::Error>(result)
    })
    .unwrap_or_else(|e| pgrx::error!("Failed to read targets: {}", e));

    // Get unique groups
    let groups: Vec<String> = {
        let mut g: Vec<String> = resources
            .iter()
            .map(|(_, group, _)| group.clone())
            .collect();
        g.sort();
        g.dedup();
        g
    };

    // Create boolean variables for each resource-target pair
    for (resource_name, _, _) in &resources {
        for target_name in &targets {
            let var_name = format!("{}_{}", resource_name, target_name);
            Spi::run_with_args(
                "INSERT INTO pgortools.variables (problem_id, name, var_type, domain_min, domain_max) VALUES ($1, $2, 'bool', 0, 1)",
                &[problem_id.into(), var_name.into()],
            ).unwrap_or_else(|e| pgrx::error!("Failed to add variable: {}", e));
        }
    }

    // Constraint: Each resource assigned to exactly 1 target
    for (resource_name, _, _) in &resources {
        let terms: Vec<String> = targets
            .iter()
            .map(|t| format!("{}_{}", resource_name, t))
            .collect();
        let expr = format!("{} == 1", terms.join(" + "));
        Spi::run_with_args(
            "INSERT INTO pgortools.constraints (problem_id, expression) VALUES ($1, $2)",
            &[problem_id.into(), expr.into()],
        )
        .unwrap_or_else(|e| pgrx::error!("Failed to add constraint: {}", e));
    }

    // Constraint: Each target gets exactly group_per_target of each group
    for target_name in &targets {
        for group in &groups {
            let terms: Vec<String> = resources
                .iter()
                .filter(|(_, g, _)| g == group)
                .map(|(r, _, _)| format!("{}_{}", r, target_name))
                .collect();
            let expr = format!("{} == {}", terms.join(" + "), group_per_target);
            Spi::run_with_args(
                "INSERT INTO pgortools.constraints (problem_id, expression) VALUES ($1, $2)",
                &[problem_id.into(), expr.into()],
            )
            .unwrap_or_else(|e| pgrx::error!("Failed to add constraint: {}", e));
        }
    }

    // Objective function: sum of cost * assignment
    let obj_terms: Vec<String> = resources
        .iter()
        .flat_map(|(r, _, cost)| targets.iter().map(move |t| format!("{}*{}_{}", cost, r, t)))
        .collect();
    let obj_expr = obj_terms.join(" + ");

    Spi::run_with_args(
        "UPDATE pgortools.problems SET objective_type = $2, objective_expr = $3 WHERE id = $1",
        &[problem_id.into(), objective.into(), obj_expr.into()],
    )
    .unwrap_or_else(|e| pgrx::error!("Failed to set objective: {}", e));

    // Queue the job for async solving
    let config = jobs::SolveJobConfig::default();
    match jobs::queue_solve_job(problem_name, &config) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Parse assignment solution into a readable table format.
///
/// Takes the raw solution JSONB and returns rows of (resource, target, assigned).
#[pg_extern]
fn parse_assignment(
    solution: pgrx::JsonB,
) -> TableIterator<
    'static,
    (
        name!(resource, String),
        name!(target, String),
        name!(assigned, bool),
    ),
> {
    let mut results = Vec::new();

    if let Some(values) = solution.0.get("values").and_then(|v| v.as_object()) {
        for (key, value) in values {
            if let Some(assigned) = value.as_i64() {
                if assigned == 1 {
                    if let Some(idx) = key.rfind('_') {
                        let resource = key[..idx].to_string();
                        let target = key[idx + 1..].to_string();
                        results.push((resource, target, true));
                    }
                }
            }
        }
    }

    TableIterator::new(results)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_create_problem() {
        let result = Spi::get_one::<i64>("SELECT pgortools.create_problem('test_problem')");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('test_problem')").ok();
    }

    #[pg_test]
    fn test_add_variables() {
        Spi::run("SELECT pgortools.create_problem('var_test')").ok();
        let result = Spi::get_one::<bool>("SELECT pgortools.add_int_var('var_test', 'x', 0, 100)");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(true));

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('var_test')").ok();
    }

    #[pg_test]
    fn test_solve_sync() {
        Spi::run("SELECT pgortools.create_problem('sync_test')").ok();
        Spi::run("SELECT pgortools.add_int_var('sync_test', 'x', 0, 100)").ok();
        Spi::run("SELECT pgortools.add_int_var('sync_test', 'y', 0, 100)").ok();
        Spi::run("SELECT pgortools.add_constraint('sync_test', 'x + y <= 150')").ok();
        Spi::run("SELECT pgortools.maximize('sync_test', '2*x + 3*y')").ok();

        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_sync('sync_test')");
        assert!(result.is_ok());

        let solution = result.unwrap().unwrap();
        let status = solution.0.get("status").and_then(|s| s.as_str());
        assert_eq!(status, Some("OPTIMAL"));

        // Check objective value: max 2*x + 3*y subject to x+y<=150
        // Optimal: x=0, y=100 -> 300? No, y max is 100, x max is 100
        // Actually x+y<=150, x in [0,100], y in [0,100]
        // Optimal: x=50, y=100 -> 100+300=400
        let obj = solution.0.get("objective").and_then(|o| o.as_f64());
        assert!(obj.is_some());
        assert!((obj.unwrap() - 400.0).abs() < 1.0);

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('sync_test')").ok();
    }

    #[pg_test]
    fn test_solve_async() {
        Spi::run("SELECT pgortools.create_problem('async_test')").ok();
        Spi::run("SELECT pgortools.add_int_var('async_test', 'x', 0, 10)").ok();
        Spi::run("SELECT pgortools.minimize('async_test', 'x')").ok();

        let result = Spi::get_one::<i64>("SELECT pgortools.solve('async_test')");
        assert!(result.is_ok());
        let job_id = result.unwrap().unwrap();
        assert!(job_id > 0);

        // Check the job was queued
        let state = Spi::get_one::<String>(&format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ));
        assert!(state.is_ok());
        // State could be 'queued' or 'solving' depending on worker timing
        let s = state.unwrap().unwrap();
        assert!(s == "queued" || s == "solving" || s == "completed");

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('async_test')").ok();
    }

    #[pg_test]
    fn test_solve_greedy() {
        Spi::run("SELECT pgortools.create_problem('greedy_test')").ok();
        Spi::run("SELECT pgortools.add_int_var('greedy_test', 'x', 0, 100)").ok();
        Spi::run("SELECT pgortools.add_int_var('greedy_test', 'y', 0, 100)").ok();
        Spi::run("SELECT pgortools.add_constraint('greedy_test', 'x + y <= 150')").ok();
        Spi::run("SELECT pgortools.maximize('greedy_test', '2*x + 3*y')").ok();

        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_greedy('greedy_test')");
        assert!(result.is_ok());

        let solution = result.unwrap().unwrap();
        let status = solution.0.get("status").and_then(|s| s.as_str());
        assert_eq!(status, Some("OPTIMAL"));

        let method = solution.0.get("method").and_then(|m| m.as_str());
        assert_eq!(method, Some("greedy"));

        // Greedy returns a feasible solution (not necessarily optimal)
        let x = solution.0["values"]["x"].as_i64().unwrap();
        let y = solution.0["values"]["y"].as_i64().unwrap();
        assert!(x >= 0 && x <= 100);
        assert!(y >= 0 && y <= 100);
        assert!(x + y <= 150);

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('greedy_test')").ok();
    }

    #[pg_test]
    fn test_cancel_solve() {
        Spi::run("SELECT pgortools.create_problem('cancel_test')").ok();
        Spi::run("SELECT pgortools.add_int_var('cancel_test', 'x', 0, 10)").ok();

        let job_id = Spi::get_one::<i64>("SELECT pgortools.solve('cancel_test')")
            .unwrap()
            .unwrap();

        let cancelled = Spi::get_one::<bool>(&format!("SELECT pgortools.cancel_solve({})", job_id));
        assert!(cancelled.is_ok());

        // Cleanup
        Spi::run("SELECT pgortools.drop_problem('cancel_test')").ok();
    }

    // =========================================================================
    // Phase 4: Metaheuristic SQL function tests
    // =========================================================================

    /// Helper: set up a 3x3 assignment problem with typed constraints
    fn setup_local_problem(name: &str) {
        Spi::run(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();
        // 3 items × 3 slots = 9 boolean variables
        for i in 0..3 {
            for j in 0..3 {
                Spi::run(&format!(
                    "SELECT pgortools.add_bool_var('{}', 'x_{}_{}')",
                    name, i, j
                ))
                .unwrap();
            }
        }
    }

    fn cleanup_problem(name: &str) {
        Spi::run(&format!("SELECT pgortools.drop_problem('{}')", name)).ok();
    }

    #[pg_test]
    fn test_add_typed_constraint() {
        setup_local_problem("tc_test");
        let result = Spi::get_one::<bool>(
            "SELECT pgortools.add_typed_constraint('tc_test', 'capacity', '{\"limit\": 1}'::jsonb)",
        );
        assert_eq!(result.unwrap(), Some(true));

        // Verify in DB
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM pgortools.constraints c \
             JOIN pgortools.problems p ON c.problem_id = p.id \
             WHERE p.name = 'tc_test' AND c.constraint_type = 'capacity'",
        );
        assert_eq!(count.unwrap(), Some(1));
        cleanup_problem("tc_test");
    }

    #[pg_test]
    fn test_add_typed_constraint_types() {
        setup_local_problem("tc_types");
        // Test each valid type
        let types = vec![
            ("capacity", r#"{"limit": 1}"#),
            (
                "group_balance",
                r#"{"group_field": "role", "count_per_target": 1}"#,
            ),
            (
                "skill_match",
                r#"{"feasible": [[true, true, true], [true, true, true], [true, true, true]]}"#,
            ),
            (
                "minimize_field",
                r#"{"costs": [1.0, 2.0, 3.0], "weight": 1.0}"#,
            ),
            (
                "minimize_cost",
                r#"{"item_costs": [1.0, 2.0, 3.0], "slot_costs": [10.0, 20.0, 30.0], "weight": 1.0}"#,
            ),
            (
                "pin_current",
                r#"{"current": [null, null, null], "weight": 10.0}"#,
            ),
        ];
        for (ctype, config) in types {
            let sql = format!(
                "SELECT pgortools.add_typed_constraint('tc_types', '{}', '{}'::jsonb)",
                ctype, config
            );
            let result = Spi::get_one::<bool>(&sql);
            assert_eq!(result.unwrap(), Some(true), "failed for type {}", ctype);
        }
        cleanup_problem("tc_types");
    }

    #[pg_test]
    #[should_panic(expected = "Invalid constraint type")]
    fn test_add_typed_constraint_bad_type() {
        setup_local_problem("tc_bad");
        Spi::get_one::<bool>(
            "SELECT pgortools.add_typed_constraint('tc_bad', 'bogus', '{}'::jsonb)",
        )
        .unwrap();
    }

    #[pg_test]
    fn test_pin_variable() {
        setup_local_problem("pin_test");
        let result = Spi::get_one::<bool>("SELECT pgortools.pin_variable('pin_test', 'x_0_0')");
        assert_eq!(result.unwrap(), Some(true));

        // Verify in DB
        let pinned = Spi::get_one::<bool>(
            "SELECT pinned FROM pgortools.variables v \
             JOIN pgortools.problems p ON v.problem_id = p.id \
             WHERE p.name = 'pin_test' AND v.name = 'x_0_0'",
        );
        assert_eq!(pinned.unwrap(), Some(true));
        cleanup_problem("pin_test");
    }

    #[pg_test]
    fn test_unpin_variable() {
        setup_local_problem("unpin_test");
        // Pin then unpin
        Spi::run("SELECT pgortools.pin_variable('unpin_test', 'x_0_0')").unwrap();
        let result = Spi::get_one::<bool>("SELECT pgortools.unpin_variable('unpin_test', 'x_0_0')");
        assert_eq!(result.unwrap(), Some(true));

        let pinned = Spi::get_one::<bool>(
            "SELECT pinned FROM pgortools.variables v \
             JOIN pgortools.problems p ON v.problem_id = p.id \
             WHERE p.name = 'unpin_test' AND v.name = 'x_0_0'",
        );
        assert_eq!(pinned.unwrap(), Some(false));
        cleanup_problem("unpin_test");
    }

    #[pg_test]
    #[should_panic(expected = "not found")]
    fn test_pin_nonexistent_variable() {
        setup_local_problem("pin_novar");
        Spi::get_one::<bool>("SELECT pgortools.pin_variable('pin_novar', 'x_99_99')").unwrap();
    }

    #[pg_test]
    fn test_solve_local_simple_assignment() {
        setup_local_problem("sl_simple");
        // Add capacity constraint
        Spi::run(
            "SELECT pgortools.add_typed_constraint('sl_simple', 'capacity', '{\"limit\": 1}'::jsonb)",
        )
        .unwrap();

        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgortools.solve_local('sl_simple', 'late_acceptance', 2)",
        );
        assert!(result.is_ok());
        let solution = result.unwrap().unwrap();
        assert_eq!(solution.0["status"], "FEASIBLE");
        assert_eq!(solution.0["method"], "late_acceptance");
        cleanup_problem("sl_simple");
    }

    #[pg_test]
    fn test_solve_local_with_soft_constraints() {
        setup_local_problem("sl_soft");
        Spi::run(
            "SELECT pgortools.add_typed_constraint('sl_soft', 'capacity', '{\"limit\": 1}'::jsonb)",
        )
        .unwrap();
        Spi::run(
            "SELECT pgortools.add_typed_constraint('sl_soft', 'minimize_cost', \
             '{\"item_costs\": [1.0, 2.0, 3.0], \"slot_costs\": [10.0, 20.0, 30.0], \"weight\": 1.0}'::jsonb)",
        )
        .unwrap();

        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgortools.solve_local('sl_soft', 'late_acceptance', 2)",
        );
        let solution = result.unwrap().unwrap();
        assert_eq!(solution.0["status"], "FEASIBLE");
        // Should have a non-zero objective
        assert!(solution.0["soft_score"].as_i64().unwrap() < 0);
        cleanup_problem("sl_soft");
    }

    #[pg_test]
    fn test_solve_local_returns_values() {
        setup_local_problem("sl_vals");
        Spi::run(
            "SELECT pgortools.add_typed_constraint('sl_vals', 'capacity', '{\"limit\": 1}'::jsonb)",
        )
        .unwrap();

        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgortools.solve_local('sl_vals', 'hill_climbing', 2)",
        );
        let solution = result.unwrap().unwrap();
        let values = solution.0["values"].as_object().unwrap();
        // Should have x_i_j format values
        assert!(values.contains_key("x_0_0") || values.contains_key("x_0_1"));
        // Each item should be assigned to exactly one slot (sum = 1)
        for i in 0..3 {
            let sum: i64 = (0..3)
                .map(|j| {
                    values
                        .get(&format!("x_{}_{}", i, j))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                })
                .sum();
            assert_eq!(sum, 1, "item {} should be assigned to exactly one slot", i);
        }
        cleanup_problem("sl_vals");
    }

    #[pg_test]
    fn test_solve_auto_small_uses_mip() {
        // Create a small MIP problem (< auto_threshold)
        Spi::run("SELECT pgortools.create_problem('auto_small')").unwrap();
        Spi::run("SELECT pgortools.add_int_var('auto_small', 'x', 0, 10)").unwrap();
        Spi::run("SELECT pgortools.add_int_var('auto_small', 'y', 0, 10)").unwrap();
        Spi::run("SELECT pgortools.add_constraint('auto_small', 'x + y <= 15')").unwrap();
        Spi::run("SELECT pgortools.maximize('auto_small', 'x + y')").unwrap();

        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgortools.solve_auto('auto_small', 10)");
        let solution = result.unwrap().unwrap();
        // Small problem → MIP → OPTIMAL
        assert_eq!(solution.0["status"], "OPTIMAL");
        cleanup_problem("auto_small");
    }

    #[pg_test]
    fn test_schema_tables_have_new_columns() {
        // Verify constraint_config column exists
        let has_config = Spi::get_one::<bool>(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema::text = 'pgortools'
                  AND table_name::text = 'constraints'
                  AND column_name::text = 'constraint_config'
            )",
        );
        assert_eq!(has_config.unwrap(), Some(true));

        // Verify pinned column exists
        let has_pinned = Spi::get_one::<bool>(
            "SELECT EXISTS(
                SELECT 1 FROM information_schema.columns
                WHERE table_schema::text = 'pgortools'
                  AND table_name::text = 'variables'
                  AND column_name::text = 'pinned'
            )",
        );
        assert_eq!(has_pinned.unwrap(), Some(true));
    }
}

/// This module is required by `cargo pgrx test` invocations.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
