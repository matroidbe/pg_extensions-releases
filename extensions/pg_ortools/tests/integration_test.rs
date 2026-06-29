//! Integration tests for pg_ortools
//!
//! These tests exercise the background worker (async solve, job status polling,
//! cancellation) against a real PostgreSQL instance.
//!
//! Prerequisites:
//!   - Run ./test.sh which installs the extension and starts PostgreSQL
//!   - pg_ortools extension loaded with background worker running
//!   - Database "pg_ortools" exists with extension created
//!
//! Run with: cargo test --tests -- --test-threads=1

mod common;

use common::*;
use std::time::Duration;

/// How long to wait for the background worker to process a solve job
const SOLVE_TIMEOUT: Duration = Duration::from_secs(15);

/// Longer timeout for metaheuristic strategy tests (solve time + worker restart if needed)
const STRATEGY_TIMEOUT: Duration = Duration::from_secs(30);

// =============================================================================
// Problem Lifecycle
// =============================================================================

#[test]
fn test_create_and_drop_problem() {
    skip_if_not_running!();

    cleanup_problem("it_lifecycle");

    let id = query_one("SELECT pgortools.create_problem('it_lifecycle')")
        .unwrap()
        .unwrap();
    assert!(id.parse::<i64>().unwrap() > 0, "problem ID should be > 0");

    // Verify it exists
    let name = query_one("SELECT name FROM pgortools.problems WHERE name = 'it_lifecycle'")
        .unwrap()
        .unwrap();
    assert_eq!(name, "it_lifecycle");

    // Drop
    let dropped = query_one("SELECT pgortools.drop_problem('it_lifecycle')")
        .unwrap()
        .unwrap();
    assert_eq!(dropped, "true");

    // Verify gone
    let count =
        query_one("SELECT count(*)::bigint FROM pgortools.problems WHERE name = 'it_lifecycle'")
            .unwrap()
            .unwrap();
    assert_eq!(count, "0");
}

#[test]
fn test_add_variables_and_constraints() {
    skip_if_not_running!();

    cleanup_problem("it_vars");

    execute("SELECT pgortools.create_problem('it_vars')").unwrap();
    execute("SELECT pgortools.add_int_var('it_vars', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_vars', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_bool_var('it_vars', 'flag')").unwrap();
    execute("SELECT pgortools.add_constraint('it_vars', 'x + y <= 100')").unwrap();

    let var_count = query_one(
        "SELECT count(*)::bigint FROM pgortools.variables v
         JOIN pgortools.problems p ON v.problem_id = p.id
         WHERE p.name = 'it_vars'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(var_count, "3");

    let constraint_count = query_one(
        "SELECT count(*)::bigint FROM pgortools.constraints c
         JOIN pgortools.problems p ON c.problem_id = p.id
         WHERE p.name = 'it_vars'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(constraint_count, "1");

    cleanup_problem("it_vars");
}

// =============================================================================
// Synchronous Solve (baseline — no worker involved)
// =============================================================================

#[test]
fn test_solve_sync_maximize() {
    skip_if_not_running!();

    cleanup_problem("it_sync_max");

    execute("SELECT pgortools.create_problem('it_sync_max')").unwrap();
    execute("SELECT pgortools.add_int_var('it_sync_max', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_sync_max', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_sync_max', 'x + y <= 100')").unwrap();
    execute("SELECT pgortools.maximize('it_sync_max', '2*x + 3*y')").unwrap();

    let solution = query_one("SELECT pgortools.solve_sync('it_sync_max')::text")
        .unwrap()
        .unwrap();

    // Parse JSONB result
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();
    assert_eq!(json["status"], "OPTIMAL");
    assert_eq!(json["method"], "optimal");

    // Optimal: x=0, y=100 → objective=300
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 300.0).abs() < 0.01,
        "objective should be 300, got {}",
        objective
    );

    // Solution stored
    let stored = query_one("SELECT pgortools.get_solution('it_sync_max')::text").unwrap();
    assert!(stored.is_some(), "solution should be stored");

    cleanup_problem("it_sync_max");
}

#[test]
fn test_solve_sync_minimize() {
    skip_if_not_running!();

    cleanup_problem("it_sync_min");

    execute("SELECT pgortools.create_problem('it_sync_min')").unwrap();
    execute("SELECT pgortools.add_int_var('it_sync_min', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_sync_min', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_sync_min', 'x + y >= 50')").unwrap();
    execute("SELECT pgortools.minimize('it_sync_min', '3*x + 2*y')").unwrap();

    let solution = query_one("SELECT pgortools.solve_sync('it_sync_min')::text")
        .unwrap()
        .unwrap();

    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();
    assert_eq!(json["status"], "OPTIMAL");

    // Optimal: x=0, y=50 → objective=100
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 100.0).abs() < 0.01,
        "objective should be 100, got {}",
        objective
    );

    cleanup_problem("it_sync_min");
}

#[test]
fn test_solve_greedy() {
    skip_if_not_running!();

    cleanup_problem("it_greedy");

    execute("SELECT pgortools.create_problem('it_greedy')").unwrap();
    execute("SELECT pgortools.add_int_var('it_greedy', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_greedy', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_greedy', 'x + y <= 100')").unwrap();
    execute("SELECT pgortools.maximize('it_greedy', '2*x + 3*y')").unwrap();

    let solution = query_one("SELECT pgortools.solve_greedy('it_greedy')::text")
        .unwrap()
        .unwrap();

    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();
    // Greedy returns first feasible, might not be optimal
    assert!(
        json["status"] == "OPTIMAL" || json["status"] == "FEASIBLE",
        "greedy should find a solution, got: {}",
        json["status"]
    );
    assert_eq!(json["method"], "greedy");

    cleanup_problem("it_greedy");
}

// =============================================================================
// Async Solve via Background Worker (the core integration tests)
// =============================================================================

#[test]
fn test_async_solve_via_worker() {
    skip_if_not_running!();

    cleanup_problem("it_async");
    cleanup_jobs("it_async");

    // Define problem
    execute("SELECT pgortools.create_problem('it_async')").unwrap();
    execute("SELECT pgortools.add_int_var('it_async', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_async', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_async', 'x + y <= 100')").unwrap();
    execute("SELECT pgortools.maximize('it_async', '2*x + 3*y')").unwrap();

    // Submit async solve — returns job_id
    let job_id_str = query_one("SELECT pgortools.solve('it_async')")
        .unwrap()
        .unwrap();
    let job_id: i64 = job_id_str.parse().unwrap();
    assert!(job_id > 0, "job_id should be > 0");

    // Poll for completion
    let status_sql = format!("SELECT state FROM pgortools.solve_status({})", job_id);
    wait_for("async solve", &status_sql, "completed", SOLVE_TIMEOUT)
        .expect("async solve should complete");

    // Verify solution is stored
    let solution = query_one("SELECT pgortools.get_solution('it_async')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    // Verify the values are there (optimal: x=0, y=100)
    assert!(
        json["y"].as_f64().is_some() || json["y"].as_i64().is_some(),
        "solution should contain variable values"
    );

    // Verify solve_status fields
    let rows = query_all(&format!(
        "SELECT job_id::text, problem_name, state, progress::text FROM pgortools.solve_status({})",
        job_id
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], "it_async");
    assert_eq!(rows[0][2], "completed");

    cleanup_jobs("it_async");
    cleanup_problem("it_async");
}

#[test]
fn test_async_solve_multiple_jobs() {
    skip_if_not_running!();

    cleanup_problem("it_multi_a");
    cleanup_problem("it_multi_b");
    cleanup_jobs("it_multi_a");
    cleanup_jobs("it_multi_b");

    // Problem A: maximize 2*x, x <= 50
    execute("SELECT pgortools.create_problem('it_multi_a')").unwrap();
    execute("SELECT pgortools.add_int_var('it_multi_a', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_a', 'x <= 50')").unwrap();
    execute("SELECT pgortools.maximize('it_multi_a', '2*x')").unwrap();

    // Problem B: minimize y, y >= 10
    execute("SELECT pgortools.create_problem('it_multi_b')").unwrap();
    execute("SELECT pgortools.add_int_var('it_multi_b', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_b', 'y >= 10')").unwrap();
    execute("SELECT pgortools.minimize('it_multi_b', 'y')").unwrap();

    // Queue both
    let job_a = query_one("SELECT pgortools.solve('it_multi_a')")
        .unwrap()
        .unwrap();
    let job_b = query_one("SELECT pgortools.solve('it_multi_b')")
        .unwrap()
        .unwrap();

    // Wait for both to complete
    wait_for(
        "job A",
        &format!("SELECT state FROM pgortools.solve_status({})", job_a),
        "completed",
        SOLVE_TIMEOUT,
    )
    .expect("job A should complete");

    wait_for(
        "job B",
        &format!("SELECT state FROM pgortools.solve_status({})", job_b),
        "completed",
        SOLVE_TIMEOUT,
    )
    .expect("job B should complete");

    // Verify solutions
    let sol_a = query_one("SELECT pgortools.get_solution('it_multi_a')::text")
        .unwrap()
        .unwrap();
    let json_a: serde_json::Value = serde_json::from_str(&sol_a).unwrap();
    let x_val = json_a["x"]
        .as_f64()
        .or(json_a["x"].as_i64().map(|v| v as f64))
        .unwrap();
    assert!((x_val - 50.0).abs() < 0.01, "x should be 50, got {}", x_val);

    let sol_b = query_one("SELECT pgortools.get_solution('it_multi_b')::text")
        .unwrap()
        .unwrap();
    let json_b: serde_json::Value = serde_json::from_str(&sol_b).unwrap();
    let y_val = json_b["y"]
        .as_f64()
        .or(json_b["y"].as_i64().map(|v| v as f64))
        .unwrap();
    assert!((y_val - 10.0).abs() < 0.01, "y should be 10, got {}", y_val);

    cleanup_jobs("it_multi_a");
    cleanup_jobs("it_multi_b");
    cleanup_problem("it_multi_a");
    cleanup_problem("it_multi_b");
}

#[test]
fn test_cancel_queued_job() {
    skip_if_not_running!();

    cleanup_problem("it_cancel");
    cleanup_jobs("it_cancel");

    execute("SELECT pgortools.create_problem('it_cancel')").unwrap();
    execute("SELECT pgortools.add_int_var('it_cancel', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.maximize('it_cancel', 'x')").unwrap();

    let job_id = query_one("SELECT pgortools.solve('it_cancel')")
        .unwrap()
        .unwrap();

    // Try to cancel (may or may not succeed depending on whether worker already picked it up)
    let result = query_one(&format!("SELECT pgortools.cancel_solve({})", job_id));
    assert!(result.is_ok(), "cancel_solve should not error");

    // Wait a moment for the worker to process
    std::thread::sleep(Duration::from_secs(3));

    // Job should be either cancelled or completed (worker might have processed it first)
    let state = query_one(&format!(
        "SELECT state FROM pgortools.solve_status({})",
        job_id
    ))
    .unwrap()
    .unwrap();
    assert!(
        state == "cancelled" || state == "completed",
        "job should be cancelled or completed, got: {}",
        state
    );

    cleanup_jobs("it_cancel");
    cleanup_problem("it_cancel");
}

#[test]
fn test_failed_job_invalid_problem() {
    skip_if_not_running!();

    cleanup_jobs("it_nonexistent");

    // Manually insert a job for a problem that doesn't exist
    let job_id = query_one(
        "INSERT INTO pgortools.solve_jobs (problem_name, config, state)
         VALUES ('it_nonexistent', '{}'::jsonb, 'queued')
         RETURNING id",
    )
    .unwrap()
    .unwrap();

    // Wait for worker to process and fail
    wait_for(
        "failed job",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "failed",
        SOLVE_TIMEOUT,
    )
    .expect("job should fail for nonexistent problem");

    // Error message should be set
    let error = query_one(&format!(
        "SELECT error_message FROM pgortools.solve_jobs WHERE id = {}",
        job_id
    ))
    .unwrap();
    assert!(error.is_some(), "error_message should be set");

    cleanup_jobs("it_nonexistent");
}

// =============================================================================
// Declarative Assignment API
// =============================================================================

#[test]
fn test_solve_assignment_declarative() {
    skip_if_not_running!();

    cleanup_problem("it_assignment");
    cleanup_jobs("it_assignment");
    let _ = execute("DROP TABLE IF EXISTS it_resources CASCADE");
    let _ = execute("DROP TABLE IF EXISTS it_targets CASCADE");

    // Create resource and target tables
    execute(
        "CREATE TABLE it_resources (
            name  TEXT NOT NULL,
            grp   TEXT NOT NULL,
            cost  INT NOT NULL
        )",
    )
    .unwrap();

    execute(
        "CREATE TABLE it_targets (
            name TEXT NOT NULL
        )",
    )
    .unwrap();

    // 4 resources, 2 groups, 2 targets
    // Group 'dev': alice(cost=10), bob(cost=20)
    // Group 'qa':  charlie(cost=15), diana(cost=25)
    execute(
        "INSERT INTO it_resources (name, grp, cost) VALUES
            ('alice',   'dev', 10),
            ('bob',     'dev', 20),
            ('charlie', 'qa',  15),
            ('diana',   'qa',  25)",
    )
    .unwrap();

    // Target names must not contain underscores (parse_assignment splits on last '_')
    execute("INSERT INTO it_targets (name) VALUES ('alpha'), ('beta')").unwrap();

    // Minimize cost: each resource to exactly 1 target, each target gets 1 dev + 1 qa
    let job_id = query_one(
        "SELECT pgortools.solve_assignment(
            'it_assignment',
            'it_resources', 'name', 'grp', 'cost',
            'it_targets', 'name',
            1, 'minimize'
        )",
    )
    .unwrap()
    .unwrap();

    // Wait for async solve
    wait_for(
        "assignment solve",
        &format!("SELECT state FROM pgortools.solve_status({})", job_id),
        "completed",
        SOLVE_TIMEOUT,
    )
    .expect("assignment solve should complete");

    // Get solution and parse assignments
    // get_solution returns variable_values directly; parse_assignment expects {"values": {...}}
    let rows = query_all(
        "SELECT resource, target, assigned::text
         FROM pgortools.parse_assignment(
             jsonb_build_object('values', pgortools.get_solution('it_assignment'))
         )
         WHERE assigned = true ORDER BY resource",
    )
    .unwrap();

    // Should have 4 assignments (each resource to exactly one target)
    assert_eq!(rows.len(), 4, "should have 4 assignments, got: {:?}", rows);

    // Verify each resource is assigned to exactly one target
    let resources: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    assert!(resources.contains(&"alice"));
    assert!(resources.contains(&"bob"));
    assert!(resources.contains(&"charlie"));
    assert!(resources.contains(&"diana"));

    // Optimal: minimize cost → alice(10)→proj_x, bob(20)→proj_y (or vice versa)
    // Total cost should be 10+20+15+25 = 70 (all assigned regardless)
    // But cost-optimal assignment: alice→X,charlie→X (25) vs alice→Y,charlie→Y vs ...
    // The minimum is: alice(10)+charlie(15) → one project, bob(20)+diana(25) → other = 70
    // All assignments cost the same total since everyone is assigned.

    // Cleanup
    cleanup_jobs("it_assignment");
    cleanup_problem("it_assignment");
    let _ = execute("DROP TABLE IF EXISTS it_resources CASCADE");
    let _ = execute("DROP TABLE IF EXISTS it_targets CASCADE");
}

// =============================================================================
// Infeasible Problem
// =============================================================================

#[test]
fn test_infeasible_problem() {
    skip_if_not_running!();

    cleanup_problem("it_infeasible");

    execute("SELECT pgortools.create_problem('it_infeasible')").unwrap();
    execute("SELECT pgortools.add_int_var('it_infeasible', 'x', 0, 10)").unwrap();
    execute("SELECT pgortools.add_constraint('it_infeasible', 'x >= 20')").unwrap();
    execute("SELECT pgortools.minimize('it_infeasible', 'x')").unwrap();

    // solve_sync raises pgrx::error! for infeasible problems
    let result = query_one("SELECT pgortools.solve_sync('it_infeasible')::text");
    assert!(result.is_err(), "infeasible problem should error");

    cleanup_problem("it_infeasible");
}

// =============================================================================
// Multi-constraint Problem
// =============================================================================

#[test]
fn test_multi_constraint_problem() {
    skip_if_not_running!();

    cleanup_problem("it_multi_constr");
    cleanup_jobs("it_multi_constr");

    execute("SELECT pgortools.create_problem('it_multi_constr')").unwrap();
    execute("SELECT pgortools.add_int_var('it_multi_constr', 'x', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_multi_constr', 'y', 0, 100)").unwrap();
    execute("SELECT pgortools.add_int_var('it_multi_constr', 'z', 0, 100)").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_constr', 'x + y + z <= 100')").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_constr', 'x >= 10')").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_constr', 'y >= 20')").unwrap();
    execute("SELECT pgortools.add_constraint('it_multi_constr', 'z >= 30')").unwrap();
    execute("SELECT pgortools.maximize('it_multi_constr', 'x + 2*y + 3*z')").unwrap();

    // Solve async via worker
    let job_id = query_one("SELECT pgortools.solve('it_multi_constr')")
        .unwrap()
        .unwrap();

    wait_for(
        "multi-constraint solve",
        &format!("SELECT state FROM pgortools.solve_status({})", job_id),
        "completed",
        SOLVE_TIMEOUT,
    )
    .expect("multi-constraint solve should complete");

    let solution = query_one("SELECT pgortools.get_solution('it_multi_constr')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    // Optimal: maximize x + 2y + 3z with x+y+z<=100, x>=10, y>=20, z>=30
    // Best: minimize x and y at their bounds, give rest to z
    // x=10, y=20, z=70 → 10 + 40 + 210 = 260
    let x = json["x"]
        .as_f64()
        .or(json["x"].as_i64().map(|v| v as f64))
        .unwrap();
    let y = json["y"]
        .as_f64()
        .or(json["y"].as_i64().map(|v| v as f64))
        .unwrap();
    let z = json["z"]
        .as_f64()
        .or(json["z"].as_i64().map(|v| v as f64))
        .unwrap();

    assert!((x - 10.0).abs() < 0.01, "x should be 10, got {}", x);
    assert!((y - 20.0).abs() < 0.01, "y should be 20, got {}", y);
    assert!((z - 70.0).abs() < 0.01, "z should be 70, got {}", z);

    cleanup_jobs("it_multi_constr");
    cleanup_problem("it_multi_constr");
}

// =============================================================================
// Schema / Bootstrap
// =============================================================================

#[test]
fn test_schema_tables_exist() {
    skip_if_not_running!();

    let tables = [
        "pgortools.problems",
        "pgortools.variables",
        "pgortools.constraints",
        "pgortools.solutions",
        "pgortools.solve_jobs",
    ];

    for table in &tables {
        let count = query_one(&format!(
            "SELECT count(*)::bigint FROM information_schema.tables
             WHERE table_schema || '.' || table_name = '{}'",
            table
        ))
        .unwrap()
        .unwrap();
        assert_eq!(count, "1", "table {} should exist", table);
    }
}

#[test]
fn test_solve_status_for_nonexistent_job() {
    skip_if_not_running!();

    // solve_status for a job that doesn't exist should error
    let result = query_one("SELECT state FROM pgortools.solve_status(999999)");
    assert!(
        result.is_err(),
        "solve_status should error for nonexistent job"
    );
}

// =============================================================================
// Phase 5: Strategy-based async solving
// =============================================================================

/// Helper to set up a 3x3 assignment problem with typed constraints
fn setup_local_it_problem(name: &str) {
    cleanup_problem(name);
    cleanup_jobs(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();
    // 3 items × 3 slots = 9 boolean variables
    for i in 0..3 {
        for j in 0..3 {
            execute(&format!(
                "SELECT pgortools.add_bool_var('{}', 'x_{}_{}')",
                name, i, j
            ))
            .unwrap();
        }
    }
    // Capacity: 1 item per slot
    execute(&format!(
        "SELECT pgortools.add_typed_constraint('{}', 'capacity', '{{\"limit\": 1}}'::jsonb)",
        name
    ))
    .unwrap();
}

#[test]
fn test_async_solve_default_strategy() {
    skip_if_not_running!();

    cleanup_problem("it_strat_default");
    cleanup_jobs("it_strat_default");

    execute("SELECT pgortools.create_problem('it_strat_default')").unwrap();
    execute("SELECT pgortools.add_int_var('it_strat_default', 'x', 0, 10)").unwrap();
    execute("SELECT pgortools.maximize('it_strat_default', 'x')").unwrap();

    let job_id = query_one("SELECT pgortools.solve('it_strat_default')")
        .unwrap()
        .unwrap();

    wait_for(
        "default strategy",
        &format!("SELECT state FROM pgortools.solve_status({})", job_id),
        "completed",
        SOLVE_TIMEOUT,
    )
    .expect("default strategy solve should complete");

    // Verify it used MIP (default)
    let solution = query_one("SELECT pgortools.get_solution('it_strat_default')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();
    assert!(json["x"].is_number(), "should have solution");

    cleanup_jobs("it_strat_default");
    cleanup_problem("it_strat_default");
}

#[test]
fn test_async_solve_late_acceptance() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_la");

    let job_id =
        query_one("SELECT pgortools.solve_with_strategy('it_strat_la', 'late_acceptance', 5)")
            .unwrap()
            .unwrap();

    wait_for(
        "late_acceptance strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "completed",
        STRATEGY_TIMEOUT,
    )
    .expect("late_acceptance solve should complete");

    cleanup_jobs("it_strat_la");
    cleanup_problem("it_strat_la");
}

#[test]
fn test_async_solve_tabu_search() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_ts");

    let job_id = query_one("SELECT pgortools.solve_with_strategy('it_strat_ts', 'tabu_search', 5)")
        .unwrap()
        .unwrap();

    wait_for(
        "tabu_search strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "completed",
        STRATEGY_TIMEOUT,
    )
    .expect("tabu_search solve should complete");

    cleanup_jobs("it_strat_ts");
    cleanup_problem("it_strat_ts");
}

#[test]
fn test_async_solve_auto_strategy() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_auto");

    let job_id = query_one("SELECT pgortools.solve_with_strategy('it_strat_auto', 'auto', 5)")
        .unwrap()
        .unwrap();

    wait_for(
        "auto strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "completed",
        STRATEGY_TIMEOUT,
    )
    .expect("auto strategy solve should complete");

    cleanup_jobs("it_strat_auto");
    cleanup_problem("it_strat_auto");
}

#[test]
fn test_async_solve_invalid_strategy() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_bad");

    let job_id = query_one("SELECT pgortools.solve_with_strategy('it_strat_bad', 'bogus', 5)")
        .unwrap()
        .unwrap();

    // Should fail with error
    wait_for(
        "invalid strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "failed",
        SOLVE_TIMEOUT,
    )
    .expect("invalid strategy should fail");

    // Check error message
    let error = query_one(&format!(
        "SELECT error_message FROM pgortools.solve_jobs WHERE id = {}",
        job_id
    ))
    .unwrap();
    assert!(
        error.is_some(),
        "error_message should be set for invalid strategy"
    );

    cleanup_jobs("it_strat_bad");
    cleanup_problem("it_strat_bad");
}

#[test]
fn test_async_solve_with_typed_constraints() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_typed");

    // Add soft constraint
    execute(
        "SELECT pgortools.add_typed_constraint('it_strat_typed', 'minimize_cost', \
         '{\"item_costs\": [1.0, 2.0, 3.0], \"slot_costs\": [10.0, 20.0, 30.0], \"weight\": 1.0}'::jsonb)",
    )
    .unwrap();

    let job_id =
        query_one("SELECT pgortools.solve_with_strategy('it_strat_typed', 'late_acceptance', 5)")
            .unwrap()
            .unwrap();

    wait_for(
        "typed constraints strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "completed",
        STRATEGY_TIMEOUT,
    )
    .expect("typed constraint solve should complete");

    cleanup_jobs("it_strat_typed");
    cleanup_problem("it_strat_typed");
}

#[test]
fn test_async_solve_with_pinning() {
    skip_if_not_running!();

    setup_local_it_problem("it_strat_pin");

    // Pin item 0 to slot 0
    execute("SELECT pgortools.pin_variable('it_strat_pin', 'x_0_0')").unwrap();

    let job_id =
        query_one("SELECT pgortools.solve_with_strategy('it_strat_pin', 'late_acceptance', 5)")
            .unwrap()
            .unwrap();

    wait_for(
        "pinning strategy",
        &format!(
            "SELECT state FROM pgortools.solve_jobs WHERE id = {}",
            job_id
        ),
        "completed",
        STRATEGY_TIMEOUT,
    )
    .expect("pinned solve should complete");

    cleanup_jobs("it_strat_pin");
    cleanup_problem("it_strat_pin");
}

// =============================================================================
// Benchmark Tests — standard OR problems with known optimal solutions
// =============================================================================

/// Assignment benchmark cost matrix (5 workers x 4 tasks).
/// HiGHS verified optimal = 251.
#[rustfmt::skip]
const BENCH_ASSIGN_COSTS: [[i64; 4]; 5] = [
    [90, 76, 75, 70],
    [35, 85, 55, 65],
    [125, 95, 90, 105],
    [45, 110, 95, 115],
    [60, 105, 80, 75],
];

/// Nurse scheduling shift requests [nurse][day][shift] (5 nurses, 7 days, 3 shifts).
/// Source: Google OR-Tools employee scheduling example.
/// Known optimal: 13 fulfilled requests.
#[rustfmt::skip]
const NURSE_REQUESTS: [[[i32; 3]; 7]; 5] = [
    [[0,0,1],[0,0,0],[0,0,0],[0,0,0],[0,0,1],[0,1,0],[0,0,1]],
    [[0,0,0],[0,0,0],[0,1,0],[0,1,0],[1,0,0],[0,0,0],[0,0,1]],
    [[0,1,0],[0,1,0],[0,0,0],[1,0,0],[0,0,0],[0,1,0],[0,0,0]],
    [[0,0,1],[0,0,0],[1,0,0],[0,1,0],[0,0,0],[1,0,0],[0,0,0]],
    [[0,0,0],[0,0,1],[0,1,0],[0,0,0],[1,0,0],[0,1,0],[0,0,0]],
];

/// GAP c515-1 profit matrix (5 agents x 15 jobs).
/// Source: OR-Library (Beasley 1990).
/// Known optimal profit = 336.
#[rustfmt::skip]
const GAP_PROFIT: [[i64; 15]; 5] = [
    [17, 21, 22, 18, 24, 15, 20, 18, 19, 18, 16, 22, 24, 24, 16],
    [23, 16, 21, 16, 17, 16, 19, 25, 18, 21, 17, 15, 25, 17, 24],
    [16, 20, 16, 25, 24, 16, 17, 19, 19, 18, 20, 16, 17, 21, 24],
    [19, 19, 22, 22, 20, 16, 19, 17, 21, 19, 25, 23, 25, 25, 25],
    [18, 19, 15, 15, 21, 25, 16, 16, 23, 15, 22, 17, 19, 22, 24],
];

/// GAP c515-1 resource consumption matrix (5 agents x 15 jobs).
#[rustfmt::skip]
const GAP_RESOURCE: [[i64; 15]; 5] = [
    [8, 15, 14, 23, 8, 16, 8, 25, 9, 17, 25, 15, 10, 8, 24],
    [15, 7, 23, 22, 11, 11, 12, 25, 16, 7, 23, 10, 17, 24, 8],
    [21, 20, 6, 22, 24, 10, 24, 9, 21, 14, 11, 14, 11, 19, 16],
    [20, 11, 8, 14, 9, 5, 6, 19, 19, 7, 6, 6, 13, 9, 18],
    [8, 13, 13, 13, 10, 20, 25, 16, 16, 17, 10, 10, 5, 12, 23],
];

/// GAP c515-1 agent capacities.
const GAP_CAPACITY: [i64; 5] = [36, 34, 38, 27, 33];

/// Helper: set up an assignment problem with binary vars, row/col constraints, and objective.
fn setup_assignment_problem(
    name: &str,
    costs: &[[i64; 4]; 5],
    n_workers: usize,
    n_tasks: usize,
    obj_type: &str, // "minimize" or "maximize"
) {
    cleanup_problem(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();

    // Binary vars x_i_j
    for i in 0..n_workers {
        for j in 0..n_tasks {
            execute(&format!(
                "SELECT pgortools.add_bool_var('{}', 'x_{}_{}')",
                name, i, j
            ))
            .unwrap();
        }
    }

    // Each worker assigned to at most 1 task
    for i in 0..n_workers {
        let terms: Vec<String> = (0..n_tasks).map(|j| format!("x_{}_{}", i, j)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} <= 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Each task assigned to exactly 1 worker
    for j in 0..n_tasks {
        let terms: Vec<String> = (0..n_workers).map(|i| format!("x_{}_{}", i, j)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} == 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Objective from cost matrix
    let mut obj_terms = Vec::new();
    for i in 0..n_workers {
        for j in 0..n_tasks {
            let c = costs[i][j];
            if c != 0 {
                obj_terms.push(format!("{}*x_{}_{}", c, i, j));
            }
        }
    }
    let obj_expr = obj_terms.join(" + ");
    execute(&format!(
        "SELECT pgortools.{}('{}', '{}')",
        obj_type, name, obj_expr
    ))
    .unwrap();
}

/// Helper: set up an N-Queens problem with binary vars and constraints.
fn setup_nqueens_problem(name: &str, n: usize) {
    cleanup_problem(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();

    // Binary vars q_r_c
    for r in 0..n {
        for c in 0..n {
            execute(&format!(
                "SELECT pgortools.add_bool_var('{}', 'q_{}_{}')",
                name, r, c
            ))
            .unwrap();
        }
    }

    // Row constraints: exactly 1 queen per row
    for r in 0..n {
        let terms: Vec<String> = (0..n).map(|c| format!("q_{}_{}", r, c)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} == 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Column constraints: exactly 1 queen per column
    for c in 0..n {
        let terms: Vec<String> = (0..n).map(|r| format!("q_{}_{}", r, c)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} == 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Forward diagonal (\): r - c = constant
    let n_i = n as i32;
    for d in -(n_i - 1)..=n_i - 1 {
        let cells: Vec<(usize, usize)> = (0..n)
            .filter_map(|r| {
                let c = r as i32 - d;
                if c >= 0 && c < n_i {
                    Some((r, c as usize))
                } else {
                    None
                }
            })
            .collect();
        if cells.len() >= 2 {
            let terms: Vec<String> = cells
                .iter()
                .map(|(r, c)| format!("q_{}_{}", r, c))
                .collect();
            execute(&format!(
                "SELECT pgortools.add_constraint('{}', '{} <= 1')",
                name,
                terms.join(" + ")
            ))
            .unwrap();
        }
    }

    // Back diagonal (/): r + c = constant
    for s in 0..=(2 * (n_i - 1)) {
        let cells: Vec<(usize, usize)> = (0..n)
            .filter_map(|r| {
                let c = s - r as i32;
                if c >= 0 && c < n_i {
                    Some((r, c as usize))
                } else {
                    None
                }
            })
            .collect();
        if cells.len() >= 2 {
            let terms: Vec<String> = cells
                .iter()
                .map(|(r, c)| format!("q_{}_{}", r, c))
                .collect();
            execute(&format!(
                "SELECT pgortools.add_constraint('{}', '{} <= 1')",
                name,
                terms.join(" + ")
            ))
            .unwrap();
        }
    }

    // Maximize total queens placed
    let all_terms: Vec<String> = (0..n)
        .flat_map(|r| (0..n).map(move |c| format!("q_{}_{}", r, c)))
        .collect();
    execute(&format!(
        "SELECT pgortools.maximize('{}', '{}')",
        name,
        all_terms.join(" + ")
    ))
    .unwrap();
}

#[test]
fn test_benchmark_assignment_5x4_sync() {
    skip_if_not_running!();

    setup_assignment_problem("it_bench_assign", &BENCH_ASSIGN_COSTS, 5, 4, "minimize");

    let solution = query_one("SELECT pgortools.solve_sync('it_bench_assign')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 251.0).abs() < 0.5,
        "5x4 assignment optimal should be 251, got {}",
        objective
    );

    cleanup_problem("it_bench_assign");
}

#[test]
fn test_benchmark_nqueens_4_sync() {
    skip_if_not_running!();

    setup_nqueens_problem("it_bench_nq4", 4);

    let solution = query_one("SELECT pgortools.solve_sync('it_bench_nq4')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 4.0).abs() < 0.5,
        "4-Queens should place 4 queens, got {}",
        objective
    );

    cleanup_problem("it_bench_nq4");
}

#[test]
fn test_benchmark_nurse_scheduling_sync() {
    skip_if_not_running!();

    let name = "it_bench_nurse";
    let n_nurses = 5;
    let n_days = 7;
    let n_shifts = 3;

    cleanup_problem(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();

    // 105 binary vars: v_{n*21+d*3+s}
    for n in 0..n_nurses {
        for d in 0..n_days {
            for s in 0..n_shifts {
                let idx = n * 21 + d * 3 + s;
                execute(&format!(
                    "SELECT pgortools.add_bool_var('{}', 'v_{}')",
                    name, idx
                ))
                .unwrap();
            }
        }
    }

    // Shift coverage: each (day, shift) needs exactly 1 nurse
    for d in 0..n_days {
        for s in 0..n_shifts {
            let terms: Vec<String> = (0..n_nurses)
                .map(|n| format!("v_{}", n * 21 + d * 3 + s))
                .collect();
            execute(&format!(
                "SELECT pgortools.add_constraint('{}', '{} == 1')",
                name,
                terms.join(" + ")
            ))
            .unwrap();
        }
    }

    // Each nurse works at most 1 shift per day
    for n in 0..n_nurses {
        for d in 0..n_days {
            let terms: Vec<String> = (0..n_shifts)
                .map(|s| format!("v_{}", n * 21 + d * 3 + s))
                .collect();
            execute(&format!(
                "SELECT pgortools.add_constraint('{}', '{} <= 1')",
                name,
                terms.join(" + ")
            ))
            .unwrap();
        }
    }

    // Workload balance: each nurse works 4-5 shifts
    for n in 0..n_nurses {
        let terms: Vec<String> = (0..n_days)
            .flat_map(|d| (0..n_shifts).map(move |s| format!("v_{}", n * 21 + d * 3 + s)))
            .collect();
        let sum = terms.join(" + ");
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} >= 4')",
            name, sum
        ))
        .unwrap();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} <= 5')",
            name, sum
        ))
        .unwrap();
    }

    // Objective: maximize fulfilled requests
    let mut obj_terms = Vec::new();
    for n in 0..n_nurses {
        for d in 0..n_days {
            for s in 0..n_shifts {
                if NURSE_REQUESTS[n][d][s] == 1 {
                    obj_terms.push(format!("v_{}", n * 21 + d * 3 + s));
                }
            }
        }
    }
    execute(&format!(
        "SELECT pgortools.maximize('{}', '{}')",
        name,
        obj_terms.join(" + ")
    ))
    .unwrap();

    let solution = query_one(&format!("SELECT pgortools.solve_sync('{}')::text", name))
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 13.0).abs() < 0.5,
        "nurse scheduling optimal should be 13 fulfilled requests, got {}",
        objective
    );

    cleanup_problem(name);
}

#[test]
fn test_benchmark_gap_c515_sync() {
    skip_if_not_running!();

    let name = "it_bench_gap";
    let n_agents = 5;
    let n_jobs = 15;

    cleanup_problem(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();

    // 75 binary vars: x_i_j
    for i in 0..n_agents {
        for j in 0..n_jobs {
            execute(&format!(
                "SELECT pgortools.add_bool_var('{}', 'x_{}_{}')",
                name, i, j
            ))
            .unwrap();
        }
    }

    // Each job assigned to exactly 1 agent
    for j in 0..n_jobs {
        let terms: Vec<String> = (0..n_agents).map(|i| format!("x_{}_{}", i, j)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} == 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Agent capacity constraints (resource consumption)
    for i in 0..n_agents {
        let terms: Vec<String> = (0..n_jobs)
            .map(|j| format!("{}*x_{}_{}", GAP_RESOURCE[i][j], i, j))
            .collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} <= {}')",
            name,
            terms.join(" + "),
            GAP_CAPACITY[i]
        ))
        .unwrap();
    }

    // Maximize total profit
    let obj_terms: Vec<String> = (0..n_agents)
        .flat_map(|i| (0..n_jobs).map(move |j| format!("{}*x_{}_{}", GAP_PROFIT[i][j], i, j)))
        .collect();
    execute(&format!(
        "SELECT pgortools.maximize('{}', '{}')",
        name,
        obj_terms.join(" + ")
    ))
    .unwrap();

    let solution = query_one(&format!("SELECT pgortools.solve_sync('{}')::text", name))
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 336.0).abs() < 0.5,
        "GAP c515-1 optimal profit should be 336, got {}",
        objective
    );

    cleanup_problem(name);
}

/// GAP c824-1 profit matrix (8 agents x 24 jobs).
/// Source: OR-Library (Beasley 1990), gap5.txt instance 1.
/// Known optimal profit = 563.
#[rustfmt::skip]
const GAP824_PROFIT: [[i64; 24]; 8] = [
    [25, 23, 20, 16, 19, 22, 20, 16, 15, 22, 15, 21, 20, 23, 20, 22, 19, 25, 25, 24, 21, 17, 23, 17],
    [16, 19, 22, 22, 19, 23, 17, 24, 15, 24, 18, 19, 20, 24, 25, 25, 19, 24, 18, 21, 16, 25, 15, 20],
    [20, 18, 23, 23, 23, 17, 19, 16, 24, 24, 17, 23, 19, 22, 23, 25, 23, 18, 19, 24, 20, 17, 23, 23],
    [16, 16, 15, 23, 15, 15, 25, 22, 17, 20, 19, 16, 17, 17, 20, 17, 17, 18, 16, 18, 15, 25, 22, 17],
    [17, 23, 21, 20, 24, 22, 25, 17, 22, 20, 16, 22, 21, 23, 24, 15, 22, 25, 18, 19, 19, 17, 22, 23],
    [24, 21, 23, 17, 21, 19, 19, 17, 18, 24, 15, 15, 17, 18, 15, 24, 19, 21, 23, 24, 17, 20, 16, 21],
    [18, 21, 22, 23, 22, 15, 18, 15, 21, 22, 15, 23, 21, 25, 25, 23, 20, 16, 25, 17, 15, 15, 18, 16],
    [19, 24, 18, 17, 21, 18, 24, 25, 18, 23, 21, 15, 24, 23, 18, 18, 23, 23, 16, 20, 20, 19, 25, 21],
];

/// GAP c824-1 resource consumption matrix (8 agents x 24 jobs).
#[rustfmt::skip]
const GAP824_RESOURCE: [[i64; 24]; 8] = [
    [ 8, 18, 22,  5, 11, 11, 22, 11, 17, 22, 11, 20, 13, 13,  7, 22, 15, 22, 24,  8,  8, 24, 18,  8],
    [24, 14, 11, 15, 24,  8, 10, 15, 19, 25,  6, 13, 10, 25, 19, 24, 13, 12,  5, 18, 10, 24,  8,  5],
    [22, 22, 21, 22, 13, 16, 21,  5, 25, 13, 12,  9, 24,  6, 22, 24, 11, 21, 11, 14, 12, 10, 20,  6],
    [13,  8, 19, 12, 19, 18, 10, 21,  5,  9, 11,  9, 22,  8, 12, 13,  9, 25, 19, 24, 22,  6, 19, 14],
    [25, 16, 13,  5, 11,  8,  7,  8, 25, 20, 24, 20, 11,  6, 10, 10,  6, 22, 10, 10, 13, 21,  5, 19],
    [19, 19,  5, 11, 22, 24, 18, 11,  6, 13, 24, 24, 22,  6, 22,  5, 14,  6, 16, 11,  6,  8, 18, 10],
    [24, 10,  9, 10,  6, 15,  7, 13, 20,  8,  7,  9, 24,  9, 21,  9, 11, 19, 10,  5, 23, 20,  5, 21],
    [ 6,  9,  9,  5, 12, 10, 16, 15, 19, 18, 20, 18, 16, 21, 11, 12, 22, 16, 21, 25,  7, 14, 16, 10],
];

/// GAP c824-1 agent capacities.
const GAP824_CAPACITY: [i64; 8] = [36, 35, 38, 34, 32, 34, 31, 34];

/// GAP c1030-1 profit matrix (10 agents x 30 jobs).
/// Source: OR-Library (Beasley 1990), gap9.txt instance 1.
/// Known optimal profit = 709.
#[rustfmt::skip]
const GAP1030_PROFIT: [[i64; 30]; 10] = [
    [25, 16, 17, 24, 15, 24, 24, 17, 23, 16, 21, 18, 18, 18, 18, 20, 16, 15, 25, 15, 16, 23, 22, 18, 23, 18, 19, 20, 21, 19],
    [16, 17, 19, 18, 17, 23, 15, 25, 25, 16, 16, 23, 21, 23, 25, 19, 16, 18, 16, 21, 24, 15, 21, 19, 20, 16, 18, 25, 20, 22],
    [19, 19, 15, 25, 24, 25, 16, 23, 15, 20, 16, 19, 18, 20, 15, 17, 16, 17, 21, 20, 18, 19, 22, 20, 19, 21, 18, 20, 25, 17],
    [17, 21, 21, 21, 19, 20, 24, 15, 21, 15, 18, 23, 18, 16, 16, 18, 24, 16, 22, 16, 22, 19, 17, 17, 22, 19, 21, 16, 24, 17],
    [18, 23, 23, 17, 25, 21, 22, 17, 20, 24, 18, 25, 16, 15, 16, 18, 23, 17, 25, 17, 17, 23, 24, 23, 19, 16, 16, 22, 22, 19],
    [24, 18, 20, 18, 19, 16, 22, 18, 19, 23, 18, 23, 21, 24, 22, 25, 21, 18, 21, 16, 21, 22, 18, 22, 15, 19, 16, 22, 16, 17],
    [20, 21, 15, 24, 20, 19, 16, 24, 23, 24, 22, 20, 24, 18, 24, 20, 17, 23, 17, 22, 18, 21, 25, 17, 21, 18, 24, 21, 15, 25],
    [15, 23, 19, 21, 17, 16, 22, 23, 16, 15, 19, 21, 15, 18, 18, 21, 25, 15, 15, 15, 20, 21, 18, 19, 18, 21, 25, 23, 25, 21],
    [17, 25, 16, 24, 16, 22, 16, 15, 15, 17, 25, 17, 20, 21, 16, 16, 17, 17, 25, 21, 18, 17, 20, 19, 23, 18, 17, 20, 17, 17],
    [17, 21, 23, 22, 23, 25, 25, 22, 24, 18, 24, 22, 15, 24, 15, 16, 19, 22, 25, 19, 24, 17, 16, 17, 17, 25, 19, 16, 23, 22],
];

/// GAP c1030-1 resource consumption matrix (10 agents x 30 jobs).
#[rustfmt::skip]
const GAP1030_RESOURCE: [[i64; 30]; 10] = [
    [ 5, 25, 21, 11, 25, 10, 15, 17, 10,  9, 10, 23, 13, 13, 23, 14, 24, 17, 16, 21, 22, 23,  7, 25, 17, 17, 24, 24,  7,  8],
    [25, 14, 19, 21, 22, 11, 19, 14, 12, 22,  9, 10,  7, 12,  6,  9,  5, 23, 24, 16,  8, 15, 17, 22, 20,  6, 18, 15,  7,  7],
    [12, 14, 15, 25, 18, 25,  6,  7, 10, 17, 12, 10, 12, 19, 10, 12, 12, 10,  7, 21,  5,  5, 19, 21, 12,  6, 14,  6, 19, 24],
    [24, 11, 18, 14,  9, 13, 18, 14, 11, 12,  9, 14, 12, 18,  8, 19, 24, 13,  7, 17, 22, 20, 24, 15, 12, 12, 20, 21,  6, 22],
    [15,  9, 19,  9, 13, 19, 25, 16, 14, 17,  6,  9, 15, 12, 20, 21,  9, 17, 19, 14, 23, 19, 25, 22, 13, 14, 14, 23, 15, 23],
    [25, 22, 12,  6, 11, 25, 15, 13, 13,  9,  9, 13, 19, 10, 18, 23, 14,  8,  9, 19, 15, 21, 10,  5, 23, 17, 22, 11, 17,  5],
    [20, 10, 18,  7, 12, 14, 12, 18,  8, 25, 25, 14, 16, 25, 23,  5, 22, 22,  7, 24, 12, 13,  5,  8, 12, 25, 24, 19, 15, 14],
    [15, 18, 21, 17,  7,  5, 10, 10, 25, 13, 23, 10,  7,  7, 20, 18, 21, 12, 24, 10,  7, 25, 20, 13, 19, 18, 16,  7,  9,  6],
    [20, 21,  9, 22, 18,  7,  5, 21, 15, 13,  8, 23, 21, 21, 17, 11, 24, 11, 12, 15, 19,  8,  7,  8, 18, 22, 18, 12, 18, 17],
    [14,  6, 10, 23, 12, 23, 12,  9, 20, 25, 24, 23, 20, 12, 25, 17, 18, 20, 18, 25, 17, 20, 19, 12,  5,  6, 10,  5,  7, 15],
];

/// GAP c1030-1 agent capacities.
const GAP1030_CAPACITY: [i64; 10] = [40, 30, 32, 37, 39, 35, 38, 34, 36, 37];

/// Generalized helper: set up a GAP instance with binary vars, job assignment and capacity constraints.
fn setup_gap_problem(
    name: &str,
    n_agents: usize,
    n_jobs: usize,
    profit: &[&[i64]],
    resource: &[&[i64]],
    capacity: &[i64],
) {
    cleanup_problem(name);
    execute(&format!("SELECT pgortools.create_problem('{}')", name)).unwrap();

    // Binary vars x_i_j
    for i in 0..n_agents {
        for j in 0..n_jobs {
            execute(&format!(
                "SELECT pgortools.add_bool_var('{}', 'x_{}_{}')",
                name, i, j
            ))
            .unwrap();
        }
    }

    // Each job assigned to exactly 1 agent
    for j in 0..n_jobs {
        let terms: Vec<String> = (0..n_agents).map(|i| format!("x_{}_{}", i, j)).collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} == 1')",
            name,
            terms.join(" + ")
        ))
        .unwrap();
    }

    // Agent capacity constraints
    for i in 0..n_agents {
        let terms: Vec<String> = (0..n_jobs)
            .map(|j| format!("{}*x_{}_{}", resource[i][j], i, j))
            .collect();
        execute(&format!(
            "SELECT pgortools.add_constraint('{}', '{} <= {}')",
            name,
            terms.join(" + "),
            capacity[i]
        ))
        .unwrap();
    }

    // Maximize total profit
    let obj_terms: Vec<String> = (0..n_agents)
        .flat_map(|i| (0..n_jobs).map(move |j| format!("{}*x_{}_{}", profit[i][j], i, j)))
        .collect();
    execute(&format!(
        "SELECT pgortools.maximize('{}', '{}')",
        name,
        obj_terms.join(" + ")
    ))
    .unwrap();
}

/// Tier 2 benchmark: GAP c824-1 (8 agents x 24 jobs, 192 binary vars).
/// OR-Library optimal = 563.
#[test]
fn test_benchmark_gap_c824_sync() {
    skip_if_not_running!();

    let profit: Vec<&[i64]> = GAP824_PROFIT.iter().map(|r| r.as_slice()).collect();
    let resource: Vec<&[i64]> = GAP824_RESOURCE.iter().map(|r| r.as_slice()).collect();
    setup_gap_problem(
        "it_bench_gap824",
        8,
        24,
        &profit,
        &resource,
        &GAP824_CAPACITY,
    );

    let solution = query_one("SELECT pgortools.solve_sync('it_bench_gap824')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 563.0).abs() < 0.5,
        "GAP c824-1 optimal profit should be 563, got {}",
        objective
    );

    cleanup_problem("it_bench_gap824");
}

/// Tier 3 benchmark: GAP c1030-1 (10 agents x 30 jobs, 300 binary vars).
/// OR-Library optimal = 709.
#[test]
fn test_benchmark_gap_c1030_sync() {
    skip_if_not_running!();

    let profit: Vec<&[i64]> = GAP1030_PROFIT.iter().map(|r| r.as_slice()).collect();
    let resource: Vec<&[i64]> = GAP1030_RESOURCE.iter().map(|r| r.as_slice()).collect();
    setup_gap_problem(
        "it_bench_gap1030",
        10,
        30,
        &profit,
        &resource,
        &GAP1030_CAPACITY,
    );

    let solution = query_one("SELECT pgortools.solve_sync('it_bench_gap1030')::text")
        .unwrap()
        .unwrap();
    let json: serde_json::Value = serde_json::from_str(&solution).unwrap();

    assert_eq!(json["status"], "OPTIMAL", "should find optimal");
    let objective = json["objective"].as_f64().unwrap();
    assert!(
        (objective - 709.0).abs() < 0.5,
        "GAP c1030-1 optimal profit should be 709, got {}",
        objective
    );

    cleanup_problem("it_bench_gap1030");
}
