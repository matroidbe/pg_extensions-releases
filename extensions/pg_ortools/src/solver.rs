//! MIP solver adapter — thin SPI wrapper around ortools_core.
//!
//! Loads problem data from the pgortools schema, delegates to
//! `ortools_core::mip::solve_mip()`, and stores the result back.

use crate::error::PgOrtoolsError;
use ortools_core::mip::{ConstraintData, ProblemData, VariableData};
use pgrx::prelude::*;

/// Solve a problem by loading it from the database and running HiGHS.
/// When `greedy` is true, accepts the first feasible solution without optimizing.
pub fn solve_problem(
    problem_name: &str,
    greedy: bool,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let problem_data = load_problem_from_db(problem_name)?;

    match ortools_core::mip::solve_mip(&problem_data, problem_name, greedy) {
        Ok(result) => {
            store_solution(problem_name, &result)?;
            Ok(result)
        }
        Err(e) => {
            // Store error result in DB too (matching original behavior)
            let error_result = serde_json::json!({
                "status": "ERROR",
                "objective": null,
                "values": {},
                "time_ms": 0,
                "error": e.to_string(),
            });
            let _ = store_solution(problem_name, &error_result);
            Err(e.into())
        }
    }
}

// =============================================================================
// Database Helpers
// =============================================================================

pub fn load_problem_from_db(problem_name: &str) -> Result<ProblemData, PgOrtoolsError> {
    let problem_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?
    .ok_or_else(|| PgOrtoolsError::ProblemNotFound(problem_name.to_string()))?;

    let objective_type = Spi::get_one_with_args::<String>(
        "SELECT objective_type FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?;

    let objective_expr = Spi::get_one_with_args::<String>(
        "SELECT objective_expr FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )?;

    let variables = Spi::connect(|client| {
        let mut vars = Vec::new();
        let query = format!(
            "SELECT name::text, var_type::text, domain_min, domain_max \
             FROM pgortools.variables WHERE problem_id = {}",
            problem_id
        );
        let table = client.select(&query, None, &[])?;

        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let var_type: String = row.get(2)?.unwrap_or_else(|| "int".to_string());
            let domain_min: i64 = row.get(3)?.unwrap_or(0);
            let domain_max: i64 = row.get(4)?.unwrap_or(i64::MAX);

            vars.push(VariableData {
                name,
                var_type,
                domain_min,
                domain_max,
            });
        }
        Ok::<_, pgrx::spi::Error>(vars)
    })?;

    let constraints = Spi::connect(|client| {
        let mut cons = Vec::new();
        let query = format!(
            "SELECT expression::text FROM pgortools.constraints WHERE problem_id = {} AND expression IS NOT NULL",
            problem_id
        );
        let table = client.select(&query, None, &[])?;

        for row in table {
            let expression: String = row.get(1)?.unwrap_or_default();
            cons.push(ConstraintData { expression });
        }
        Ok::<_, pgrx::spi::Error>(cons)
    })?;

    Ok(ProblemData {
        id: problem_id,
        variables,
        constraints,
        objective_type,
        objective_expr,
    })
}

pub fn store_solution(
    problem_name: &str,
    solution: &serde_json::Value,
) -> Result<(), PgOrtoolsError> {
    let status = solution["status"].as_str().unwrap_or("UNKNOWN");
    let objective = solution["objective"].as_f64();
    let values = pgrx::JsonB(solution["values"].clone());
    let time_ms = solution["time_ms"].as_i64().unwrap_or(0) as i32;

    Spi::run_with_args(
        r#"
        INSERT INTO pgortools.solutions (problem_id, status, objective_value, variable_values, solve_time_ms)
        SELECT p.id, $2, $3, $4, $5
        FROM pgortools.problems p
        WHERE p.name = $1
        "#,
        &[
            problem_name.into(),
            status.into(),
            objective.into(),
            values.into(),
            time_ms.into(),
        ],
    )?;

    Ok(())
}
