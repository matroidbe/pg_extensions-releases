//! Load assignment problems from the pgortools database tables.
//!
//! Reads variables, typed constraints, and item/slot metadata from the
//! pgortools schema and constructs an `AssignmentProblem` for the
//! metaheuristic solver.

use std::time::Duration;

use pgrx::prelude::*;
use serde_json::Value as JsonValue;

use crate::error::PgOrtoolsError;
use ortools_core::metaheuristic::{build_assignment_problem, format_oriented_result};
use ortools_core::Algorithm;

/// Load problem from DB, solve with metaheuristic, return JSONB solution.
///
/// The problem is built by `ortools_core::metaheuristic::build_assignment_problem`
/// from the rows read here: it honours an `assignment` typed constraint (which
/// side gets exactly one partner) and refuses a typed constraint whose config
/// does not parse, rather than solving without it.
pub fn solve_from_db(
    problem_name: &str,
    algorithm: &Algorithm,
    time_limit: Duration,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let problem_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?
    .ok_or_else(|| PgOrtoolsError::ProblemNotFound(problem_name.to_string()))?;

    let variables = load_variables(problem_id)?;
    let typed = load_typed_constraints(problem_id)?;
    let (problem, orientation) = build_assignment_problem(&variables, &typed)?;

    let result = ortools_core::metaheuristic::solve_local(&problem, algorithm, time_limit, 42);
    let names: Vec<String> = variables.into_iter().map(|(name, _)| name).collect();
    Ok(format_oriented_result(&result, &names, orientation))
}

/// The problem's variables as (name, pinned), in creation order.
fn load_variables(problem_id: i64) -> Result<Vec<(String, bool)>, PgOrtoolsError> {
    let mut out = Vec::new();
    Spi::connect(|client| {
        let table = client.select(
            "SELECT name, pinned FROM pgortools.variables WHERE problem_id = $1 ORDER BY id",
            None,
            &[problem_id.into()],
        )?;
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let pinned: bool = row.get(2)?.unwrap_or(false);
            out.push((name, pinned));
        }
        Ok::<_, pgrx::spi::Error>(())
    })
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?;
    Ok(out)
}

/// The problem's typed constraints as (type, config).
fn load_typed_constraints(problem_id: i64) -> Result<Vec<(String, JsonValue)>, PgOrtoolsError> {
    let mut out = Vec::new();
    Spi::connect(|client| {
        let table = client.select(
            "SELECT constraint_type, constraint_config::text FROM pgortools.constraints \
             WHERE problem_id = $1 AND constraint_config IS NOT NULL ORDER BY id",
            None,
            &[problem_id.into()],
        )?;
        for row in table {
            let ctype: String = row.get(1)?.unwrap_or_default();
            let config: String = row.get(2)?.unwrap_or_default();
            out.push((
                ctype,
                serde_json::from_str(&config).unwrap_or(JsonValue::Null),
            ));
        }
        Ok::<_, pgrx::spi::Error>(())
    })
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?;
    Ok(out)
}

/// Validate constraint_type string.
pub fn is_valid_constraint_type(ctype: &str) -> bool {
    ortools_core::metaheuristic::is_valid_constraint_type(ctype)
}

/// Parse algorithm name string into Algorithm enum.
pub fn parse_algorithm(name: &str) -> Result<Algorithm, PgOrtoolsError> {
    Ok(ortools_core::metaheuristic::parse_algorithm(name)?)
}
