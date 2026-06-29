//! Load assignment problems from the pgortools database tables.
//!
//! Reads variables, typed constraints, and item/slot metadata from the
//! pgortools schema and constructs an `AssignmentProblem` for the
//! metaheuristic solver.

use std::collections::HashMap;
use std::time::Duration;

use pgrx::prelude::*;
use serde_json::Value as JsonValue;

use crate::error::PgOrtoolsError;
use ortools_core::metaheuristic::{
    format_result, parse_typed_constraint, parse_var_indices, AssignmentProblem, ItemData, SlotData,
};
use ortools_core::Algorithm;

/// Load problem from DB, solve with metaheuristic, return JSONB solution.
pub fn solve_from_db(
    problem_name: &str,
    algorithm: &Algorithm,
    time_limit: Duration,
) -> Result<serde_json::Value, PgOrtoolsError> {
    let (problem, var_names, item_count, slot_count) = load_problem(problem_name)?;

    let result = ortools_core::metaheuristic::solve_local(&problem, algorithm, time_limit, 42);

    Ok(format_result(&result, &var_names, item_count, slot_count)?)
}

/// Load an AssignmentProblem from the database.
/// Returns (problem, var_names, item_count, slot_count).
fn load_problem(
    problem_name: &str,
) -> Result<(AssignmentProblem, Vec<String>, usize, usize), PgOrtoolsError> {
    // Get problem ID
    let problem_id = Spi::get_one_with_args::<i64>(
        "SELECT id FROM pgortools.problems WHERE name = $1",
        &[problem_name.into()],
    )
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?
    .ok_or_else(|| PgOrtoolsError::ProblemNotFound(problem_name.to_string()))?;

    // Read variables to determine grid shape
    let (var_names, pinned_flags, item_count, slot_count) = load_variables(problem_id)?;

    // Read typed constraints
    let constraints = load_typed_constraints(problem_id)?;

    // Build item_data and slot_data (minimal for now)
    let item_data: Vec<ItemData> = (0..item_count)
        .map(|_| ItemData {
            group: None,
            fields: HashMap::new(),
        })
        .collect();

    let slot_data: Vec<SlotData> = (0..slot_count)
        .map(|_| SlotData {
            fields: HashMap::new(),
        })
        .collect();

    let problem = AssignmentProblem {
        item_count,
        slot_count,
        constraints,
        pinned: pinned_flags,
        item_data,
        slot_data,
    };

    Ok((problem, var_names, item_count, slot_count))
}

/// Load variables and determine the assignment grid shape.
/// Variables named `x_i_j` form an item x slot grid.
/// Returns (var_names, pinned_flags, item_count, slot_count).
fn load_variables(
    problem_id: i64,
) -> Result<(Vec<String>, Vec<bool>, usize, usize), PgOrtoolsError> {
    let sql = format!(
        "SELECT name, pinned FROM pgortools.variables WHERE problem_id = {} ORDER BY id",
        problem_id
    );

    let mut var_names = Vec::new();
    let mut var_pinned = Vec::new();
    let mut max_item = 0usize;
    let mut max_slot = 0usize;

    Spi::connect(|client| {
        let table = client.select(&sql, None, &[])?;
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let pinned: bool = row.get(2)?.unwrap_or(false);
            var_names.push(name.clone());
            var_pinned.push(pinned);

            // Parse x_i_j pattern
            if let Some((i, j)) = parse_var_indices(&name) {
                max_item = max_item.max(i + 1);
                max_slot = max_slot.max(j + 1);
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    })
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?;

    if max_item == 0 || max_slot == 0 {
        return Err(PgOrtoolsError::InvalidParameter(
            "No assignment variables (x_i_j pattern) found".to_string(),
        ));
    }

    // Build per-item pinned flags from per-variable pinned flags.
    let mut item_pinned = vec![false; max_item];
    for (name, &pinned) in var_names.iter().zip(var_pinned.iter()) {
        if pinned {
            if let Some((i, _)) = parse_var_indices(name) {
                item_pinned[i] = true;
            }
        }
    }

    Ok((var_names, item_pinned, max_item, max_slot))
}

/// Load typed constraints from constraint_config JSONB.
fn load_typed_constraints(
    problem_id: i64,
) -> Result<Vec<ortools_core::TypedConstraint>, PgOrtoolsError> {
    let sql = format!(
        "SELECT constraint_type, constraint_config::text FROM pgortools.constraints \
         WHERE problem_id = {} AND constraint_config IS NOT NULL",
        problem_id
    );

    let mut constraints = Vec::new();

    Spi::connect(|client| {
        let table = client.select(&sql, None, &[])?;
        for row in table {
            let ctype: String = row.get(1)?.unwrap_or_default();
            let config_str: String = row.get(2)?.unwrap_or_default();

            let config: JsonValue = serde_json::from_str(&config_str).unwrap_or(JsonValue::Null);
            if let Some(tc) = parse_typed_constraint(&ctype, &config) {
                constraints.push(tc);
            }
        }
        Ok::<_, pgrx::spi::Error>(())
    })
    .map_err(|e| PgOrtoolsError::SpiError(e.to_string()))?;

    Ok(constraints)
}

/// Validate constraint_type string.
pub fn is_valid_constraint_type(ctype: &str) -> bool {
    ortools_core::metaheuristic::is_valid_constraint_type(ctype)
}

/// Parse algorithm name string into Algorithm enum.
pub fn parse_algorithm(name: &str) -> Result<Algorithm, PgOrtoolsError> {
    Ok(ortools_core::metaheuristic::parse_algorithm(name)?)
}
