//! Cell operations for pg_sheet.
//!
//! Get, set, clear individual cells. Bulk operations for efficiency.
//! All writes go through the overlay table and record audit entries.

use pgrx::prelude::*;

use crate::formulas;

/// Try to get a single optional value. Returns Ok(None) if no rows found.
/// pgrx 0.16 `get_one_with_args` throws InvalidPosition on empty results
/// instead of returning None, so we catch that case.
fn try_get_one<'a, T: FromDatum + pgrx::IntoDatum>(
    query: &str,
    args: &[pgrx::datum::DatumWithOid<'a>],
) -> Result<Option<T>, String> {
    match Spi::get_one_with_args::<T>(query, args) {
        Ok(val) => Ok(val),
        Err(pgrx::spi::Error::InvalidPosition) => Ok(None),
        Err(e) => Err(e.to_string()),
    }
}

/// Set a cell value in the overlay.
///
/// If the cell already has a value, it's updated. An audit entry is always created.
pub fn set_value_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    value: &str,
) -> Result<bool, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let audit_table = format!("pgsheet.\"_audit_{}\"", sheet_name);

    // Check for lock
    check_cell_lock(sheet_name, entity_id, col_name)?;

    // Get old value for audit
    let old_value = try_get_one::<String>(
        &format!(
            "SELECT value FROM {} WHERE entity_id = $1 AND col_name = $2",
            overlay_table
        ),
        &[entity_id.into(), col_name.into()],
    )?;

    let old_formula = try_get_one::<String>(
        &format!(
            "SELECT formula FROM {} WHERE entity_id = $1 AND col_name = $2",
            overlay_table
        ),
        &[entity_id.into(), col_name.into()],
    )?;

    // Upsert the cell value
    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (entity_id, col_name, value, formula, evaluated, refs)
               VALUES ($1, $2, $3, NULL, NULL, NULL)
               ON CONFLICT (entity_id, col_name) DO UPDATE
               SET value = $3, formula = NULL, evaluated = NULL, refs = NULL,
                   modified_at = now(),
                   modified_by = current_setting('app.user_id', true)"#,
            overlay_table
        ),
        &[entity_id.into(), col_name.into(), value.into()],
    )
    .map_err(|e| e.to_string())?;

    // Audit entry
    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (entity_id, col_name, old_value, new_value, old_formula, new_formula)
               VALUES ($1, $2, $3, $4, $5, NULL)"#,
            audit_table
        ),
        &[
            entity_id.into(),
            col_name.into(),
            old_value.into(),
            value.into(),
            old_formula.into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    Ok(true)
}

/// Set a cell formula in the overlay.
///
/// The formula is parsed, dependencies extracted, and (if possible) evaluated
/// server-side via SQL translation.
pub fn set_formula_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    formula: &str,
) -> Result<pgrx::JsonB, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let audit_table = format!("pgsheet.\"_audit_{}\"", sheet_name);

    // Check for lock
    check_cell_lock(sheet_name, entity_id, col_name)?;

    // Parse the formula
    let parsed = formulas::parse_formula(formula)?;

    // Extract refs as text array
    let refs: Vec<String> = parsed
        .cell_refs
        .iter()
        .map(|r| format!("{}{}", r.col, r.row))
        .chain(parsed.col_refs.iter().cloned())
        .collect();

    // Get old values for audit
    let old_formula = try_get_one::<String>(
        &format!(
            "SELECT formula FROM {} WHERE entity_id = $1 AND col_name = $2",
            overlay_table
        ),
        &[entity_id.into(), col_name.into()],
    )?;

    // Upsert the formula
    let refs_literal = if refs.is_empty() {
        "NULL".to_string()
    } else {
        format!(
            "ARRAY[{}]",
            refs.iter()
                .map(|r| format!("'{}'", r.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",")
        )
    };

    // Try to evaluate the formula SQL to get the actual computed result.
    // For simple expressions (no column refs) we can evaluate directly.
    // For column-ref formulas, substitute values from the source entity.
    let evaluated = if let Some(ref sql_expr) = parsed.sql {
        // Get sheet metadata for source table resolution
        let (src_schema, src_table, _) = crate::sheets::get_sheet_meta(sheet_name)?;
        // Build a query that resolves column refs from the source entity
        let eval_sql = format!(
            "SELECT ({})::text FROM {}.\"{}\" WHERE id = $1",
            sql_expr, src_schema, src_table
        );
        match Spi::get_one_with_args::<String>(&eval_sql, &[entity_id.into()]) {
            Ok(Some(val)) => Some(val),
            Ok(None) => Some(sql_expr.clone()),
            Err(_) => Some(sql_expr.clone()), // fallback to expression text
        }
    } else {
        None
    };

    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (entity_id, col_name, value, formula, evaluated, refs)
               VALUES ($1, $2, NULL, $3, $4, {})
               ON CONFLICT (entity_id, col_name) DO UPDATE
               SET value = NULL, formula = $3, evaluated = $4, refs = {},
                   modified_at = now(),
                   modified_by = current_setting('app.user_id', true)"#,
            overlay_table, refs_literal, refs_literal
        ),
        &[
            entity_id.into(),
            col_name.into(),
            formula.into(),
            evaluated.into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    // Audit entry
    Spi::run_with_args(
        &format!(
            r#"INSERT INTO {} (entity_id, col_name, old_formula, new_formula)
               VALUES ($1, $2, $3, $4)"#,
            audit_table
        ),
        &[
            entity_id.into(),
            col_name.into(),
            old_formula.into(),
            formula.into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    // Return parsed formula info
    let result = serde_json::json!({
        "valid": true,
        "formula": formula,
        "refs": refs,
        "functions": parsed.functions,
        "sql_translatable": parsed.sql_translatable,
        "sql": parsed.sql,
    });

    Ok(pgrx::JsonB(result))
}

/// Clear a cell (remove from overlay).
pub fn clear_cell_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
) -> Result<bool, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    let audit_table = format!("pgsheet.\"_audit_{}\"", sheet_name);

    check_cell_lock(sheet_name, entity_id, col_name)?;

    // Get old value for audit
    let old_value = try_get_one::<String>(
        &format!(
            "SELECT COALESCE(value, formula) FROM {} WHERE entity_id = $1 AND col_name = $2",
            overlay_table
        ),
        &[entity_id.into(), col_name.into()],
    )?;

    // Delete from overlay
    Spi::run_with_args(
        &format!(
            "DELETE FROM {} WHERE entity_id = $1 AND col_name = $2",
            overlay_table
        ),
        &[entity_id.into(), col_name.into()],
    )
    .map_err(|e| e.to_string())?;

    // Audit
    if old_value.is_some() {
        Spi::run_with_args(
            &format!(
                r#"INSERT INTO {} (entity_id, col_name, old_value, new_value)
                   VALUES ($1, $2, $3, NULL)"#,
                audit_table
            ),
            &[entity_id.into(), col_name.into(), old_value.into()],
        )
        .map_err(|e| e.to_string())?;
    }

    Ok(true)
}

/// Get a single cell's value, formula, and metadata.
pub fn get_cell_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
) -> Result<pgrx::JsonB, String> {
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);

    let result: Option<(Option<String>, Option<String>, Option<String>)> = Spi::connect(|client| {
        let mut table = client.select(
            &format!(
                "SELECT value, formula, evaluated FROM {} WHERE entity_id = $1 AND col_name = $2",
                overlay_table
            ),
            None,
            &[entity_id.into(), col_name.into()],
        )?;
        if let Some(row) = table.next() {
            let value: Option<String> = row.get(1)?;
            let formula: Option<String> = row.get(2)?;
            let evaluated: Option<String> = row.get(3)?;
            return Ok(Some((value, formula, evaluated)));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::Error| e.to_string())?;

    let json = match result {
        Some((value, formula, evaluated)) => serde_json::json!({
            "entity_id": entity_id.to_string(),
            "col_name": col_name,
            "value": value,
            "formula": formula,
            "evaluated": evaluated,
            "has_overlay": true,
        }),
        None => serde_json::json!({
            "entity_id": entity_id.to_string(),
            "col_name": col_name,
            "value": null,
            "formula": null,
            "evaluated": null,
            "has_overlay": false,
        }),
    };

    Ok(pgrx::JsonB(json))
}

/// Bulk set multiple cell values at once.
///
/// Input: JSONB array of { entity_id, col_name, value?, formula? }
pub fn set_values_impl(sheet_name: &str, cells: pgrx::JsonB) -> Result<i32, String> {
    let arr = cells.0.as_array().ok_or("Input must be a JSON array")?;

    let mut count = 0;

    for cell in arr {
        let entity_id_str = cell
            .get("entity_id")
            .and_then(|v| v.as_str())
            .ok_or("Each cell must have entity_id")?;

        let col_name = cell
            .get("col_name")
            .and_then(|v| v.as_str())
            .ok_or("Each cell must have col_name")?;

        // Parse UUID string into pgrx::Uuid via SPI
        let pg_uuid =
            Spi::get_one_with_args::<pgrx::Uuid>("SELECT $1::uuid", &[entity_id_str.into()])
                .map_err(|e| format!("Invalid UUID '{}': {}", entity_id_str, e))?
                .ok_or_else(|| format!("Invalid UUID: {}", entity_id_str))?;

        if let Some(formula) = cell.get("formula").and_then(|v| v.as_str()) {
            set_formula_impl(sheet_name, pg_uuid, col_name, formula)?;
        } else if let Some(value) = cell.get("value").and_then(|v| v.as_str()) {
            set_value_impl(sheet_name, pg_uuid, col_name, value)?;
        } else {
            clear_cell_impl(sheet_name, pg_uuid, col_name)?;
        }

        count += 1;
    }

    Ok(count)
}

/// Check if a cell is locked by another user.
fn check_cell_lock(sheet_name: &str, entity_id: pgrx::Uuid, col_name: &str) -> Result<(), String> {
    let lock_table = format!("pgsheet.\"_lock_{}\"", sheet_name);

    let locked_by = try_get_one::<String>(
        &format!(
            "SELECT locked_by FROM {} WHERE entity_id = $1 AND col_name = $2",
            lock_table
        ),
        &[entity_id.into(), col_name.into()],
    )?;

    if let Some(locker) = locked_by {
        let current_user = Spi::get_one::<String>("SELECT current_setting('app.user_id', true)")
            .map_err(|e| e.to_string())?
            .unwrap_or_default();

        if locker != current_user {
            return Err(format!("Cell is locked by user '{}'", locker));
        }
    }

    Ok(())
}
