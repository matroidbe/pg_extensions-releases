//! Column management for pg_sheet.
//!
//! Add, drop, rename overlay columns. Source columns are read-only.
//! Formula columns store a SQL expression that is embedded in the merged view.

use pgrx::prelude::*;

use crate::formulas;
use crate::sheets;
use crate::types;

/// Add an overlay column to a sheet.
pub fn add_column_impl(
    sheet_name: &str,
    col_name: &str,
    col_type: &str,
    default_value: Option<&str>,
) -> Result<bool, String> {
    // Validate column type
    if !types::validate_column_type(col_type) {
        return Err(format!("Invalid column type: {}", col_type));
    }

    // Check column doesn't already exist
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgsheet._columns WHERE sheet_name = $1 AND col_name = $2)",
        &[sheet_name.into(), col_name.into()],
    )
    .map_err(|e| e.to_string())?;

    if exists == Some(true) {
        return Err(format!(
            "Column '{}' already exists in sheet '{}'",
            col_name, sheet_name
        ));
    }

    // Insert column metadata
    Spi::run_with_args(
        r#"INSERT INTO pgsheet._columns
           (sheet_name, col_name, col_type, is_source, is_formula, default_value)
           VALUES ($1, $2, $3, false, false, $4)"#,
        &[
            sheet_name.into(),
            col_name.into(),
            col_type.into(),
            default_value.map(|s| s.to_string()).into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    // Rebuild merged view
    let (schema, table, filter) = sheets::get_sheet_meta(sheet_name)?;
    sheets::rebuild_merged_view(sheet_name, &schema, &table, filter.as_deref())?;

    Ok(true)
}

/// Add a formula column to a sheet.
///
/// Formula columns are not stored in the overlay — they're computed SQL expressions
/// in the merged view.
pub fn add_formula_column_impl(
    sheet_name: &str,
    col_name: &str,
    formula: &str,
) -> Result<bool, String> {
    // Parse and translate the formula
    let parsed = formulas::parse_formula(formula)?;

    if !parsed.sql_translatable {
        return Err(format!(
            "Formula '{}' contains cell references and cannot be used as a column formula. \
             Use column references {{name}} instead.",
            formula
        ));
    }

    let sql_expr = parsed.sql.ok_or("Formula could not be translated to SQL")?;

    // Check if column already exists
    let existing = Spi::get_one_with_args::<bool>(
        "SELECT is_source FROM pgsheet._columns WHERE sheet_name = $1 AND col_name = $2",
        &[sheet_name.into(), col_name.into()],
    );

    match existing {
        Ok(Some(true)) => {
            return Err(format!(
                "Column '{}' is a source column and cannot be converted to a formula",
                col_name
            ));
        }
        Ok(Some(false)) | Ok(None) => {
            // Column exists (overlay or formula) — update it to a formula column
            Spi::run_with_args(
                r#"UPDATE pgsheet._columns
                   SET is_formula = true, formula_expr = $3, formula_sql = $4, col_type = 'numeric'
                   WHERE sheet_name = $1 AND col_name = $2"#,
                &[
                    sheet_name.into(),
                    col_name.into(),
                    formula.into(),
                    sql_expr.into(),
                ],
            )
            .map_err(|e| e.to_string())?;
        }
        Err(pgrx::spi::Error::InvalidPosition) => {
            // Column doesn't exist — insert new
            Spi::run_with_args(
                r#"INSERT INTO pgsheet._columns
                   (sheet_name, col_name, col_type, is_source, is_formula, formula_expr, formula_sql)
                   VALUES ($1, $2, 'numeric', false, true, $3, $4)"#,
                &[
                    sheet_name.into(),
                    col_name.into(),
                    formula.into(),
                    sql_expr.into(),
                ],
            )
            .map_err(|e| e.to_string())?;
        }
        Err(e) => return Err(e.to_string()),
    }

    // Rebuild merged view
    let (schema, table, filter) = sheets::get_sheet_meta(sheet_name)?;
    sheets::rebuild_merged_view(sheet_name, &schema, &table, filter.as_deref())?;

    Ok(true)
}

/// Drop an overlay or formula column from a sheet.
pub fn drop_column_impl(sheet_name: &str, col_name: &str) -> Result<bool, String> {
    // Check column exists and is not a source column
    let is_source = match Spi::get_one_with_args::<bool>(
        "SELECT is_source FROM pgsheet._columns WHERE sheet_name = $1 AND col_name = $2",
        &[sheet_name.into(), col_name.into()],
    ) {
        Ok(val) => val,
        Err(pgrx::spi::Error::InvalidPosition) => None,
        Err(e) => return Err(e.to_string()),
    };

    match is_source {
        None => {
            return Err(format!(
                "Column '{}' not found in sheet '{}'",
                col_name, sheet_name
            ))
        }
        Some(true) => {
            return Err(format!(
                "Column '{}' is a source column and cannot be dropped",
                col_name
            ))
        }
        Some(false) => {}
    }

    // Remove any overlay data for this column
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    Spi::run_with_args(
        &format!("DELETE FROM {} WHERE col_name = $1", overlay_table),
        &[col_name.into()],
    )
    .map_err(|e| e.to_string())?;

    // Remove column metadata
    Spi::run_with_args(
        "DELETE FROM pgsheet._columns WHERE sheet_name = $1 AND col_name = $2",
        &[sheet_name.into(), col_name.into()],
    )
    .map_err(|e| e.to_string())?;

    // Rebuild merged view
    let (schema, table, filter) = sheets::get_sheet_meta(sheet_name)?;
    sheets::rebuild_merged_view(sheet_name, &schema, &table, filter.as_deref())?;

    Ok(true)
}

/// Rename an overlay or formula column.
pub fn rename_column_impl(
    sheet_name: &str,
    old_name: &str,
    new_name: &str,
) -> Result<bool, String> {
    let is_source = Spi::get_one_with_args::<bool>(
        "SELECT is_source FROM pgsheet._columns WHERE sheet_name = $1 AND col_name = $2",
        &[sheet_name.into(), old_name.into()],
    )
    .map_err(|e| e.to_string())?;

    match is_source {
        None => {
            return Err(format!(
                "Column '{}' not found in sheet '{}'",
                old_name, sheet_name
            ))
        }
        Some(true) => {
            return Err(format!(
                "Column '{}' is a source column and cannot be renamed",
                old_name
            ))
        }
        Some(false) => {}
    }

    // Update column metadata
    Spi::run_with_args(
        "UPDATE pgsheet._columns SET col_name = $3 WHERE sheet_name = $1 AND col_name = $2",
        &[sheet_name.into(), old_name.into(), new_name.into()],
    )
    .map_err(|e| e.to_string())?;

    // Update overlay data
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);
    Spi::run_with_args(
        &format!(
            "UPDATE {} SET col_name = $2 WHERE col_name = $1",
            overlay_table
        ),
        &[old_name.into(), new_name.into()],
    )
    .map_err(|e| e.to_string())?;

    // Rebuild merged view
    let (schema, table, filter) = sheets::get_sheet_meta(sheet_name)?;
    sheets::rebuild_merged_view(sheet_name, &schema, &table, filter.as_deref())?;

    Ok(true)
}
