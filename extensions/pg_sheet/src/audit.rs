//! Audit trail for pg_sheet.
//!
//! Every cell change is recorded in the per-sheet audit table.
//! This module provides the query API.

use pgrx::prelude::*;

/// Get the history of a specific cell.
pub fn cell_history_impl(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    limit: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(old_value, Option<String>),
            name!(new_value, Option<String>),
            name!(old_formula, Option<String>),
            name!(new_formula, Option<String>),
            name!(modified_by, Option<String>),
            name!(modified_at, Option<pgrx::datum::TimestampWithTimeZone>),
        ),
    >,
    String,
> {
    let audit_table = format!("pgsheet.\"_audit_{}\"", sheet_name);

    let rows: Vec<(
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<pgrx::datum::TimestampWithTimeZone>,
    )> = Spi::connect(|client| {
        let mut results = Vec::new();
        let query = format!(
            r#"SELECT old_value, new_value, old_formula, new_formula, modified_by, modified_at
               FROM {} WHERE entity_id = $1 AND col_name = $2
               ORDER BY modified_at DESC LIMIT $3"#,
            audit_table
        );
        let table = client.select(
            &query,
            None,
            &[entity_id.into(), col_name.into(), limit.into()],
        )?;
        for row in table {
            results.push((
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .map_err(|e| e.to_string())?;

    Ok(TableIterator::new(rows))
}

/// Get recent changes across the entire sheet.
pub fn sheet_changes_impl(
    sheet_name: &str,
    limit: i32,
) -> Result<
    TableIterator<
        'static,
        (
            name!(entity_id, pgrx::Uuid),
            name!(col_name, String),
            name!(old_value, Option<String>),
            name!(new_value, Option<String>),
            name!(modified_by, Option<String>),
            name!(modified_at, Option<pgrx::datum::TimestampWithTimeZone>),
        ),
    >,
    String,
> {
    let audit_table = format!("pgsheet.\"_audit_{}\"", sheet_name);

    let rows: Vec<(
        pgrx::Uuid,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<pgrx::datum::TimestampWithTimeZone>,
    )> = Spi::connect(|client| {
        let mut results = Vec::new();
        let query = format!(
            r#"SELECT entity_id, col_name, old_value, new_value, modified_by, modified_at
               FROM {} ORDER BY modified_at DESC LIMIT $1"#,
            audit_table
        );
        let table = client.select(&query, None, &[limit.into()])?;
        for row in table {
            results.push((
                row.get(1)?.unwrap(),
                row.get(2)?.unwrap_or_default(),
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .map_err(|e| e.to_string())?;

    Ok(TableIterator::new(rows))
}
