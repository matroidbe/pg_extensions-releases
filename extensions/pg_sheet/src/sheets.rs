//! Sheet lifecycle management: create, drop, list.
//!
//! Each sheet creates:
//! - An overlay table: pgsheet._overlay_{name} (entity_id, col values, formulas)
//! - A merged view: pgsheet._view_{name} (source LEFT JOIN overlay)
//! - Metadata in pgsheet._sheets and pgsheet._columns

use pgrx::prelude::*;

/// Create a new spreadsheet overlay on a source table.
///
/// This:
/// 1. Validates the source table exists
/// 2. Records sheet metadata
/// 3. Discovers source columns
/// 4. Creates the overlay table (initially empty — just entity_id + formula storage)
/// 5. Creates the merged view
pub fn create_sheet_impl(
    name: &str,
    source_schema: &str,
    source_table: &str,
    source_filter: Option<&str>,
) -> Result<bool, String> {
    // Check sheet doesn't already exist
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgsheet._sheets WHERE name = $1)",
        &[name.into()],
    )
    .map_err(|e| e.to_string())?;

    if exists == Some(true) {
        return Err(format!("Sheet '{}' already exists", name));
    }

    // Validate source table exists and get its columns
    let col_query = format!(
        r#"SELECT column_name::text, data_type::text, is_nullable::text
           FROM information_schema.columns
           WHERE table_schema = '{}' AND table_name = '{}'
           ORDER BY ordinal_position"#,
        source_schema.replace('\'', "''"),
        source_table.replace('\'', "''")
    );

    let source_columns: Vec<(String, String)> = Spi::connect(|client| {
        let mut cols = Vec::new();
        let table = client.select(&col_query, None, &[])?;
        for row in table {
            let col_name: String = row.get(1)?.unwrap_or_default();
            let data_type: String = row.get(2)?.unwrap_or_default();
            cols.push((col_name, data_type));
        }
        Ok::<_, pgrx::spi::Error>(cols)
    })
    .map_err(|e| e.to_string())?;

    if source_columns.is_empty() {
        return Err(format!(
            "Source table {}.{} not found or has no columns",
            source_schema, source_table
        ));
    }

    // Insert sheet metadata
    Spi::run_with_args(
        r#"INSERT INTO pgsheet._sheets (name, source_schema, source_table, source_filter)
           VALUES ($1, $2, $3, $4)"#,
        &[
            name.into(),
            source_schema.into(),
            source_table.into(),
            source_filter.map(|s| s.to_string()).into(),
        ],
    )
    .map_err(|e| e.to_string())?;

    // Record source columns in _columns table
    for (col_name, data_type) in &source_columns {
        Spi::run_with_args(
            r#"INSERT INTO pgsheet._columns
               (sheet_name, col_name, col_type, is_source, is_formula)
               VALUES ($1, $2, $3, true, false)"#,
            &[name.into(), col_name.into(), data_type.into()],
        )
        .map_err(|e| e.to_string())?;
    }

    // Create overlay table
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", name);
    let overlay_sql = format!(
        r#"CREATE TABLE {} (
            entity_id uuid NOT NULL,
            col_name text NOT NULL,
            value text,
            formula text,
            evaluated text,
            refs text[],
            modified_by text DEFAULT current_setting('app.user_id', true),
            modified_at timestamptz DEFAULT now(),
            PRIMARY KEY (entity_id, col_name)
        )"#,
        overlay_table
    );
    Spi::run(&overlay_sql).map_err(|e| e.to_string())?;

    // Create audit table
    let audit_table = format!("pgsheet.\"_audit_{}\"", name);
    let audit_sql = format!(
        r#"CREATE TABLE {} (
            id bigserial PRIMARY KEY,
            entity_id uuid NOT NULL,
            col_name text NOT NULL,
            old_value text,
            new_value text,
            old_formula text,
            new_formula text,
            modified_by text DEFAULT current_setting('app.user_id', true),
            modified_at timestamptz DEFAULT now()
        )"#,
        audit_table
    );
    Spi::run(&audit_sql).map_err(|e| e.to_string())?;

    // Create snapshot table
    let snap_table = format!("pgsheet.\"_snap_{}\"", name);
    let snap_sql = format!(
        r#"CREATE TABLE {} (
            snapshot_id uuid NOT NULL,
            entity_id uuid NOT NULL,
            col_name text NOT NULL,
            value text,
            formula text,
            PRIMARY KEY (snapshot_id, entity_id, col_name)
        )"#,
        snap_table
    );
    Spi::run(&snap_sql).map_err(|e| e.to_string())?;

    // Create lock table
    let lock_table = format!("pgsheet.\"_lock_{}\"", name);
    let lock_sql = format!(
        r#"CREATE TABLE {} (
            entity_id uuid NOT NULL,
            col_name text NOT NULL,
            locked_by text NOT NULL DEFAULT current_setting('app.user_id', true),
            locked_at timestamptz DEFAULT now(),
            PRIMARY KEY (entity_id, col_name)
        )"#,
        lock_table
    );
    Spi::run(&lock_sql).map_err(|e| e.to_string())?;

    // Grant permissions on per-sheet tables to app_user
    // (finalize GRANT ALL TABLES only covers tables existing at install time)
    for tbl in &[&overlay_table, &audit_table, &snap_table, &lock_table] {
        Spi::run(&format!(
            "GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO app_user",
            tbl
        ))
        .ok();
    }

    // Build and create the merged view
    rebuild_merged_view(name, source_schema, source_table, source_filter)?;

    Ok(true)
}

/// Drop a sheet and all associated objects.
pub fn drop_sheet_impl(name: &str) -> Result<bool, String> {
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgsheet._sheets WHERE name = $1)",
        &[name.into()],
    )
    .map_err(|e| e.to_string())?;

    if exists != Some(true) {
        return Err(format!("Sheet '{}' not found", name));
    }

    // Drop all associated objects
    let view_name = format!("pgsheet.\"_view_{}\"", name);
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", name);
    let audit_table = format!("pgsheet.\"_audit_{}\"", name);
    let snap_table = format!("pgsheet.\"_snap_{}\"", name);
    let lock_table = format!("pgsheet.\"_lock_{}\"", name);

    Spi::run(&format!("DROP VIEW IF EXISTS {} CASCADE", view_name)).ok();
    Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", lock_table)).ok();
    Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", snap_table)).ok();
    Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", audit_table)).ok();
    Spi::run(&format!("DROP TABLE IF EXISTS {} CASCADE", overlay_table)).ok();

    // Remove metadata
    Spi::run_with_args(
        "DELETE FROM pgsheet._columns WHERE sheet_name = $1",
        &[name.into()],
    )
    .map_err(|e| e.to_string())?;

    Spi::run_with_args(
        "DELETE FROM pgsheet._snapshots WHERE sheet_name = $1",
        &[name.into()],
    )
    .map_err(|e| e.to_string())?;

    Spi::run_with_args(
        "DELETE FROM pgsheet._sheets WHERE name = $1",
        &[name.into()],
    )
    .map_err(|e| e.to_string())?;

    Ok(true)
}

/// Rebuild the merged view after column changes.
///
/// The view is: source table LEFT JOIN overlay (pivoted), with formula columns
/// as SQL expressions.
pub fn rebuild_merged_view(
    sheet_name: &str,
    source_schema: &str,
    source_table: &str,
    source_filter: Option<&str>,
) -> Result<(), String> {
    // Get all columns (source + overlay + formula)
    let columns: Vec<(String, String, bool, bool, Option<String>)> = Spi::connect(|client| {
        let mut cols = Vec::new();
        let table = client.select(
            "SELECT col_name, col_type, is_source, is_formula, formula_sql
             FROM pgsheet._columns WHERE sheet_name = $1
             ORDER BY col_name",
            None,
            &[sheet_name.into()],
        )?;
        for row in table {
            let col_name: String = row.get(1)?.unwrap_or_default();
            let col_type: String = row.get(2)?.unwrap_or_default();
            let is_source: bool = row.get(3)?.unwrap_or(false);
            let is_formula: bool = row.get(4)?.unwrap_or(false);
            let formula_sql: Option<String> = row.get(5)?;
            cols.push((col_name, col_type, is_source, is_formula, formula_sql));
        }
        Ok::<_, pgrx::spi::Error>(cols)
    })
    .map_err(|e| e.to_string())?;

    // Build SELECT list
    let mut select_parts = Vec::new();
    let overlay_table = format!("pgsheet.\"_overlay_{}\"", sheet_name);

    for (col_name, _col_type, is_source, is_formula, formula_sql) in &columns {
        // Skip 'id' — already included as s.id
        if col_name == "id" && *is_source {
            continue;
        }
        if *is_formula {
            if let Some(sql) = formula_sql {
                select_parts.push(format!("({}) AS \"{}\"", sql, col_name));
            }
        } else if *is_source {
            // Source columns: COALESCE (evaluated formula → plain value → source)
            select_parts.push(format!(
                "COALESCE(\
                    (SELECT COALESCE(o.evaluated, o.value) FROM {} o WHERE o.entity_id = s.id AND o.col_name = '{}'),\
                    s.\"{}\"::text\
                ) AS \"{}\"",
                overlay_table,
                col_name.replace('\'', "''"),
                col_name,
                col_name,
            ));
        } else {
            // Overlay-only (non-formula) columns: evaluated formula → plain value
            select_parts.push(format!(
                "(SELECT COALESCE(o.evaluated, o.value) FROM {} o WHERE o.entity_id = s.id AND o.col_name = '{}') AS \"{}\"",
                overlay_table,
                col_name.replace('\'', "''"),
                col_name,
            ));
        }
    }

    // Always include the source id
    let select_clause = if select_parts.is_empty() {
        "s.id".to_string()
    } else {
        format!("s.id, {}", select_parts.join(", "))
    };

    let filter_clause = match source_filter {
        Some(f) if !f.is_empty() => format!(" WHERE {}", f),
        _ => String::new(),
    };

    let view_name = format!("pgsheet.\"_view_{}\"", sheet_name);

    // DROP first because CREATE OR REPLACE VIEW cannot add/rename columns
    Spi::run(&format!("DROP VIEW IF EXISTS {} CASCADE", view_name)).ok();

    let view_sql = format!(
        "CREATE VIEW {} AS SELECT {} FROM {}.\"{}\" s{}",
        view_name, select_clause, source_schema, source_table, filter_clause
    );

    Spi::run(&view_sql).map_err(|e| format!("Failed to create merged view: {}", e))?;

    // Re-grant SELECT to app_user after view recreation
    let grant_sql = format!("GRANT SELECT ON {} TO app_user", view_name);
    Spi::run(&grant_sql).ok();

    Ok(())
}

/// Get sheet metadata for view rebuilding.
pub fn get_sheet_meta(name: &str) -> Result<(String, String, Option<String>), String> {
    let result: (String, String, Option<String>) = Spi::connect(|client| {
        let mut table = client.select(
            "SELECT source_schema, source_table, source_filter FROM pgsheet._sheets WHERE name = $1",
            None,
            &[name.into()],
        )?;
        if let Some(row) = table.next() {
            let schema: String = row.get(1)?.unwrap_or_default();
            let tbl: String = row.get(2)?.unwrap_or_default();
            let filter: Option<String> = row.get(3)?;
            return Ok((schema, tbl, filter));
        }
        Err(pgrx::spi::Error::InvalidPosition)
    })
    .map_err(|_| format!("Sheet '{}' not found", name))?;

    Ok(result)
}
