#![allow(clippy::type_complexity)]
//! pg_sheet: Domain-aware spreadsheet overlays for PostgreSQL
//!
//! Creates transparent overlay tables on top of existing entity tables,
//! enabling spreadsheet-like editing with formulas, snapshots, audit trails,
//! and cell locking — without modifying the source data.
//!
//! Key concepts:
//! - **Source table**: the real entity data (read-only from the sheet's perspective)
//! - **Overlay table**: per-cell overrides, formulas, and extra columns
//! - **Merged view**: source LEFT JOIN overlay — what users see
//! - **Formula columns**: SQL expressions computed in the merged view
//! - **Cell formulas**: per-cell Excel-like formulas (=SUM, =IF, etc.)
//! - **Snapshots**: point-in-time captures of overlay state
//! - **Audit**: every cell change is recorded
//! - **Locking**: prevent concurrent cell edits

use pgrx::prelude::*;

mod audit;
mod cells;
mod columns;
pub mod error;
pub mod formulas;
mod locking;
mod sheets;
mod snapshots;
pub mod types;

pub use error::PgSheetError;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// =============================================================================
// Bootstrap SQL — Creates metadata tables
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Schema is created by control file (schema = pgsheet)

-- Sheet metadata
CREATE TABLE IF NOT EXISTS pgsheet._sheets (
    name text PRIMARY KEY,
    source_schema text NOT NULL,
    source_table text NOT NULL,
    source_filter text,
    created_at timestamptz DEFAULT now(),
    created_by text DEFAULT current_setting('app.user_id', true)
);

-- Column metadata (source + overlay + formula columns)
CREATE TABLE IF NOT EXISTS pgsheet._columns (
    sheet_name text NOT NULL REFERENCES pgsheet._sheets(name) ON DELETE CASCADE,
    col_name text NOT NULL,
    col_type text NOT NULL DEFAULT 'text',
    is_source boolean NOT NULL DEFAULT false,
    is_formula boolean NOT NULL DEFAULT false,
    formula_expr text,
    formula_sql text,
    default_value text,
    PRIMARY KEY (sheet_name, col_name)
);

-- Snapshot metadata
CREATE TABLE IF NOT EXISTS pgsheet._snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    sheet_name text NOT NULL REFERENCES pgsheet._sheets(name) ON DELETE CASCADE,
    label text,
    cell_count bigint DEFAULT 0,
    created_at timestamptz DEFAULT now(),
    created_by text DEFAULT current_setting('app.user_id', true)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_sheet
    ON pgsheet._snapshots(sheet_name, created_at DESC);
"#,
    name = "bootstrap",
    bootstrap
);

// =============================================================================
// Finalize SQL — Permissions
// =============================================================================

pgrx::extension_sql!(
    r#"
GRANT USAGE ON SCHEMA pgsheet TO PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA pgsheet TO PUBLIC;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA pgsheet TO PUBLIC;
"#,
    name = "permissions",
    finalize
);

// =============================================================================
// Sheet Lifecycle
// =============================================================================

/// Create a new spreadsheet overlay on a source table.
///
/// Example:
/// ```sql
/// SELECT pgsheet.create_sheet('pipeline', 'sales', 'deal', 'status IN (''prospect'', ''negotiation'')');
/// ```
#[pg_extern]
fn create_sheet(
    name: &str,
    source_schema: &str,
    source_table: &str,
    source_filter: default!(Option<String>, "NULL"),
) -> bool {
    match sheets::create_sheet_impl(name, source_schema, source_table, source_filter.as_deref()) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Drop a sheet and all associated objects (overlay, audit, snapshots, locks).
#[pg_extern]
fn drop_sheet(name: &str) -> bool {
    match sheets::drop_sheet_impl(name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// List all sheets.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_sheets() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(source_schema, String),
        name!(source_table, String),
        name!(source_filter, Option<String>),
        name!(created_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    let rows: Vec<(
        String,
        String,
        String,
        Option<String>,
        Option<pgrx::datum::TimestampWithTimeZone>,
    )> = Spi::connect(|client| {
        let mut results = Vec::new();
        let table = client.select(
            "SELECT name, source_schema, source_table, source_filter, created_at FROM pgsheet._sheets ORDER BY name",
            None,
            &[],
        )?;
        for row in table {
            results.push((
                row.get(1)?.unwrap_or_default(),
                row.get(2)?.unwrap_or_default(),
                row.get(3)?.unwrap_or_default(),
                row.get(4)?,
                row.get(5)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_else(|e| pgrx::error!("Failed to list sheets: {}", e));

    TableIterator::new(rows)
}

// =============================================================================
// Column Management
// =============================================================================

/// Add an overlay column to a sheet.
///
/// Example:
/// ```sql
/// SELECT pgsheet.add_column('pipeline', 'confidence', 'numeric');
/// SELECT pgsheet.add_column('pipeline', 'notes', 'text', 'N/A');
/// ```
#[pg_extern]
fn add_column(
    sheet_name: &str,
    col_name: &str,
    col_type: &str,
    default_value: default!(Option<String>, "NULL"),
) -> bool {
    match columns::add_column_impl(sheet_name, col_name, col_type, default_value.as_deref()) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Add a formula column (computed from other columns via SQL).
///
/// Formula must use column references: ={revenue} * {confidence}
///
/// Example:
/// ```sql
/// SELECT pgsheet.add_formula('pipeline', 'weighted', '={revenue} * {confidence}');
/// SELECT pgsheet.add_formula('pipeline', 'status_label', '=IF({confidence} > 0.7, "high", "low")');
/// ```
#[pg_extern]
fn add_formula(sheet_name: &str, col_name: &str, formula: &str) -> bool {
    match columns::add_formula_column_impl(sheet_name, col_name, formula) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Drop an overlay or formula column.
#[pg_extern]
fn drop_column(sheet_name: &str, col_name: &str) -> bool {
    match columns::drop_column_impl(sheet_name, col_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Rename an overlay or formula column.
#[pg_extern]
fn rename_column(sheet_name: &str, old_name: &str, new_name: &str) -> bool {
    match columns::rename_column_impl(sheet_name, old_name, new_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// List all columns for a sheet (source + overlay + formula).
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_columns(
    sheet_name: &str,
) -> TableIterator<
    'static,
    (
        name!(col_name, String),
        name!(col_type, String),
        name!(is_source, bool),
        name!(is_formula, bool),
        name!(formula_expr, Option<String>),
        name!(default_value, Option<String>),
    ),
> {
    let rows: Vec<(String, String, bool, bool, Option<String>, Option<String>)> =
        Spi::connect(|client| {
            let mut results = Vec::new();
            let table = client.select(
                r#"SELECT col_name, col_type, is_source, is_formula, formula_expr, default_value
                   FROM pgsheet._columns WHERE sheet_name = $1 ORDER BY is_source DESC, col_name"#,
                None,
                &[sheet_name.into()],
            )?;
            for row in table {
                results.push((
                    row.get(1)?.unwrap_or_default(),
                    row.get(2)?.unwrap_or_default(),
                    row.get(3)?.unwrap_or(false),
                    row.get(4)?.unwrap_or(false),
                    row.get(5)?,
                    row.get(6)?,
                ));
            }
            Ok::<_, pgrx::spi::Error>(results)
        })
        .unwrap_or_else(|e| pgrx::error!("Failed to list columns: {}", e));

    TableIterator::new(rows)
}

// =============================================================================
// Cell Operations
// =============================================================================

/// Set a cell value in the overlay.
///
/// Example:
/// ```sql
/// SELECT pgsheet.set_value('pipeline', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'confidence', '0.85');
/// ```
#[pg_extern]
fn set_value(sheet_name: &str, entity_id: pgrx::Uuid, col_name: &str, value: &str) -> bool {
    match cells::set_value_impl(sheet_name, entity_id, col_name, value) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Set a cell formula.
///
/// Returns JSONB with parse info: refs, functions, sql_translatable, sql.
///
/// Example:
/// ```sql
/// SELECT pgsheet.set_formula('pipeline', 'a0eebc99-...', 'weighted', '=B2*C2');
/// SELECT pgsheet.set_formula('pipeline', 'a0eebc99-...', 'score', '={revenue} * {confidence}');
/// ```
#[pg_extern]
fn set_formula(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    formula: &str,
) -> pgrx::JsonB {
    match cells::set_formula_impl(sheet_name, entity_id, col_name, formula) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Clear a cell (remove from overlay, reverting to source value).
#[pg_extern]
fn clear_cell(sheet_name: &str, entity_id: pgrx::Uuid, col_name: &str) -> bool {
    match cells::clear_cell_impl(sheet_name, entity_id, col_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Get a cell's value, formula, and metadata as JSONB.
#[pg_extern]
fn get_cell(sheet_name: &str, entity_id: pgrx::Uuid, col_name: &str) -> pgrx::JsonB {
    match cells::get_cell_impl(sheet_name, entity_id, col_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Bulk set multiple cell values/formulas.
///
/// Input: JSONB array of { "entity_id": "uuid", "col_name": "name", "value": "val" }
/// or { "entity_id": "uuid", "col_name": "name", "formula": "=..." }
///
/// Returns count of cells modified.
#[pg_extern]
fn set_values(sheet_name: &str, cells: pgrx::JsonB) -> i32 {
    match cells::set_values_impl(sheet_name, cells) {
        Ok(count) => count,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Read the merged sheet data (source + overlay + formulas).
///
/// This queries the merged view directly.
#[pg_extern]
fn get_data(sheet_name: &str, limit: default!(i32, 100), offset: default!(i32, 0)) -> pgrx::JsonB {
    let view_name = format!("pgsheet.\"_view_{}\"", sheet_name);
    let query = format!(
        "SELECT row_to_json(v)::jsonb FROM {} v LIMIT {} OFFSET {}",
        view_name, limit, offset
    );

    let rows: Vec<serde_json::Value> = Spi::connect(|client| {
        let mut results = Vec::new();
        let table = client.select(&query, None, &[])?;
        for row in table {
            let json: pgrx::JsonB = row.get(1)?.unwrap();
            results.push(json.0);
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_else(|e| pgrx::error!("Failed to get sheet data: {}", e));

    pgrx::JsonB(serde_json::json!(rows))
}

/// Parse a formula and return metadata without storing it.
///
/// Useful for validation and preview.
#[pg_extern(immutable, parallel_safe)]
fn parse_formula(formula: &str) -> pgrx::JsonB {
    match formulas::parse_formula(formula) {
        Ok(parsed) => pgrx::JsonB(serde_json::json!({
            "valid": true,
            "cell_refs": parsed.cell_refs,
            "col_refs": parsed.col_refs,
            "functions": parsed.functions,
            "sql_translatable": parsed.sql_translatable,
            "sql": parsed.sql,
        })),
        Err(e) => pgrx::JsonB(serde_json::json!({
            "valid": false,
            "error": e,
        })),
    }
}

// =============================================================================
// Snapshots
// =============================================================================

/// Create a snapshot of the current overlay state.
///
/// Returns the snapshot UUID.
#[pg_extern]
fn snapshot(sheet_name: &str, label: default!(Option<String>, "NULL")) -> String {
    match snapshots::create_snapshot_impl(sheet_name, label.as_deref()) {
        Ok(id) => id,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Restore a snapshot, replacing the current overlay state.
#[pg_extern]
fn restore(sheet_name: &str, snapshot_id: pgrx::Uuid) -> bool {
    match snapshots::restore_snapshot_impl(sheet_name, snapshot_id) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Diff a snapshot against the current overlay state.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn diff(
    sheet_name: &str,
    snapshot_id: pgrx::Uuid,
) -> TableIterator<
    'static,
    (
        name!(entity_id, pgrx::Uuid),
        name!(col_name, String),
        name!(change_type, String),
        name!(snapshot_value, Option<String>),
        name!(current_value, Option<String>),
    ),
> {
    match snapshots::diff_snapshot_impl(sheet_name, snapshot_id) {
        Ok(iter) => iter,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// List snapshots for a sheet.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_snapshots(
    sheet_name: &str,
) -> TableIterator<
    'static,
    (
        name!(id, pgrx::Uuid),
        name!(label, Option<String>),
        name!(cell_count, i64),
        name!(created_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(created_by, Option<String>),
    ),
> {
    let rows: Vec<(
        pgrx::Uuid,
        Option<String>,
        i64,
        Option<pgrx::datum::TimestampWithTimeZone>,
        Option<String>,
    )> = Spi::connect(|client| {
        let mut results = Vec::new();
        let table = client.select(
            r#"SELECT id, label, cell_count, created_at, created_by
               FROM pgsheet._snapshots WHERE sheet_name = $1
               ORDER BY created_at DESC"#,
            None,
            &[sheet_name.into()],
        )?;
        for row in table {
            results.push((
                row.get(1)?.unwrap(),
                row.get(2)?,
                row.get(3)?.unwrap_or(0),
                row.get(4)?,
                row.get(5)?,
            ));
        }
        Ok::<_, pgrx::spi::Error>(results)
    })
    .unwrap_or_else(|e| pgrx::error!("Failed to list snapshots: {}", e));

    TableIterator::new(rows)
}

// =============================================================================
// Audit
// =============================================================================

/// Get the history of changes for a specific cell.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn cell_history(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    limit: default!(i32, 20),
) -> TableIterator<
    'static,
    (
        name!(old_value, Option<String>),
        name!(new_value, Option<String>),
        name!(old_formula, Option<String>),
        name!(new_formula, Option<String>),
        name!(modified_by, Option<String>),
        name!(modified_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    match audit::cell_history_impl(sheet_name, entity_id, col_name, limit) {
        Ok(iter) => iter,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Get recent changes across the entire sheet.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn sheet_changes(
    sheet_name: &str,
    limit: default!(i32, 50),
) -> TableIterator<
    'static,
    (
        name!(entity_id, pgrx::Uuid),
        name!(col_name, String),
        name!(old_value, Option<String>),
        name!(new_value, Option<String>),
        name!(modified_by, Option<String>),
        name!(modified_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    match audit::sheet_changes_impl(sheet_name, limit) {
        Ok(iter) => iter,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Cell Locking
// =============================================================================

/// Lock a cell for the current user.
#[pg_extern]
fn lock_cell(sheet_name: &str, entity_id: pgrx::Uuid, col_name: &str) -> bool {
    match locking::lock_cell_impl(sheet_name, entity_id, col_name) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Unlock a cell. Set force=true to unlock even if locked by another user.
#[pg_extern]
fn unlock_cell(
    sheet_name: &str,
    entity_id: pgrx::Uuid,
    col_name: &str,
    force: default!(bool, false),
) -> bool {
    match locking::unlock_cell_impl(sheet_name, entity_id, col_name, force) {
        Ok(result) => result,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// List all locked cells in a sheet.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn locked_cells(
    sheet_name: &str,
) -> TableIterator<
    'static,
    (
        name!(entity_id, pgrx::Uuid),
        name!(col_name, String),
        name!(locked_by, String),
        name!(locked_at, Option<pgrx::datum::TimestampWithTimeZone>),
    ),
> {
    match locking::locked_cells_impl(sheet_name) {
        Ok(iter) => iter,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Expire locks older than a given interval.
///
/// Example:
/// ```sql
/// SELECT pgsheet.expire_locks('pipeline', '30 minutes');
/// ```
#[pg_extern]
fn expire_locks(sheet_name: &str, older_than: &str) -> i64 {
    match locking::expire_locks_impl(sheet_name, older_than) {
        Ok(count) => count,
        Err(e) => pgrx::error!("{}", e),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_formula_parse() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgsheet.parse_formula('={revenue} * {confidence}')",
        );
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        assert_eq!(json.0["valid"], true);
        assert_eq!(json.0["sql_translatable"], true);
    }

    #[pg_test]
    fn test_formula_parse_cell_refs() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgsheet.parse_formula('=SUM(A1:A10)')");
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        assert_eq!(json.0["valid"], true);
        assert_eq!(json.0["sql_translatable"], false);
    }

    #[pg_test]
    fn test_formula_parse_if() {
        let result = Spi::get_one::<pgrx::JsonB>(
            "SELECT pgsheet.parse_formula('=IF({score} > 0.5, {value}, 0)')",
        );
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        assert_eq!(json.0["valid"], true);
        assert_eq!(json.0["sql_translatable"], true);
        assert!(json.0["sql"].as_str().unwrap().contains("CASE WHEN"));
    }

    #[pg_test]
    fn test_formula_parse_invalid() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT pgsheet.parse_formula('no equals sign')");
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        assert_eq!(json.0["valid"], false);
    }

    #[pg_test]
    fn test_create_and_drop_sheet() {
        // Create a test table
        Spi::run("CREATE SCHEMA IF NOT EXISTS test_schema").ok();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS test_schema.deals (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                name text NOT NULL,
                revenue numeric DEFAULT 0
            )",
        )
        .ok();

        // Create sheet
        let result = Spi::get_one::<bool>(
            "SELECT pgsheet.create_sheet('test_deals', 'test_schema', 'deals')",
        );
        assert_eq!(result.unwrap(), Some(true));

        // Verify it shows in list
        let count =
            Spi::get_one::<i64>("SELECT COUNT(*) FROM pgsheet._sheets WHERE name = 'test_deals'");
        assert_eq!(count.unwrap(), Some(1));

        // Drop sheet
        let result = Spi::get_one::<bool>("SELECT pgsheet.drop_sheet('test_deals')");
        assert_eq!(result.unwrap(), Some(true));

        // Cleanup
        Spi::run("DROP TABLE IF EXISTS test_schema.deals CASCADE").ok();
        Spi::run("DROP SCHEMA IF EXISTS test_schema CASCADE").ok();
    }
}

/// Required by `cargo pgrx test`.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
