//! Delta Lake **index mode** — "query without copying".
//!
//! A third operating mode alongside ingest/export: parse a Delta table's
//! transaction log into a Postgres catalog (`delta.indexed_tables` +
//! `delta.files`) holding per-file stats, then answer selective queries by
//! pruning files at plan time and reading only the survivors from object
//! storage. See `design/pg_delta/indexing.md`.

mod catalog;
pub mod fdw;
mod filter;
mod indexer;
mod reader;
mod stats;

pub use indexer::{drop_index, index_info, index_table, list_indexes, refresh_index};

use serde_json::Value;

/// Orchestrate a fetch: tier-1 prune via the catalog, then read and row-filter
/// the surviving parquet files. Shared by the SRF (`delta.fetch`) and the FDW.
pub fn fetch_rows(
    name: &str,
    filter: &Value,
    columns: Option<&[String]>,
    limit: usize,
) -> Result<Vec<Value>, String> {
    let table = catalog::get_indexed_table(name)?.ok_or_else(|| {
        format!(
            "indexed table '{}' not found; call delta.index_table() first",
            name
        )
    })?;

    let schema = filter::table_schema_from_snapshot(&table.schema_snapshot, &table.partition_keys);
    let where_clause = filter::build_where_clause(filter, &schema)?;
    let files = catalog::select_surviving_files(table.id, table.current_version, &where_clause)?;

    reader::read_files(reader::ReadRequest {
        location: &table.location,
        storage_options: table.storage_options.as_ref(),
        files: &files,
        filter,
        columns,
        limit,
    })
}

/// Estimate rows for the planner: Σ `num_records` over files surviving tier-1.
/// Falls back to a per-file heuristic when stats lack `numRecords`.
pub fn estimate_rows(name: &str, filter: &Value) -> Result<f64, String> {
    let table = match catalog::get_indexed_table(name)? {
        Some(t) => t,
        None => return Ok(0.0),
    };
    let schema = filter::table_schema_from_snapshot(&table.schema_snapshot, &table.partition_keys);
    let where_clause = filter::build_where_clause(filter, &schema)?;
    let files = catalog::select_surviving_files(table.id, table.current_version, &where_clause)?;

    let mut rows = 0.0;
    for f in &files {
        rows += f.num_records.map(|n| n as f64).unwrap_or(10_000.0);
    }
    Ok(rows)
}
