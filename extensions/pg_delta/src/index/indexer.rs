//! Indexing: parse a Delta table's transaction log into the
//! `delta.indexed_tables` and `delta.files` catalog. No parquet bytes are
//! copied — only `_delta_log` metadata (`add` actions and their `stats`).

use crate::index::{catalog, stats};
use crate::storage::{merge_storage_options, register_storage_handlers};
use deltalake::kernel::{DataType as DeltaDataType, StructType};
use pgrx::prelude::*;
use serde_json::{json, Map, Value};

/// Index (or re-index) a Delta table, returning the number of files catalogued.
pub fn index_table(name: &str, location: &str, storage_options: Option<pgrx::JsonB>) -> i64 {
    let override_val = storage_options.map(|j| j.0);
    match do_index(name, location, override_val.as_ref()) {
        Ok(count) => {
            log!(
                "pg_delta: indexed {} files for '{}' ({})",
                count,
                name,
                location
            );
            count
        }
        Err(e) => pgrx::error!("index_table failed: {}", e),
    }
}

/// Re-read the Delta log for an already-indexed table and replace its file
/// catalog at the current version.
pub fn refresh_index(name: &str) -> i64 {
    let table = match catalog::get_indexed_table(name) {
        Ok(Some(t)) => t,
        Ok(None) => {
            pgrx::error!(
                "indexed table '{}' not found; call delta.index_table() first",
                name
            )
        }
        Err(e) => pgrx::error!("refresh_index failed: {}", e),
    };

    match do_index(name, &table.location, table.storage_options.as_ref()) {
        Ok(count) => {
            log!("pg_delta: refreshed index '{}' ({} files)", name, count);
            count
        }
        Err(e) => pgrx::error!("refresh_index failed: {}", e),
    }
}

/// Drop an index registration (cascades to `delta.files`).
pub fn drop_index(name: &str) -> bool {
    match catalog::delete_indexed_table(name) {
        Ok(true) => {
            log!("pg_delta: dropped index '{}'", name);
            true
        }
        Ok(false) => {
            pgrx::warning!("indexed table '{}' not found", name);
            false
        }
        Err(e) => pgrx::error!("drop_index failed: {}", e),
    }
}

/// List all indexed tables with file counts.
pub fn list_indexes() -> pgrx::JsonB {
    let result = Spi::connect(|client| {
        let rows = client
            .select(
                r#"
                SELECT t.name, t.location, t.current_version, t.partition_keys,
                       count(f.id) AS file_count, t.indexed_at::text
                FROM delta.indexed_tables t
                LEFT JOIN delta.files f ON f.table_id = t.id
                GROUP BY t.id
                ORDER BY t.name
                "#,
                None,
                &[],
            )
            .map_err(|e| format!("list query failed: {:?}", e))?;

        let mut out = Vec::new();
        for row in rows {
            let name: String = row.get::<String>(1).map_err(se)?.unwrap_or_default();
            let location: String = row.get::<String>(2).map_err(se)?.unwrap_or_default();
            let version: i64 = row.get::<i64>(3).map_err(se)?.unwrap_or(0);
            let keys: Option<Vec<String>> = row.get::<Vec<String>>(4).map_err(se)?;
            let file_count: i64 = row.get::<i64>(5).map_err(se)?.unwrap_or(0);
            out.push(json!({
                "name": name,
                "location": location,
                "current_version": version,
                "partition_keys": keys,
                "file_count": file_count,
            }));
        }
        Ok::<Vec<Value>, String>(out)
    });

    match result {
        Ok(list) => pgrx::JsonB(json!({ "indexes": list })),
        Err(e) => pgrx::error!("list_indexes failed: {}", e),
    }
}

/// Detailed info for one indexed table.
pub fn index_info(name: &str) -> pgrx::JsonB {
    let result: Result<Option<Value>, String> = Spi::connect(|client| {
        let mut rows = client
            .select(
                r#"
                SELECT t.name, t.location, t.current_version, t.partition_keys,
                       count(f.id) AS file_count,
                       coalesce(sum(f.size_bytes), 0)::bigint AS total_bytes,
                       coalesce(sum(f.num_records), 0)::bigint AS est_rows,
                       count(*) FILTER (WHERE f.has_dv) AS dv_files
                FROM delta.indexed_tables t
                LEFT JOIN delta.files f ON f.table_id = t.id
                WHERE t.name = $1
                GROUP BY t.id
                "#,
                None,
                &[name.into()],
            )
            .map_err(|e| format!("info query failed: {:?}", e))?;

        let row = match rows.next() {
            Some(r) => r,
            None => return Ok(None),
        };

        let location: String = row.get::<String>(2).map_err(se)?.unwrap_or_default();
        let version: i64 = row.get::<i64>(3).map_err(se)?.unwrap_or(0);
        let keys: Option<Vec<String>> = row.get::<Vec<String>>(4).map_err(se)?;
        let file_count: i64 = row.get::<i64>(5).map_err(se)?.unwrap_or(0);
        let total_bytes: i64 = row.get::<i64>(6).map_err(se)?.unwrap_or(0);
        let est_rows: i64 = row.get::<i64>(7).map_err(se)?.unwrap_or(0);
        let dv_files: i64 = row.get::<i64>(8).map_err(se)?.unwrap_or(0);

        Ok(Some(json!({
            "name": name,
            "location": location,
            "current_version": version,
            "partition_keys": keys,
            "file_count": file_count,
            "total_bytes": total_bytes,
            "estimated_rows": est_rows,
            "files_with_deletion_vectors": dv_files,
        })))
    });

    match result {
        Ok(Some(info)) => pgrx::JsonB(info),
        Ok(None) => pgrx::error!("indexed table '{}' not found", name),
        Err(e) => pgrx::error!("index_info failed: {}", e),
    }
}

// =============================================================================
// Core
// =============================================================================

fn do_index(name: &str, location: &str, storage_override: Option<&Value>) -> Result<i64, String> {
    validate_uri(location)?;
    register_storage_handlers(location);
    let storage_options = merge_storage_options(location, storage_override);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {}", e))?;

    // Read everything from the Delta log inside the async block; return owned
    // data so SPI runs after the runtime is dropped (no SPI inside async).
    let (version, adds, schema_snapshot, partition_keys) = runtime.block_on(async {
        let table = deltalake::open_table_with_storage_options(location, storage_options)
            .await
            .map_err(|e| format!("failed to open Delta table: {}", e))?;

        let version = table.version();
        let state = table
            .snapshot()
            .map_err(|e| format!("failed to get snapshot: {}", e))?;
        let adds: Vec<deltalake::kernel::Add> = state
            .file_actions()
            .map_err(|e| format!("failed to read file actions: {}", e))?;
        let schema = table
            .get_schema()
            .map_err(|e| format!("failed to read schema: {}", e))?;
        let schema_snapshot = build_schema_snapshot(schema);
        let partition_keys = table
            .metadata()
            .map_err(|e| format!("failed to read metadata: {}", e))?
            .partition_columns
            .clone();

        Ok::<_, String>((version, adds, schema_snapshot, partition_keys))
    })?;

    // Persist the catalog (SPI).
    let table_id = catalog::upsert_indexed_table(
        name,
        location,
        version,
        &schema_snapshot,
        &partition_keys,
        storage_override,
    )?;
    catalog::clear_files(table_id)?;

    let mut count = 0i64;
    for add in &adds {
        let parsed = stats::parse_add_stats(add.stats.as_deref());
        let partition_values = stats::partition_values_to_json(&add.partition_values);
        catalog::insert_file(
            table_id,
            version,
            &add.path,
            add.size,
            parsed.num_records,
            partition_values.as_ref(),
            parsed.column_stats.as_ref(),
            add.deletion_vector.is_some(),
            add.modification_time,
        )?;
        count += 1;
    }

    Ok(count)
}

fn validate_uri(uri: &str) -> Result<(), String> {
    let u = uri.to_lowercase();
    if u.starts_with("s3://")
        || u.starts_with("az://")
        || u.starts_with("azure://")
        || u.starts_with("gs://")
        || u.starts_with("file://")
    {
        Ok(())
    } else {
        Err(format!(
            "location must start with s3://, az://, azure://, gs://, or file://, got '{}'",
            uri
        ))
    }
}

/// Build the `schema_snapshot` JSON: `{"col": {"type", "nullable", "ordinal"}}`.
fn build_schema_snapshot(schema: &StructType) -> Value {
    let mut obj = Map::new();
    for (i, field) in schema.fields().enumerate() {
        obj.insert(
            field.name().to_string(),
            json!({
                "type": delta_type_name(field.data_type()),
                "nullable": field.is_nullable(),
                "ordinal": i,
            }),
        );
    }
    Value::Object(obj)
}

/// Canonical Delta type name for a column (e.g. `string`, `long`,
/// `decimal(10,2)`). Nested types fall back to `string` (not tier-1 filterable).
fn delta_type_name(dt: &DeltaDataType) -> String {
    match dt {
        DeltaDataType::Primitive(p) => p.to_string(),
        _ => "string".to_string(),
    }
}

fn se<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}
