//! Index catalog: the `delta.indexed_tables` + `delta.files` tables and the SPI
//! helpers that read/write them.
//!
//! NOTE on naming: the design doc (`indexing.md`) calls the catalog
//! `delta.tables`, but that name is already taken by the managed-ingest catalog
//! (see `lib.rs` bootstrap). We use `delta.indexed_tables` here; `delta.files`
//! does not collide.

use pgrx::prelude::*;
use serde_json::Value;

pgrx::extension_sql!(
    r#"
-- Catalogued Delta tables (index mode). One row per indexed table.
CREATE TABLE delta.indexed_tables (
    id              BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    location        TEXT NOT NULL,
    current_version BIGINT NOT NULL,
    schema_snapshot JSONB NOT NULL,                 -- column → {type, ordinal}
    partition_keys  TEXT[],
    storage_options JSONB,                          -- per-table credential override
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-file catalog: the pruning index. Rows stay in parquet.
CREATE TABLE delta.files (
    id               BIGSERIAL PRIMARY KEY,
    table_id         BIGINT NOT NULL REFERENCES delta.indexed_tables(id) ON DELETE CASCADE,
    delta_version    BIGINT NOT NULL,
    path             TEXT NOT NULL,                 -- relative to indexed_tables.location
    size_bytes       BIGINT NOT NULL,
    num_records      BIGINT,
    partition_values JSONB,
    column_stats     JSONB,                         -- {"col":{"min":..,"max":..,"null_count":N}}
    has_dv           BOOLEAN NOT NULL DEFAULT false,
    modification_ts  TIMESTAMPTZ,
    UNIQUE (table_id, delta_version, path)
);

CREATE INDEX files_table            ON delta.files (table_id);
CREATE INDEX files_table_version    ON delta.files (table_id, delta_version);
CREATE INDEX files_partition_gin    ON delta.files USING GIN (partition_values);
CREATE INDEX files_column_stats_gin ON delta.files USING GIN (column_stats jsonb_path_ops);
"#,
    name = "index_catalog",
    requires = ["bootstrap_schema"]
);

/// A row from `delta.indexed_tables`.
// Some fields (e.g. `name`) round-trip the full catalog row for completeness
// even though not every caller reads every field.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IndexedTable {
    pub id: i64,
    pub name: String,
    pub location: String,
    pub current_version: i64,
    pub schema_snapshot: Value,
    pub partition_keys: Vec<String>,
    pub storage_options: Option<Value>,
}

/// A surviving file row from `delta.files`, used by the reader.
// `size_bytes`/`has_dv`/`column_stats` are carried for future use (deletion
// vectors, cost refinement) even if the v1 reader doesn't consult them all.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct FileRow {
    pub path: String,
    pub size_bytes: i64,
    pub num_records: Option<i64>,
    pub partition_values: Option<Value>,
    pub column_stats: Option<Value>,
    pub has_dv: bool,
}

/// Insert or update an indexed-table catalog row, returning its id.
#[allow(clippy::too_many_arguments)]
pub fn upsert_indexed_table(
    name: &str,
    location: &str,
    current_version: i64,
    schema_snapshot: &Value,
    partition_keys: &[String],
    storage_options: Option<&Value>,
) -> Result<i64, String> {
    let schema_str = schema_snapshot.to_string();
    let storage_str = storage_options.map(|v| v.to_string());
    let keys: Option<Vec<String>> = if partition_keys.is_empty() {
        None
    } else {
        Some(partition_keys.to_vec())
    };

    Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO delta.indexed_tables
            (name, location, current_version, schema_snapshot, partition_keys, storage_options)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6::jsonb)
        ON CONFLICT (name) DO UPDATE SET
            location = EXCLUDED.location,
            current_version = EXCLUDED.current_version,
            schema_snapshot = EXCLUDED.schema_snapshot,
            partition_keys = EXCLUDED.partition_keys,
            storage_options = EXCLUDED.storage_options,
            indexed_at = now()
        RETURNING id
        "#,
        &[
            name.into(),
            location.into(),
            current_version.into(),
            schema_str.into(),
            keys.into(),
            storage_str.into(),
        ],
    )
    .map_err(|e| format!("upsert indexed_table failed: {}", e))?
    .ok_or_else(|| "no id returned from indexed_tables upsert".to_string())
}

/// Delete all `delta.files` rows for a table (used before re-populating).
pub fn clear_files(table_id: i64) -> Result<(), String> {
    Spi::run_with_args(
        "DELETE FROM delta.files WHERE table_id = $1",
        &[table_id.into()],
    )
    .map_err(|e| format!("clear files failed: {}", e))
}

/// Insert one file row.
#[allow(clippy::too_many_arguments)]
pub fn insert_file(
    table_id: i64,
    delta_version: i64,
    path: &str,
    size_bytes: i64,
    num_records: Option<i64>,
    partition_values: Option<&Value>,
    column_stats: Option<&Value>,
    has_dv: bool,
    modification_ts_unix_ms: i64,
) -> Result<(), String> {
    let pv = partition_values.map(|v| v.to_string());
    let cs = column_stats.map(|v| v.to_string());

    Spi::run_with_args(
        r#"
        INSERT INTO delta.files
            (table_id, delta_version, path, size_bytes, num_records,
             partition_values, column_stats, has_dv, modification_ts)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, to_timestamp($9 / 1000.0))
        ON CONFLICT (table_id, delta_version, path) DO NOTHING
        "#,
        &[
            table_id.into(),
            delta_version.into(),
            path.into(),
            size_bytes.into(),
            num_records.into(),
            pv.into(),
            cs.into(),
            has_dv.into(),
            modification_ts_unix_ms.into(),
        ],
    )
    .map_err(|e| format!("insert file failed: {}", e))
}

/// Look up an indexed table by name.
pub fn get_indexed_table(name: &str) -> Result<Option<IndexedTable>, String> {
    Spi::connect(|client| {
        let mut rows = client
            .select(
                r#"
                SELECT id, name, location, current_version,
                       schema_snapshot::text, partition_keys, storage_options::text
                FROM delta.indexed_tables
                WHERE name = $1
                "#,
                None,
                &[name.into()],
            )
            .map_err(|e| format!("select indexed_table failed: {:?}", e))?;

        let row = match rows.next() {
            Some(r) => r,
            None => return Ok(None),
        };

        let id = row.get::<i64>(1).map_err(stringify)?.ok_or("id null")?;
        let name = row
            .get::<String>(2)
            .map_err(stringify)?
            .ok_or("name null")?;
        let location = row
            .get::<String>(3)
            .map_err(stringify)?
            .ok_or("location null")?;
        let current_version = row.get::<i64>(4).map_err(stringify)?.unwrap_or(0);
        let schema_text = row
            .get::<String>(5)
            .map_err(stringify)?
            .ok_or("schema null")?;
        let partition_keys = row
            .get::<Vec<String>>(6)
            .map_err(stringify)?
            .unwrap_or_default();
        let storage_text = row.get::<String>(7).map_err(stringify)?;

        let schema_snapshot: Value =
            serde_json::from_str(&schema_text).map_err(|e| format!("schema parse: {}", e))?;
        let storage_options = match storage_text {
            Some(s) => Some(serde_json::from_str(&s).map_err(|e| format!("storage parse: {}", e))?),
            None => None,
        };

        Ok(Some(IndexedTable {
            id,
            name,
            location,
            current_version,
            schema_snapshot,
            partition_keys,
            storage_options,
        }))
    })
}

/// Run a tier-1 prune query and return the surviving files. `where_clause` is
/// the SQL fragment from `filter::build_where_clause` (already trusted/escaped).
pub fn select_surviving_files(
    table_id: i64,
    pinned_version: i64,
    where_clause: &str,
) -> Result<Vec<FileRow>, String> {
    let query = format!(
        r#"
        SELECT path, size_bytes, num_records,
               partition_values::text, column_stats::text, has_dv
        FROM delta.files
        WHERE table_id = $1
          AND delta_version <= $2
          AND ({where_clause})
        "#
    );

    Spi::connect(|client| {
        let rows = client
            .select(&query, None, &[table_id.into(), pinned_version.into()])
            .map_err(|e| format!("prune query failed: {:?}", e))?;

        let mut out = Vec::new();
        for row in rows {
            let path = row
                .get::<String>(1)
                .map_err(stringify)?
                .ok_or("path null")?;
            let size_bytes = row.get::<i64>(2).map_err(stringify)?.unwrap_or(0);
            let num_records = row.get::<i64>(3).map_err(stringify)?;
            let pv_text = row.get::<String>(4).map_err(stringify)?;
            let cs_text = row.get::<String>(5).map_err(stringify)?;
            let has_dv = row.get::<bool>(6).map_err(stringify)?.unwrap_or(false);

            let partition_values = parse_opt_json(pv_text)?;
            let column_stats = parse_opt_json(cs_text)?;

            out.push(FileRow {
                path,
                size_bytes,
                num_records,
                partition_values,
                column_stats,
                has_dv,
            });
        }
        Ok(out)
    })
}

/// Delete an indexed table (cascades to delta.files). Returns true if removed.
pub fn delete_indexed_table(name: &str) -> Result<bool, String> {
    let deleted = Spi::get_one_with_args::<i64>(
        "DELETE FROM delta.indexed_tables WHERE name = $1 RETURNING id",
        &[name.into()],
    )
    .map_err(|e| format!("delete indexed_table failed: {}", e))?;
    Ok(deleted.is_some())
}

fn parse_opt_json(text: Option<String>) -> Result<Option<Value>, String> {
    match text {
        Some(s) => Ok(Some(
            serde_json::from_str(&s).map_err(|e| format!("json parse: {}", e))?,
        )),
        None => Ok(None),
    }
}

fn stringify<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}
