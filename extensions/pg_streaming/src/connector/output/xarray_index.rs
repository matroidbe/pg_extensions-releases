//! `xarray_index` sink — populates `pgx.chunks` from pg_streaming
//! pipeline records.
//!
//! Implements the SYNC `OutputConnector` trait (not async) because it
//! calls SPI to upsert into the catalog. Registered via
//! `pg_streaming::connector::registry::register_sync_sink` at this
//! extension's `_PG_init()` time; referenced from pipeline DSL as
//! `{ "output": { "custom": { "name": "xarray_index", "config": {...} } } }`.
//!
//! ## Expected record shape
//!
//! Records arriving at the sink must carry these fields (set by an
//! upstream `mapping` processor):
//!
//! ```jsonc
//! {
//!   "variable":      "t2m",                              // required
//!   "uri":           "s3://noaa-gfs-bdp-pds/...",        // required
//!   "time_from":     "2024-11-15T00:00:00Z",             // optional
//!   "time_to":       "2024-11-15T01:00:00Z",             // optional
//!   "bbox_wkt":      "POLYGON((...))",                   // optional
//!   "byte_offset":   8192,                               // optional
//!   "byte_length":   5242880,                            // optional
//!   "chunk_key":     "var/0.5.12",                       // optional (Zarr)
//!   "metadata":      { ... }                             // optional
//! }
//! ```
//!
//! The sink's config carries the dataset-level constants (`dataset`,
//! `format`, etc.) so individual records don't need to repeat them.
//!
//! ## Config (DSL)
//!
//! ```jsonc
//! { "xarray_index": {
//!     "dataset":     "gfs",
//!     "format":      "grib2",
//!     "mesh_kind":   "regular_grid",
//!     "mesh_motion": "fixed",
//!     "auto_create": true
//! }}
//! ```

use crate::connector::OutputConnector;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use serde::Deserialize;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Debug, Clone, Deserialize)]
pub struct XarrayIndexSinkConfig {
    pub dataset: String,
    pub format: String,
    /// Default mesh kind to auto-create. Required when `auto_create=true`.
    #[serde(default)]
    pub mesh_kind: Option<String>,
    /// "fixed" (default), "versioned", "deforming", "lagrangian".
    #[serde(default = "default_motion")]
    pub mesh_motion: String,
    /// If true, ensures dataset + mesh + variable exist before the
    /// first chunk upsert.
    #[serde(default = "default_auto_create")]
    pub auto_create: bool,
}

fn default_motion() -> String {
    "fixed".to_string()
}
fn default_auto_create() -> bool {
    true
}

#[derive(Debug)]
pub struct XarrayIndexSink {
    config: XarrayIndexSinkConfig,
    /// One-shot guard so we only run the bootstrap (register_dataset +
    /// register_mesh) once per sink instance.
    bootstrapped: AtomicBool,
}

impl XarrayIndexSink {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: XarrayIndexSinkConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("xarray_index: invalid config: {}", e))?;
        if config.dataset.is_empty() {
            return Err("xarray_index: dataset is required".to_string());
        }
        if config.format.is_empty() {
            return Err("xarray_index: format is required".to_string());
        }
        if config.auto_create && config.mesh_kind.is_none() {
            return Err("xarray_index: mesh_kind is required when auto_create=true".to_string());
        }
        Ok(Self {
            config,
            bootstrapped: AtomicBool::new(false),
        })
    }

    /// Run once per sink: register the dataset + the dataset's mesh.
    /// Idempotent on the pg_xarray side; this guard just avoids
    /// redundant SPI calls.
    fn bootstrap(&self) -> Result<(), String> {
        if self.bootstrapped.load(Ordering::Acquire) {
            return Ok(());
        }
        if !self.config.auto_create {
            self.bootstrapped.store(true, Ordering::Release);
            return Ok(());
        }

        // 1. Dataset
        Spi::run_with_args(
            "SELECT pgx.register_dataset($1, $2, NULL, NULL)",
            &[
                self.config.dataset.as_str().into(),
                self.config.format.as_str().into(),
            ],
        )
        .map_err(|e| format!("xarray_index: register_dataset failed: {}", e))?;

        // 2. Mesh (if a kind was provided — required by auto_create above).
        if let Some(ref kind) = self.config.mesh_kind {
            Spi::run_with_args(
                "SELECT pgx.register_mesh($1, $2, $3, NULL, NULL, NULL, NULL, NULL)",
                &[
                    self.config.dataset.as_str().into(),
                    kind.as_str().into(),
                    self.config.mesh_motion.as_str().into(),
                ],
            )
            .map_err(|e| format!("xarray_index: register_mesh failed: {}", e))?;
        }

        self.bootstrapped.store(true, Ordering::Release);
        Ok(())
    }

    /// Ensure a variable exists for the given record before inserting
    /// the chunk. Idempotent.
    fn ensure_variable(&self, variable: &str) -> Result<(), String> {
        Spi::run_with_args(
            "SELECT pgx.register_variable($1, $2, NULL, NULL, NULL, NULL, NULL)",
            &[self.config.dataset.as_str().into(), variable.into()],
        )
        .map_err(|e| {
            format!(
                "xarray_index: register_variable '{}' failed: {}",
                variable, e
            )
        })?;
        Ok(())
    }

    /// Insert a single chunk by calling pgx.register_chunk.
    fn insert_chunk(&self, record: &Value) -> Result<(), String> {
        let variable = record
            .get("variable")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "xarray_index: record missing 'variable'".to_string())?;
        let uri = record
            .get("uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "xarray_index: record missing 'uri'".to_string())?;

        if self.config.auto_create {
            self.ensure_variable(variable)?;
        }

        // Optional fields
        let time_from = record.get("time_from").and_then(|v| v.as_str());
        let time_to = record.get("time_to").and_then(|v| v.as_str());
        let bbox_wkt = record.get("bbox_wkt").and_then(|v| v.as_str());
        let byte_offset = record.get("byte_offset").and_then(|v| v.as_i64());
        let byte_length = record.get("byte_length").and_then(|v| v.as_i64());
        let chunk_key = record.get("chunk_key").and_then(|v| v.as_str());
        // Z / level extent — populated by xarray_header when `z_axis`
        // is configured on the processor. Becomes pgx.chunks.level_range
        // (NUMRANGE) so SRF queries with level_from/to can prune.
        let level_from = record.get("level_from").and_then(|v| v.as_f64());
        let level_to = record.get("level_to").and_then(|v| v.as_f64());

        // Build the SQL with optional timestamp casts.
        let sql = "
            SELECT pgx.register_chunk(
                $1::text, $2::text, $3::text,
                CASE WHEN $4::text IS NULL THEN NULL ELSE $4::text::timestamptz END,
                CASE WHEN $5::text IS NULL THEN NULL ELSE $5::text::timestamptz END,
                $6::text,
                $7::bigint, $8::bigint, $9::text,
                NULL, NULL,
                $10::float8, $11::float8
            )
        ";

        Spi::run_with_args(
            sql,
            &[
                self.config.dataset.as_str().into(),
                variable.into(),
                uri.into(),
                time_from.into(),
                time_to.into(),
                bbox_wkt.into(),
                byte_offset.into(),
                byte_length.into(),
                chunk_key.into(),
                level_from.into(),
                level_to.into(),
            ],
        )
        .map_err(|e| format!("xarray_index: register_chunk failed: {}", e))?;

        Ok(())
    }
}

impl OutputConnector for XarrayIndexSink {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        self.bootstrap()?;
        for record in records {
            // Skip records that aren't objects — they're noise.
            if !record.is_object() {
                pgrx::warning!("xarray_index: skipping non-object record: {}", record);
                continue;
            }
            self.insert_chunk(record)?;
        }
        Ok(())
    }
}

/// Factory for the registry — called by pipeline compile with the
/// pre-resolved (secrets-substituted) config.
pub fn factory(config: &Value) -> Result<Box<dyn OutputConnector>, String> {
    Ok(Box::new(XarrayIndexSink::from_config(config)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn from_config_minimal_valid() {
        let cfg = json!({
            "dataset": "gfs",
            "format":  "grib2",
            "mesh_kind": "regular_grid"
        });
        let sink = XarrayIndexSink::from_config(&cfg).unwrap();
        assert_eq!(sink.config.dataset, "gfs");
        assert_eq!(sink.config.format, "grib2");
        assert_eq!(sink.config.mesh_motion, "fixed");
        assert!(sink.config.auto_create);
    }

    #[test]
    fn from_config_rejects_missing_dataset() {
        let cfg = json!({"format": "grib2", "mesh_kind": "regular_grid"});
        let err = XarrayIndexSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("invalid config"), "unexpected: {}", err);
    }

    #[test]
    fn from_config_rejects_missing_mesh_kind_when_auto_create() {
        let cfg = json!({"dataset": "gfs", "format": "grib2"});
        let err = XarrayIndexSink::from_config(&cfg).unwrap_err();
        assert!(err.contains("mesh_kind"));
    }

    #[test]
    fn from_config_accepts_no_mesh_kind_when_auto_create_off() {
        let cfg = json!({
            "dataset":     "gfs",
            "format":      "grib2",
            "auto_create": false
        });
        let sink = XarrayIndexSink::from_config(&cfg).unwrap();
        assert!(!sink.config.auto_create);
        assert!(sink.config.mesh_kind.is_none());
    }

    #[test]
    fn from_config_respects_explicit_motion() {
        let cfg = json!({
            "dataset":     "amr",
            "format":      "netcdf",
            "mesh_kind":   "regular_grid",
            "mesh_motion": "versioned"
        });
        let sink = XarrayIndexSink::from_config(&cfg).unwrap();
        assert_eq!(sink.config.mesh_motion, "versioned");
    }
}
