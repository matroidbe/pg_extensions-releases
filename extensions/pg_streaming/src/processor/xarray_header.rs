//! `xarray_header` processor — file → many chunks fan-out.
//!
//! For each input record that carries a store `uri`, this processor
//! reads the format-specific metadata header(s) via OpenDAL and emits
//! one output record per (variable, chunk) tuple discovered inside.
//! The output records are shaped to feed directly into the
//! [`xarray_index`](crate::sink::xarray_index) sink.
//!
//! ## DSL config
//!
//! ```jsonc
//! { "xarray_header": {
//!     "uri_field":   "uri",       // record field carrying the store URI
//!     "format":      "zarr",      // "zarr" supported in this build;
//!                                 // grib / netcdf / hdf5 are follow-ups
//!     "variables":   ["t2m", "u10", "v10"],   // required for zarr
//!     "max_header_bytes": 16384   // safety cap on header reads
//! }}
//! ```
//!
//! ## Output record shape (downstream is `xarray_index`)
//!
//! ```jsonc
//! {
//!   "variable":     "t2m",
//!   "uri":          "fs:///var/data/era5.zarr",
//!   "chunk_key":    "t2m/c/0/0",
//!   "byte_offset":  null,
//!   "byte_length":  null,
//!   "time_from":    null,
//!   "time_to":      null,
//!   "bbox_wkt":     "POLYGON((...))"   // real bbox via pgx_zarr_walker
//! }
//! ```
//!
//! The bbox computation lives in the shared `pgx_zarr_walker` rlib so
//! the pipeline path and the no-pipeline `pgx.register_file` path
//! produce identical catalog rows.
//!
//! pg_streaming's sync `Processor` trait drives this; async I/O for
//! the header reads happens on a lazily-built tokio runtime inside
//! `process()` via `block_on`. One header read per input record; an
//! input record with N variables × M chunks each fans out to N×M
//! output records. The processor is stateless.

use crate::processor::Processor;
use crate::record::{Record, RecordBatch};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Deserialize)]
pub struct XarrayHeaderConfig {
    /// Record field that holds the store URI. Default: "uri".
    #[serde(default = "default_uri_field")]
    pub uri_field: String,
    /// Format: "zarr" supported now; grib/netcdf/hdf5 follow-ups.
    pub format: String,
    /// For "zarr": list of variable paths inside the store to enumerate.
    #[serde(default)]
    pub variables: Vec<String>,
    /// Explicit lat-axis coord group name. NULL → auto-detect from
    /// `dimension_names[rank-2]` or fall back to "latitude".
    #[serde(default)]
    pub lat_axis: Option<String>,
    /// Explicit lon-axis coord group name. NULL → auto-detect from
    /// `dimension_names[rank-1]` or fall back to "longitude".
    #[serde(default)]
    pub lon_axis: Option<String>,
    /// Optional time-axis coord group name. When set, emitted records
    /// carry per-chunk `time_from` / `time_to` decoded from the axis's
    /// CF-style `"units": "<unit> since <date>"` attribute. When NULL,
    /// the time fields stay NULL.
    #[serde(default)]
    pub time_axis: Option<String>,
    /// Optional vertical (Z / level / depth / altitude) axis coord
    /// group name. When set, emitted records carry per-chunk
    /// `level_from` / `level_to` so the catalog's `level_range`
    /// NUMRANGE can prune by Z slabs. When NULL, no Z indexing.
    #[serde(default)]
    pub z_axis: Option<String>,
    /// Safety cap on header read size (kept for forward-compat; the
    /// shared walker does its own bounds).
    #[serde(default = "default_max_header_bytes")]
    #[allow(dead_code)]
    pub max_header_bytes: u64,
    /// Per-fetch timeout in seconds (safety cap).
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,
}

fn default_uri_field() -> String {
    "uri".to_string()
}
fn default_max_header_bytes() -> u64 {
    16 * 1024
}
fn default_timeout_secs() -> u64 {
    30
}

#[derive(Debug)]
pub struct XarrayHeaderProcessor {
    config: XarrayHeaderConfig,
}

impl XarrayHeaderProcessor {
    pub fn from_config(value: &Value) -> Result<Self, String> {
        let config: XarrayHeaderConfig = serde_json::from_value(value.clone())
            .map_err(|e| format!("xarray_header: invalid config: {}", e))?;
        if config.format != "zarr" {
            return Err(format!(
                "xarray_header: format '{}' not yet supported (zarr only in this build)",
                config.format
            ));
        }
        if config.variables.is_empty() {
            return Err(
                "xarray_header: variables list must be non-empty for zarr format".to_string(),
            );
        }
        Ok(Self { config })
    }
}

impl Processor for XarrayHeaderProcessor {
    fn name(&self) -> &str {
        "xarray_header"
    }

    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        let rt = runtime();
        let mut out: RecordBatch = Vec::new();
        for record in batch {
            let uri = match extract_uri(&record, &self.config.uri_field) {
                Some(u) => u,
                None => continue,
            };
            let timeout = Duration::from_secs(self.config.timeout_secs);
            let fanned = rt.block_on(async {
                tokio::time::timeout(timeout, enumerate_zarr_chunks(&uri, &self.config)).await
            });
            match fanned {
                Ok(Ok(records)) => out.extend(records),
                Ok(Err(e)) => {
                    return Err(format!("xarray_header: '{uri}': {e}"));
                }
                Err(_) => {
                    return Err(format!(
                        "xarray_header: '{uri}' header read timed out after {}s",
                        self.config.timeout_secs
                    ));
                }
            }
        }
        Ok(out)
    }
}

/// Factory used by the pg_streaming registry.
pub fn factory(config: &Value) -> Result<Box<dyn Processor>, String> {
    Ok(Box::new(XarrayHeaderProcessor::from_config(config)?))
}

// =============================================================================
// Zarr v3 header walker
// =============================================================================

/// Extract the store URI from a record's named field. Returns None for
/// missing/non-string values so the processor can skip silently.
fn extract_uri(record: &Record, field: &str) -> Option<String> {
    record.get(field).and_then(|v| v.as_str()).map(String::from)
}

/// Read the zarr.json for each configured variable and emit one record
/// per chunk in its chunk grid. Delegates to the shared
/// `pgx_zarr_walker` rlib so the pipeline path and
/// `pgx.register_file` produce identical catalog rows (including the
/// per-chunk `bbox_wkt` derived from coord arrays and `time_from`/`time_to`
/// when a `time_axis` is configured).
async fn enumerate_zarr_chunks(
    store_uri: &str,
    config: &XarrayHeaderConfig,
) -> Result<Vec<Record>, String> {
    let dims = pgx_zarr_walker::DimensionMapping {
        lat_axis: config.lat_axis.clone(),
        lon_axis: config.lon_axis.clone(),
        time_axis: config.time_axis.clone(),
        z_axis: config.z_axis.clone(),
    };
    let walks = pgx_zarr_walker::enumerate_zarr_chunks(store_uri, &config.variables, &dims).await?;
    // Flatten Vec<VariableWalk> → one record per chunk across every
    // variable. The walker now also returns VariableMeta for each
    // variable; threading it into pipeline records is a follow-up
    // (xarray_index sink would also need to learn variable-metadata
    // upserts). For today, drop meta and emit identical chunk shape.
    let mut out = Vec::new();
    for walk in walks {
        for c in walk.chunks {
            let (level_from, level_to) = match c.z_range {
                Some((lo, hi)) => (Value::from(lo), Value::from(hi)),
                None => (Value::Null, Value::Null),
            };
            out.push(json!({
                "variable":     c.variable,
                "uri":          c.uri,
                "chunk_key":    c.chunk_key,
                "byte_offset":  c.byte_offset.map(Value::from).unwrap_or(Value::Null),
                "byte_length":  c.byte_length.map(Value::from).unwrap_or(Value::Null),
                "time_from":    c.time_from.map(|dt| Value::from(dt.to_rfc3339())).unwrap_or(Value::Null),
                "time_to":      c.time_to.map(|dt| Value::from(dt.to_rfc3339())).unwrap_or(Value::Null),
                "level_from":   level_from,
                "level_to":     level_to,
                "bbox_wkt":     c.bbox_wkt.map(Value::from).unwrap_or(Value::Null),
            }));
        }
    }
    Ok(out)
}

/// One-time tokio runtime shared by all xarray_header process() calls.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("pg_streaming_xarray_rt")
            .build()
            .expect("xarray_header: failed to build tokio runtime")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn from_config_minimal_valid() {
        let p = XarrayHeaderProcessor::from_config(&json!({
            "format": "zarr",
            "variables": ["t2m"]
        }))
        .unwrap();
        assert_eq!(p.config.uri_field, "uri");
        assert_eq!(p.config.format, "zarr");
        assert_eq!(p.config.variables, vec!["t2m"]);
        assert_eq!(p.config.max_header_bytes, 16 * 1024);
    }

    #[test]
    fn from_config_rejects_unknown_format() {
        let err =
            XarrayHeaderProcessor::from_config(&json!({"format": "fits", "variables": ["x"]}))
                .unwrap_err();
        assert!(err.contains("not yet supported"));
    }

    #[test]
    fn from_config_rejects_empty_variables() {
        let err = XarrayHeaderProcessor::from_config(&json!({
            "format": "zarr", "variables": []
        }))
        .unwrap_err();
        assert!(err.contains("variables list"));
    }

    #[test]
    fn chunks_per_dim_ceiling() {
        assert_eq!(chunks_per_dim(&[10, 20], &[3, 7]), vec![4, 3]);
        assert_eq!(chunks_per_dim(&[100], &[100]), vec![1]);
        assert_eq!(
            chunks_per_dim(&[365, 721, 1440], &[1, 721, 1440]),
            vec![365, 1, 1]
        );
        assert_eq!(chunks_per_dim(&[10], &[0]), vec![0]); // safety
    }

    #[test]
    fn build_chunk_key_default_v3() {
        let k = build_chunk_key("t2m", &[0, 5, 12], "/", false);
        assert_eq!(k, "t2m/c/0/5/12");
    }

    #[test]
    fn build_chunk_key_v2_dot() {
        let k = build_chunk_key("t2m", &[0, 5, 12], ".", true);
        assert_eq!(k, "t2m/0.5.12");
    }

    #[test]
    fn extract_uri_present() {
        let r = json!({"uri": "fs:///x.zarr", "other": 1});
        assert_eq!(extract_uri(&r, "uri"), Some("fs:///x.zarr".to_string()));
    }

    #[test]
    fn extract_uri_missing() {
        let r = json!({"other": 1});
        assert!(extract_uri(&r, "uri").is_none());
    }

    #[test]
    fn extract_uri_non_string() {
        let r = json!({"uri": 42});
        assert!(extract_uri(&r, "uri").is_none());
    }

    #[test]
    fn build_store_operator_fs() {
        let (_op, path) = build_store_operator("fs:///data/era5.zarr").unwrap();
        assert_eq!(path, "data/era5.zarr");
    }

    #[test]
    fn build_store_operator_rejects_bad_uri() {
        assert!(build_store_operator("not a uri").is_err());
    }

    /// End-to-end: write a tiny Zarr v3 store with one variable to a
    /// tempdir, then walk it via the processor and assert the fan-out
    /// produces the right chunk_keys.
    #[tokio::test]
    async fn enumerate_zarr_chunks_fs_tempdir() {
        let dir = std::env::temp_dir().join(format!(
            "pgsx_header_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let store = dir.join("demo.zarr");
        let var_dir = store.join("t2m");
        std::fs::create_dir_all(&var_dir).unwrap();
        // 2D, shape [10, 20], chunk_shape [5, 10] → 2×2 = 4 chunks.
        let meta = r#"{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [10, 20],
            "data_type":   "float32",
            "chunk_grid":  {"name":"regular","configuration":{"chunk_shape":[5,10]}},
            "chunk_key_encoding": {"name":"default","configuration":{"separator":"/"}},
            "fill_value":  0,
            "codecs":      [{"name":"bytes","configuration":{"endian":"little"}}]
        }"#;
        std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

        let store_uri = format!("fs://{}", store.display());
        let cfg = XarrayHeaderConfig {
            uri_field: "uri".into(),
            format: "zarr".into(),
            variables: vec!["t2m".into()],
            max_header_bytes: 4096,
            timeout_secs: 10,
        };
        let records = enumerate_zarr_chunks(&store_uri, &cfg).await.unwrap();
        assert_eq!(records.len(), 4);
        let keys: Vec<String> = records
            .iter()
            .map(|r| r["chunk_key"].as_str().unwrap().to_string())
            .collect();
        assert!(keys.contains(&"t2m/c/0/0".to_string()));
        assert!(keys.contains(&"t2m/c/0/1".to_string()));
        assert!(keys.contains(&"t2m/c/1/0".to_string()));
        assert!(keys.contains(&"t2m/c/1/1".to_string()));
        for r in &records {
            assert_eq!(r["variable"], "t2m");
            assert_eq!(r["uri"], store_uri);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn enumerate_zarr_chunks_v2_style() {
        let dir = std::env::temp_dir().join(format!(
            "pgsx_header_v2_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let store = dir.join("legacy.zarr");
        let var_dir = store.join("rain");
        std::fs::create_dir_all(&var_dir).unwrap();
        let meta = r#"{
            "shape":      [4, 4],
            "data_type":  "float32",
            "chunk_grid": {"name":"regular","configuration":{"chunk_shape":[2,2]}},
            "chunk_key_encoding": {"name":"v2","configuration":{"separator":"."}}
        }"#;
        std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

        let store_uri = format!("fs://{}", store.display());
        let cfg = XarrayHeaderConfig {
            uri_field: "uri".into(),
            format: "zarr".into(),
            variables: vec!["rain".into()],
            max_header_bytes: 4096,
            timeout_secs: 10,
        };
        let records = enumerate_zarr_chunks(&store_uri, &cfg).await.unwrap();
        assert_eq!(records.len(), 4);
        let keys: Vec<String> = records
            .iter()
            .map(|r| r["chunk_key"].as_str().unwrap().to_string())
            .collect();
        assert!(keys.contains(&"rain/0.0".to_string()));
        assert!(keys.contains(&"rain/1.1".to_string()));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
