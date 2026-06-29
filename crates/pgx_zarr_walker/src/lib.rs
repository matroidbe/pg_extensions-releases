//! Zarr v3 header walker — self-contained, no pgrx.
//!
//! Opens a Zarr v3 store via OpenDAL, reads `zarr.json` for each
//! requested variable, walks the chunk grid, and for each chunk
//! computes a `chunk_key` + spatial `bbox_wkt` (and optionally
//! byte ranges).
//!
//! Used by:
//!  * `pg_xarray::header::zarr` (powers `pgx.register_file`)
//!  * `pg_streaming`'s `xarray_header` processor (powers the
//!    pipeline path)
//!
//! Shared as a plain rlib so both extensions can statically link
//! without colliding on pgrx's `_PG_init`/`Pg_magic_func` symbols.

#![allow(clippy::too_many_arguments)]

mod meta;
pub use meta::{CfPacking, VariableMeta, VariableWalk};

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use opendal::{Operator, Scheme};
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use url::Url;

// =============================================================================
// Public types
// =============================================================================

/// Explicit dimension → coord-axis mapping. Each field is the *axis
/// name* — a top-level Zarr group at the store root (e.g. `"valid_time"`,
/// `"latitude"`, `"lat"`). Leave as `None` to fall back to convention.
///
/// Defaults when a field is `None`:
///   * `lat_axis` → axis named at `dimension_names[rank - 2]`, or the
///     literal `"latitude"` if dim names are missing.
///   * `lon_axis` → axis named at `dimension_names[rank - 1]`, or
///     `"longitude"`.
///   * `time_axis` → `None` (no time indexing is performed; chunks
///     have NULL `time_from` / `time_to` in the catalog).
///   * `z_axis` → `None` (no vertical / level indexing; chunks have
///     NULL `z_range` in the catalog).
#[derive(Debug, Clone, Default)]
pub struct DimensionMapping {
    pub lat_axis: Option<String>,
    pub lon_axis: Option<String>,
    pub time_axis: Option<String>,
    /// The vertical / depth / altitude / model-Z axis name. When set,
    /// the walker reads this 1-D coord array and emits a `z_range` on
    /// each chunk so the catalog's `level_range` NUMRANGE can prune.
    /// `"level"` / `"altitude"` / `"depth"` / `"z"` / `"plev"` etc.
    pub z_axis: Option<String>,
}

/// One chunk's worth of catalog-ready metadata.
#[derive(Debug, Clone)]
pub struct ChunkRecord {
    pub variable: String,
    pub uri: String,
    pub chunk_key: String,
    pub bbox_wkt: Option<String>,
    /// Populated when a `time_axis` is configured and the axis carries
    /// a CF-style `"units": "<unit> since <date>"` attribute.
    pub time_from: Option<DateTime<Utc>>,
    pub time_to: Option<DateTime<Utc>>,
    /// (min, max) of the Z coord values inside this chunk's z slice.
    /// Populated only when `dims.z_axis` is set. Inclusive on both
    /// ends — matches the `'[]'` bound the catalog uses for time.
    pub z_range: Option<(f64, f64)>,
    /// Zarr v3 stores one chunk per file at a fixed path, so byte_offset
    /// is 0 and byte_length is the file size. Left `None` here — the
    /// Zarr reader reads the whole chunk file anyway.
    pub byte_offset: Option<i64>,
    pub byte_length: Option<i64>,
}

// =============================================================================
// Public API
// =============================================================================

/// Walk every chunk of every requested variable in a Zarr v3 store at
/// `uri`. Returns one [`VariableWalk`] per requested variable — its
/// parsed metadata (`units`, `standard_name`, CF packing, `dtype`,
/// dim names) PLUS the list of per-chunk records with real `bbox_wkt`
/// when the resolved lat/lon axes map to readable 1-D coord arrays
/// and `time_from`/`time_to` when `dims.time_axis` is set.
pub async fn enumerate_zarr_chunks(
    uri: &str,
    variables: &[String],
    dims: &DimensionMapping,
) -> Result<Vec<VariableWalk>, String> {
    let (op, store_path) = build_store_operator(uri)?;
    let mut walks: Vec<VariableWalk> = Vec::with_capacity(variables.len());
    for var in variables {
        let var_path = if store_path.is_empty() {
            var.clone()
        } else {
            format!("{}/{}", store_path.trim_end_matches('/'), var)
        };
        let meta = read_array_meta(&op, &var_path).await?;
        let mut variable_meta = VariableMeta::from_attributes(&meta.attributes);
        if !meta.data_type.is_empty() {
            variable_meta.dtype = Some(meta.data_type.clone());
        }
        variable_meta.dim_order = meta.dimension_names.clone();
        let rank = meta.shape.len();
        if rank < 2 {
            return Err(format!(
                "pgx_zarr_walker: variable '{}' has rank {} (need >= 2)",
                var, rank
            ));
        }
        let chunk_shape = &meta.chunk_grid.configuration.chunk_shape;
        if chunk_shape.len() != rank {
            return Err(format!(
                "pgx_zarr_walker: variable '{}' chunk_shape rank {} != shape rank {}",
                var,
                chunk_shape.len(),
                rank
            ));
        }

        // Resolve dim indices + axis-group names. lat/lon default to
        // the last two dims; time + z have no default.
        let lat_dim = resolve_dim_idx(&meta.dimension_names, dims.lat_axis.as_deref(), rank - 2)?;
        let lon_dim = resolve_dim_idx(&meta.dimension_names, dims.lon_axis.as_deref(), rank - 1)?;
        let time_dim = match &dims.time_axis {
            Some(name) => Some(require_dim_idx(&meta.dimension_names, name)?),
            None => None,
        };
        let z_dim = match &dims.z_axis {
            Some(name) => Some(require_dim_idx(&meta.dimension_names, name)?),
            None => None,
        };
        let lat_axis_name = dims
            .lat_axis
            .clone()
            .unwrap_or_else(|| guess_axis_name(&meta.dimension_names, lat_dim, "latitude"));
        let lon_axis_name = dims
            .lon_axis
            .clone()
            .unwrap_or_else(|| guess_axis_name(&meta.dimension_names, lon_dim, "longitude"));

        let coords_root = store_path.trim_end_matches('/');
        let lats = read_full_axis(&op, coords_root, &lat_axis_name, meta.shape[lat_dim]).await;
        let lons = read_full_axis(&op, coords_root, &lon_axis_name, meta.shape[lon_dim]).await;

        // Time axis values + units (CF-style: "<unit> since <date>").
        let (time_values, time_ref) = if let Some(td) = time_dim {
            let name = dims.time_axis.as_ref().unwrap();
            let (vs, units) =
                read_full_axis_with_units(&op, coords_root, name, meta.shape[td]).await;
            let parsed = units.and_then(|u| parse_cf_units(&u));
            (vs, parsed)
        } else {
            (Vec::new(), None)
        };

        // Z axis values — read in the variable's own units. Most CF
        // stores carry these as physical floats already; the catalog
        // stores whatever the file declared (no unit conversion).
        let z_values: Vec<f64> = if let Some(zd) = z_dim {
            let name = dims.z_axis.as_ref().unwrap();
            read_full_axis(&op, coords_root, name, meta.shape[zd]).await
        } else {
            Vec::new()
        };

        // Row-major iteration through the chunk grid.
        let chunk_counts = chunks_per_dim(&meta.shape, chunk_shape);
        let mut indices = vec![0u64; chunk_counts.len()];
        let mut chunks: Vec<ChunkRecord> = Vec::new();
        loop {
            let chunk_key = chunk_file_path(var, &indices, &meta.chunk_key_encoding);
            let bbox_wkt = compute_bbox(
                &lats,
                &lons,
                indices[lat_dim],
                indices[lon_dim],
                chunk_shape[lat_dim],
                chunk_shape[lon_dim],
                meta.shape[lat_dim],
                meta.shape[lon_dim],
            );
            let (time_from, time_to) = match (time_dim, time_ref.as_ref()) {
                (Some(td), Some(tref)) => compute_time_range(
                    &time_values,
                    indices[td],
                    chunk_shape[td],
                    meta.shape[td],
                    tref,
                ),
                _ => (None, None),
            };
            let z_range = z_dim.and_then(|zd| {
                compute_numeric_range(&z_values, indices[zd], chunk_shape[zd], meta.shape[zd])
            });
            chunks.push(ChunkRecord {
                variable: var.clone(),
                uri: uri.to_string(),
                chunk_key,
                bbox_wkt,
                time_from,
                time_to,
                z_range,
                byte_offset: None,
                byte_length: None,
            });
            if !increment_indices(&mut indices, &chunk_counts) {
                break;
            }
        }
        walks.push(VariableWalk {
            name: var.clone(),
            meta: variable_meta,
            chunks,
        });
    }
    Ok(walks)
}

/// A variable discovered inside a Zarr store by `list_store_variables`:
/// just the group name + its shape + dim names. The caller can filter
/// to "data variables" (rank >= 2) or use the full list (rank 1 = coord
/// axes).
#[derive(Debug, Clone)]
pub struct StoreVariable {
    pub name: String,
    pub shape: Vec<u64>,
    pub dimension_names: Vec<Option<String>>,
    pub data_type: String,
}

impl StoreVariable {
    /// Heuristic: a "data variable" has rank >= 2. 1-D arrays at the
    /// store root are coord axes (lat / lon / level / time) that
    /// support the data variables.
    pub fn is_data_variable(&self) -> bool {
        self.shape.len() >= 2
    }
}

/// Discover every Zarr array directly under the store root —
/// directories at depth 1 that contain a `zarr.json`. Used by
/// `pgx.list_zarr_variables` / `pgx.register_zarr_store` for the
/// "register every variable in this store" UX so users don't have to
/// call `register_file` per variable.
///
/// Returns one [`StoreVariable`] per array, sorted by name. Whether
/// it's a data variable or a coord axis is left to the caller — use
/// `is_data_variable()` to filter.
pub async fn list_store_variables(uri: &str) -> Result<Vec<StoreVariable>, String> {
    let (op, store_path) = build_store_operator(uri)?;
    let store_root = store_path.trim_end_matches('/');
    let list_root = if store_root.is_empty() {
        String::new()
    } else {
        format!("{}/", store_root)
    };
    // op.list returns Vec<Entry> directly — no Stream poll needed for
    // single-level directory listings.
    let entries = op
        .list(&list_root)
        .await
        .map_err(|e| format!("pgx_zarr_walker: list '{list_root}': {e}"))?;
    let mut names: Vec<String> = Vec::new();
    for entry in entries {
        let path = entry.path();
        if !path.ends_with('/') {
            continue; // not a directory — Zarr arrays are dirs
        }
        let name = path
            .trim_end_matches('/')
            .trim_start_matches(store_root)
            .trim_start_matches('/');
        if name.is_empty() || name.contains('/') {
            continue;
        }
        names.push(name.to_string());
    }
    names.sort();
    names.dedup();

    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let var_path = if store_root.is_empty() {
            name.clone()
        } else {
            format!("{}/{}", store_root, name)
        };
        // Skip directories without a zarr.json — they're not arrays.
        let meta = match read_array_meta(&op, &var_path).await {
            Ok(m) => m,
            Err(_) => continue,
        };
        out.push(StoreVariable {
            name,
            shape: meta.shape,
            dimension_names: meta.dimension_names,
            data_type: meta.data_type,
        });
    }
    Ok(out)
}

/// Build an OpenDAL operator + the variable-group path under it.
/// `fs://`, `file://`, `s3://`, `gs://`, `azblob://`, `http(s)://` wired up.
pub fn build_store_operator(uri: &str) -> Result<(Operator, String), String> {
    let parsed =
        Url::parse(uri).map_err(|e| format!("pgx_zarr_walker: invalid URI '{uri}': {e}"))?;
    let scheme_str = parsed.scheme();
    let scheme = Scheme::from_str(scheme_str)
        .map_err(|e| format!("pgx_zarr_walker: unsupported scheme '{scheme_str}': {e}"))?;
    let mut cfg: HashMap<String, String> = HashMap::new();
    let store_path: String = match scheme_str {
        "fs" | "file" => {
            cfg.insert("root".into(), "/".into());
            parsed.path().trim_start_matches('/').to_string()
        }
        "s3" | "gs" | "azblob" => {
            let bucket = parsed
                .host_str()
                .ok_or_else(|| format!("pgx_zarr_walker: '{uri}' missing bucket"))?
                .to_string();
            cfg.insert("bucket".into(), bucket);
            cfg.insert("anonymous".into(), "true".into());
            parsed.path().trim_start_matches('/').to_string()
        }
        "http" | "https" => {
            let host = parsed
                .host_str()
                .ok_or_else(|| format!("pgx_zarr_walker: '{uri}' missing host"))?;
            cfg.insert("endpoint".into(), format!("{scheme_str}://{host}"));
            parsed.path().trim_start_matches('/').to_string()
        }
        other => return Err(format!("pgx_zarr_walker: scheme '{other}' not wired up")),
    };
    let op = Operator::via_iter(scheme, cfg)
        .map_err(|e| format!("pgx_zarr_walker: operator build: {e}"))?;
    Ok((op, store_path))
}

// =============================================================================
// Internals
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
struct ZarrArrayMeta {
    pub shape: Vec<u64>,
    #[serde(default)]
    pub data_type: String,
    pub chunk_grid: ChunkGrid,
    #[serde(default = "default_chunk_key_encoding")]
    pub chunk_key_encoding: ChunkKeyEncoding,
    #[serde(default)]
    pub codecs: Vec<serde_json::Value>,
    #[serde(default)]
    pub dimension_names: Vec<Option<String>>,
    /// Zarr v3 attribute bag — CF stores `"units"`, `"standard_name"`,
    /// etc. here. We only inspect `"units"` for time-axis decoding.
    #[serde(default)]
    pub attributes: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct ChunkGrid {
    pub configuration: ChunkGridConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct ChunkGridConfig {
    pub chunk_shape: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct ChunkKeyEncoding {
    #[serde(default = "default_encoding_name")]
    pub name: String,
    #[serde(default)]
    pub configuration: ChunkKeyEncodingConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ChunkKeyEncodingConfig {
    #[serde(default = "default_separator")]
    pub separator: String,
}

fn default_separator() -> String {
    "/".to_string()
}
fn default_encoding_name() -> String {
    "default".to_string()
}
fn default_chunk_key_encoding() -> ChunkKeyEncoding {
    ChunkKeyEncoding {
        name: default_encoding_name(),
        configuration: ChunkKeyEncodingConfig {
            separator: default_separator(),
        },
    }
}

async fn read_array_meta(op: &Operator, var_path: &str) -> Result<ZarrArrayMeta, String> {
    let meta_path = format!("{}/zarr.json", var_path.trim_end_matches('/'));
    let bytes = op
        .read(&meta_path)
        .await
        .map_err(|e| format!("pgx_zarr_walker: read {meta_path}: {e}"))?;
    serde_json::from_slice::<ZarrArrayMeta>(&bytes.to_vec())
        .map_err(|e| format!("pgx_zarr_walker: parse {meta_path}: {e}"))
}

/// Read every chunk of a 1-D coord axis and concatenate. Best-effort —
/// returns empty vec on any failure so the caller can emit chunks with
/// NULL bbox (still queryable, just unprunable).
///
/// Exposed for readers (e.g. `pg_xarray::reader::zarr`) that need to
/// slice the full coord array per data-chunk position rather than
/// chunk-index-aligned reads.
pub async fn read_full_axis(
    op: &Operator,
    coords_root: &str,
    axis_name: &str,
    expected_len: u64,
) -> Vec<f64> {
    let axis_path = if coords_root.is_empty() {
        axis_name.to_string()
    } else {
        format!("{}/{}", coords_root, axis_name)
    };
    let meta = match read_array_meta(op, &axis_path).await {
        Ok(m) => m,
        Err(_) => return Vec::new(),
    };
    if meta.shape.is_empty() {
        return Vec::new();
    }
    let chunk_size = meta.chunk_grid.configuration.chunk_shape[0];
    if chunk_size == 0 {
        return Vec::new();
    }
    let total = meta.shape[0];
    let n_chunks = total.div_ceil(chunk_size);
    let mut out = Vec::with_capacity(total as usize);
    for ci in 0..n_chunks {
        let this_size = (total - ci * chunk_size).min(chunk_size);
        match read_coord_chunk(op, &axis_path, ci, this_size, &meta).await {
            Ok(values) => out.extend(values),
            Err(_) => return Vec::new(),
        }
    }
    if (out.len() as u64) != expected_len {
        return Vec::new();
    }
    out
}

async fn read_coord_chunk(
    op: &Operator,
    axis_path: &str,
    chunk_index: u64,
    chunk_size: u64,
    meta: &ZarrArrayMeta,
) -> Result<Vec<f64>, String> {
    let chunk_file = chunk_file_path(axis_path, &[chunk_index], &meta.chunk_key_encoding);
    let bytes = op
        .read(&chunk_file)
        .await
        .map_err(|e| format!("pgx_zarr_walker: coord chunk read {chunk_file}: {e}"))?
        .to_vec();
    // Coord axes are always physical floats — never CF-packed in any
    // real-world store. Identity packing is the right default here.
    decode_chunk_values_identity(&bytes, &meta.data_type, &meta.codecs, chunk_size)
}

/// Build the chunk file path under the variable group.
/// Default Zarr v3 encoding: `<var>/c/<i>[/<j>...]`.
/// v2 encoding: `<var>/<i>[.<j>...]` (or whatever separator is configured).
fn chunk_file_path(var: &str, indices: &[u64], encoding: &ChunkKeyEncoding) -> String {
    let sep = if encoding.configuration.separator.is_empty() {
        "/"
    } else {
        encoding.configuration.separator.as_str()
    };
    let joined = indices
        .iter()
        .map(|i| i.to_string())
        .collect::<Vec<_>>()
        .join(sep);
    if encoding.name == "v2" {
        format!("{}/{}", var, joined)
    } else {
        format!("{}/c/{}", var, joined)
    }
}

fn guess_axis_name(dims: &[Option<String>], axis: usize, fallback: &str) -> String {
    dims.get(axis)
        .and_then(|d| d.clone())
        .unwrap_or_else(|| fallback.to_string())
}

/// Resolve a dim index from an optional explicit axis name. Looks the
/// name up in `dimension_names`; falls back to `fallback_idx` if no
/// name was provided. Errors if a name was given but isn't in the dims.
fn resolve_dim_idx(
    dims: &[Option<String>],
    explicit: Option<&str>,
    fallback_idx: usize,
) -> Result<usize, String> {
    match explicit {
        None => Ok(fallback_idx),
        Some(name) => require_dim_idx(dims, name),
    }
}

fn require_dim_idx(dims: &[Option<String>], name: &str) -> Result<usize, String> {
    for (i, d) in dims.iter().enumerate() {
        if d.as_deref() == Some(name) {
            return Ok(i);
        }
    }
    Err(format!(
        "pgx_zarr_walker: axis '{}' not found in dimension_names {:?}",
        name, dims
    ))
}

/// Like [`read_full_axis`] but also returns the axis's CF `units`
/// attribute (e.g. `"hours since 1970-01-01"`), when present.
async fn read_full_axis_with_units(
    op: &Operator,
    coords_root: &str,
    axis_name: &str,
    expected_len: u64,
) -> (Vec<f64>, Option<String>) {
    let axis_path = if coords_root.is_empty() {
        axis_name.to_string()
    } else {
        format!("{}/{}", coords_root, axis_name)
    };
    let meta = match read_array_meta(op, &axis_path).await {
        Ok(m) => m,
        Err(_) => return (Vec::new(), None),
    };
    let units = meta
        .attributes
        .get("units")
        .and_then(|u| u.as_str())
        .map(String::from);
    let values = read_full_axis(op, coords_root, axis_name, expected_len).await;
    (values, units)
}

/// Parsed CF time units: a base unit + a reference timestamp.
/// Numeric coord values v map to `ref_dt + v * base_unit`.
#[derive(Debug, Clone, Copy)]
enum CfUnit {
    Seconds,
    Minutes,
    Hours,
    Days,
}

#[derive(Debug, Clone)]
struct CfTimeRef {
    unit: CfUnit,
    base: DateTime<Utc>,
}

/// Public helper: parse a CF `"<unit> since <date>"` string and apply
/// it to a numeric coord value, yielding the absolute timestamp.
/// Returns `None` when the units string doesn't match the expected
/// grammar (callers fall back to a NULL `time` in their output).
///
/// Exposed for readers (e.g. `pg_xarray::reader::zarr`) that need to
/// decode a single coord value to a timestamp without going through
/// the full walker.
pub fn decode_cf_time(units: &str, value: f64) -> Option<DateTime<Utc>> {
    let tref = parse_cf_units(units)?;
    Some(apply_cf_unit(&tref, value))
}

/// Parse `"<unit> since <date>"` (CF convention). Returns `None` when
/// the units string doesn't match the expected grammar.
fn parse_cf_units(s: &str) -> Option<CfTimeRef> {
    let (unit_str, ref_str) = s.split_once(" since ")?;
    let unit = match unit_str.trim() {
        "seconds" | "second" | "s" | "secs" | "sec" => CfUnit::Seconds,
        "minutes" | "minute" | "min" | "mins" => CfUnit::Minutes,
        "hours" | "hour" | "h" | "hr" | "hrs" => CfUnit::Hours,
        "days" | "day" | "d" => CfUnit::Days,
        _ => return None,
    };
    let base = parse_cf_reference_date(ref_str.trim())?;
    Some(CfTimeRef { unit, base })
}

fn parse_cf_reference_date(s: &str) -> Option<DateTime<Utc>> {
    // RFC 3339 ("2024-01-01T00:00:00Z" or with offset)
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    // "YYYY-MM-DD HH:MM:SS" (CF's common form)
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }
    // Date-only "YYYY-MM-DD"
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0)?));
    }
    None
}

fn apply_cf_unit(tref: &CfTimeRef, value: f64) -> DateTime<Utc> {
    let seconds = match tref.unit {
        CfUnit::Seconds => value,
        CfUnit::Minutes => value * 60.0,
        CfUnit::Hours => value * 3600.0,
        CfUnit::Days => value * 86_400.0,
    };
    let nanos = (seconds * 1e9) as i64;
    tref.base + chrono::Duration::nanoseconds(nanos)
}

/// Compute (time_from, time_to) for a chunk by slicing the resolved
/// time-axis values per the chunk's index along the time dim, then
/// converting via the parsed CF units.
fn compute_time_range(
    time_values: &[f64],
    chunk_time_idx: u64,
    chunk_time_size: u64,
    total_time: u64,
    tref: &CfTimeRef,
) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    if time_values.is_empty() {
        return (None, None);
    }
    let origin = chunk_time_idx * chunk_time_size;
    let end = (origin + chunk_time_size).min(total_time) as usize;
    let origin = origin as usize;
    if origin >= time_values.len() {
        return (None, None);
    }
    let slice = &time_values[origin..end.min(time_values.len())];
    if slice.is_empty() {
        return (None, None);
    }
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for &v in slice {
        if v < lo {
            lo = v;
        }
        if v > hi {
            hi = v;
        }
    }
    (Some(apply_cf_unit(tref, lo)), Some(apply_cf_unit(tref, hi)))
}

/// Slice `values` along a chunk's position on some dim and return
/// `(min, max)`. Used for the Z axis today; could serve any 1-D
/// coord-array range pruning. Returns `None` when the slice is empty
/// or the values array couldn't be read.
fn compute_numeric_range(
    values: &[f64],
    chunk_idx: u64,
    chunk_size: u64,
    total: u64,
) -> Option<(f64, f64)> {
    if values.is_empty() {
        return None;
    }
    let origin = chunk_idx * chunk_size;
    let end = (origin + chunk_size).min(total) as usize;
    let origin = origin as usize;
    if origin >= values.len() {
        return None;
    }
    let slice = &values[origin..end.min(values.len())];
    if slice.is_empty() {
        return None;
    }
    let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
    for &v in slice {
        if v < lo {
            lo = v;
        }
        if v > hi {
            hi = v;
        }
    }
    Some((lo, hi))
}

fn chunks_per_dim(shape: &[u64], chunk_shape: &[u64]) -> Vec<u64> {
    shape
        .iter()
        .zip(chunk_shape.iter())
        .map(|(s, c)| if *c == 0 { 0 } else { s.div_ceil(*c) })
        .collect()
}

fn increment_indices(indices: &mut [u64], counts: &[u64]) -> bool {
    if indices.is_empty() {
        return false;
    }
    let mut d = indices.len();
    while d > 0 {
        d -= 1;
        indices[d] += 1;
        if indices[d] < counts[d] {
            return true;
        }
        indices[d] = 0;
    }
    false
}

fn compute_bbox(
    lats: &[f64],
    lons: &[f64],
    chunk_lat_idx: u64,
    chunk_lon_idx: u64,
    chunk_lat_size: u64,
    chunk_lon_size: u64,
    total_lat: u64,
    total_lon: u64,
) -> Option<String> {
    if lats.is_empty() || lons.is_empty() {
        return None;
    }
    let lat_origin = chunk_lat_idx * chunk_lat_size;
    let lon_origin = chunk_lon_idx * chunk_lon_size;
    let lat_end = (lat_origin + chunk_lat_size).min(total_lat) as usize;
    let lon_end = (lon_origin + chunk_lon_size).min(total_lon) as usize;
    let lat_origin = lat_origin as usize;
    let lon_origin = lon_origin as usize;
    if lat_origin >= lats.len() || lon_origin >= lons.len() {
        return None;
    }
    let lat_slice = &lats[lat_origin..lat_end.min(lats.len())];
    let lon_slice = &lons[lon_origin..lon_end.min(lons.len())];
    if lat_slice.is_empty() || lon_slice.is_empty() {
        return None;
    }
    let (mut min_lat, mut max_lat) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_lon, mut max_lon) = (f64::INFINITY, f64::NEG_INFINITY);
    for &v in lat_slice {
        if v < min_lat {
            min_lat = v;
        }
        if v > max_lat {
            max_lat = v;
        }
    }
    for &v in lon_slice {
        let lo = if v > 180.0 { v - 360.0 } else { v };
        if lo < min_lon {
            min_lon = lo;
        }
        if lo > max_lon {
            max_lon = lo;
        }
    }
    Some(format!(
        "POLYGON(({min_lon} {min_lat}, {max_lon} {min_lat}, \
         {max_lon} {max_lat}, {min_lon} {max_lat}, {min_lon} {min_lat}))"
    ))
}

/// Decode a Zarr v3 chunk by walking the codec chain in REVERSE order
/// (codecs are listed in encode order — encode goes data → bytes →
/// compress; decode is the mirror), then turning the inner byte stream
/// into a `Vec<f64>` of physical values.
///
/// Supported codecs:
///   * `bytes`  — endian-aware byte serialisation (always innermost)
///   * `gzip`   — `flate2::read::GzDecoder`
///   * `zstd`   — `zstd::stream::decode_all`
///
/// Supported dtypes: `float32`/`float64` plus signed and unsigned ints
/// at 8/16/32/64 bits (Zarr v3 long form like `int16` and the v2-style
/// short forms `<i2` / `>u4` etc. both accepted). For `int64`/`uint64`
/// values whose magnitudes exceed 2^53 the f64 cast loses precision —
/// most physical-quantity datasets are well below that.
///
/// `packing` is the CF triple `physical = stored * scale + offset`,
/// with `bytes-equal-to-fill_value → NaN` applied **before** the
/// scale/offset (matches xarray/numpy semantics). `CfPacking::identity()`
/// is the no-op for the common already-float case.
pub fn decode_chunk_values(
    bytes: &[u8],
    data_type: &str,
    codecs: &[serde_json::Value],
    expected_cells: u64,
    packing: &CfPacking,
) -> Result<Vec<f64>, String> {
    // Peel compression layers off in reverse order until we reach
    // either the `bytes` codec or run out (treated as implicit
    // little-endian raw bytes).
    let mut current = std::borrow::Cow::Borrowed(bytes);
    let mut endianness = "little";

    for codec in codecs.iter().rev() {
        let name = codec
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or_default();
        match name {
            "bytes" => {
                endianness = codec
                    .get("configuration")
                    .and_then(|cfg| cfg.get("endian"))
                    .and_then(|e| e.as_str())
                    .unwrap_or("little");
                break;
            }
            "gzip" => {
                use std::io::Read;
                let mut decoder = flate2::read::GzDecoder::new(current.as_ref());
                let mut out = Vec::new();
                decoder
                    .read_to_end(&mut out)
                    .map_err(|e| format!("pgx_zarr_walker: gzip decode failed: {e}"))?;
                current = std::borrow::Cow::Owned(out);
            }
            "zstd" => {
                let out = zstd::stream::decode_all(current.as_ref())
                    .map_err(|e| format!("pgx_zarr_walker: zstd decode failed: {e}"))?;
                current = std::borrow::Cow::Owned(out);
            }
            other => {
                return Err(format!(
                    "pgx_zarr_walker: unsupported codec '{other}' \
                     (supported: bytes, gzip, zstd)"
                ));
            }
        }
    }

    let cells = expected_cells as usize;
    let bytes = current.as_ref();
    let little = endianness == "little";

    // Closure applies CF packing uniformly across every dtype path.
    let pack = |v: f64| -> f64 {
        if let Some(fv) = packing.fill_value {
            if v == fv {
                return f64::NAN;
            }
        }
        v * packing.scale + packing.offset
    };

    // Each dtype path: check size, read N-byte chunks, decode to f64,
    // apply packing, push.
    match data_type {
        "float32" | "f4" | "<f4" | ">f4" => decode_fixed::<4>(bytes, cells, |arr| {
            let v = if little {
                f32::from_le_bytes(arr)
            } else {
                f32::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "float64" | "f8" | "<f8" | ">f8" => decode_fixed::<8>(bytes, cells, |arr| {
            let v = if little {
                f64::from_le_bytes(arr)
            } else {
                f64::from_be_bytes(arr)
            };
            pack(v)
        }),
        "int8" | "i1" | "<i1" | ">i1" => {
            decode_fixed::<1>(bytes, cells, |arr| pack(i8::from_le_bytes(arr) as f64))
        }
        "uint8" | "u1" | "<u1" | ">u1" => {
            decode_fixed::<1>(bytes, cells, |arr| pack(u8::from_le_bytes(arr) as f64))
        }
        "int16" | "i2" | "<i2" | ">i2" => decode_fixed::<2>(bytes, cells, |arr| {
            let v = if little {
                i16::from_le_bytes(arr)
            } else {
                i16::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "uint16" | "u2" | "<u2" | ">u2" => decode_fixed::<2>(bytes, cells, |arr| {
            let v = if little {
                u16::from_le_bytes(arr)
            } else {
                u16::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "int32" | "i4" | "<i4" | ">i4" => decode_fixed::<4>(bytes, cells, |arr| {
            let v = if little {
                i32::from_le_bytes(arr)
            } else {
                i32::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "uint32" | "u4" | "<u4" | ">u4" => decode_fixed::<4>(bytes, cells, |arr| {
            let v = if little {
                u32::from_le_bytes(arr)
            } else {
                u32::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "int64" | "i8" | "<i8" | ">i8" => decode_fixed::<8>(bytes, cells, |arr| {
            let v = if little {
                i64::from_le_bytes(arr)
            } else {
                i64::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        "uint64" | "u8" | "<u8" | ">u8" => decode_fixed::<8>(bytes, cells, |arr| {
            let v = if little {
                u64::from_le_bytes(arr)
            } else {
                u64::from_be_bytes(arr)
            };
            pack(v as f64)
        }),
        other => Err(format!(
            "pgx_zarr_walker: dtype '{other}' not supported \
             (supported: float32/64, int8/16/32/64, uint8/16/32/64)"
        )),
    }
}

/// Read a fixed-width-N chunk-of-bytes per cell, decoding via a
/// closure that turns the bytes into the final packed f64. Single
/// definition keeps every dtype branch terse.
fn decode_fixed<const N: usize>(
    bytes: &[u8],
    cells: usize,
    mut decode_one: impl FnMut([u8; N]) -> f64,
) -> Result<Vec<f64>, String> {
    if bytes.len() != cells * N {
        return Err(format!(
            "pgx_zarr_walker: chunk size mismatch — got {} bytes, expected {} for {} cells of {} bytes each",
            bytes.len(),
            cells * N,
            cells,
            N
        ));
    }
    let mut out = Vec::with_capacity(cells);
    for i in 0..cells {
        let arr: [u8; N] = bytes[i * N..i * N + N].try_into().unwrap();
        out.push(decode_one(arr));
    }
    Ok(out)
}

/// Backwards-compatible wrapper for callers that don't have any CF
/// packing to apply — keeps the pre-step-2 call sites short and clear.
#[inline]
fn decode_chunk_values_identity(
    bytes: &[u8],
    data_type: &str,
    codecs: &[serde_json::Value],
    expected_cells: u64,
) -> Result<Vec<f64>, String> {
    decode_chunk_values(
        bytes,
        data_type,
        codecs,
        expected_cells,
        &CfPacking::identity(),
    )
}

// =============================================================================
// Unit tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_file_path_v3_default() {
        let enc = default_chunk_key_encoding();
        assert_eq!(chunk_file_path("t2m", &[0, 0], &enc), "t2m/c/0/0");
        assert_eq!(chunk_file_path("t2m", &[1, 2], &enc), "t2m/c/1/2");
    }

    #[test]
    fn chunk_file_path_v2_dot() {
        let enc = ChunkKeyEncoding {
            name: "v2".to_string(),
            configuration: ChunkKeyEncodingConfig {
                separator: ".".to_string(),
            },
        };
        assert_eq!(chunk_file_path("t2m", &[0, 0], &enc), "t2m/0.0");
    }

    #[test]
    fn chunks_per_dim_uniform() {
        assert_eq!(chunks_per_dim(&[6, 8], &[3, 4]), vec![2, 2]);
    }

    #[test]
    fn iterate_row_major() {
        let counts = vec![2u64, 2];
        let mut idx = vec![0u64, 0];
        let mut order = vec![idx.clone()];
        while increment_indices(&mut idx, &counts) {
            order.push(idx.clone());
        }
        assert_eq!(order, vec![vec![0, 0], vec![0, 1], vec![1, 0], vec![1, 1]]);
    }

    #[test]
    fn compute_bbox_picks_window() {
        let lats: Vec<f64> = vec![50.0, 51.0, 52.0, 53.0];
        let lons: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0];
        let bbox = compute_bbox(&lats, &lons, 1, 0, 2, 2, 4, 4).unwrap();
        assert!(bbox.contains("0 52"));
        assert!(bbox.contains("1 53"));
    }

    #[test]
    fn compute_bbox_normalises_360() {
        let lats: Vec<f64> = vec![10.0, 11.0];
        let lons: Vec<f64> = vec![350.0, 10.0];
        let bbox = compute_bbox(&lats, &lons, 0, 0, 2, 2, 2, 2).unwrap();
        assert!(bbox.contains("-10"));
    }

    #[test]
    fn decode_f32_le() {
        let bytes = [0u8, 0, 0x80, 0x3F, 0, 0, 0, 0x40];
        let codecs = vec![serde_json::json!({
            "name": "bytes",
            "configuration": {"endian": "little"}
        })];
        let v = decode_chunk_values(&bytes, "float32", &codecs, 2, &CfPacking::identity()).unwrap();
        assert!((v[0] - 1.0).abs() < 1e-6);
        assert!((v[1] - 2.0).abs() < 1e-6);
    }

    #[test]
    fn decode_int16_with_scale_and_fill() {
        // Two cells: stored=100 → physical=100*0.01+10=11.0; stored=-9999 → NaN.
        let bytes = [100u8, 0, 0xF1, 0xD8]; // 100 LE, -9999 LE (0xD8F1 = -9999)
        let codecs = vec![serde_json::json!({
            "name": "bytes",
            "configuration": {"endian": "little"}
        })];
        let packing = CfPacking {
            scale: 0.01,
            offset: 10.0,
            fill_value: Some(-9999.0),
        };
        let v = decode_chunk_values(&bytes, "int16", &codecs, 2, &packing).unwrap();
        assert!((v[0] - 11.0).abs() < 1e-9);
        assert!(v[1].is_nan(), "fill should map to NaN, got {}", v[1]);
    }

    #[test]
    fn decode_uint8_no_packing() {
        let bytes = [0u8, 255];
        let codecs: Vec<serde_json::Value> = vec![];
        let v = decode_chunk_values(&bytes, "uint8", &codecs, 2, &CfPacking::identity()).unwrap();
        assert_eq!(v[0], 0.0);
        assert_eq!(v[1], 255.0);
    }

    #[test]
    fn decode_int32_big_endian() {
        // 0x00000001 = 1, 0xFFFFFFFE = -2
        let bytes = [0u8, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFE];
        let codecs = vec![serde_json::json!({
            "name": "bytes",
            "configuration": {"endian": "big"}
        })];
        let v = decode_chunk_values(&bytes, "int32", &codecs, 2, &CfPacking::identity()).unwrap();
        assert_eq!(v[0], 1.0);
        assert_eq!(v[1], -2.0);
    }

    #[test]
    fn unsupported_dtype_errors() {
        let bytes = [0u8; 8];
        let codecs: Vec<serde_json::Value> = vec![];
        let err = decode_chunk_values(&bytes, "complex64", &codecs, 1, &CfPacking::identity())
            .unwrap_err();
        assert!(err.contains("complex64"), "error mentions dtype: {err}");
    }
}
