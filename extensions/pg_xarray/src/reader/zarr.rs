// `ZarrArrayMeta` / `ChunkGrid` / `ChunkKeyEncoding` fields are pub for
// future SDK surface (xarray_header processor in pg_streaming_xarray
// owns its own parser, but reuses field semantics from here when in-tree).
#![allow(dead_code)]

//! Zarr v3 reader (minimal — supports the `bytes` codec for f32/f64 arrays).
//!
//! For each chunk:
//!   1. Build an OpenDAL `Operator` from the store URI scheme.
//!   2. Read the variable's `zarr.json` to learn `data_type`,
//!      `chunk_grid`, `chunk_key_encoding`, `codecs`, `fill_value`.
//!   3. Parse the requested chunk key, locate the chunk file under
//!      the encoding scheme (default `c/0/5/12` or v2-style `0.5.12`),
//!      range-fetch its bytes.
//!   4. Decode the chunk according to its codec chain (currently only
//!      `bytes` / endian; blosc/zstd/sharding deferred to follow-up).
//!   5. Read the associated `latitude` / `longitude` 1-D coordinate
//!      arrays (cached) and slice them to the chunk's cell window.
//!   6. Emit `Cell` rows; apply the bbox filter and `max_cells` cap.
//!
//! This is the cleanest target because Zarr was designed for exactly
//! this access pattern — one HTTP range request per chunk.
//!
//! See `design/pg_xarray/indexing.md` Example 2 (ARCO-ERA5).

use super::{Cell, ChunkLocator, ChunkReader, CoordFilter};
use async_trait::async_trait;
use opendal::{Operator, Scheme};
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Default)]
pub struct ZarrReader;

impl ZarrReader {
    pub fn new() -> Self {
        Self
    }

    /// Build an OpenDAL operator pointing at the Zarr store root.
    /// The `path` returned is the variable/group path inside the store
    /// (e.g., `"2m_temperature"`).
    pub(crate) fn build_store_operator(uri: &str) -> Result<(Operator, String), String> {
        let parsed = Url::parse(uri).map_err(|e| format!("zarr: invalid URI '{uri}': {e}"))?;
        let scheme_str = parsed.scheme();
        let scheme = Scheme::from_str(scheme_str)
            .map_err(|e| format!("zarr: unsupported scheme '{scheme_str}': {e}"))?;
        let mut cfg: HashMap<String, String> = HashMap::new();

        let store_path: String = match scheme_str {
            "fs" | "file" => {
                cfg.insert("root".to_string(), "/".to_string());
                parsed.path().trim_start_matches('/').to_string()
            }
            "s3" | "gs" | "azblob" => {
                let bucket = parsed
                    .host_str()
                    .ok_or_else(|| format!("zarr: '{uri}' missing bucket"))?
                    .to_string();
                cfg.insert("bucket".to_string(), bucket);
                cfg.insert("anonymous".to_string(), "true".to_string());
                parsed.path().trim_start_matches('/').to_string()
            }
            "http" | "https" => {
                let endpoint = format!(
                    "{}://{}",
                    scheme_str,
                    parsed
                        .host_str()
                        .ok_or_else(|| format!("zarr: '{uri}' missing host"))?
                );
                cfg.insert("endpoint".to_string(), endpoint);
                parsed.path().trim_start_matches('/').to_string()
            }
            other => {
                return Err(format!("zarr: scheme '{other}' not wired up"));
            }
        };

        let op = Operator::via_iter(scheme, cfg)
            .map_err(|e| format!("zarr: failed to build operator: {e}"))?;
        Ok((op, store_path))
    }
}

#[async_trait]
impl ChunkReader for ZarrReader {
    fn format_name(&self) -> &'static str {
        "zarr"
    }

    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String> {
        let (op, store_path) = Self::build_store_operator(&locator.uri)?;
        let chunk_key = locator
            .chunk_key
            .as_deref()
            .ok_or_else(|| "zarr: chunk_key is required".to_string())?;

        // Parse "<variable>/<i.j.k>" or "<variable>/c/i/j/k" form into
        // (variable_path, chunk_indices).
        let (var_path, chunk_indices) = parse_chunk_key(chunk_key)?;

        let full_var_path = if store_path.is_empty() {
            var_path.clone()
        } else {
            format!("{}/{}", store_path.trim_end_matches('/'), var_path)
        };

        // The fetch SRF reads the variable's CF packing once per call
        // and threads it through here. None → identity (no-op).
        let packing = locator
            .packing
            .unwrap_or_else(pgx_zarr_walker::CfPacking::identity);

        tokio::time::timeout(
            Duration::from_secs(120),
            decode_zarr_chunk(
                &op,
                &full_var_path,
                &chunk_indices,
                &store_path,
                filter,
                &packing,
            ),
        )
        .await
        .map_err(|_| "zarr: chunk fetch timed out after 120s".to_string())?
    }
}

// =============================================================================
// Zarr v3 metadata + chunk decoding
// =============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct ZarrArrayMeta {
    #[serde(default)]
    pub zarr_format: u8,
    #[serde(default)]
    pub node_type: String,
    pub shape: Vec<u64>,
    pub data_type: String,
    pub chunk_grid: ChunkGrid,
    #[serde(default = "default_chunk_key_encoding")]
    pub chunk_key_encoding: ChunkKeyEncoding,
    #[serde(default)]
    pub fill_value: serde_json::Value,
    #[serde(default)]
    pub codecs: Vec<serde_json::Value>,
    #[serde(default)]
    pub dimension_names: Vec<Option<String>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkGrid {
    pub name: String,
    pub configuration: ChunkGridConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkGridConfig {
    pub chunk_shape: Vec<u64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChunkKeyEncoding {
    pub name: String,
    #[serde(default)]
    pub configuration: ChunkKeyEncodingConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct ChunkKeyEncodingConfig {
    #[serde(default = "default_separator")]
    pub separator: String,
}

fn default_separator() -> String {
    "/".to_string()
}

fn default_chunk_key_encoding() -> ChunkKeyEncoding {
    ChunkKeyEncoding {
        name: "default".to_string(),
        configuration: ChunkKeyEncodingConfig {
            separator: "/".to_string(),
        },
    }
}

/// Split `"<var>/<i.j.k>"` or `"<var>/c/i/j/k"` (both common in the
/// wild) into a variable path + integer chunk indices.
pub fn parse_chunk_key(key: &str) -> Result<(String, Vec<u64>), String> {
    let key = key.trim_matches('/');
    if key.is_empty() {
        return Err("zarr: empty chunk_key".to_string());
    }

    // Try the v3 default form: "<var>/c/<i>/<j>/<k>"
    if let Some(c_pos) = key.find("/c/") {
        let var = key[..c_pos].to_string();
        let indices_str = &key[c_pos + 3..];
        let indices = parse_indices_slash(indices_str)?;
        return Ok((var, indices));
    }

    // Try the v2/legacy form: "<var>/<i>.<j>.<k>"
    if let Some(last_slash) = key.rfind('/') {
        let var = key[..last_slash].to_string();
        let indices_str = &key[last_slash + 1..];
        if let Ok(indices) = parse_indices_dot(indices_str) {
            return Ok((var, indices));
        }
    }

    Err(format!("zarr: cannot parse chunk_key '{key}'"))
}

fn parse_indices_dot(s: &str) -> Result<Vec<u64>, String> {
    s.split('.')
        .map(|p| p.parse::<u64>().map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("zarr: bad chunk-index '{s}': {e}"))
}

fn parse_indices_slash(s: &str) -> Result<Vec<u64>, String> {
    s.trim_matches('/')
        .split('/')
        .map(|p| p.parse::<u64>().map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("zarr: bad chunk-index '{s}': {e}"))
}

/// Build the relative path of a chunk file given the array's metadata
/// and the chunk's integer indices.
pub fn chunk_file_path(var_path: &str, indices: &[u64], encoding: &ChunkKeyEncoding) -> String {
    let var = var_path.trim_matches('/');
    let sep = if encoding.configuration.separator.is_empty() {
        "/"
    } else {
        &encoding.configuration.separator
    };
    match encoding.name.as_str() {
        "default" => {
            // v3 default: "<var>/c/<i><sep><j><sep><k>"
            let joined = indices
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(sep);
            format!("{}/c/{}", var, joined)
        }
        "v2" => {
            // v2: "<var>/<i>.<j>.<k>" (sep usually ".")
            let joined = indices
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(sep);
            format!("{}/{}", var, joined)
        }
        other => {
            // Fallback — same as default
            pgrx_warn(&format!(
                "zarr: unknown chunk_key_encoding '{other}', defaulting to v3"
            ));
            let joined = indices
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(sep);
            format!("{}/c/{}", var, joined)
        }
    }
}

fn pgrx_warn(msg: &str) {
    // Warn under pgrx if present; otherwise no-op for isolation tests.
    #[cfg(any(test, not(feature = "_inside_pgrx")))]
    eprintln!("[zarr warning] {msg}");
}

/// Number of cells per chunk (product of chunk_shape).
pub fn cells_per_chunk(chunk_shape: &[u64]) -> u64 {
    chunk_shape.iter().product()
}

/// Decode a Zarr v3 chunk. Delegates to the shared `pgx_zarr_walker`
/// crate so the codec list — bytes / gzip / zstd today — and the dtype
/// dispatch (float32/64 + signed/unsigned ints) plus CF packing live
/// in one place and behave identically across the data-fetch path and
/// the header-walking path.
///
/// `packing` is the CF triplet (`scale`, `offset`, `fill_value`) the
/// upstream Zarr reader collected from the variable's catalog row.
/// Pass `&CfPacking::identity()` when there's no packing (e.g.,
/// coord-axis decode).
pub fn decode_chunk_values(
    bytes: &[u8],
    data_type: &str,
    codecs: &[serde_json::Value],
    expected_cells: u64,
    packing: &pgx_zarr_walker::CfPacking,
) -> Result<Vec<f64>, String> {
    pgx_zarr_walker::decode_chunk_values(bytes, data_type, codecs, expected_cells, packing)
}

/// Compute (chunk_origin_indices_per_dim) so a chunk at indices
/// `[ci, cj, ck]` with chunk_shape `[sh_i, sh_j, sh_k]` starts at
/// `[ci*sh_i, cj*sh_j, ck*sh_k]` in the global array.
pub fn chunk_origin(chunk_indices: &[u64], chunk_shape: &[u64]) -> Vec<u64> {
    chunk_indices
        .iter()
        .zip(chunk_shape.iter())
        .map(|(ci, sh)| ci * sh)
        .collect()
}

/// End-to-end chunk decode for a 2D or 3D variable: reads zarr.json,
/// fetches the chunk file via opendal, decodes values, reads coord
/// arrays, maps to cells with bbox filter.
async fn decode_zarr_chunk(
    op: &Operator,
    var_path: &str,
    chunk_indices: &[u64],
    store_root: &str,
    filter: &CoordFilter,
    packing: &pgx_zarr_walker::CfPacking,
) -> Result<Vec<Cell>, String> {
    // 1. Read variable's zarr.json
    let meta_path = format!("{}/zarr.json", var_path.trim_end_matches('/'));
    let meta_bytes = op
        .read(&meta_path)
        .await
        .map_err(|e| format!("zarr: failed to read {meta_path}: {e}"))?;
    let meta: ZarrArrayMeta = serde_json::from_slice(&meta_bytes.to_vec())
        .map_err(|e| format!("zarr: invalid zarr.json at {meta_path}: {e}"))?;

    if meta.shape.len() != chunk_indices.len() {
        return Err(format!(
            "zarr: chunk_indices len {} doesn't match array rank {}",
            chunk_indices.len(),
            meta.shape.len()
        ));
    }
    if meta.shape.len() < 2 || meta.shape.len() > 4 {
        return Err(format!(
            "zarr: this build supports rank 2-4 only (got rank {})",
            meta.shape.len()
        ));
    }

    // 2. Build the chunk-file path and range-fetch it.
    let chunk_shape = &meta.chunk_grid.configuration.chunk_shape;
    let chunk_file = chunk_file_path(var_path, chunk_indices, &meta.chunk_key_encoding);
    let chunk_bytes = op
        .read(&chunk_file)
        .await
        .map_err(|e| format!("zarr: failed to read chunk {chunk_file}: {e}"))?
        .to_vec();
    let expected_cells = cells_per_chunk(chunk_shape);
    // Apply the variable's CF packing — physical = stored*scale+offset,
    // with bytes-equal-to-fill_value → NaN. For unpacked (already-
    // physical) stores the SRF passes `CfPacking::identity()`, which
    // is the byte-identical no-op path.
    let values = decode_chunk_values(
        &chunk_bytes,
        &meta.data_type,
        &meta.codecs,
        expected_cells,
        packing,
    )?;

    // 3. Read coordinate arrays. We assume the last two dims are
    //    (lat, lon) by convention — this is true for almost all
    //    CF-compliant Zarr stores (time, level, lat, lon).
    let rank = meta.shape.len();
    let lat_dim = rank - 2;
    let lon_dim = rank - 1;
    let lat_axis_name = guess_axis_name(&meta.dimension_names, lat_dim, "latitude");
    let lon_axis_name = guess_axis_name(&meta.dimension_names, lon_dim, "longitude");
    let coords_root = store_root.trim_end_matches('/');

    let lats = read_coord_axis(
        op,
        coords_root,
        &lat_axis_name,
        chunk_indices[lat_dim],
        chunk_shape[lat_dim],
    )
    .await
    .unwrap_or_default();
    let lons = read_coord_axis(
        op,
        coords_root,
        &lon_axis_name,
        chunk_indices[lon_dim],
        chunk_shape[lon_dim],
    )
    .await
    .unwrap_or_default();

    // 3b. Detect time + level dims by name, read the chunk's slice
    //     of each, and (for time) decode the CF units. For chunks
    //     that span exactly one slice along these dims (the common
    //     CF-tile shape — e.g., chunk_shape=[1, 1, n_lat, n_lon] for
    //     (time, level, lat, lon)) every cell shares the same time
    //     + level value, so we read it once outside the cell loop.
    //     For multi-slice chunks the first-slice value is used —
    //     conservative but correct for the chunk-time-pruning that
    //     fetch_impl actually does via time_range.
    let (chunk_time, chunk_level) =
        read_time_level_for_chunk(op, coords_root, &meta, chunk_indices).await;

    // 4. Compute the chunk's cell window and emit rows.
    let origin = chunk_origin(chunk_indices, chunk_shape);
    let chunk_lat_dim_size = chunk_shape[lat_dim] as usize;
    let chunk_lon_dim_size = chunk_shape[lon_dim] as usize;
    let max = filter.max_cells.unwrap_or(usize::MAX);
    let mut cells = Vec::with_capacity(values.len().min(max));

    // For rank 2: row-major (lat, lon).
    // For rank 3 or 4: we emit only the slab at slice [0,0,..,:,:] —
    // the indexer pipeline is expected to chunk so that each chunk row
    // covers one (time, level) slice.
    let slab_stride = chunk_lat_dim_size * chunk_lon_dim_size;
    for j in 0..chunk_lat_dim_size {
        for i in 0..chunk_lon_dim_size {
            let v_idx = j * chunk_lon_dim_size + i;
            if v_idx >= slab_stride || v_idx >= values.len() {
                break;
            }
            let lat = lats.get(j).copied();
            let lon = lons
                .get(i)
                .copied()
                .map(|l| if l > 180.0 { l - 360.0 } else { l });
            if let (Some(b), Some(la), Some(lo)) = (&filter.bbox_2d, lat, lon) {
                if !b.contains(la, lo) {
                    continue;
                }
            }
            cells.push(Cell {
                lat,
                lon,
                level: chunk_level,
                time: chunk_time,
                node_id: Some(
                    (origin[lat_dim] + j as u64) as i64 * 1_000_000
                        + (origin[lon_dim] + i as u64) as i64,
                ),
                value: values[v_idx],
            });
            if cells.len() >= max {
                return Ok(cells);
            }
        }
    }

    Ok(cells)
}

pub(crate) fn guess_axis_name(dims: &[Option<String>], axis: usize, fallback: &str) -> String {
    dims.get(axis)
        .and_then(|d| d.clone())
        .unwrap_or_else(|| fallback.to_string())
}

/// Read one chunk's worth of a 1-D coordinate axis. Best-effort —
/// returns an empty vec on any error so the caller can still emit cells
/// with NULL lat/lon and not fail the whole query.
pub(crate) async fn read_coord_axis(
    op: &Operator,
    store_root: &str,
    axis_name: &str,
    chunk_index: u64,
    chunk_size: u64,
) -> Result<Vec<f64>, String> {
    let axis_path = if store_root.is_empty() {
        axis_name.to_string()
    } else {
        format!("{}/{}", store_root, axis_name)
    };
    let meta_path = format!("{}/zarr.json", axis_path);
    let meta_bytes = op
        .read(&meta_path)
        .await
        .map_err(|e| format!("zarr: coord {axis_name}: {e}"))?;
    let meta: ZarrArrayMeta = serde_json::from_slice(&meta_bytes.to_vec())
        .map_err(|e| format!("zarr: coord {axis_name} bad zarr.json: {e}"))?;
    let chunk_file = chunk_file_path(&axis_path, &[chunk_index], &meta.chunk_key_encoding);
    let bytes = op
        .read(&chunk_file)
        .await
        .map_err(|e| format!("zarr: coord {axis_name} chunk read: {e}"))?
        .to_vec();
    // Coord arrays are always physical floats; no CF packing.
    decode_chunk_values(
        &bytes,
        &meta.data_type,
        &meta.codecs,
        chunk_size,
        &pgx_zarr_walker::CfPacking::identity(),
    )
}

/// Classify a Zarr dim name as time / level / something-else by
/// matching the common CF / xarray conventions. Used by
/// [`read_time_level_for_chunk`] below to populate `cell.time` and
/// `cell.level` for rank-3 / rank-4 arrays.
fn classify_axis(name: &str) -> AxisKind {
    let lower = name.to_ascii_lowercase();
    match lower.as_str() {
        "time" | "valid_time" | "t" | "forecast_time" | "ref_time" => AxisKind::Time,
        "level" | "lev" | "z" | "depth" | "altitude" | "height" | "plev" | "isobaricinhpa"
        | "isobaric_in_hpa" | "pressure" => AxisKind::Level,
        _ => AxisKind::Other,
    }
}

enum AxisKind {
    Time,
    Level,
    Other,
}

/// Read the time + level coord values for a chunk's position. The
/// axis's chunk grid may differ from the data variable's (most
/// real-world CF stores chunk the data along time+level while the
/// time/level axes themselves live in a single contig array), so we
/// read the FULL axis once via `pgx_zarr_walker::read_full_axis` and
/// then slice by data-chunk position rather than axis-chunk index.
///
/// For the common CF tile shape (`chunk_shape[time_dim] == 1` and
/// `chunk_shape[level_dim] == 1`) every cell in the chunk shares the
/// same time + level, so the caller can populate them once outside
/// the cell loop. For larger chunks we use the first-slice value —
/// conservative but matches the rest of the reader which only emits
/// the first slab anyway.
async fn read_time_level_for_chunk(
    op: &Operator,
    coords_root: &str,
    meta: &ZarrArrayMeta,
    chunk_indices: &[u64],
) -> (Option<chrono::DateTime<chrono::Utc>>, Option<f64>) {
    let rank = meta.shape.len();
    if rank < 3 {
        return (None, None);
    }
    let lat_dim = rank - 2;
    let lon_dim = rank - 1;
    let chunk_shape = &meta.chunk_grid.configuration.chunk_shape;
    let mut time_value: Option<chrono::DateTime<chrono::Utc>> = None;
    let mut level_value: Option<f64> = None;

    for dim in 0..rank {
        if dim == lat_dim || dim == lon_dim {
            continue;
        }
        let name = match guess_axis_name(&meta.dimension_names, dim, "").as_str() {
            "" => continue,
            other => other.to_string(),
        };
        let kind = classify_axis(&name);
        if matches!(kind, AxisKind::Other) {
            continue;
        }
        // Read the whole axis (axis may be one-chunk; data chunk
        // indices don't align with axis chunks in general). Then
        // slice at the chunk's start position along this dim.
        let full = pgx_zarr_walker::read_full_axis(op, coords_root, &name, meta.shape[dim]).await;
        let start = (chunk_indices[dim] * chunk_shape[dim]) as usize;
        let v = match full.get(start) {
            Some(&v) => v,
            None => continue,
        };
        match kind {
            AxisKind::Time => {
                if let Some(units) = read_axis_units(op, coords_root, &name).await {
                    time_value = pgx_zarr_walker::decode_cf_time(&units, v);
                }
            }
            AxisKind::Level => {
                level_value = Some(v);
            }
            AxisKind::Other => {}
        }
    }
    (time_value, level_value)
}

/// Best-effort read of a 1-D coord axis's `units` attribute from its
/// `zarr.json`. Returns `None` on any failure (missing file, bad JSON,
/// no attributes block, no `units` key, non-string value).
async fn read_axis_units(op: &Operator, store_root: &str, axis_name: &str) -> Option<String> {
    let axis_path = if store_root.is_empty() {
        axis_name.to_string()
    } else {
        format!("{}/{}", store_root, axis_name)
    };
    let meta_path = format!("{}/zarr.json", axis_path);
    let bytes = op.read(&meta_path).await.ok()?;
    let v: serde_json::Value = serde_json::from_slice(&bytes.to_vec()).ok()?;
    v.get("attributes")?
        .get("units")?
        .as_str()
        .map(String::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_name_is_zarr() {
        assert_eq!(ZarrReader::new().format_name(), "zarr");
    }

    #[test]
    fn build_store_operator_fs() {
        let (_op, path) = ZarrReader::build_store_operator("fs:///data/era5.zarr").unwrap();
        assert_eq!(path, "data/era5.zarr");
    }

    #[test]
    fn build_store_operator_https() {
        let (_op, path) =
            ZarrReader::build_store_operator("https://example.com/data/era5.zarr").unwrap();
        assert_eq!(path, "data/era5.zarr");
    }

    #[test]
    fn build_store_operator_rejects_bad_uri() {
        let err = ZarrReader::build_store_operator("not a uri").unwrap_err();
        assert!(err.contains("invalid URI"));
    }

    #[test]
    fn parse_chunk_key_v3_default() {
        let (var, idx) = parse_chunk_key("2m_temperature/c/0/5/12").unwrap();
        assert_eq!(var, "2m_temperature");
        assert_eq!(idx, vec![0, 5, 12]);
    }

    #[test]
    fn parse_chunk_key_v2_dot() {
        let (var, idx) = parse_chunk_key("t2m/0.5.12").unwrap();
        assert_eq!(var, "t2m");
        assert_eq!(idx, vec![0, 5, 12]);
    }

    #[test]
    fn parse_chunk_key_single_dim() {
        let (var, idx) = parse_chunk_key("latitude/0").unwrap();
        assert_eq!(var, "latitude");
        assert_eq!(idx, vec![0]);
    }

    #[test]
    fn parse_chunk_key_empty_errors() {
        assert!(parse_chunk_key("").is_err());
        assert!(parse_chunk_key("/").is_err());
    }

    #[test]
    fn parse_chunk_key_garbage_errors() {
        assert!(parse_chunk_key("foo/abc").is_err());
    }

    #[test]
    fn chunk_file_path_default_v3() {
        let enc = default_chunk_key_encoding();
        let path = chunk_file_path("t2m", &[0, 5, 12], &enc);
        assert_eq!(path, "t2m/c/0/5/12");
    }

    #[test]
    fn chunk_file_path_v2() {
        let enc = ChunkKeyEncoding {
            name: "v2".into(),
            configuration: ChunkKeyEncodingConfig {
                separator: ".".into(),
            },
        };
        let path = chunk_file_path("t2m", &[0, 5, 12], &enc);
        assert_eq!(path, "t2m/0.5.12");
    }

    #[test]
    fn cells_per_chunk_product() {
        assert_eq!(cells_per_chunk(&[1, 721, 1440]), 1_038_240);
        assert_eq!(cells_per_chunk(&[10]), 10);
        assert_eq!(cells_per_chunk(&[]), 1); // empty product
    }

    #[test]
    fn chunk_origin_arithmetic() {
        assert_eq!(
            chunk_origin(&[0, 5, 12], &[1, 100, 100]),
            vec![0, 500, 1200]
        );
        assert_eq!(chunk_origin(&[3, 2], &[10, 20]), vec![30, 40]);
    }

    #[test]
    fn decode_f32_little_endian() {
        // Two f32 values: 1.0 and -3.5
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1.0f32.to_le_bytes());
        bytes.extend_from_slice(&(-3.5f32).to_le_bytes());
        let out = decode_chunk_values(
            &bytes,
            "float32",
            &[],
            2,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap();
        assert_eq!(out, vec![1.0, -3.5]);
    }

    #[test]
    fn decode_f32_big_endian() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2.5f32.to_be_bytes());
        let codecs = vec![json!({"name": "bytes", "configuration": {"endian": "big"}})];
        let out = decode_chunk_values(
            &bytes,
            "float32",
            &codecs,
            1,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap();
        assert_eq!(out, vec![2.5]);
    }

    #[test]
    fn decode_f64_little_endian() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&3.14159f64.to_le_bytes());
        bytes.extend_from_slice(&2.71828f64.to_le_bytes());
        let out = decode_chunk_values(
            &bytes,
            "float64",
            &[],
            2,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap();
        assert!((out[0] - 3.14159).abs() < 1e-12);
        assert!((out[1] - 2.71828).abs() < 1e-12);
    }

    #[test]
    fn decode_with_bytes_codec_no_endian_field_defaults_little() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1.0f32.to_le_bytes());
        let codecs = vec![json!({"name": "bytes"})];
        let out = decode_chunk_values(
            &bytes,
            "float32",
            &codecs,
            1,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap();
        assert_eq!(out, vec![1.0]);
    }

    #[test]
    fn decode_rejects_unsupported_codec_chain() {
        let codecs = vec![json!({"name": "bytes"}), json!({"name": "blosc"})];
        let err = decode_chunk_values(
            &[0u8; 4],
            "float32",
            &codecs,
            1,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap_err();
        assert!(err.contains("unsupported codec"), "got: {err}");
        assert!(err.contains("blosc"), "got: {err}");
    }

    #[test]
    fn decode_rejects_size_mismatch() {
        let err = decode_chunk_values(
            &[0u8; 5],
            "float32",
            &[],
            2,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap_err();
        assert!(err.contains("size mismatch"));
    }

    #[test]
    fn decode_rejects_unsupported_dtype() {
        // int8/16/32/64, uint8/16/32/64 and float32/64 are all supported by
        // pgx_zarr_walker; use a still-unsupported dtype like complex64.
        let err = decode_chunk_values(
            &[0u8; 16],
            "complex64",
            &[],
            2,
            &pgx_zarr_walker::CfPacking::identity(),
        )
        .unwrap_err();
        assert!(err.contains("not supported"), "got: {err}");
        assert!(err.contains("complex64"), "got: {err}");
    }

    #[test]
    fn zarr_array_meta_parses_minimal_v3() {
        let json = r#"{
            "zarr_format": 3,
            "node_type": "array",
            "shape": [365, 721, 1440],
            "data_type": "float32",
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [1, 721, 1440]}},
            "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
            "fill_value": "NaN",
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            "dimension_names": ["time", "latitude", "longitude"]
        }"#;
        let meta: ZarrArrayMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.shape, vec![365, 721, 1440]);
        assert_eq!(meta.data_type, "float32");
        assert_eq!(
            meta.chunk_grid.configuration.chunk_shape,
            vec![1, 721, 1440]
        );
        assert_eq!(meta.chunk_key_encoding.name, "default");
        assert_eq!(meta.chunk_key_encoding.configuration.separator, "/");
        assert_eq!(meta.dimension_names.len(), 3);
    }

    #[test]
    fn zarr_array_meta_parses_v2_style_via_defaults() {
        // v2 stores don't always set chunk_key_encoding; we fall back to v3 default.
        let json = r#"{
            "shape": [10, 20],
            "data_type": "float64",
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [10, 20]}}
        }"#;
        let meta: ZarrArrayMeta = serde_json::from_str(json).unwrap();
        assert_eq!(meta.shape, vec![10, 20]);
        assert_eq!(meta.chunk_key_encoding.name, "default");
        assert!(meta.codecs.is_empty());
    }

    /// End-to-end: write a tiny Zarr v3 store to a tempdir, then read it
    /// back through the full reader path (OpenDAL fs → zarr.json →
    /// chunk-file fetch → decode → cells).
    #[tokio::test]
    async fn end_to_end_fs_roundtrip_2d_f32() {
        let dir = std::env::temp_dir().join(format!(
            "pgx_zarr_test_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let store = dir.join("demo.zarr");
        let var_dir = store.join("t2m");
        std::fs::create_dir_all(&var_dir).unwrap();

        // 2D regular grid: 3 lats × 4 lons, single chunk = whole array.
        let zarr_json = r#"{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [3, 4],
            "data_type":   "float32",
            "chunk_grid":  {"name": "regular", "configuration": {"chunk_shape": [3, 4]}},
            "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
            "fill_value":  0,
            "codecs":      [{"name": "bytes", "configuration": {"endian": "little"}}],
            "dimension_names": ["latitude", "longitude"]
        }"#;
        std::fs::write(var_dir.join("zarr.json"), zarr_json).unwrap();

        // 12 cells of f32, values 0.0 .. 11.0
        let mut chunk_bytes = Vec::new();
        for i in 0u32..12 {
            chunk_bytes.extend_from_slice(&(i as f32).to_le_bytes());
        }
        let chunk_subdir = var_dir.join("c");
        std::fs::create_dir_all(&chunk_subdir).unwrap();
        // chunk indices for single-chunk array = [0, 0] → path c/0/0
        let chunk_inner = chunk_subdir.join("0");
        std::fs::create_dir_all(&chunk_inner).unwrap();
        std::fs::write(chunk_inner.join("0"), &chunk_bytes).unwrap();

        // Write the coordinate axes.
        for (name, values) in [
            ("latitude", vec![50.0f32, 51.0, 52.0]),
            ("longitude", vec![2.0f32, 3.0, 4.0, 5.0]),
        ] {
            let axis_dir = store.join(name);
            std::fs::create_dir_all(&axis_dir).unwrap();
            let n = values.len();
            let meta = format!(
                r#"{{
                    "zarr_format": 3,
                    "node_type":   "array",
                    "shape":       [{n}],
                    "data_type":   "float32",
                    "chunk_grid":  {{"name": "regular", "configuration": {{"chunk_shape": [{n}]}}}},
                    "chunk_key_encoding": {{"name": "default", "configuration": {{"separator": "/"}}}},
                    "fill_value":  0,
                    "codecs":      [{{"name": "bytes", "configuration": {{"endian": "little"}}}}]
                }}"#
            );
            std::fs::write(axis_dir.join("zarr.json"), meta).unwrap();
            let mut bytes = Vec::new();
            for v in &values {
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            let axis_chunk_dir = axis_dir.join("c");
            std::fs::create_dir_all(&axis_chunk_dir).unwrap();
            std::fs::write(axis_chunk_dir.join("0"), &bytes).unwrap();
        }

        // Now read it back through the public reader interface.
        let reader = ZarrReader::new();
        let store_uri = format!("fs://{}", store.display());
        let locator = ChunkLocator {
            uri: store_uri,
            byte_offset: None,
            byte_length: None,
            chunk_key: Some("t2m/c/0/0".to_string()),
            packing: None,
        };
        let cells = reader
            .read_chunk(&locator, &CoordFilter::default())
            .await
            .expect("read_chunk should succeed");

        // 3 × 4 = 12 cells, values 0..11.
        assert_eq!(cells.len(), 12);
        let center = cells
            .iter()
            .find(|c| c.lat == Some(51.0) && c.lon == Some(3.0));
        assert!(center.is_some(), "expected cell at (51, 3)");
        // Row-major (lat-major) emission: (lat=50,lon=2) → value 0;
        // (lat=51, lon=3) → value 1*4 + 1 = 5
        assert_eq!(center.unwrap().value, 5.0);

        // bbox filter test
        let filter = CoordFilter {
            bbox_2d: Some(crate::reader::Bbox2D {
                min_lat: 51.0,
                min_lon: 3.0,
                max_lat: 52.0,
                max_lon: 4.0,
            }),
            ..Default::default()
        };
        let filtered = reader.read_chunk(&locator, &filter).await.unwrap();
        // bbox keeps lats {51, 52} × lons {3, 4} = 4 cells
        assert_eq!(filtered.len(), 4);
        for c in &filtered {
            assert!(c.lat.unwrap() >= 51.0 && c.lat.unwrap() <= 52.0);
            assert!(c.lon.unwrap() >= 3.0 && c.lon.unwrap() <= 4.0);
        }

        let _ = std::fs::remove_dir_all(&dir);
    }
}
