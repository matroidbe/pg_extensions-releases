//! NetCDF reader using the `netcdf` Rust crate.
//!
//! Handles both NetCDF-3 classic and NetCDF-4 (HDF5-backed) — the
//! crate transparently dispatches. The `reader-netcdf` Cargo feature
//! gates the actual decode path; without the feature, the trait impl
//! is still compiled (so `reader_for("netcdf")` resolves) but every
//! decode call returns a clear "feature not enabled" error.
//!
//! Build requirement: `libnetcdf-dev` + `libhdf5-dev` system packages.
//! On Debian/Ubuntu the build needs
//! `PKG_CONFIG_PATH=/usr/lib/x86_64-linux-gnu/pkgconfig` so the
//! `hdf5-metno-sys` build script finds the system HDF5 1.14 instead of
//! a too-new Homebrew copy.
//!
//! Catalog model (V1):
//!   * One `pgx.chunks` row PER VARIABLE — whole-variable read.
//!   * `chunk_key` stores the variable name so the reader knows which
//!     variable to decode (the URI is the whole `.nc` file).
//!   * `byte_offset` / `byte_length` are NULL — chunk-internal slicing
//!     is a future optimisation (would enumerate HDF5 chunks).
//!
//! URI handling (V1):
//!   * `fs:///path/to/file.nc` and bare absolute paths supported.
//!   * Remote URIs (s3, gs, http) error with a clear message — the
//!     typical workflow is `pg_streaming` downloading SFTP files to
//!     local disk first, then `register_file` against the local copy.

use super::{Cell, ChunkLocator, ChunkReader, CoordFilter};
use async_trait::async_trait;
#[allow(unused_imports)]
use pgx_zarr_walker::{CfPacking, ChunkRecord, DimensionMapping, VariableMeta, VariableWalk};

#[derive(Debug, Default)]
pub struct NetcdfReader;

impl NetcdfReader {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl ChunkReader for NetcdfReader {
    fn format_name(&self) -> &'static str {
        "netcdf"
    }

    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String> {
        let path = local_path(&locator.uri)?;
        let chunk_key = locator
            .chunk_key
            .as_deref()
            .ok_or_else(|| "netcdf: chunk_key is required".to_string())?
            .to_string();
        // netcdf is sync; spawn_blocking keeps the async runtime healthy.
        let filter = filter.clone();
        tokio::task::spawn_blocking(move || decode_variable(&path, &chunk_key, &filter))
            .await
            .map_err(|e| format!("netcdf: blocking task join: {e}"))?
    }
}

/// Split a chunk_key into `(variable_name, optional chunk indices)`.
/// `"t2m"` → `("t2m", None)`. `"t2m#0,1,2,3"` → `("t2m", Some([0,1,2,3]))`.
fn parse_chunk_key(key: &str) -> Result<(String, Option<Vec<usize>>), String> {
    match key.split_once('#') {
        None => Ok((key.to_string(), None)),
        Some((name, idx_str)) => {
            let indices: Result<Vec<usize>, _> = idx_str
                .split(',')
                .filter(|s| !s.is_empty())
                .map(|s| s.parse::<usize>())
                .collect();
            let indices = indices.map_err(|e| format!("netcdf: bad chunk_key '{key}': {e}"))?;
            Ok((name.to_string(), Some(indices)))
        }
    }
}

/// Walk a NetCDF file's header for `variable` and return a single
/// whole-variable `VariableWalk`. Mirrors `enumerate_zarr_chunks` so
/// `register_file` can drive both formats identically.
pub fn walk_netcdf(
    uri: &str,
    variable: &str,
    dims: &DimensionMapping,
) -> Result<VariableWalk, String> {
    let path = local_path(uri)?;
    walk_netcdf_local(&path, variable, dims)
}

/// Strip `fs://` / `file://` prefix and return a local filesystem
/// path. Remote URIs are rejected with a clear message — V1 supports
/// only local files.
fn local_path(uri: &str) -> Result<String, String> {
    if let Some(rest) = uri.strip_prefix("fs://") {
        Ok(rest.to_string())
    } else if let Some(rest) = uri.strip_prefix("file://") {
        Ok(rest.to_string())
    } else if uri.starts_with('/') {
        Ok(uri.to_string())
    } else {
        Err(format!(
            "netcdf: URI '{uri}' not supported — V1 reads only local files (fs:// or absolute path). \
             Download to local disk first (e.g., via pg_streaming opendal_sink) and re-register."
        ))
    }
}

// =============================================================================
// Feature-gated implementations
// =============================================================================

#[cfg(feature = "reader-netcdf")]
fn walk_netcdf_local(
    path: &str,
    variable: &str,
    dims: &DimensionMapping,
) -> Result<VariableWalk, String> {
    let file = netcdf::open(path).map_err(|e| format!("netcdf: open '{path}': {e}"))?;
    let var = file
        .variable(variable)
        .ok_or_else(|| format!("netcdf: variable '{variable}' not found in '{path}'"))?;

    // CF attributes (same for every chunk of this variable).
    let units = attr_string(&var, "units");
    let standard_name = attr_string(&var, "standard_name");
    let long_name = attr_string(&var, "long_name");
    let scale_factor = attr_f64(&var, "scale_factor");
    let add_offset = attr_f64(&var, "add_offset");
    let fill_value = attr_f64(&var, "_FillValue").or_else(|| attr_f64(&var, "missing_value"));
    let valid_min = attr_f64(&var, "valid_min");
    let valid_max = attr_f64(&var, "valid_max");

    let meta = VariableMeta {
        dtype: Some(nc_type_name(var.vartype())),
        dim_order: var
            .dimensions()
            .iter()
            .map(|d| Some(d.name().to_string()))
            .collect(),
        units,
        standard_name,
        long_name,
        packing: cf_packing(scale_factor, add_offset, fill_value),
        valid_min,
        valid_max,
        raw_attrs: serde_json::Value::Object(serde_json::Map::new()),
    };

    // Auto-detect axes — same heuristic as the Zarr walker.
    let dim_names: Vec<String> = var.dimensions().iter().map(|d| d.name().into()).collect();
    let dim_lens: Vec<usize> = var.dimensions().iter().map(|d| d.len()).collect();
    let lat_axis = pick_axis(&dim_names, dims.lat_axis.as_deref(), LAT_HINTS);
    let lon_axis = pick_axis(&dim_names, dims.lon_axis.as_deref(), LON_HINTS);
    let time_axis = pick_axis(&dim_names, dims.time_axis.as_deref(), TIME_HINTS);
    let z_axis = pick_axis(&dim_names, dims.z_axis.as_deref(), Z_HINTS);

    // Coord vectors (read once; small 1-D arrays — cheap).
    let lat_vals = lat_axis
        .as_deref()
        .and_then(|n| coord_f64_vector(&file, n).ok());
    let lon_vals = lon_axis
        .as_deref()
        .and_then(|n| coord_f64_vector(&file, n).ok());
    let z_vals = z_axis
        .as_deref()
        .and_then(|n| coord_f64_vector(&file, n).ok());
    let time_dts = time_axis
        .as_deref()
        .and_then(|n| coord_time_vector(&file, n).ok());

    let lat_idx = lat_axis
        .as_ref()
        .and_then(|n| dim_names.iter().position(|d| d == n));
    let lon_idx = lon_axis
        .as_ref()
        .and_then(|n| dim_names.iter().position(|d| d == n));
    let z_idx = z_axis
        .as_ref()
        .and_then(|n| dim_names.iter().position(|d| d == n));
    let time_idx = time_axis
        .as_ref()
        .and_then(|n| dim_names.iter().position(|d| d == n));

    // Decide whether to emit one row per HDF5 chunk (V2 — the path that
    // makes 100 GB ERA5 tractable) or a single contiguous row (NC3 /
    // NC4-unchunked).
    let chunking = var
        .chunking()
        .map_err(|e| format!("netcdf: chunking('{variable}'): {e}"))?;
    let uri_out = format!("fs://{path}");

    let chunks: Vec<ChunkRecord> = match chunking {
        Some(chunk_shape) if chunk_shape.len() == dim_lens.len() => {
            // Per-HDF5-chunk catalog rows. Enumerate every chunk position
            // across the cartesian product of chunk_grid[i].
            let chunk_grid: Vec<usize> = dim_lens
                .iter()
                .zip(chunk_shape.iter())
                .map(|(&dim, &cs)| dim.div_ceil(cs.max(1)))
                .collect();
            let total_chunks: usize = chunk_grid.iter().product();
            let mut out = Vec::with_capacity(total_chunks);
            for chunk_idx_flat in 0..total_chunks {
                let chunk_idx = unravel_index(chunk_idx_flat, &row_major_strides(&chunk_grid));
                let ranges: Vec<std::ops::Range<usize>> = chunk_idx
                    .iter()
                    .zip(chunk_shape.iter().zip(dim_lens.iter()))
                    .map(|(&ci, (&cs, &dim))| {
                        let lo = ci * cs;
                        let hi = ((ci + 1) * cs).min(dim);
                        lo..hi
                    })
                    .collect();
                // Per-chunk bbox / time / z come from slicing the
                // already-read coord arrays at this chunk's index range.
                let bbox_wkt = chunk_bbox(lat_idx, lon_idx, &lat_vals, &lon_vals, &ranges);
                let (time_from, time_to) = chunk_time_range(time_idx, time_dts.as_ref(), &ranges);
                let z_range = chunk_z_range(z_idx, z_vals.as_ref(), &ranges);
                let key = format!(
                    "{}#{}",
                    variable,
                    chunk_idx
                        .iter()
                        .map(|i| i.to_string())
                        .collect::<Vec<_>>()
                        .join(",")
                );
                out.push(ChunkRecord {
                    variable: variable.to_string(),
                    uri: uri_out.clone(),
                    chunk_key: key,
                    bbox_wkt,
                    time_from,
                    time_to,
                    z_range,
                    byte_offset: None,
                    byte_length: None,
                });
            }
            out
        }
        _ => {
            // Contiguous (NC3 or unchunked NC4) — fall back to one
            // whole-variable ChunkRecord with file-wide coord extents.
            let bbox_wkt = match (&lat_axis, &lon_axis) {
                (Some(lat), Some(lon)) => coord_bbox(&file, lat, lon),
                _ => None,
            };
            let (time_from, time_to) = match &time_axis {
                Some(t) => coord_time_range(&file, t),
                None => (None, None),
            };
            let z_range = match &z_axis {
                Some(z) => coord_range(&file, z),
                None => None,
            };
            vec![ChunkRecord {
                variable: variable.to_string(),
                uri: uri_out,
                chunk_key: variable.to_string(),
                bbox_wkt,
                time_from,
                time_to,
                z_range,
                byte_offset: None,
                byte_length: None,
            }]
        }
    };

    Ok(VariableWalk {
        name: variable.to_string(),
        meta,
        chunks,
    })
}

#[cfg(feature = "reader-netcdf")]
fn decode_variable(path: &str, chunk_key: &str, filter: &CoordFilter) -> Result<Vec<Cell>, String> {
    let (variable, chunk_idx) = parse_chunk_key(chunk_key)?;
    let file = netcdf::open(path).map_err(|e| format!("netcdf: open '{path}': {e}"))?;
    let var = file
        .variable(&variable)
        .ok_or_else(|| format!("netcdf: variable '{variable}' not found in '{path}'"))?;

    let scale = attr_f64(&var, "scale_factor").unwrap_or(1.0);
    let offset = attr_f64(&var, "add_offset").unwrap_or(0.0);
    let fill = attr_f64(&var, "_FillValue").or_else(|| attr_f64(&var, "missing_value"));

    let dim_names: Vec<String> = var.dimensions().iter().map(|d| d.name().into()).collect();
    let dim_lens: Vec<usize> = var.dimensions().iter().map(|d| d.len()).collect();

    // Build slab ranges: per-HDF5-chunk if we have indices, else whole
    // variable. This is the "only fetch the bytes the catalog routed
    // us to" step that makes 100 GB ERA5 tractable.
    let (slab_ranges, slab_offsets) = match chunk_idx {
        Some(indices) => {
            let chunk_shape = var
                .chunking()
                .map_err(|e| format!("netcdf: chunking('{variable}'): {e}"))?
                .ok_or_else(|| {
                    format!(
                        "netcdf: chunk_key '{chunk_key}' has indices but variable is contiguous"
                    )
                })?;
            if indices.len() != chunk_shape.len() {
                return Err(format!(
                    "netcdf: chunk_key '{chunk_key}' has {} indices, variable has {} dims",
                    indices.len(),
                    chunk_shape.len()
                ));
            }
            let ranges: Vec<std::ops::Range<usize>> = indices
                .iter()
                .zip(chunk_shape.iter().zip(dim_lens.iter()))
                .map(|(&ci, (&cs, &dim))| {
                    let lo = ci * cs;
                    let hi = ((ci + 1) * cs).min(dim);
                    lo..hi
                })
                .collect();
            let offsets: Vec<usize> = ranges.iter().map(|r| r.start).collect();
            (ranges, offsets)
        }
        None => {
            // Whole-variable read (contiguous NC3 / unchunked NC4).
            let ranges: Vec<std::ops::Range<usize>> = dim_lens.iter().map(|&n| 0..n).collect();
            let offsets = vec![0usize; dim_lens.len()];
            (ranges, offsets)
        }
    };
    let slab_lens: Vec<usize> = slab_ranges.iter().map(|r| r.end - r.start).collect();

    let values: Vec<f64> = var
        .get_values::<f64, _>(slab_ranges.clone())
        .map_err(|e| format!("netcdf: get_values('{variable}'): {e}"))?;

    // Coord vectors over the *file* (not the slab). We index into them
    // using the global (slab_offset + local_idx) position, so coord
    // values for time/lat/lon/level remain absolute regardless of slab.
    let lat_axis_idx = find_axis(&dim_names, LAT_HINTS);
    let lon_axis_idx = find_axis(&dim_names, LON_HINTS);
    let time_axis_idx = find_axis(&dim_names, TIME_HINTS);
    let z_axis_idx = find_axis(&dim_names, Z_HINTS);
    let lat_vals = lat_axis_idx.and_then(|i| coord_f64_vector(&file, &dim_names[i]).ok());
    let lon_vals = lon_axis_idx.and_then(|i| coord_f64_vector(&file, &dim_names[i]).ok());
    let z_vals = z_axis_idx.and_then(|i| coord_f64_vector(&file, &dim_names[i]).ok());
    let time_dts = time_axis_idx.and_then(|i| coord_time_vector(&file, &dim_names[i]).ok());

    let mut cells = Vec::new();
    let max = filter.max_cells.unwrap_or(usize::MAX);
    let strides = row_major_strides(&slab_lens);
    for (flat_idx, raw) in values.iter().copied().enumerate() {
        let local = unravel_index(flat_idx, &strides);
        // Map slab-local index → global index along each dim.
        let global: Vec<usize> = local
            .iter()
            .zip(slab_offsets.iter())
            .map(|(&l, &off)| l + off)
            .collect();
        let physical = if fill
            .map(|f| (raw - f).abs() < f64::EPSILON)
            .unwrap_or(false)
        {
            f64::NAN
        } else {
            raw * scale + offset
        };
        let lat = lat_axis_idx.and_then(|ax| lat_vals.as_ref().map(|v| v[global[ax]]));
        let lon = lon_axis_idx.and_then(|ax| lon_vals.as_ref().map(|v| v[global[ax]]));
        let level = z_axis_idx.and_then(|ax| z_vals.as_ref().map(|v| v[global[ax]]));
        let time = time_axis_idx.and_then(|ax| time_dts.as_ref().map(|v| v[global[ax]]));
        if let (Some(b), Some(la), Some(lo)) = (filter.bbox_2d.as_ref(), lat, lon) {
            if !b.contains(la, lo) {
                continue;
            }
        }
        cells.push(Cell {
            lat,
            lon,
            level,
            time,
            node_id: None,
            value: physical,
        });
        if cells.len() >= max {
            break;
        }
    }
    Ok(cells)
}

// ----- helpers ----------------------------------------------------------------

#[cfg(feature = "reader-netcdf")]
const LAT_HINTS: &[&str] = &["latitude", "lat", "y"];
#[cfg(feature = "reader-netcdf")]
const LON_HINTS: &[&str] = &["longitude", "lon", "x"];
#[cfg(feature = "reader-netcdf")]
const TIME_HINTS: &[&str] = &["time", "valid_time", "forecast_time", "t"];
#[cfg(feature = "reader-netcdf")]
const Z_HINTS: &[&str] = &[
    "level", "lev", "z", "depth", "altitude", "height", "plev", "pressure",
];

#[cfg(feature = "reader-netcdf")]
fn pick_axis(dim_names: &[String], explicit: Option<&str>, hints: &[&str]) -> Option<String> {
    if let Some(name) = explicit {
        if dim_names.iter().any(|d| d == name) {
            return Some(name.to_string());
        }
    }
    for hint in hints {
        if let Some(name) = dim_names
            .iter()
            .find(|d| d.eq_ignore_ascii_case(hint))
            .cloned()
        {
            return Some(name);
        }
    }
    None
}

#[cfg(feature = "reader-netcdf")]
fn find_axis(dim_names: &[String], hints: &[&str]) -> Option<usize> {
    for hint in hints {
        if let Some(idx) = dim_names.iter().position(|d| d.eq_ignore_ascii_case(hint)) {
            return Some(idx);
        }
    }
    None
}

#[cfg(feature = "reader-netcdf")]
fn attr_string(var: &netcdf::Variable, name: &str) -> Option<String> {
    let attr = var.attribute(name)?;
    match attr.value().ok()? {
        netcdf::AttributeValue::Str(s) => Some(s),
        _ => None,
    }
}

#[cfg(feature = "reader-netcdf")]
fn attr_f64(var: &netcdf::Variable, name: &str) -> Option<f64> {
    let attr = var.attribute(name)?;
    match attr.value().ok()? {
        netcdf::AttributeValue::Float(v) => Some(v as f64),
        netcdf::AttributeValue::Double(v) => Some(v),
        netcdf::AttributeValue::Schar(v) => Some(v as f64),
        netcdf::AttributeValue::Uchar(v) => Some(v as f64),
        netcdf::AttributeValue::Short(v) => Some(v as f64),
        netcdf::AttributeValue::Ushort(v) => Some(v as f64),
        netcdf::AttributeValue::Int(v) => Some(v as f64),
        netcdf::AttributeValue::Uint(v) => Some(v as f64),
        netcdf::AttributeValue::Longlong(v) => Some(v as f64),
        netcdf::AttributeValue::Ulonglong(v) => Some(v as f64),
        _ => None,
    }
}

#[cfg(feature = "reader-netcdf")]
fn nc_type_name(t: netcdf::types::NcVariableType) -> String {
    use netcdf::types::{FloatType, IntType, NcVariableType};
    match t {
        NcVariableType::Int(IntType::I8) => "int8".into(),
        NcVariableType::Int(IntType::I16) => "int16".into(),
        NcVariableType::Int(IntType::I32) => "int32".into(),
        NcVariableType::Int(IntType::I64) => "int64".into(),
        NcVariableType::Int(IntType::U8) => "uint8".into(),
        NcVariableType::Int(IntType::U16) => "uint16".into(),
        NcVariableType::Int(IntType::U32) => "uint32".into(),
        NcVariableType::Int(IntType::U64) => "uint64".into(),
        NcVariableType::Float(FloatType::F32) => "float32".into(),
        NcVariableType::Float(FloatType::F64) => "float64".into(),
        other => format!("{:?}", other),
    }
}

#[cfg(feature = "reader-netcdf")]
fn cf_packing(scale: Option<f64>, offset: Option<f64>, fill: Option<f64>) -> Option<CfPacking> {
    if scale.is_none() && offset.is_none() && fill.is_none() {
        return None;
    }
    Some(CfPacking {
        scale: scale.unwrap_or(1.0),
        offset: offset.unwrap_or(0.0),
        fill_value: fill,
    })
}

#[cfg(feature = "reader-netcdf")]
fn coord_f64_vector(file: &netcdf::File, name: &str) -> Result<Vec<f64>, String> {
    let v = file
        .variable(name)
        .ok_or_else(|| format!("netcdf: coord var '{name}' not found"))?;
    v.get_values::<f64, _>(..)
        .map_err(|e| format!("netcdf: get_values('{name}'): {e}"))
}

/// Per-HDF5-chunk bbox: slice the already-read lat/lon coord arrays at
/// the chunk's index ranges and return the envelope. None if either
/// axis is missing from the variable.
#[cfg(feature = "reader-netcdf")]
fn chunk_bbox(
    lat_idx: Option<usize>,
    lon_idx: Option<usize>,
    lat_vals: &Option<Vec<f64>>,
    lon_vals: &Option<Vec<f64>>,
    ranges: &[std::ops::Range<usize>],
) -> Option<String> {
    let lat_idx = lat_idx?;
    let lon_idx = lon_idx?;
    let lats = lat_vals.as_ref()?;
    let lons = lon_vals.as_ref()?;
    let lr = &ranges[lat_idx];
    let or = &ranges[lon_idx];
    if lr.is_empty() || or.is_empty() {
        return None;
    }
    let (lat_min, lat_max) = lats[lr.clone()]
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
            (a.0.min(x), a.1.max(x))
        });
    let (lon_min, lon_max) = lons[or.clone()]
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
            (a.0.min(x), a.1.max(x))
        });
    Some(format!(
        "POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, {lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"
    ))
}

/// Per-HDF5-chunk time range — slice the time coord at the chunk's
/// time-axis range.
#[cfg(feature = "reader-netcdf")]
fn chunk_time_range(
    time_idx: Option<usize>,
    time_dts: Option<&Vec<chrono::DateTime<chrono::Utc>>>,
    ranges: &[std::ops::Range<usize>],
) -> (
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
) {
    let (Some(ax), Some(times)) = (time_idx, time_dts) else {
        return (None, None);
    };
    let r = &ranges[ax];
    if r.is_empty() {
        return (None, None);
    }
    let slab = &times[r.clone()];
    let lo = *slab.iter().min().unwrap();
    let hi = *slab.iter().max().unwrap();
    (Some(lo), Some(hi))
}

/// Per-HDF5-chunk Z range — slice the level coord at the chunk's
/// z-axis range.
#[cfg(feature = "reader-netcdf")]
fn chunk_z_range(
    z_idx: Option<usize>,
    z_vals: Option<&Vec<f64>>,
    ranges: &[std::ops::Range<usize>],
) -> Option<(f64, f64)> {
    let ax = z_idx?;
    let z = z_vals?;
    let r = &ranges[ax];
    if r.is_empty() {
        return None;
    }
    let (lo, hi) = z[r.clone()]
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
            (a.0.min(x), a.1.max(x))
        });
    Some((lo, hi))
}

#[cfg(feature = "reader-netcdf")]
fn coord_bbox(file: &netcdf::File, lat: &str, lon: &str) -> Option<String> {
    let lats = coord_f64_vector(file, lat).ok()?;
    let lons = coord_f64_vector(file, lon).ok()?;
    if lats.is_empty() || lons.is_empty() {
        return None;
    }
    let (lat_min, lat_max) = lats
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
            (a.0.min(x), a.1.max(x))
        });
    let (lon_min, lon_max) = lons
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
            (a.0.min(x), a.1.max(x))
        });
    Some(format!(
        "POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, {lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"
    ))
}

#[cfg(feature = "reader-netcdf")]
fn coord_range(file: &netcdf::File, name: &str) -> Option<(f64, f64)> {
    let v = coord_f64_vector(file, name).ok()?;
    if v.is_empty() {
        return None;
    }
    let (lo, hi) = v.iter().fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
        (a.0.min(x), a.1.max(x))
    });
    Some((lo, hi))
}

#[cfg(feature = "reader-netcdf")]
fn coord_time_range(
    file: &netcdf::File,
    name: &str,
) -> (
    Option<chrono::DateTime<chrono::Utc>>,
    Option<chrono::DateTime<chrono::Utc>>,
) {
    let dts = match coord_time_vector(file, name) {
        Ok(v) if !v.is_empty() => v,
        _ => return (None, None),
    };
    let lo = *dts.iter().min().unwrap();
    let hi = *dts.iter().max().unwrap();
    (Some(lo), Some(hi))
}

#[cfg(feature = "reader-netcdf")]
fn coord_time_vector(
    file: &netcdf::File,
    name: &str,
) -> Result<Vec<chrono::DateTime<chrono::Utc>>, String> {
    let raw = coord_f64_vector(file, name)?;
    let v = file
        .variable(name)
        .ok_or_else(|| format!("netcdf: coord var '{name}' not found"))?;
    let units = attr_string(&v, "units")
        .ok_or_else(|| format!("netcdf: coord var '{name}' has no 'units' attribute"))?;
    let mut out = Vec::with_capacity(raw.len());
    for x in raw {
        let dt = pgx_zarr_walker::decode_cf_time(&units, x).ok_or_else(|| {
            format!("netcdf: decode_cf_time('{units}', {x}) returned None — unsupported CF units?")
        })?;
        out.push(dt);
    }
    Ok(out)
}

#[cfg(feature = "reader-netcdf")]
fn row_major_strides(dim_lens: &[usize]) -> Vec<usize> {
    if dim_lens.is_empty() {
        return Vec::new();
    }
    let mut out = vec![1usize; dim_lens.len()];
    for i in (0..dim_lens.len() - 1).rev() {
        out[i] = out[i + 1] * dim_lens[i + 1];
    }
    out
}

#[cfg(feature = "reader-netcdf")]
fn unravel_index(flat: usize, strides: &[usize]) -> Vec<usize> {
    let mut rem = flat;
    let mut out = Vec::with_capacity(strides.len());
    for &s in strides {
        out.push(rem / s);
        rem %= s;
    }
    out
}

// ----- feature-disabled stubs -------------------------------------------------

#[cfg(not(feature = "reader-netcdf"))]
fn walk_netcdf_local(
    _path: &str,
    _variable: &str,
    _dims: &DimensionMapping,
) -> Result<VariableWalk, String> {
    Err(
        "netcdf: reader-netcdf Cargo feature not enabled. Build with `--features reader-netcdf`."
            .to_string(),
    )
}

#[cfg(not(feature = "reader-netcdf"))]
fn decode_variable(
    _path: &str,
    _variable: &str,
    _filter: &CoordFilter,
) -> Result<Vec<Cell>, String> {
    Err(
        "netcdf: reader-netcdf Cargo feature not enabled. Build with `--features reader-netcdf`."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_name_is_netcdf() {
        assert_eq!(NetcdfReader::new().format_name(), "netcdf");
    }

    #[test]
    fn local_path_strips_fs_prefix() {
        assert_eq!(local_path("fs:///tmp/x.nc").unwrap(), "/tmp/x.nc");
        assert_eq!(local_path("/tmp/x.nc").unwrap(), "/tmp/x.nc");
        assert_eq!(local_path("file:///tmp/x.nc").unwrap(), "/tmp/x.nc");
    }

    #[test]
    fn local_path_rejects_remote_uris() {
        let err = local_path("s3://bucket/x.nc").unwrap_err();
        assert!(err.contains("V1 reads only local files"));
    }

    #[cfg(feature = "reader-netcdf")]
    #[test]
    fn strides_match_row_major() {
        assert_eq!(row_major_strides(&[3, 4, 5]), vec![20, 5, 1]);
        assert_eq!(row_major_strides(&[2]), vec![1]);
        assert!(row_major_strides(&[]).is_empty());
    }

    #[cfg(feature = "reader-netcdf")]
    #[test]
    fn unravel_inverts_ravel() {
        let strides = row_major_strides(&[3, 4]);
        assert_eq!(unravel_index(0, &strides), vec![0, 0]);
        assert_eq!(unravel_index(5, &strides), vec![1, 1]);
        assert_eq!(unravel_index(11, &strides), vec![2, 3]);
    }
}
