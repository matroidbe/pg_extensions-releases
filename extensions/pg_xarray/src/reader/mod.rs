//! `ChunkReader` trait and per-format implementations.
//!
//! Readers decode bytes from object storage (via OpenDAL) and return
//! `Vec<Cell>` for a requested slab. They're stateless — constructed
//! fresh per query — and pure-async so the SRF can call them via a
//! tokio runtime.
//!
//! Per-format implementations live in sibling files and are gated by
//! Cargo features (`reader-zarr`, `reader-grib`, ...).

// `FileHeader` / `VariableMeta` / `ChunkMeta` + `read_header` are SDK
// surface used by the (future) xarray_header processor — they're public
// API regardless of in-tree usage.
#![allow(dead_code)]

pub mod memory;

/// GRIB2 reader. The trait impl is always compiled (so `reader_for("grib2")`
/// can find it); the actual `gribberish` decode path is gated behind the
/// `reader-grib` Cargo feature — without the feature, `read_chunk` returns
/// a clear "feature not enabled" error.
pub mod grib;

/// NetCDF reader (NC3 + NC4). Same feature-gate pattern as GRIB:
/// `reader-netcdf` enables the `netcdf` crate (which transitively needs
/// libnetcdf-dev + libhdf5-dev on the build host).
pub mod netcdf;

/// SELAFIN / SERAFIN reader — TELEMAC's unstructured-mesh binary
/// format for hydraulic simulations. Pure-Rust parser (no external
/// crate); always compiled.
pub mod selafin;

/// Zarr v3 reader. Minimal in-tree implementation supporting the
/// `bytes` codec for f32/f64 — no external Zarr crate dependency.
/// Uses OpenDAL for chunk-file byte fetches. Cloud backends (s3, gcs,
/// azblob) require the corresponding `opendal-*` feature flags.
pub mod zarr;

use async_trait::async_trait;
use std::collections::HashMap;

/// Locator pointing at the bytes of one chunk. Either a byte range
/// inside a file (GRIB/NetCDF/SELAFIN messages) OR a chunk key (Zarr).
///
/// `packing` carries the CF data-packing triple (`scale_factor`,
/// `add_offset`, `_FillValue`) the catalog stored on the parent
/// `pgx.variables` row. The SRF dispatch fills it in once per
/// `pgx.fetch` call; the Zarr reader hands it to
/// `pgx_zarr_walker::decode_chunk_values` so stored ints decode to
/// physical floats. `None` is the no-packing fast path.
#[derive(Debug, Clone, Default)]
pub struct ChunkLocator {
    pub uri: String,
    pub byte_offset: Option<i64>,
    pub byte_length: Option<i64>,
    pub chunk_key: Option<String>,
    pub packing: Option<pgx_zarr_walker::CfPacking>,
}

/// Predicate to apply during decoding to keep memory pressure low.
/// All fields are optional; absent means "no filter on this dimension".
#[derive(Debug, Default, Clone)]
pub struct CoordFilter {
    pub bbox_2d: Option<Bbox2D>,
    pub time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub level_range: Option<(f64, f64)>,
    pub node_ids: Option<Vec<i64>>,
    /// Cap on rows returned (defensive — protect against bbox-over-the-globe queries).
    pub max_cells: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
pub struct Bbox2D {
    pub min_lat: f64,
    pub min_lon: f64,
    pub max_lat: f64,
    pub max_lon: f64,
}

impl Bbox2D {
    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }
}

/// A single decoded cell. The minimal product surface the SRF returns.
/// For higher-dimensional data (level, time, ensemble member), additional
/// fields are populated as appropriate.
#[derive(Debug, Clone, PartialEq)]
pub struct Cell {
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub level: Option<f64>,
    pub time: Option<chrono::DateTime<chrono::Utc>>,
    pub node_id: Option<i64>,
    pub value: f64,
}

/// Header information about what's inside a file — used by
/// `pg_streaming`'s `xarray_header` processor to enumerate chunks
/// without reading values.
#[derive(Debug, Clone)]
pub struct FileHeader {
    pub variables: Vec<VariableMeta>,
    pub mesh_kind: Option<String>,
    pub mesh_motion: Option<String>,
    pub mesh_extent_wkt: Option<String>,
}

#[derive(Debug, Clone)]
pub struct VariableMeta {
    pub name: String,
    pub dtype: String,
    pub dim_order: Vec<String>,
    pub units: Option<String>,
    pub standard_name: Option<String>,
    pub chunks: Vec<ChunkMeta>,
    pub attrs: HashMap<String, String>,
}

/// One chunk discovered inside a file's header.
#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub locator: ChunkLocator,
    pub time_range: Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub level_range: Option<(f64, f64)>,
    pub bbox: Option<Bbox2D>,
    pub estimated_cells: Option<i64>,
}

/// The format-agnostic reader trait. One impl per format.
///
/// Implementations must be `Send + Sync` so they can be called from
/// multiple SRF invocations concurrently. They should be cheap to
/// construct — typically just hold a reference to an OpenDAL operator.
#[async_trait]
pub trait ChunkReader: Send + Sync {
    /// Format name (e.g., "zarr", "grib2", "memory"). Must match
    /// `pgx.datasets.format`.
    fn format_name(&self) -> &'static str;

    /// Decode one chunk and return the cells matching `filter`.
    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String>;

    /// Read only the header of a file — used by `xarray_header`
    /// processor to enumerate variables + chunks without reading
    /// values. Implementations that don't support this can return Err.
    async fn read_header(&self, _uri: &str) -> Result<FileHeader, String> {
        Err(format!(
            "{}: read_header() not implemented for this reader",
            self.format_name()
        ))
    }
}

/// Dispatch to the appropriate reader given a dataset format.
/// Returns `None` if no reader is registered for that format (e.g.,
/// the relevant Cargo feature is disabled).
pub fn reader_for(format: &str) -> Option<Box<dyn ChunkReader>> {
    match format {
        "memory" => Some(Box::new(memory::MemoryReader::new())),
        "zarr" => Some(Box::new(zarr::ZarrReader::new())),
        "grib" | "grib2" => Some(Box::new(grib::GribReader::new())),
        "netcdf" | "nc" => Some(Box::new(netcdf::NetcdfReader::new())),
        "selafin" | "slf" => Some(Box::new(selafin::SelafinReader::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_contains_logic() {
        let b = Bbox2D {
            min_lat: 50.0,
            min_lon: 2.0,
            max_lat: 52.0,
            max_lon: 6.0,
        };
        assert!(b.contains(51.0, 4.0));
        assert!(b.contains(50.0, 2.0)); // corner inclusive
        assert!(b.contains(52.0, 6.0));
        assert!(!b.contains(53.0, 4.0));
        assert!(!b.contains(51.0, 7.0));
        assert!(!b.contains(49.0, 4.0));
    }

    #[test]
    fn reader_for_known_formats() {
        assert!(reader_for("memory").is_some());
        // GRIB dispatch is always available (the gribberish-touching
        // decode is the feature-gated part, not the reader struct).
        assert!(reader_for("grib").is_some());
        assert!(reader_for("grib2").is_some());
        // Zarr is always available — minimal in-tree decoder, no external dep.
        assert!(reader_for("zarr").is_some());
        // NetCDF dispatch always resolves; the `netcdf`-crate decode path
        // is feature-gated like GRIB.
        assert!(reader_for("netcdf").is_some());
        assert!(reader_for("nc").is_some());
    }

    #[test]
    fn reader_for_unknown_format_returns_none() {
        assert!(reader_for("xml").is_none());
        assert!(reader_for("nonexistent").is_none());
    }
}
