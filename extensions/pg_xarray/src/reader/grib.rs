//! GRIB2 reader using the `gribberish` crate.
//!
//! For each chunk:
//!   1. Fetch the byte range `[byte_offset, byte_offset + byte_length)`
//!      from the URI via OpenDAL — typically one HTTP range request
//!      for ~MB-scale GRIB messages.
//!   2. Parse exactly one GRIB2 message from those bytes via gribberish.
//!   3. Extract latitude / longitude / value arrays for the message's
//!      grid.
//!   4. Apply the user's fine bbox filter (post-decode) and the
//!      max_cells cap.
//!   5. Yield `Cell` rows.
//!
//! This reader assumes one chunk row = one GRIB2 message, which is the
//! shape produced by the standard `.idx` sidecar indexer pipeline
//! (Pattern A in `design/pg_xarray/integration.md`).

use super::{Cell, ChunkLocator, ChunkReader, CoordFilter};
use async_trait::async_trait;
use opendal::{Operator, Scheme};
use pgx_zarr_walker::VariableMeta;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Default)]
pub struct GribReader;

impl GribReader {
    pub fn new() -> Self {
        Self
    }

    /// Build an OpenDAL operator for a given URI. The scheme determines
    /// the backend; `config` map is empty here because chunk URIs are
    /// absolute and self-contained.
    fn build_operator(uri: &str) -> Result<(Operator, String), String> {
        let parsed = Url::parse(uri).map_err(|e| format!("grib: invalid URI '{uri}': {e}"))?;
        let scheme_str = parsed.scheme();
        let scheme = Scheme::from_str(scheme_str)
            .map_err(|e| format!("grib: unsupported scheme '{scheme_str}': {e}"))?;

        let mut cfg: std::collections::HashMap<String, String> = Default::default();

        let path: String = match scheme_str {
            "fs" | "file" => {
                // For fs:///var/data/x.grib2, root="/", path="var/data/x.grib2".
                cfg.insert("root".to_string(), "/".to_string());
                parsed.path().trim_start_matches('/').to_string()
            }
            "s3" | "gs" | "azblob" => {
                let bucket = parsed
                    .host_str()
                    .ok_or_else(|| format!("grib: '{uri}' missing bucket"))?
                    .to_string();
                cfg.insert("bucket".to_string(), bucket);
                cfg.insert("anonymous".to_string(), "true".to_string());
                parsed.path().trim_start_matches('/').to_string()
            }
            "http" | "https" => {
                let host = parsed
                    .host_str()
                    .ok_or_else(|| format!("grib: '{uri}' missing host"))?;
                // host_str() drops the port — include it explicitly so
                // localhost / non-default-port endpoints (like the
                // test.sh fixture HTTP server) reach the right server.
                let endpoint = match parsed.port() {
                    Some(p) => format!("{scheme_str}://{host}:{p}"),
                    None => format!("{scheme_str}://{host}"),
                };
                cfg.insert("endpoint".to_string(), endpoint);
                parsed.path().trim_start_matches('/').to_string()
            }
            other => {
                return Err(format!("grib: scheme '{other}' not yet wired up"));
            }
        };

        let op = Operator::via_iter(scheme, cfg)
            .map_err(|e| format!("grib: failed to build operator: {e}"))?;
        Ok((op, path))
    }

    /// Read just the chunk's byte range from the file. Single range
    /// HTTP request for cloud backends; a `pread` for fs.
    async fn fetch_chunk_bytes(
        op: &Operator,
        path: &str,
        byte_offset: Option<i64>,
        byte_length: Option<i64>,
    ) -> Result<Vec<u8>, String> {
        let buf = match (byte_offset, byte_length) {
            (Some(off), Some(len)) if off >= 0 && len > 0 => op
                .read_with(path)
                .range(off as u64..(off as u64 + len as u64))
                .await
                .map_err(|e| format!("grib: range read '{path}' failed: {e}"))?,
            (Some(off), None) if off >= 0 => op
                .read_with(path)
                .range(off as u64..)
                .await
                .map_err(|e| format!("grib: range-from read '{path}' failed: {e}"))?,
            _ => op
                .read(path)
                .await
                .map_err(|e| format!("grib: read '{path}' failed: {e}"))?,
        };
        Ok(buf.to_vec())
    }
}

#[async_trait]
impl ChunkReader for GribReader {
    fn format_name(&self) -> &'static str {
        "grib2"
    }

    async fn read_chunk(
        &self,
        locator: &ChunkLocator,
        filter: &CoordFilter,
    ) -> Result<Vec<Cell>, String> {
        let (op, path) = Self::build_operator(&locator.uri)?;
        let bytes = tokio::time::timeout(
            Duration::from_secs(120),
            Self::fetch_chunk_bytes(&op, &path, locator.byte_offset, locator.byte_length),
        )
        .await
        .map_err(|_| "grib: byte fetch timed out after 120s".to_string())??;

        decode_grib_message(&bytes, filter)
    }
}

// =============================================================================
// Decode logic — separated for testability with synthetic / fixture bytes
// without needing OpenDAL.
// =============================================================================

/// Decode a single GRIB2 message from `bytes` and return matching cells.
///
/// The byte slice is expected to contain a complete GRIB2 message
/// starting at offset 0 (the magic bytes `GRIB`).
#[cfg(feature = "reader-grib")]
pub fn decode_grib_message(bytes: &[u8], filter: &CoordFilter) -> Result<Vec<Cell>, String> {
    use gribberish::message::Message;

    if bytes.len() < 16 || &bytes[0..4] != b"GRIB" {
        return Err(format!(
            "grib: expected GRIB2 magic at offset 0, got '{}'",
            String::from_utf8_lossy(&bytes[0..bytes.len().min(8)])
        ));
    }

    let msg = Message::from_data(bytes, 0)
        .ok_or_else(|| "grib: could not parse message header".to_string())?;

    // Lat/lon arrays come from the LatLngProjection — `latlng_projector()`
    // returns a `LatLngProjection` whose `lat_lng()` returns
    // `(Vec<f64>, Vec<f64>)`. Two shapes are possible depending on the
    // GRIB grid type:
    //
    //   * PlateCaree (regular lat/lon grid — the common case):
    //     lats.len() == n_rows, lons.len() == n_cols,
    //     values.len() == n_rows * n_cols, row-major (j*n_cols + i).
    //   * LambertConformal (and other non-regular): per-cell scattered;
    //     lats.len() == lons.len() == values.len().
    //
    // We detect the shape and iterate accordingly.
    let proj = msg
        .latlng_projector()
        .map_err(|e| format!("grib: latlng_projector(): {e:?}"))?;
    let (lats, lons) = proj.lat_lng();
    let values = msg.data().map_err(|e| format!("grib: data(): {e:?}"))?;

    // Per-cell time + level — used to populate Cell.time/.level on
    // every cell of this message so the SRF returns rows complete
    // enough to JOIN against without re-reading the catalog.
    let cell_time = msg.forecast_date().ok();
    let cell_level = msg.first_fixed_surface().ok().and_then(|(_, v)| v);

    let scattered = lats.len() == values.len() && lons.len() == values.len() && !lats.is_empty();
    let regular = lats.len() * lons.len() == values.len()
        && !lats.is_empty()
        && !lons.is_empty()
        && !scattered;
    if !scattered && !regular {
        return Err(format!(
            "grib: shape mismatch (lats={}, lons={}, values={})",
            lats.len(),
            lons.len(),
            values.len()
        ));
    }

    let max = filter.max_cells.unwrap_or(usize::MAX);
    let mut cells = Vec::with_capacity(values.len().min(max));
    // GRIB lons are often [0, 360); normalise to [-180, 180] for
    // bbox comparison consistency with PostGIS / user expectations.
    let norm = |x: f64| if x > 180.0 { x - 360.0 } else { x };

    if scattered {
        for ((&lat, &lon), &v) in lats.iter().zip(lons.iter()).zip(values.iter()) {
            let lon_n = norm(lon);
            if let Some(b) = &filter.bbox_2d {
                if !b.contains(lat, lon_n) {
                    continue;
                }
            }
            cells.push(Cell {
                lat: Some(lat),
                lon: Some(lon_n),
                level: cell_level,
                time: cell_time,
                node_id: None,
                value: v,
            });
            if cells.len() >= max {
                break;
            }
        }
    } else {
        // Regular grid: outer = row (lat), inner = col (lon),
        // row-major values.
        let n_cols = lons.len();
        for (j, &lat) in lats.iter().enumerate() {
            for (i, &lon) in lons.iter().enumerate() {
                let lon_n = norm(lon);
                if let Some(b) = &filter.bbox_2d {
                    if !b.contains(lat, lon_n) {
                        continue;
                    }
                }
                let v = values[j * n_cols + i];
                cells.push(Cell {
                    lat: Some(lat),
                    lon: Some(lon_n),
                    level: cell_level,
                    time: cell_time,
                    node_id: None,
                    value: v,
                });
                if cells.len() >= max {
                    return Ok(cells);
                }
            }
        }
    }
    Ok(cells)
}

/// Fallback used when the `reader-grib` feature is disabled at build
/// time. Returns a clear error so users know to enable the feature.
#[cfg(not(feature = "reader-grib"))]
pub fn decode_grib_message(_bytes: &[u8], _filter: &CoordFilter) -> Result<Vec<Cell>, String> {
    Err(
        "grib: reader-grib Cargo feature not enabled. Build with `--features reader-grib`."
            .to_string(),
    )
}

/// Extract self-describing metadata from a single GRIB2 message —
/// parameter name → `standard_name`, units → `units`, the GRIB
/// element abbreviation → `long_name`. Per-message metadata; GRIB
/// files in practice are homogeneous across messages of the same
/// parameter, so calling this on the first message gives a fair
/// summary for `register_file`'s variable upsert.
///
/// Currently exposed as a public Rust helper; an SQL wrapper that
/// reads a byte range from a URI and surfaces the result is a
/// follow-up. The `walk_grib` branch of `register_file` will use it
/// once GRIB-via-register_file lands (today users still ingest GRIB
/// through the `xarray_header` pipeline + manual register_chunk).
#[cfg(feature = "reader-grib")]
#[allow(dead_code)]
pub fn inspect_grib_message(bytes: &[u8]) -> Result<VariableMeta, String> {
    use gribberish::message::Message;

    if bytes.len() < 16 || &bytes[0..4] != b"GRIB" {
        return Err("grib: expected GRIB2 magic at offset 0".to_string());
    }
    let msg = Message::from_data(bytes, 0)
        .ok_or_else(|| "grib: could not parse message header".to_string())?;

    let mut meta = VariableMeta::default();
    // gribberish surfaces these as Result<...>; we treat any error
    // as "not available" rather than propagating — the catalog is
    // happier with NULL than with the whole register call failing.
    if let Ok(abbrev) = msg.variable_abbrev() {
        meta.long_name = Some(abbrev);
    }
    if let Ok(unit) = msg.unit() {
        meta.units = Some(unit);
    }
    if let Ok(name) = msg.variable_name() {
        meta.standard_name = Some(name);
    }
    // GRIB samples come back from gribberish::Message::data() as
    // f64 already (already physical-unit), so no CF packing.
    meta.dtype = Some("float64".to_string());
    Ok(meta)
}

#[cfg(not(feature = "reader-grib"))]
pub fn inspect_grib_message(_bytes: &[u8]) -> Result<VariableMeta, String> {
    Err(
        "grib: reader-grib Cargo feature not enabled. Build with `--features reader-grib`."
            .to_string(),
    )
}

// =============================================================================
// File-header walker for `pgx.register_file('...', '...', file.grib2, 'grib2')`
// =============================================================================

/// Walk a GRIB2 file, return one [`pgx_zarr_walker::VariableWalk`] for
/// every message whose parameter abbrev / name / long-name matches
/// `variable`. Each match becomes one `pgx.chunks` row with
/// `byte_offset` + `byte_length` pointing at the message's slab, so
/// the SRF can later fetch just those bytes via OpenDAL.
///
/// Caller spelling is flexible — any of:
///   * GRIB shortname (e.g., `TMP`, `2t`, `t2m`)
///   * GRIB long name (e.g., `Temperature`)
///   * CF-style standard name when the table maps it
/// matches case-insensitively. First-matched message wins the
/// `VariableMeta` (units, standard_name); all matches contribute a
/// `ChunkRecord`. Multiple variables in one file → one `register_file`
/// call per variable; a future `register_grib_file(dataset, uri)` SRF
/// can wrap N of these calls.
#[cfg(feature = "reader-grib")]
pub async fn walk_grib(
    uri: &str,
    variable: &str,
    _dims: &pgx_zarr_walker::DimensionMapping,
) -> Result<pgx_zarr_walker::VariableWalk, String> {
    use gribberish::message::MessageIterator;
    use pgx_zarr_walker::{ChunkRecord, VariableWalk};

    // OpenDAL-backed read — same operator/path pair the GribReader
    // uses for per-message decode. `fs://` falls through to a local
    // pread; `https://` / `s3://` / `gs://` become a single GET (or
    // a small number of range GETs depending on the backend) for the
    // whole file. The walker is a one-time scan at register time —
    // subsequent `pgx.fetch` calls only range-read the matched
    // messages via `GribReader::read_chunk`.
    let (op, path) = GribReader::build_operator(uri)?;
    let buf = op.read(&path).await.map_err(|e| {
        let mut chain = format!("grib: read '{path}': {e}");
        let mut source: Option<&dyn std::error::Error> = std::error::Error::source(&e);
        let mut depth = 1;
        while let Some(s) = source {
            chain.push_str(&format!("\n  source[{depth}]: {s}"));
            source = s.source();
            depth += 1;
        }
        chain
    })?;
    let bytes = buf.to_vec();
    let needle = variable.to_ascii_lowercase();

    let mut chunks: Vec<ChunkRecord> = Vec::new();
    let mut meta: Option<pgx_zarr_walker::VariableMeta> = None;
    let iter = MessageIterator::from_data(&bytes, 0);

    for msg in iter {
        let abbrev = msg.variable_abbrev().ok().unwrap_or_default();
        let name = msg.variable_name().ok().unwrap_or_default();
        let abbrev_lc = abbrev.to_ascii_lowercase();
        let name_lc = name.to_ascii_lowercase();
        if abbrev_lc != needle
            && name_lc != needle
            && !abbrev_lc.contains(&needle)
            && !name_lc.contains(&needle)
        {
            continue;
        }

        let byte_offset = msg.byte_offset() as i64;
        let byte_length = msg.len() as i64;

        // Per-message metadata: bbox from the lat/lon arrays, time
        // from the forecast date, level from the first fixed surface.
        let bbox_wkt = msg.latlng_projector().ok().map(|proj| {
            let (lats, lons) = proj.lat_lng();
            let (lat_min, lat_max) = lats
                .iter()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
                    (a.0.min(x), a.1.max(x))
                });
            let (lon_min_raw, lon_max_raw) = lons
                .iter()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |a, &x| {
                    (a.0.min(x), a.1.max(x))
                });
            // Normalise [0, 360) → [-180, 180] to match the catalog's
            // GIST conventions and what the reader emits at decode time.
            let norm = |x: f64| if x > 180.0 { x - 360.0 } else { x };
            let (lon_min, lon_max) = (norm(lon_min_raw), norm(lon_max_raw));
            format!(
                "POLYGON(({lon_min} {lat_min}, {lon_max} {lat_min}, \
                 {lon_max} {lat_max}, {lon_min} {lat_max}, {lon_min} {lat_min}))"
            )
        });

        let forecast_dt = msg.forecast_date().ok();
        let time_from = forecast_dt;
        let time_to = forecast_dt;

        let z_range = msg
            .first_fixed_surface()
            .ok()
            .and_then(|(_, v)| v.map(|level_value| (level_value, level_value)));

        if meta.is_none() {
            let mut m = pgx_zarr_walker::VariableMeta::default();
            m.dtype = Some("float64".to_string());
            if let Ok(abbrev) = msg.variable_abbrev() {
                m.long_name = Some(abbrev);
            }
            if let Ok(unit) = msg.unit() {
                m.units = Some(unit);
            }
            if let Ok(stdname) = msg.variable_name() {
                m.standard_name = Some(stdname);
            }
            meta = Some(m);
        }

        // chunk_key encodes the byte offset so re-running register_file
        // deduplicates idempotently on (variable, uri, byte_offset).
        let chunk_key = format!("{}@{}", variable, byte_offset);
        chunks.push(ChunkRecord {
            variable: variable.to_string(),
            uri: uri.to_string(),
            chunk_key,
            bbox_wkt,
            time_from,
            time_to,
            z_range,
            byte_offset: Some(byte_offset),
            byte_length: Some(byte_length),
        });
    }

    if chunks.is_empty() {
        return Err(format!(
            "grib: no messages in '{uri}' match variable '{variable}' \
             (matched against shortname, full name, and case-insensitive substring)"
        ));
    }
    let meta = meta.unwrap_or_default();
    Ok(VariableWalk {
        name: variable.to_string(),
        meta,
        chunks,
    })
}

#[cfg(not(feature = "reader-grib"))]
pub async fn walk_grib(
    _uri: &str,
    _variable: &str,
    _dims: &pgx_zarr_walker::DimensionMapping,
) -> Result<pgx_zarr_walker::VariableWalk, String> {
    Err(
        "grib: reader-grib Cargo feature not enabled. Build with `--features reader-grib`."
            .to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::Bbox2D;

    #[test]
    fn format_name_is_grib2() {
        assert_eq!(GribReader::new().format_name(), "grib2");
    }

    #[test]
    fn build_operator_fs_uri() {
        let (_op, path) = GribReader::build_operator("fs:///var/data/x.grib2").unwrap();
        assert_eq!(path, "var/data/x.grib2");
    }

    #[test]
    fn build_operator_s3_anonymous() {
        // Path extraction is what we test here; whether the S3 backend is
        // actually wired depends on Cargo feature flags (`opendal-s3`),
        // which may not be enabled in every build. Accept either:
        //   - Ok((_, path)) — feature enabled, operator built
        //   - Err(...)      — feature not enabled in this build
        let uri = "s3://noaa-gfs-bdp-pds/gfs.20241115/00/atmos/gfs.t00z.pgrb2.0p25.f000";
        match GribReader::build_operator(uri) {
            Ok((_op, path)) => {
                assert_eq!(path, "gfs.20241115/00/atmos/gfs.t00z.pgrb2.0p25.f000");
            }
            Err(e) => {
                // Acceptable: either the s3 scheme isn't compiled in, OR
                // the S3 backend is wired but anonymous + no region; OpenDAL
                // 0.50 dropped automatic region detection from the URI host
                // so building without an explicit region now errors.
                assert!(
                    e.contains("scheme is not enabled")
                        || e.contains("unsupported")
                        || e.contains("not yet wired up")
                        || e.contains("region is missing"),
                    "unexpected error: {e}"
                );
            }
        }
    }

    #[test]
    fn build_operator_https() {
        let (_op, path) =
            GribReader::build_operator("https://opendata.dwd.de/weather/x.grib2").unwrap();
        assert_eq!(path, "weather/x.grib2");
    }

    #[test]
    fn build_operator_http_with_port() {
        // localhost / non-default-port URIs: the port must reach the
        // OpenDAL endpoint string or requests hit the wrong server.
        let (_op, path) =
            GribReader::build_operator("http://127.0.0.1:29980/weather.grib2").unwrap();
        assert_eq!(path, "weather.grib2");
    }

    #[test]
    fn build_operator_rejects_unknown_scheme() {
        let err = GribReader::build_operator("ftp://example.com/x.grib2").unwrap_err();
        assert!(err.contains("not yet wired up") || err.contains("unsupported scheme"));
    }

    #[test]
    fn build_operator_rejects_invalid_uri() {
        let err = GribReader::build_operator("not a uri").unwrap_err();
        assert!(err.contains("invalid URI"));
    }

    #[cfg(not(feature = "reader-grib"))]
    #[test]
    fn decode_without_feature_returns_clear_error() {
        let err = decode_grib_message(b"GRIB....", &CoordFilter::default()).unwrap_err();
        assert!(err.contains("reader-grib"));
    }

    #[cfg(feature = "reader-grib")]
    #[test]
    fn decode_rejects_non_grib_bytes() {
        let err =
            decode_grib_message(b"not a grib at all123456", &CoordFilter::default()).unwrap_err();
        assert!(err.contains("GRIB2 magic"));
    }

    #[test]
    fn bbox_filter_logic_via_synthetic_path() {
        // Sanity-check the bbox filter independently of GRIB decoding.
        let b = Bbox2D {
            min_lat: 50.0,
            min_lon: 2.0,
            max_lat: 52.0,
            max_lon: 6.0,
        };
        assert!(b.contains(51.0, 4.0));
        assert!(!b.contains(45.0, 4.0));
        assert!(!b.contains(51.0, 10.0));
    }

    #[test]
    fn lon_normalization_from_0_360_to_minus_180_180() {
        // The reader normalizes [0, 360) → [-180, 180]. Verify the
        // arithmetic of that mapping here (which is what the inner
        // loop does for each cell).
        fn norm(lon: f64) -> f64 {
            if lon > 180.0 {
                lon - 360.0
            } else {
                lon
            }
        }
        assert_eq!(norm(0.0), 0.0);
        assert_eq!(norm(90.0), 90.0);
        assert_eq!(norm(180.0), 180.0);
        assert_eq!(norm(181.0), -179.0);
        assert_eq!(norm(270.0), -90.0);
        assert_eq!(norm(359.5), -0.5);
    }
}
