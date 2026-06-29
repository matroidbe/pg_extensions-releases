//! `pgx.fetch()` — the user-facing query SRF.
//!
//! Flow:
//!   1. PG plans a query against the catalog using the dataset + variable
//!      + (time, bbox) predicates, hitting the B-tree and GIST indexes.
//!   2. For each candidate chunk row, dispatch to the format's reader.
//!   3. Reader issues a (single, range-bound) byte fetch via OpenDAL
//!      and decodes only the cells matching the fine filter.
//!   4. Return rows.

use crate::reader::{reader_for, Bbox2D, Cell, ChunkLocator, CoordFilter};
use pgrx::prelude::*;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// One row returned by `pgx.fetch()`.
type FetchRow = (
    Option<f64>,                                // lat
    Option<f64>,                                // lon
    Option<f64>,                                // level
    Option<pgrx::datum::TimestampWithTimeZone>, // time
    f64,                                        // value
);

/// Look up candidate chunks for `(dataset, variable, time, bbox)` and
/// decode each via the appropriate `ChunkReader`. Returns a flattened
/// `Vec<FetchRow>`.
///
/// `max_cells` caps total returned rows defensively to avoid an
/// accidental whole-grid blowout.
pub fn fetch_impl(
    dataset: &str,
    variable: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    level_from: Option<f64>,
    level_to: Option<f64>,
    max_cells: i32,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
) -> Vec<FetchRow> {
    if dataset.is_empty() || variable.is_empty() {
        pgrx::error!("pgx.fetch: dataset and variable are required");
    }
    if max_cells <= 0 {
        pgrx::error!("pgx.fetch: max_cells must be positive (got {})", max_cells);
    }

    // No predicate at all means a full-catalog scan that touches every
    // chunk for the variable. For real datasets that's many GB of bytes
    // pulled from object storage and decoded in PG memory. Warn loudly —
    // production queries should always carry at least one of bbox / time
    // / level so the GIST + range indexes can prune.
    if at_time.is_none()
        && bbox_wkt.is_none()
        && level_from.is_none()
        && level_to.is_none()
        && time_from.is_none()
        && time_to.is_none()
    {
        pgrx::warning!(
            "pgx.fetch('{}','{}'): called with no bbox / time / level \
             predicate. This forces a full-dataset scan — every chunk \
             is fetched and decoded. Supply at least one of (at_time, \
             time_from/to, bbox_wkt, level_from/to) so the catalog \
             GIST/range indexes can prune.",
            dataset,
            variable
        );
    }

    // 1. Catalog lookup — collect candidate chunks.
    let candidates = lookup_candidates(
        dataset, variable, at_time, bbox_wkt, level_from, level_to, time_from, time_to,
    );
    if candidates.is_empty() {
        return Vec::new();
    }

    // 2. Build CoordFilter once.
    let filter = CoordFilter {
        bbox_2d: bbox_wkt.and_then(parse_envelope_wkt),
        level_range: match (level_from, level_to) {
            (Some(f), Some(t)) => Some((f, t)),
            _ => None,
        },
        max_cells: Some(max_cells as usize),
        ..Default::default()
    };

    // Look up the variable's CF packing once — every chunk for this
    // (dataset, variable) shares the same scale/offset/fill. None
    // means no packing needed (the common already-float case); the
    // reader's `CfPacking::identity()` fast-path keeps existing
    // stores byte-identical.
    let packing = lookup_variable_packing(dataset, variable);

    // 3. Decode each chunk on the dedicated tokio runtime.
    let mut rows: Vec<FetchRow> = Vec::new();
    let rt = runtime();
    for candidate in candidates {
        let reader = match reader_for(&candidate.format) {
            Some(r) => r,
            None => {
                pgrx::warning!(
                    "pgx.fetch: no reader registered for format '{}' (chunk {})",
                    candidate.format,
                    candidate.chunk_id
                );
                continue;
            }
        };
        let locator = ChunkLocator {
            uri: candidate.uri,
            byte_offset: candidate.byte_offset,
            byte_length: candidate.byte_length,
            chunk_key: candidate.chunk_key,
            packing,
        };
        let cells_result = rt.block_on(async { reader.read_chunk(&locator, &filter).await });
        match cells_result {
            Ok(cells) => {
                for c in cells {
                    rows.push(cell_to_row(c));
                    if rows.len() >= max_cells as usize {
                        return rows;
                    }
                }
            }
            Err(e) => {
                pgrx::warning!(
                    "pgx.fetch: reader failed for chunk {}: {}",
                    candidate.chunk_id,
                    e
                );
            }
        }
    }
    rows
}

/// chrono::DateTime is Unix-epoch-based (microseconds since 1970-01-01);
/// pgrx's `TimestampWithTimeZone::try_from(i64)` interprets its argument
/// as microseconds since the PG epoch (2000-01-01). Subtract the
/// 30-year offset so the SRF returns the right timestamp.
pub(crate) const PG_EPOCH_OFFSET_MICROS: i64 = 946_684_800 * 1_000_000;

fn cell_to_row(c: Cell) -> FetchRow {
    let time = c.time.and_then(|t| {
        pgrx::datum::TimestampWithTimeZone::try_from(t.timestamp_micros() - PG_EPOCH_OFFSET_MICROS)
            .ok()
    });
    (c.lat, c.lon, c.level, time, c.value)
}

/// A row collected from the catalog lookup, fully owned (no borrows
/// of the SPI client).
struct Candidate {
    chunk_id: i64,
    format: String,
    uri: String,
    byte_offset: Option<i64>,
    byte_length: Option<i64>,
    chunk_key: Option<String>,
}

fn lookup_candidates(
    dataset: &str,
    variable: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    level_from: Option<f64>,
    level_to: Option<f64>,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
) -> Vec<Candidate> {
    // The SQL builds the filter incrementally so that the GIST indexes
    // are hit only when the relevant predicate is present.
    // The `bbox_wkt` predicate is interpreted in the variable's
    // effective SRID — its own `srid` if set, else the dataset's
    // `default_srid`. PostGIS's `&&` operator requires both sides to
    // share an SRID, so we tag the user's WKT with the same SRID the
    // catalog stored the bbox under.
    //
    // Time can come in as either:
    //   * `at_time` — exact point: chunk's time_range must contain it
    //   * `time_from`/`time_to` — overlap with `tstzrange($f,$t,'[]')`
    //     using `&&`. Either endpoint may be NULL (= open interval).
    // The two forms compose: a query that supplies both is an
    // AND-join. Most callers will use only one.
    let sql = r#"
        SELECT c.id, d.format, c.uri, c.byte_offset, c.byte_length, c.chunk_key
        FROM pgx.chunks c
        JOIN pgx.variables v ON v.id = c.variable_id
        JOIN pgx.datasets  d ON d.id = v.dataset_id
        WHERE d.name = $1
          AND v.name = $2
          AND ($3::timestamptz IS NULL OR c.time_range @> $3::timestamptz)
          AND ($4::text IS NULL
               OR c.bbox_envelope IS NULL
               -- Schema-qualify functions AND operators so CREATE/REFRESH
               -- MATERIALIZED VIEW works: those ops force search_path =
               -- pg_catalog, pg_temp for security, so unqualified PostGIS
               -- names go missing. `OPERATOR(public.&&)` is the syntax for
               -- a schema-qualified operator.
               OR c.bbox_envelope OPERATOR(public.&&) public.ST_GeomFromText($4,
                     COALESCE(v.srid, d.default_srid, 4326)))
          AND ($5::numeric IS NULL OR $6::numeric IS NULL
               OR c.level_range IS NULL
               OR c.level_range && numrange($5, $6, '[]'))
          AND (($7::timestamptz IS NULL AND $8::timestamptz IS NULL)
               OR c.time_range IS NULL
               OR c.time_range && tstzrange($7, $8, '[]'))
        ORDER BY c.id
    "#;

    Spi::connect(|client| {
        let table = client.select(
            sql,
            None,
            &[
                dataset.into(),
                variable.into(),
                at_time.into(),
                bbox_wkt.into(),
                level_from
                    .map(pgrx::AnyNumeric::try_from)
                    .and_then(Result::ok)
                    .into(),
                level_to
                    .map(pgrx::AnyNumeric::try_from)
                    .and_then(Result::ok)
                    .into(),
                time_from.into(),
                time_to.into(),
            ],
        )?;
        let mut rows = Vec::new();
        for row in table {
            let chunk_id: i64 = row.get(1)?.unwrap_or(0);
            let format: String = row.get(2)?.unwrap_or_default();
            let uri: String = row.get(3)?.unwrap_or_default();
            let byte_offset: Option<i64> = row.get(4)?;
            let byte_length: Option<i64> = row.get(5)?;
            let chunk_key: Option<String> = row.get(6)?;
            rows.push(Candidate {
                chunk_id,
                format,
                uri,
                byte_offset,
                byte_length,
                chunk_key,
            });
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Pull the CF data-packing triple stored on the variable row, if any.
/// Returns `Some(CfPacking)` only when at least one of scale_factor /
/// add_offset / fill_value is non-NULL — the reader's identity
/// fast-path kicks in otherwise, so existing float stores are
/// byte-identical to before this step.
fn lookup_variable_packing(dataset: &str, variable: &str) -> Option<pgx_zarr_walker::CfPacking> {
    let sql = r#"
        SELECT v.scale_factor, v.add_offset, v.fill_value
        FROM pgx.variables v
        JOIN pgx.datasets  d ON d.id = v.dataset_id
        WHERE d.name = $1 AND v.name = $2
    "#;
    let out: Result<Option<pgx_zarr_walker::CfPacking>, spi::Error> = Spi::connect(|client| {
        let mut table = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        let row = match table.next() {
            Some(r) => r,
            None => return Ok(None),
        };
        let scale: Option<f64> = row.get(1)?;
        let offset: Option<f64> = row.get(2)?;
        let fill: Option<f64> = row.get(3)?;
        if scale.is_none() && offset.is_none() && fill.is_none() {
            return Ok(None);
        }
        Ok(Some(pgx_zarr_walker::CfPacking {
            scale: scale.unwrap_or(1.0),
            offset: offset.unwrap_or(0.0),
            fill_value: fill,
        }))
    });
    out.unwrap_or(None)
}

/// Parse a WKT POLYGON envelope into a `Bbox2D`. Returns `None` for
/// non-rectangular polygons (those still get GIST-pruned by PG, but
/// the reader doesn't apply a fine bbox filter — it falls back to
/// returning all cells in the chunk).
fn parse_envelope_wkt(wkt: &str) -> Option<Bbox2D> {
    let trimmed = wkt.trim();
    let lower = trimmed.to_ascii_lowercase();
    let prefix = "polygon((";
    if !lower.starts_with(prefix) {
        return None;
    }
    let body_start = prefix.len();
    let body_end = lower.rfind("))")?;
    let coords_str = &trimmed[body_start..body_end];
    let mut min_lat = f64::INFINITY;
    let mut max_lat = f64::NEG_INFINITY;
    let mut min_lon = f64::INFINITY;
    let mut max_lon = f64::NEG_INFINITY;
    let mut count = 0usize;
    for pair in coords_str.split(',') {
        let mut parts = pair.split_whitespace();
        let lon: f64 = parts.next()?.parse().ok()?;
        let lat: f64 = parts.next()?.parse().ok()?;
        if lon < min_lon {
            min_lon = lon;
        }
        if lon > max_lon {
            max_lon = lon;
        }
        if lat < min_lat {
            min_lat = lat;
        }
        if lat > max_lat {
            max_lat = lat;
        }
        count += 1;
    }
    if count < 4 {
        return None;
    }
    Some(Bbox2D {
        min_lat,
        min_lon,
        max_lat,
        max_lon,
    })
}

/// One-time tokio runtime for SRF calls. Lazy-built, multi-thread
/// (1 worker) so async readers can run concurrent fetches if they
/// choose.
fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("pg_xarray_rt")
            .build()
            .expect("pg_xarray: failed to build tokio runtime")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_envelope_simple() {
        let b = parse_envelope_wkt("POLYGON((2 50, 6 50, 6 52, 2 52, 2 50))").unwrap();
        assert!((b.min_lon - 2.0).abs() < 1e-9);
        assert!((b.max_lon - 6.0).abs() < 1e-9);
        assert!((b.min_lat - 50.0).abs() < 1e-9);
        assert!((b.max_lat - 52.0).abs() < 1e-9);
    }

    #[test]
    fn parse_envelope_case_insensitive() {
        let b = parse_envelope_wkt("polygon((0 0, 1 0, 1 1, 0 1, 0 0))").unwrap();
        assert_eq!(b.min_lon, 0.0);
        assert_eq!(b.max_lon, 1.0);
    }

    #[test]
    fn parse_envelope_rejects_non_polygon() {
        assert!(parse_envelope_wkt("POINT(0 0)").is_none());
        assert!(parse_envelope_wkt("garbage").is_none());
    }

    #[test]
    fn parse_envelope_rejects_too_few_vertices() {
        // Need at least 4 (closed polygon).
        assert!(parse_envelope_wkt("POLYGON((0 0, 1 1, 0 0))").is_none());
    }

    #[test]
    fn parse_envelope_extracts_global() {
        let b =
            parse_envelope_wkt("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))").unwrap();
        assert_eq!(b.min_lon, -180.0);
        assert_eq!(b.max_lon, 180.0);
        assert_eq!(b.min_lat, -90.0);
        assert_eq!(b.max_lat, 90.0);
    }
}
