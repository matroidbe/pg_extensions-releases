//! `pgx.fetch_mesh()` — query SRF for unstructured-mesh variables.
//!
//! Flow mirrors `pgx.fetch()` but the spatial-prune step happens
//! against `pgx.mesh_nodes.geom` / `pgx.mesh_cells.centroid` (post-decode
//! JOIN) instead of against the chunk's bbox envelope. The chunk-level
//! pruning still uses `chunks.bbox_envelope` + `time_range` so callers
//! get the same "cheap first pass" semantics.
//!
//! Each returned row is `(node_id, cell_id, geom_wkt, time, value)`:
//!   * `node_id` set for node-indexed variables (`dim_order` includes 'node')
//!   * `cell_id` set for cell-indexed variables (`dim_order` includes 'cell')
//!   * `geom_wkt` is the node geometry / cell centroid as WKT — clients
//!     re-parse it with `ST_GeomFromText(...)` when they want PostGIS
//!     geometry, otherwise treat it as plain text.

use crate::reader::{reader_for, Cell, ChunkLocator, CoordFilter};
use crate::srf::fetch::PG_EPOCH_OFFSET_MICROS;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::sync::OnceLock;
use tokio::runtime::Runtime;

/// One row returned by `pgx.fetch_mesh()`.
pub type FetchMeshRow = (
    Option<i64>,                                // node_id
    Option<i64>,                                // cell_id
    Option<String>,                             // geom_wkt
    Option<pgrx::datum::TimestampWithTimeZone>, // time
    f64,                                        // value
);

/// `kind` discriminates which side of the mesh the variable is indexed
/// against. Auto-detected from the variable's `dim_order`:
///   * contains "cell" → MeshKind::Cell
///   * contains "node" → MeshKind::Node
///   * neither → defaults to `MeshKind::Node` (most common)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MeshKind {
    Node,
    Cell,
}

pub fn fetch_mesh_impl(
    dataset: &str,
    variable: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    max_cells: i32,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
) -> Vec<FetchMeshRow> {
    if dataset.is_empty() || variable.is_empty() {
        pgrx::error!("pgx.fetch_mesh: dataset and variable are required");
    }
    if max_cells <= 0 {
        pgrx::error!(
            "pgx.fetch_mesh: max_cells must be positive (got {})",
            max_cells
        );
    }

    let kind = lookup_mesh_kind(dataset, variable);

    // Catalog → candidate chunks. Same prune semantics as pgx.fetch but
    // joined to the mesh_version too so we only consider chunks tied to
    // the mesh (not chunks tied to a regular grid for the same variable
    // name on a different dataset).
    let candidates = lookup_candidates(dataset, variable, at_time, bbox_wkt, time_from, time_to);
    if candidates.is_empty() {
        return Vec::new();
    }

    let filter = CoordFilter {
        max_cells: Some(max_cells as usize),
        ..Default::default()
    };

    let mut rows: Vec<FetchMeshRow> = Vec::new();
    let rt = runtime();
    for candidate in candidates {
        let reader = match reader_for(&candidate.format) {
            Some(r) => r,
            None => {
                pgrx::warning!(
                    "pgx.fetch_mesh: no reader registered for format '{}' (chunk {})",
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
            packing: None,
        };
        let cells = match rt.block_on(async { reader.read_chunk(&locator, &filter).await }) {
            Ok(c) => c,
            Err(e) => {
                pgrx::warning!(
                    "pgx.fetch_mesh: reader failed for chunk {}: {}",
                    candidate.chunk_id,
                    e
                );
                continue;
            }
        };
        // Single JOIN per chunk: load the geom-by-id map for every id
        // the chunk emitted, then merge with the cell values.
        let ids: Vec<i64> = cells.iter().filter_map(|c| c.node_id).collect();
        if ids.is_empty() {
            continue;
        }
        let geom_map = lookup_geoms(
            candidate.mesh_version_id,
            &ids,
            kind,
            bbox_wkt,
            candidate.effective_srid,
        );
        for c in cells {
            let id = match c.node_id {
                Some(v) => v,
                None => continue,
            };
            // Cell dropped if it has no matching geom row (e.g., when
            // `bbox_wkt` is applied: rows outside the bbox are absent
            // from the map). This is the spatial-prune step.
            let geom_wkt = match geom_map.get(&id) {
                Some(w) => w.clone(),
                None => continue,
            };
            rows.push(cell_to_row(c, kind, &geom_wkt, candidate.time_lo));
            if rows.len() >= max_cells as usize {
                return rows;
            }
        }
    }
    rows
}

fn cell_to_row(
    c: Cell,
    kind: MeshKind,
    geom_wkt: &str,
    fallback_time: Option<pgrx::datum::TimestampWithTimeZone>,
) -> FetchMeshRow {
    // Prefer the reader's per-cell timestamp; fall back to the chunk's
    // `lower(time_range)` so per-chunk time models (e.g., SELAFIN, where
    // each timestep is one chunk with no per-cell time) still surface a
    // time column instead of NULL.
    let time = c
        .time
        .and_then(|t| {
            pgrx::datum::TimestampWithTimeZone::try_from(
                t.timestamp_micros() - PG_EPOCH_OFFSET_MICROS,
            )
            .ok()
        })
        .or(fallback_time);
    let (node_id, cell_id) = match kind {
        MeshKind::Node => (c.node_id, None),
        MeshKind::Cell => (None, c.node_id),
    };
    (node_id, cell_id, Some(geom_wkt.to_string()), time, c.value)
}

struct Candidate {
    chunk_id: i64,
    format: String,
    uri: String,
    byte_offset: Option<i64>,
    byte_length: Option<i64>,
    chunk_key: Option<String>,
    mesh_version_id: i64,
    effective_srid: i32,
    time_lo: Option<pgrx::datum::TimestampWithTimeZone>,
}

/// Pick chunks for the (dataset, variable) that ALSO have a
/// `mesh_version_id` — i.e., they're tied to a registered mesh.
/// Bbox/time predicates prune at the chunk level via the same
/// GIST/range indexes as `pgx.fetch`.
fn lookup_candidates(
    dataset: &str,
    variable: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
) -> Vec<Candidate> {
    let sql = r#"
        SELECT c.id, d.format, c.uri, c.byte_offset, c.byte_length, c.chunk_key,
               c.mesh_version_id,
               COALESCE(v.srid, d.default_srid, 4326) AS srid,
               lower(c.time_range) AS time_lo
        FROM pgx.chunks c
        JOIN pgx.variables v ON v.id = c.variable_id
        JOIN pgx.datasets  d ON d.id = v.dataset_id
        WHERE d.name = $1
          AND v.name = $2
          AND c.mesh_version_id IS NOT NULL
          AND ($3::timestamptz IS NULL OR c.time_range @> $3::timestamptz)
          AND ($4::text IS NULL
               OR c.bbox_envelope IS NULL
               -- See [src/srf/fetch.rs] for the MV-search_path rationale.
               OR c.bbox_envelope OPERATOR(public.&&) public.ST_GeomFromText($4,
                     COALESCE(v.srid, d.default_srid, 4326)))
          AND (($5::timestamptz IS NULL AND $6::timestamptz IS NULL)
               OR c.time_range IS NULL
               OR c.time_range && tstzrange($5, $6, '[]'))
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
            let mesh_version_id: i64 = row.get(7)?.unwrap_or(0);
            let effective_srid: i32 = row.get(8)?.unwrap_or(4326);
            let time_lo: Option<pgrx::datum::TimestampWithTimeZone> = row.get(9)?;
            rows.push(Candidate {
                chunk_id,
                format,
                uri,
                byte_offset,
                byte_length,
                chunk_key,
                mesh_version_id,
                effective_srid,
                time_lo,
            });
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// For a list of node/cell ids, look up their WKT geoms within a
/// specific mesh_version. When `bbox_wkt` is set, the join is
/// constrained spatially via PostGIS `&&` against the SRID already
/// stored on the mesh — that's how spatial pruning happens for
/// unstructured grids (cell-by-cell, not chunk-by-chunk).
fn lookup_geoms(
    mesh_version_id: i64,
    ids: &[i64],
    kind: MeshKind,
    bbox_wkt: Option<&str>,
    effective_srid: i32,
) -> HashMap<i64, String> {
    if ids.is_empty() {
        return HashMap::new();
    }
    let (id_col, geom_col, table) = match kind {
        MeshKind::Node => ("node_id", "geom", "pgx.mesh_nodes"),
        MeshKind::Cell => ("cell_id", "centroid", "pgx.mesh_cells"),
    };
    let sql = format!(
        "SELECT {id_col}, public.ST_AsText({geom_col}) \
         FROM {table} \
         WHERE mesh_version_id = $1 \
           AND {id_col} = ANY($2::bigint[]) \
           AND ($3::text IS NULL \
                OR {geom_col} OPERATOR(public.&&) public.ST_GeomFromText($3, $4))"
    );
    let mut out = HashMap::new();
    let res: Result<(), spi::Error> = Spi::connect(|client| {
        let table = client.select(
            &sql,
            None,
            &[
                mesh_version_id.into(),
                ids.to_vec().into(),
                bbox_wkt.into(),
                effective_srid.into(),
            ],
        )?;
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let wkt: String = row.get(2)?.unwrap_or_default();
            out.insert(id, wkt);
        }
        Ok(())
    });
    if let Err(e) = res {
        pgrx::warning!("pgx.fetch_mesh: lookup_geoms SPI failed: {}", e);
    }
    out
}

/// Decide whether the variable's values are indexed by node or by cell.
/// Heuristic: `dim_order` contains "cell" → Cell, contains "node" → Node,
/// otherwise default to Node (the most common case in UGRID/SELAFIN).
fn lookup_mesh_kind(dataset: &str, variable: &str) -> MeshKind {
    let sql = r#"
        SELECT v.dim_order
        FROM pgx.variables v
        JOIN pgx.datasets  d ON d.id = v.dataset_id
        WHERE d.name = $1 AND v.name = $2
    "#;
    let res: Result<Option<Vec<Option<String>>>, spi::Error> = Spi::connect(|client| {
        let mut table = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match table.next() {
            Some(row) => Ok(row.get(1)?),
            None => Ok(None),
        }
    });
    if let Ok(Some(dims)) = res {
        let has = |needle: &str| {
            dims.iter()
                .flatten()
                .any(|d| d.eq_ignore_ascii_case(needle))
        };
        if has("cell") {
            return MeshKind::Cell;
        }
        if has("node") {
            return MeshKind::Node;
        }
    }
    MeshKind::Node
}

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("pgx_fetch_mesh_rt")
            .build()
            .expect("fetch_mesh: failed to build tokio runtime")
    })
}
