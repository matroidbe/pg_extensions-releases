//! Read mesh topology + per-timestep values into a uniform shape the
//! GLB scene builder can consume.
//!
//! Two assembly paths today:
//!   * [`assemble_ugrid_triangle`] — TELEMAC/SELAFIN-style triangle
//!     mesh. Topology comes from `pgx.mesh_nodes` + `pgx.mesh_cells`,
//!     values from `pgx.fetch_mesh`.
//!   * [`assemble_regular_grid`] — lat/lon Cartesian grid. Topology is
//!     synthesised from the sampled coord values returned by
//!     `pgx.fetch`; triangles are emitted on the fly.
//!
//! Each returns an [`AssembledMesh`] with a dense, contiguous vertex
//! array and one or more [`Keyframe`]s. Time-seconds are relative to
//! the first keyframe so the GLB animation starts at t=0.

use crate::srf::fetch;
use crate::srf::fetch_mesh;
use pgrx::prelude::*;
use std::collections::BTreeMap;

/// A vertex-indexed mesh + one or more time-stamped value arrays.
#[derive(Debug, Default)]
pub struct AssembledMesh {
    pub vertex_count: u32,
    /// 2-D node positions, length = `vertex_count * 2`, layout `[x0, y0, x1, y1, ...]`.
    pub base_xy: Vec<f32>,
    /// Triangle indices (TRIANGLES primitive), length = `triangle_count * 3`.
    pub triangles: Vec<u32>,
    pub keyframes: Vec<Keyframe>,
    /// Effective SRID — for `extras` round-tripping.
    pub srid: i32,
}

#[derive(Debug, Default)]
pub struct Keyframe {
    /// Seconds since the first keyframe (glTF convention).
    pub time_seconds: f32,
    /// Per-vertex Z (surface displacement / scalar driving the height).
    /// `NaN` for vertices not present at this timestep.
    pub z: Vec<f64>,
    /// Per-vertex value used for colormap lookup. `NaN` if absent.
    pub color: Vec<f64>,
    /// Per-vertex (u, v) for flow arrows. `None` if flow wasn't requested.
    pub flow_uv: Option<Vec<(f64, f64)>>,
}

// =============================================================================
// UGRID (triangle) assembly
// =============================================================================

/// Pull mesh topology + per-timestep values for a UGRID-triangle dataset.
///
/// Reads `pgx.mesh_nodes` for vertex coordinates, `pgx.mesh_cells` for
/// triangle connectivity, and `pgx.fetch_mesh` for the per-vertex
/// value series. Falls back to a single keyframe when the reader
/// emits `time: None` rows (e.g., the memory reader's test path).
pub fn assemble_ugrid_triangle(
    dataset: &str,
    surface_var: &str,
    color_var: Option<&str>,
    flow_uv: Option<&[String]>,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    max_cells: i32,
) -> Result<AssembledMesh, String> {
    let mv = lookup_mesh_version_id(dataset, surface_var)
        .ok_or_else(|| format!("xarray_to_glb: no mesh found for '{dataset}.{surface_var}'"))?;

    let node_rows = read_mesh_nodes(mv.mesh_version_id, bbox_wkt, mv.srid)?;
    if node_rows.is_empty() {
        return Err(format!(
            "xarray_to_glb: mesh_version {} has no nodes{}",
            mv.mesh_version_id,
            if bbox_wkt.is_some() {
                " inside the requested bbox"
            } else {
                ""
            }
        ));
    }
    let vertex_count = node_rows.len() as u32;

    // Dense index by ascending node_id (matches the SQL ORDER BY).
    let mut base_xy = Vec::with_capacity(node_rows.len() * 2);
    let mut node_id_to_index: BTreeMap<i64, u32> = BTreeMap::new();
    for (i, (node_id, x, y)) in node_rows.iter().enumerate() {
        node_id_to_index.insert(*node_id, i as u32);
        base_xy.push(*x as f32);
        base_xy.push(*y as f32);
    }

    let cell_rows = read_mesh_cells(mv.mesh_version_id)?;
    let mut triangles = Vec::with_capacity(cell_rows.len() * 3);
    let mut dropped_non_triangle = 0usize;
    for nodes in cell_rows {
        if nodes.len() != 3 {
            dropped_non_triangle += 1;
            continue;
        }
        let a = node_id_to_index.get(&nodes[0]);
        let b = node_id_to_index.get(&nodes[1]);
        let c = node_id_to_index.get(&nodes[2]);
        match (a, b, c) {
            (Some(a), Some(b), Some(c)) => {
                triangles.push(*a);
                triangles.push(*b);
                triangles.push(*c);
            }
            _ => {
                dropped_non_triangle += 1;
            }
        }
    }
    if dropped_non_triangle > 0 {
        pgrx::warning!(
            "xarray_to_glb: dropped {} non-triangle / unresolved cells in mesh_version {}",
            dropped_non_triangle,
            mv.mesh_version_id
        );
    }

    // -------- value series --------
    let surface_series = fetch_mesh_grouped(
        dataset,
        surface_var,
        time_from,
        time_to,
        bbox_wkt,
        max_cells,
        &node_id_to_index,
        vertex_count as usize,
    );
    if surface_series.is_empty() {
        return Err(format!(
            "xarray_to_glb: no values returned by pgx.fetch_mesh for '{dataset}.{surface_var}'"
        ));
    }

    let color_series = match color_var {
        Some(name) if name != surface_var => Some(fetch_mesh_grouped(
            dataset,
            name,
            time_from,
            time_to,
            bbox_wkt,
            max_cells,
            &node_id_to_index,
            vertex_count as usize,
        )),
        _ => None,
    };

    let flow_series: Option<Vec<Vec<TimedValues>>> = match flow_uv {
        Some(components) if !components.is_empty() => {
            // 1 or 2 components — pull each as a scalar series.
            let mut out: Vec<Vec<TimedValues>> = Vec::with_capacity(components.len());
            for comp in components {
                let s = fetch_mesh_grouped(
                    dataset,
                    comp,
                    time_from,
                    time_to,
                    bbox_wkt,
                    max_cells,
                    &node_id_to_index,
                    vertex_count as usize,
                );
                out.push(s);
            }
            Some(out)
        }
        _ => None,
    };

    // -------- align all series to the surface timeline --------
    let mut keyframes = Vec::with_capacity(surface_series.len());
    let t0 = surface_series[0].time_micros;
    for (i, frame) in surface_series.iter().enumerate() {
        let time_seconds = ((frame.time_micros - t0) as f64 / 1_000_000.0) as f32;
        let color = match &color_series {
            Some(c) => pick_aligned(c, i, frame.time_micros, vertex_count as usize),
            None => frame.values.clone(),
        };
        let flow_uv = flow_series.as_ref().map(|series| {
            let n = vertex_count as usize;
            let u = pick_aligned(&series[0], i, frame.time_micros, n);
            let v = if series.len() >= 2 {
                pick_aligned(&series[1], i, frame.time_micros, n)
            } else {
                vec![0.0; n]
            };
            (0..n)
                .map(|j| (*u.get(j).unwrap_or(&0.0), *v.get(j).unwrap_or(&0.0)))
                .collect::<Vec<(f64, f64)>>()
        });
        keyframes.push(Keyframe {
            time_seconds,
            z: frame.values.clone(),
            color,
            flow_uv,
        });
    }

    Ok(AssembledMesh {
        vertex_count,
        base_xy,
        triangles,
        keyframes,
        srid: mv.srid,
    })
}

/// Pick the i-th frame from `series` if it exists at a matching time;
/// otherwise return a NaN-filled fallback so the downstream encoder
/// still produces a well-formed COLOR_0 / flow buffer.
fn pick_aligned(series: &[TimedValues], i: usize, target_micros: i64, n: usize) -> Vec<f64> {
    if let Some(s) = series.get(i) {
        if s.time_micros == target_micros {
            return s.values.clone();
        }
    }
    // Linear search fallback — datasets with mismatched timelines.
    for s in series {
        if s.time_micros == target_micros {
            return s.values.clone();
        }
    }
    vec![f64::NAN; n]
}

// =============================================================================
// Regular-grid (lat/lon) assembly
// =============================================================================

/// Synthesise a triangulated regular lat/lon grid from `pgx.fetch`
/// rows. Each unique `(lat, lon)` becomes a vertex, two triangles per
/// grid cell, per-vertex values by timestep.
///
/// Assumes a complete rectangular grid (every `(lat, lon)` pair appears
/// in every timestep). Sparse grids will leave NaN gaps that the
/// colormap renders as the LUT's first entry.
pub fn assemble_regular_grid(
    dataset: &str,
    surface_var: &str,
    color_var: Option<&str>,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    max_cells: i32,
) -> Result<AssembledMesh, String> {
    let rows = fetch::fetch_impl(
        dataset,
        surface_var,
        None,
        bbox_wkt,
        None,
        None,
        max_cells,
        time_from,
        time_to,
    );
    if rows.is_empty() {
        return Err(format!(
            "xarray_to_glb: no values returned by pgx.fetch for '{dataset}.{surface_var}'"
        ));
    }

    // Collect ordered-unique lats and lons (using bit-pattern keys to
    // tolerate floating-point exactness without an Ord wrapper).
    let mut lat_set: BTreeMap<u64, f64> = BTreeMap::new();
    let mut lon_set: BTreeMap<u64, f64> = BTreeMap::new();
    for (lat, lon, _, _, _) in &rows {
        if let Some(la) = lat {
            lat_set.insert(la.to_bits(), *la);
        }
        if let Some(lo) = lon {
            lon_set.insert(lo.to_bits(), *lo);
        }
    }
    let mut lats: Vec<f64> = lat_set.into_values().collect();
    let mut lons: Vec<f64> = lon_set.into_values().collect();
    lats.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    lons.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if lats.is_empty() || lons.is_empty() {
        return Err("xarray_to_glb: regular_grid rows lack lat/lon coordinates".into());
    }

    let nlat = lats.len();
    let nlon = lons.len();
    let vertex_count = (nlat * nlon) as u32;

    // Index helpers — bit-pattern → axis index.
    let lat_idx: BTreeMap<u64, usize> = lats
        .iter()
        .enumerate()
        .map(|(i, v)| (v.to_bits(), i))
        .collect();
    let lon_idx: BTreeMap<u64, usize> = lons
        .iter()
        .enumerate()
        .map(|(j, v)| (v.to_bits(), j))
        .collect();

    // Vertex positions (X = lon, Y = lat, Z baked-in elsewhere).
    let mut base_xy = Vec::with_capacity((vertex_count as usize) * 2);
    for la in &lats {
        for lo in &lons {
            base_xy.push(*lo as f32);
            base_xy.push(*la as f32);
        }
    }

    // Triangulate: two triangles per cell, winding CCW when viewed from +Z.
    let triangles = triangulate_grid(nlat, nlon);

    // Group rows by time → per-keyframe value vector indexed by dense vertex.
    let mut by_time: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for (lat, lon, _, time, value) in rows {
        let t_micros: i64 = time.map(|t| t.into()).unwrap_or(i64::MIN);
        let (Some(la), Some(lo)) = (lat, lon) else {
            continue;
        };
        let (i, j) = match (lat_idx.get(&la.to_bits()), lon_idx.get(&lo.to_bits())) {
            (Some(i), Some(j)) => (*i, *j),
            _ => continue,
        };
        let entry = by_time
            .entry(t_micros)
            .or_insert_with(|| vec![f64::NAN; vertex_count as usize]);
        entry[i * nlon + j] = value;
    }

    let t0 = by_time.keys().next().copied().unwrap_or(0);
    let mut surface_keyframes: Vec<TimedValues> = by_time
        .into_iter()
        .map(|(t, values)| TimedValues {
            time_micros: t,
            values,
        })
        .collect();
    surface_keyframes.sort_by_key(|k| k.time_micros);

    // Optional second-variable color series.
    let color_series = match color_var {
        Some(name) if name != surface_var => {
            let cr = fetch::fetch_impl(
                dataset, name, None, bbox_wkt, None, None, max_cells, time_from, time_to,
            );
            let mut acc: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
            for (lat, lon, _, time, value) in cr {
                let t_micros: i64 = time.map(|t| t.into()).unwrap_or(i64::MIN);
                let (Some(la), Some(lo)) = (lat, lon) else {
                    continue;
                };
                let (i, j) = match (lat_idx.get(&la.to_bits()), lon_idx.get(&lo.to_bits())) {
                    (Some(i), Some(j)) => (*i, *j),
                    _ => continue,
                };
                let entry = acc
                    .entry(t_micros)
                    .or_insert_with(|| vec![f64::NAN; vertex_count as usize]);
                entry[i * nlon + j] = value;
            }
            Some(
                acc.into_iter()
                    .map(|(t, values)| TimedValues {
                        time_micros: t,
                        values,
                    })
                    .collect::<Vec<_>>(),
            )
        }
        _ => None,
    };

    let mut keyframes = Vec::with_capacity(surface_keyframes.len());
    for (i, frame) in surface_keyframes.iter().enumerate() {
        let time_seconds = ((frame.time_micros - t0) as f64 / 1_000_000.0) as f32;
        let color = match &color_series {
            Some(c) => pick_aligned(c, i, frame.time_micros, vertex_count as usize),
            None => frame.values.clone(),
        };
        keyframes.push(Keyframe {
            time_seconds,
            z: frame.values.clone(),
            color,
            flow_uv: None,
        });
    }

    let srid = lookup_dataset_srid(dataset, surface_var);

    Ok(AssembledMesh {
        vertex_count,
        base_xy,
        triangles,
        keyframes,
        srid,
    })
}

/// Emit a flat triangle-strip-style index buffer for a `nlat × nlon`
/// lat-major grid. Vertex `(i, j)` lives at index `i * nlon + j`.
pub fn triangulate_grid(nlat: usize, nlon: usize) -> Vec<u32> {
    if nlat < 2 || nlon < 2 {
        return Vec::new();
    }
    let mut out = Vec::with_capacity((nlat - 1) * (nlon - 1) * 6);
    for i in 0..(nlat - 1) {
        for j in 0..(nlon - 1) {
            let a = (i * nlon + j) as u32;
            let b = (i * nlon + (j + 1)) as u32;
            let c = ((i + 1) * nlon + j) as u32;
            let d = ((i + 1) * nlon + (j + 1)) as u32;
            // Two triangles: (a, b, d) and (a, d, c) — CCW when looking down +Z.
            out.extend_from_slice(&[a, b, d, a, d, c]);
        }
    }
    out
}

// =============================================================================
// fetch_mesh grouping helper
// =============================================================================

#[derive(Debug, Clone)]
struct TimedValues {
    time_micros: i64,
    values: Vec<f64>,
}

/// Call `pgx.fetch_mesh` for the variable and group rows into one
/// `Vec<f64>` per timestep (indexed by `node_id_to_index`).
fn fetch_mesh_grouped(
    dataset: &str,
    variable: &str,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    max_cells: i32,
    node_id_to_index: &BTreeMap<i64, u32>,
    vertex_count: usize,
) -> Vec<TimedValues> {
    let rows = fetch_mesh::fetch_mesh_impl(
        dataset, variable, None, bbox_wkt, max_cells, time_from, time_to,
    );
    if rows.is_empty() {
        return Vec::new();
    }
    let mut by_time: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
    for (node_id, _cell_id, _geom_wkt, time, value) in rows {
        let Some(node_id) = node_id else { continue };
        let Some(idx) = node_id_to_index.get(&node_id) else {
            continue;
        };
        let t_micros: i64 = time.map(|t| t.into()).unwrap_or(i64::MIN);
        let entry = by_time
            .entry(t_micros)
            .or_insert_with(|| vec![f64::NAN; vertex_count]);
        entry[*idx as usize] = value;
    }
    by_time
        .into_iter()
        .map(|(t, values)| TimedValues {
            time_micros: t,
            values,
        })
        .collect()
}

// =============================================================================
// SPI helpers
// =============================================================================

struct MeshVersionInfo {
    mesh_version_id: i64,
    srid: i32,
}

/// Look up the source SRID for a regular_grid variable from
/// `pgx.variables.srid` → `pgx.datasets.default_srid` → 4326. Mirrors the
/// hierarchy in `lookup_mesh_version_id` for the UGRID path; the difference
/// is that regular_grid has no `mesh_version_id`, so we resolve via the
/// catalog tables directly.
pub fn lookup_dataset_srid(dataset: &str, variable: &str) -> i32 {
    let sql = r#"
        SELECT COALESCE(v.srid, d.default_srid, 4326) AS srid
        FROM   pgx.variables v
        JOIN   pgx.datasets  d ON d.id = v.dataset_id
        WHERE  d.name = $1 AND v.name = $2
        LIMIT  1
    "#;
    let res: Result<Option<i32>, spi::Error> = Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match t.next() {
            Some(row) => Ok(row.get::<i32>(1)?),
            None => Ok(None),
        }
    });
    res.ok().flatten().unwrap_or(4326)
}

fn lookup_mesh_version_id(dataset: &str, variable: &str) -> Option<MeshVersionInfo> {
    let sql = r#"
        SELECT c.mesh_version_id,
               COALESCE(v.srid, d.default_srid, 4326) AS srid
        FROM   pgx.chunks    c
        JOIN   pgx.variables v ON v.id = c.variable_id
        JOIN   pgx.datasets  d ON d.id = v.dataset_id
        WHERE  d.name = $1
          AND  v.name = $2
          AND  c.mesh_version_id IS NOT NULL
        ORDER  BY c.id
        LIMIT  1
    "#;
    #[allow(clippy::type_complexity)]
    let res: Result<Option<(Option<i64>, Option<i32>)>, spi::Error> = Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match t.next() {
            Some(row) => Ok(Some((row.get::<i64>(1)?, row.get::<i32>(2)?))),
            None => Ok(None),
        }
    });
    match res {
        Ok(Some((Some(mv), srid))) => Some(MeshVersionInfo {
            mesh_version_id: mv,
            srid: srid.unwrap_or(4326),
        }),
        _ => None,
    }
}

fn read_mesh_nodes(
    mesh_version_id: i64,
    bbox_wkt: Option<&str>,
    srid: i32,
) -> Result<Vec<(i64, f64, f64)>, String> {
    let sql = r#"
        SELECT node_id,
               public.ST_X(geom) AS x,
               public.ST_Y(geom) AS y
        FROM   pgx.mesh_nodes
        WHERE  mesh_version_id = $1
          AND  ($2::text IS NULL
                OR geom && public.ST_GeomFromText($2, $3))
        ORDER  BY node_id
    "#;
    let res: Result<Vec<(i64, f64, f64)>, spi::Error> = Spi::connect(|client| {
        let table = client.select(
            sql,
            None,
            &[mesh_version_id.into(), bbox_wkt.into(), srid.into()],
        )?;
        let mut out = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let x: f64 = row.get(2)?.unwrap_or(0.0);
            let y: f64 = row.get(3)?.unwrap_or(0.0);
            out.push((id, x, y));
        }
        Ok(out)
    });
    res.map_err(|e| format!("read_mesh_nodes SPI failed: {e}"))
}

fn read_mesh_cells(mesh_version_id: i64) -> Result<Vec<Vec<i64>>, String> {
    let sql = r#"
        SELECT node_ids
        FROM   pgx.mesh_cells
        WHERE  mesh_version_id = $1
        ORDER  BY cell_id
    "#;
    let res: Result<Vec<Vec<i64>>, spi::Error> = Spi::connect(|client| {
        let table = client.select(sql, None, &[mesh_version_id.into()])?;
        let mut out = Vec::new();
        for row in table {
            let nodes: Option<Vec<Option<i64>>> = row.get(1)?;
            if let Some(ns) = nodes {
                let nodes: Vec<i64> = ns.into_iter().flatten().collect();
                out.push(nodes);
            }
        }
        Ok(out)
    });
    res.map_err(|e| format!("read_mesh_cells SPI failed: {e}"))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triangulate_2x2_is_one_quad() {
        let t = triangulate_grid(2, 2);
        // 1 quad → 2 triangles → 6 indices.
        assert_eq!(t, vec![0, 1, 3, 0, 3, 2]);
    }

    #[test]
    fn triangulate_3x3_is_eight_triangles() {
        let t = triangulate_grid(3, 3);
        assert_eq!(t.len(), 4 * 6);
        for idx in &t {
            assert!(*idx < 9);
        }
    }

    #[test]
    fn triangulate_degenerate_is_empty() {
        assert!(triangulate_grid(1, 5).is_empty());
        assert!(triangulate_grid(5, 1).is_empty());
        assert!(triangulate_grid(0, 0).is_empty());
    }
}
