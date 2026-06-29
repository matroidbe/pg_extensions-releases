//! glTF 2.0 Binary (GLB) exporter for indexed datasets.
//!
//! Public entry: [`xarray_to_glb_impl`]. Reads a registered dataset's
//! mesh + value time series and emits a GLB byte sequence with:
//!   * a TRIANGLES primitive for the water surface (Z displaced by
//!     the surface variable, vertex-coloured by the colormap of the
//!     surface or a sibling variable)
//!   * one morph target per subsequent timestep, animated via a single
//!     STEP-weighted sampler (POSITION + COLOR_0 morph deltas)
//!   * optional LINES primitive carrying flow arrows when `flow_uv` is set
//!
//! Topology is read once from `pgx.mesh_nodes` / `pgx.mesh_cells` for
//! UGRID datasets, or synthesised from sampled coords for regular
//! lat/lon grids.

pub mod animation;
pub mod builder;
pub mod colormap;
pub mod encoder;
pub mod mesh_assembly;

use pgrx::prelude::*;
use serde_json::{json, Value};

use animation::SceneOptions;
use mesh_assembly::AssembledMesh;

/// Top-level implementation called from the `#[pg_extern]` in `lib.rs`.
///
/// `target_srid` is the SRID written into the GLB's `asset.extras.srid`.
/// If it differs from the source mesh's SRID, a `WARNING` is raised
/// (vertices stay in the source CRS — pg_xarray does not embed proj).
#[allow(clippy::too_many_arguments)]
pub fn xarray_to_glb_impl(
    dataset: &str,
    surface_var: &str,
    color_var: Option<&str>,
    flow_uv: Option<&[String]>,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    colormap_name: &str,
    z_scale: f64,
    options: Option<pgrx::JsonB>,
    target_srid: i32,
) -> Vec<u8> {
    if dataset.is_empty() || surface_var.is_empty() {
        pgrx::error!("pgx.xarray_to_glb: dataset and surface_var are required");
    }
    let kind = lookup_mesh_kind(dataset, surface_var);
    let opts = parse_options(options.as_ref());

    let mesh = match kind.as_deref() {
        Some("ugrid_triangle") | Some("ugrid_polygon") => mesh_assembly::assemble_ugrid_triangle(
            dataset,
            surface_var,
            color_var,
            flow_uv,
            time_from,
            time_to,
            bbox_wkt,
            opts.max_cells,
        ),
        Some("regular_grid") => mesh_assembly::assemble_regular_grid(
            dataset,
            surface_var,
            color_var,
            time_from,
            time_to,
            bbox_wkt,
            opts.max_cells,
        ),
        Some(other) => Err(format!(
            "pgx.xarray_to_glb: mesh kind '{other}' not yet supported (v1 covers ugrid_triangle and regular_grid)"
        )),
        None => Err(format!(
            "pgx.xarray_to_glb: no mesh registered for '{dataset}.{surface_var}'. \
             Use pgx.register_file / pgx.register_mesh first."
        )),
    };
    let mesh = match mesh {
        Ok(m) => m,
        Err(e) => pgrx::error!("{}", e),
    };

    let (vmin, vmax) = resolve_value_range(
        opts.vmin,
        opts.vmax,
        dataset,
        color_var.unwrap_or(surface_var),
        &mesh,
    );
    let sim_duration_seconds = mesh
        .keyframes
        .last()
        .map(|k| k.time_seconds as f64)
        .unwrap_or(0.0);
    if mesh.srid != target_srid {
        pgrx::warning!(
            "pgx.xarray_to_glb: target_srid={target_srid} differs from source mesh SRID {} \
             — vertices are emitted in the source CRS. Use PostGIS ST_Transform on the \
             dataset, or pass target_srid matching the source SRID to silence this.",
            mesh.srid
        );
    }
    let extras = make_asset_extras(
        dataset,
        surface_var,
        color_var,
        flow_uv,
        colormap_name,
        vmin,
        vmax,
        z_scale,
        opts.arrow_scale,
        opts.time_scale,
        sim_duration_seconds,
        target_srid,
        mesh.srid,
        mesh.keyframes.len(),
    );

    let scene_opts = SceneOptions {
        colormap_name,
        vmin,
        vmax,
        z_scale,
        arrow_scale: opts.arrow_scale,
        time_scale: opts.time_scale,
        extras: Some(extras),
    };

    animation::build_scene(&mesh, &scene_opts).build()
}

// =============================================================================
// Options + value range resolution
// =============================================================================

struct ExportOptions {
    vmin: Option<f64>,
    vmax: Option<f64>,
    arrow_scale: f64,
    max_cells: i32,
    /// Multiplier applied to keyframe times when packed into the GLB
    /// sampler input. Values > 1 compress playback (e.g., a 1-hour
    /// simulation set to `time_scale = 60` plays in ~1 minute). Real
    /// time by default.
    time_scale: f64,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            vmin: None,
            vmax: None,
            arrow_scale: 1.0,
            max_cells: 1_000_000,
            time_scale: 1.0,
        }
    }
}

fn parse_options(j: Option<&pgrx::JsonB>) -> ExportOptions {
    let mut out = ExportOptions::default();
    let Some(j) = j else { return out };
    if let Some(v) = j.0.get("vmin").and_then(|v| v.as_f64()) {
        out.vmin = Some(v);
    }
    if let Some(v) = j.0.get("vmax").and_then(|v| v.as_f64()) {
        out.vmax = Some(v);
    }
    if let Some(v) = j.0.get("arrow_scale").and_then(|v| v.as_f64()) {
        out.arrow_scale = v;
    }
    if let Some(v) = j.0.get("max_cells").and_then(|v| v.as_i64()) {
        out.max_cells = v as i32;
    }
    if let Some(v) = j.0.get("time_scale").and_then(|v| v.as_f64()) {
        if v > 0.0 {
            out.time_scale = v;
        }
    }
    out
}

/// Resolve the colormap value range. Order:
///   1. Explicit `options.vmin`/`vmax` (NaN-tolerant — caller errs once).
///   2. `pgx.variables.valid_min` / `valid_max` for the color variable.
///   3. Computed from the actual fetched color data.
fn resolve_value_range(
    opt_min: Option<f64>,
    opt_max: Option<f64>,
    dataset: &str,
    color_var: &str,
    mesh: &AssembledMesh,
) -> (f64, f64) {
    if let (Some(a), Some(b)) = (opt_min, opt_max) {
        return (a, b);
    }
    let cat = lookup_valid_min_max(dataset, color_var);
    let mut min = opt_min.or(cat.0);
    let mut max = opt_max.or(cat.1);
    if min.is_none() || max.is_none() {
        let (cmin, cmax) = compute_value_range(mesh);
        min = min.or(Some(cmin));
        max = max.or(Some(cmax));
    }
    let mut min = min.unwrap_or(0.0);
    let mut max = max.unwrap_or(1.0);
    if !(min.is_finite() && max.is_finite()) || (max - min).abs() < f64::EPSILON {
        // Degenerate range — fall back to [0, 1] so the colormap still
        // works without producing Inf weights.
        min = 0.0;
        max = 1.0;
    }
    (min, max)
}

fn compute_value_range(mesh: &AssembledMesh) -> (f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    for k in &mesh.keyframes {
        for v in &k.color {
            if v.is_finite() {
                if *v < min {
                    min = *v;
                }
                if *v > max {
                    max = *v;
                }
            }
        }
    }
    if !min.is_finite() || !max.is_finite() {
        (0.0, 1.0)
    } else {
        (min, max)
    }
}

// =============================================================================
// SPI helpers — catalog lookups
// =============================================================================

pub(crate) fn lookup_mesh_kind(dataset: &str, variable: &str) -> Option<String> {
    let sql = r#"
        SELECT m.kind
        FROM   pgx.chunks         c
        JOIN   pgx.variables      v ON v.id = c.variable_id
        JOIN   pgx.datasets       d ON d.id = v.dataset_id
        JOIN   pgx.mesh_versions  mv ON mv.id = c.mesh_version_id
        JOIN   pgx.meshes         m  ON m.id  = mv.mesh_id
        WHERE  d.name = $1 AND v.name = $2
        ORDER  BY c.id
        LIMIT  1
    "#;
    let res: Result<Option<String>, spi::Error> = Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match t.next() {
            Some(row) => Ok(row.get::<String>(1)?),
            None => Ok(None),
        }
    });
    match res {
        Ok(Some(k)) => Some(k),
        Ok(None) => {
            // No mesh chunk for this variable — fall back to checking
            // for plain `pgx.chunks` rows so we know whether the
            // variable exists at all.
            let plain_sql = r#"
                SELECT 'regular_grid'::text
                FROM   pgx.chunks    c
                JOIN   pgx.variables v ON v.id = c.variable_id
                JOIN   pgx.datasets  d ON d.id = v.dataset_id
                WHERE  d.name = $1 AND v.name = $2
                LIMIT  1
            "#;
            let plain: Result<Option<String>, spi::Error> = Spi::connect(|client| {
                let mut t =
                    client.select(plain_sql, Some(1), &[dataset.into(), variable.into()])?;
                match t.next() {
                    Some(row) => Ok(row.get::<String>(1)?),
                    None => Ok(None),
                }
            });
            plain.ok().flatten()
        }
        Err(_) => None,
    }
}

pub(crate) fn lookup_valid_min_max(dataset: &str, variable: &str) -> (Option<f64>, Option<f64>) {
    let sql = r#"
        SELECT v.valid_min, v.valid_max
        FROM   pgx.variables v
        JOIN   pgx.datasets  d ON d.id = v.dataset_id
        WHERE  d.name = $1 AND v.name = $2
        LIMIT  1
    "#;
    let res: Result<(Option<f64>, Option<f64>), spi::Error> = Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), &[dataset.into(), variable.into()])?;
        match t.next() {
            Some(row) => Ok((row.get::<f64>(1)?, row.get::<f64>(2)?)),
            None => Ok((None, None)),
        }
    });
    res.unwrap_or((None, None))
}

// =============================================================================
// Asset extras — round-trippable metadata in the GLB asset.extras
// =============================================================================

/// Human-readable axis-order hint matching the SRID written into
/// `asset.extras.srid`. Mirrors `pg_solid::ffi::mesh::axis_order_for_srid`
/// so downstream viewers don't have to special-case per producer.
fn axis_order_for_srid(srid: i32) -> &'static str {
    match srid {
        4326 => "longitude_deg,latitude_deg,ellipsoidal_height_m",
        4978 => "ecef_x_m,ecef_y_m,ecef_z_m",
        _ => "easting_m,northing_m,height_m",
    }
}

#[allow(clippy::too_many_arguments)]
fn make_asset_extras(
    dataset: &str,
    surface_var: &str,
    color_var: Option<&str>,
    flow_uv: Option<&[String]>,
    colormap_name: &str,
    vmin: f64,
    vmax: f64,
    z_scale: f64,
    arrow_scale: f64,
    time_scale: f64,
    sim_duration_seconds: f64,
    target_srid: i32,
    source_srid: i32,
    n_keyframes: usize,
) -> Value {
    let flow: Vec<String> = flow_uv.map(|v| v.to_vec()).unwrap_or_default();
    let glb_duration_seconds = if time_scale > 0.0 {
        sim_duration_seconds / time_scale
    } else {
        sim_duration_seconds
    };
    json!({
        "producer": "pg_xarray",
        "dataset": dataset,
        "surface_var": surface_var,
        "color_var": color_var.unwrap_or(surface_var),
        "flow_uv": flow,
        "colormap": colormap_name,
        "vmin": vmin,
        "vmax": vmax,
        "z_scale": z_scale,
        "arrow_scale": arrow_scale,
        "time_scale": time_scale,
        "sim_duration_seconds": sim_duration_seconds,
        "glb_duration_seconds": glb_duration_seconds,
        "srid": target_srid,
        "source_srid": source_srid,
        "axis_order": axis_order_for_srid(target_srid),
        "n_keyframes": n_keyframes,
    })
}
