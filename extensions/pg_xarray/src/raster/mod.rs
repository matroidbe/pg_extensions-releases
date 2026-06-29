//! 2D raster rendering of indexed datasets.
//!
//! Public entry: [`xarray_to_png_impl`]. Reuses the GLB pipeline's
//! [`crate::glb::mesh_assembly`] for catalog → topology + per-vertex
//! value series, then rasterises the first matching timestep into a
//! viridis-coloured RGBA buffer and encodes it as PNG.
//!
//! The same `bbox_wkt + time + colormap` plumbing the GLB path uses —
//! and the same catalog pruning the WMS bgworker exposes through
//! GetMap. Single-timestep; animation is by re-requesting tiles.

pub mod encoder;
pub mod rasterise;
pub mod viewport;

use serde_json::Value;

use crate::glb::colormap;
use crate::glb::mesh_assembly::{self, AssembledMesh, Keyframe};
use viewport::{Viewport, WorldBbox};

/// Top-level implementation called from the `#[pg_extern]` in `lib.rs`.
#[allow(clippy::too_many_arguments)]
pub fn xarray_to_png_impl(
    dataset: &str,
    surface_var: &str,
    at_time: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    width: i32,
    height: i32,
    colormap_name: &str,
    options: Option<pgrx::JsonB>,
) -> Vec<u8> {
    if dataset.is_empty() || surface_var.is_empty() {
        pgrx::error!("pgx.xarray_to_png: dataset and surface_var are required");
    }
    if width <= 0 || height <= 0 || width > 8192 || height > 8192 {
        pgrx::error!(
            "pgx.xarray_to_png: width/height must be in (0, 8192], got {}x{}",
            width,
            height
        );
    }
    let opts = parse_options(options.as_ref());

    // Reuse the GLB mesh-assembly path. `at_time` narrows the time range
    // to a single timestep; if not supplied, we render the first available.
    let kind = crate::glb::lookup_mesh_kind(dataset, surface_var);
    let mesh = match kind.as_deref() {
        Some("ugrid_triangle") | Some("ugrid_polygon") => mesh_assembly::assemble_ugrid_triangle(
            dataset,
            surface_var,
            None,
            None,
            at_time,
            at_time,
            bbox_wkt,
            opts.max_cells,
        ),
        Some("regular_grid") => mesh_assembly::assemble_regular_grid(
            dataset,
            surface_var,
            None,
            at_time,
            at_time,
            bbox_wkt,
            opts.max_cells,
        ),
        Some(other) => Err(format!(
            "pgx.xarray_to_png: mesh kind '{other}' not yet supported (v1 covers ugrid_triangle and regular_grid)"
        )),
        None => Err(format!(
            "pgx.xarray_to_png: no mesh registered for '{dataset}.{surface_var}'. \
             Use pgx.register_file / pgx.register_mesh first."
        )),
    };
    let mesh = match mesh {
        Ok(m) => m,
        Err(e) => pgrx::error!("{}", e),
    };
    if mesh.keyframes.is_empty() {
        pgrx::error!(
            "pgx.xarray_to_png: no values returned for '{}.{}'",
            dataset,
            surface_var
        );
    }
    let frame = pick_frame(&mesh, at_time);

    let (vmin, vmax) = resolve_value_range(opts.vmin, opts.vmax, dataset, surface_var, &mesh);
    let lut = colormap::lookup(colormap_name);

    // Pick the viewport bbox: user-supplied takes precedence, else the
    // mesh's own envelope (so the user can call the function with NULL
    // bbox and still get a sensible image).
    let bbox = match bbox_wkt {
        Some(wkt) => WorldBbox::from_wkt(wkt).unwrap_or_else(|| {
            pgrx::error!("pgx.xarray_to_png: could not parse bbox_wkt '{}'", wkt)
        }),
        None => WorldBbox::from_xy(&mesh.base_xy).unwrap_or_else(|| {
            pgrx::error!("pgx.xarray_to_png: empty mesh — cannot compute extent")
        }),
    };
    if bbox.is_degenerate() {
        pgrx::error!(
            "pgx.xarray_to_png: degenerate bbox ({}x{}) — supply a non-zero envelope",
            bbox.width(),
            bbox.height()
        );
    }

    let vp = Viewport::new(bbox, width as u32, height as u32);
    let buf = rasterise::rasterise_keyframe(
        &mesh.triangles,
        &mesh.base_xy,
        frame,
        &vp,
        lut,
        vmin,
        vmax,
        opts.background,
    );

    match encoder::encode_png(&buf) {
        Ok(bytes) => bytes,
        Err(e) => pgrx::error!("{}", e),
    }
}

/// Pick the keyframe matching `at_time`, falling back to the first
/// available. The mesh assembly already filters down to candidates
/// whose chunk's `time_range @> at_time`, so usually there's only one
/// keyframe to pick.
fn pick_frame(
    mesh: &AssembledMesh,
    _at_time: Option<pgrx::datum::TimestampWithTimeZone>,
) -> &Keyframe {
    // For v1 the assembly path narrows on at_time, so the first frame
    // is the right one. Future: search by exact timestamp match here.
    &mesh.keyframes[0]
}

// =============================================================================
// Options + value range resolution (mirrors the GLB path)
// =============================================================================

struct RasterOptions {
    vmin: Option<f64>,
    vmax: Option<f64>,
    max_cells: i32,
    background: [u8; 4],
}

impl Default for RasterOptions {
    fn default() -> Self {
        Self {
            vmin: None,
            vmax: None,
            max_cells: 1_000_000,
            background: [0, 0, 0, 0], // fully transparent
        }
    }
}

fn parse_options(j: Option<&pgrx::JsonB>) -> RasterOptions {
    let mut out = RasterOptions::default();
    let Some(j) = j else { return out };
    if let Some(v) = j.0.get("vmin").and_then(|v| v.as_f64()) {
        out.vmin = Some(v);
    }
    if let Some(v) = j.0.get("vmax").and_then(|v| v.as_f64()) {
        out.vmax = Some(v);
    }
    if let Some(v) = j.0.get("max_cells").and_then(|v| v.as_i64()) {
        out.max_cells = v as i32;
    }
    if let Some(v) = j.0.get("background_color") {
        if let Some(rgba) = parse_color(v) {
            out.background = rgba;
        }
    }
    out
}

/// Parse `"#RRGGBB"` or `"#RRGGBBAA"` (with or without leading `#`)
/// into an RGBA byte tuple. Returns `None` for malformed input.
fn parse_color(v: &Value) -> Option<[u8; 4]> {
    let s = v.as_str()?.trim();
    let hex = s.strip_prefix('#').unwrap_or(s);
    let bytes = (0..hex.len() / 2)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
        .collect::<Option<Vec<u8>>>()?;
    match bytes.as_slice() {
        [r, g, b] => Some([*r, *g, *b, 255]),
        [r, g, b, a] => Some([*r, *g, *b, *a]),
        _ => None,
    }
}

fn resolve_value_range(
    opt_min: Option<f64>,
    opt_max: Option<f64>,
    dataset: &str,
    variable: &str,
    mesh: &AssembledMesh,
) -> (f64, f64) {
    if let (Some(a), Some(b)) = (opt_min, opt_max) {
        return (a, b);
    }
    let cat = crate::glb::lookup_valid_min_max(dataset, variable);
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

#[cfg(test)]
mod tests {
    use super::parse_color;
    use serde_json::json;

    #[test]
    fn parse_color_accepts_3_and_4_byte_hex() {
        assert_eq!(parse_color(&json!("#ff0000")), Some([255, 0, 0, 255]));
        assert_eq!(parse_color(&json!("ff0000aa")), Some([255, 0, 0, 0xAA]));
        assert_eq!(parse_color(&json!("#00ff00ff")), Some([0, 255, 0, 255]));
    }

    #[test]
    fn parse_color_rejects_garbage() {
        assert_eq!(parse_color(&json!("not-hex")), None);
        assert_eq!(parse_color(&json!("#abc")), None); // 3 chars, odd length
        assert_eq!(parse_color(&json!(42)), None);
    }
}
