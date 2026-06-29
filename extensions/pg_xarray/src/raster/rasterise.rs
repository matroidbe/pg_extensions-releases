//! Triangle rasteriser with barycentric per-vertex value interpolation.
//!
//! Walks each triangle of an [`AssembledMesh`], computes barycentric
//! coords for every pixel inside the triangle's bounding box, and
//! writes an RGBA byte per pixel using the colormap. NaN colour values
//! leave the pixel at the background colour.
//!
//! Single-threaded for v1 — straightforward, no scanline tricks. At
//! ~200k pixels × O(triangle_count) scans typical TELEMAC meshes
//! render in tens of milliseconds, well within WMS tile budgets.

use super::viewport::Viewport;
use crate::glb::colormap;
use crate::glb::mesh_assembly::Keyframe;

/// RGBA u8 pixel buffer, row-major, length = `width * height * 4`.
pub struct RasterBuffer {
    pub width: u32,
    pub height: u32,
    pub pixels: Vec<u8>,
}

impl RasterBuffer {
    pub fn new(width: u32, height: u32, background: [u8; 4]) -> Self {
        let mut pixels = Vec::with_capacity((width as usize) * (height as usize) * 4);
        for _ in 0..(width as usize) * (height as usize) {
            pixels.extend_from_slice(&background);
        }
        Self {
            width,
            height,
            pixels,
        }
    }

    #[inline]
    fn put(&mut self, col: i32, row: i32, rgba: [u8; 4]) {
        if col < 0 || row < 0 {
            return;
        }
        let (col, row) = (col as u32, row as u32);
        if col >= self.width || row >= self.height {
            return;
        }
        let idx = ((row * self.width + col) * 4) as usize;
        self.pixels[idx] = rgba[0];
        self.pixels[idx + 1] = rgba[1];
        self.pixels[idx + 2] = rgba[2];
        self.pixels[idx + 3] = rgba[3];
    }
}

/// Render a triangulated mesh at one timestep into a raster buffer.
///
/// `keyframe.color` carries the per-vertex scalar (NaN = skip). Colour
/// values are normalised by `(vmin, vmax)` and looked up in the LUT.
pub fn rasterise_keyframe(
    triangles: &[u32],
    base_xy: &[f32],
    keyframe: &Keyframe,
    viewport: &Viewport,
    lut: &colormap::Lut,
    vmin: f64,
    vmax: f64,
    background: [u8; 4],
) -> RasterBuffer {
    let mut buf = RasterBuffer::new(viewport.width, viewport.height, background);

    if triangles.len() < 3 {
        return buf;
    }

    let mut tri_idx = 0;
    while tri_idx + 2 < triangles.len() {
        let i0 = triangles[tri_idx] as usize;
        let i1 = triangles[tri_idx + 1] as usize;
        let i2 = triangles[tri_idx + 2] as usize;
        tri_idx += 3;

        if i0 >= keyframe.color.len() || i1 >= keyframe.color.len() || i2 >= keyframe.color.len() {
            continue;
        }
        if i0 * 2 + 1 >= base_xy.len() || i1 * 2 + 1 >= base_xy.len() || i2 * 2 + 1 >= base_xy.len()
        {
            continue;
        }

        // World vertices.
        let p0w = (base_xy[i0 * 2] as f64, base_xy[i0 * 2 + 1] as f64);
        let p1w = (base_xy[i1 * 2] as f64, base_xy[i1 * 2 + 1] as f64);
        let p2w = (base_xy[i2 * 2] as f64, base_xy[i2 * 2 + 1] as f64);

        // Project to pixel space (Y flipped).
        let p0 = viewport.world_to_pixel(p0w.0, p0w.1);
        let p1 = viewport.world_to_pixel(p1w.0, p1w.1);
        let p2 = viewport.world_to_pixel(p2w.0, p2w.1);

        // Per-vertex colour values.
        let v0 = keyframe.color[i0];
        let v1 = keyframe.color[i1];
        let v2 = keyframe.color[i2];

        rasterise_triangle(&mut buf, p0, p1, p2, v0, v1, v2, lut, vmin, vmax);
    }

    buf
}

/// Fill one triangle. Uses the half-plane test on integer pixel
/// centres + the barycentric coords for value interpolation.
fn rasterise_triangle(
    buf: &mut RasterBuffer,
    p0: (f64, f64),
    p1: (f64, f64),
    p2: (f64, f64),
    v0: f64,
    v1: f64,
    v2: f64,
    lut: &colormap::Lut,
    vmin: f64,
    vmax: f64,
) {
    // Triangle pixel bbox, clipped to the raster.
    let min_x = p0.0.min(p1.0).min(p2.0).floor() as i32;
    let max_x = p0.0.max(p1.0).max(p2.0).ceil() as i32;
    let min_y = p0.1.min(p1.1).min(p2.1).floor() as i32;
    let max_y = p0.1.max(p1.1).max(p2.1).ceil() as i32;

    let x_lo = min_x.max(0);
    let x_hi = max_x.min(buf.width as i32 - 1);
    let y_lo = min_y.max(0);
    let y_hi = max_y.min(buf.height as i32 - 1);

    if x_lo > x_hi || y_lo > y_hi {
        return;
    }

    // Edge function denominator — signed twice the triangle area.
    // Skip degenerate triangles (collinear vertices).
    let denom = edge(p0, p1, p2);
    if denom.abs() < 1e-12 {
        return;
    }
    let inv_denom = 1.0 / denom;

    for py in y_lo..=y_hi {
        let cy = py as f64 + 0.5;
        for px in x_lo..=x_hi {
            let cx = px as f64 + 0.5;
            let p = (cx, cy);

            // Signed barycentric weights via edge functions.
            let w0 = edge(p1, p2, p) * inv_denom;
            let w1 = edge(p2, p0, p) * inv_denom;
            let w2 = edge(p0, p1, p) * inv_denom;

            // Inside if all weights ≥ 0 (or all ≤ 0 — depends on winding).
            // We accept either, so the rasteriser is winding-agnostic.
            let inside =
                (w0 >= 0.0 && w1 >= 0.0 && w2 >= 0.0) || (w0 <= 0.0 && w1 <= 0.0 && w2 <= 0.0);
            if !inside {
                continue;
            }

            // Interpolate the scalar value. Skip if any vertex value is NaN
            // — the triangle is effectively a hole at this pixel.
            if !v0.is_finite() || !v1.is_finite() || !v2.is_finite() {
                continue;
            }
            let value = w0 * v0 + w1 * v1 + w2 * v2;
            let t = colormap::normalize(value, vmin, vmax);
            let rgb = colormap::sample(lut, t);
            buf.put(px, py, [rgb[0], rgb[1], rgb[2], 255]);
        }
    }
}

#[inline]
fn edge(a: (f64, f64), b: (f64, f64), c: (f64, f64)) -> f64 {
    // (b - a) × (c - a)
    (b.0 - a.0) * (c.1 - a.1) - (b.1 - a.1) * (c.0 - a.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::glb::colormap::VIRIDIS;
    use crate::glb::mesh_assembly::Keyframe;
    use crate::raster::viewport::{Viewport, WorldBbox};

    fn make_viewport() -> Viewport {
        Viewport::new(
            WorldBbox {
                min_x: 0.0,
                min_y: 0.0,
                max_x: 10.0,
                max_y: 10.0,
            },
            10,
            10,
        )
    }

    fn alpha_at(buf: &RasterBuffer, col: u32, row: u32) -> u8 {
        let idx = ((row * buf.width + col) * 4 + 3) as usize;
        buf.pixels[idx]
    }

    #[test]
    fn single_triangle_covers_expected_pixels() {
        // Right triangle covering the bottom-left half of a 10x10 raster.
        // World coords: (0,0), (10,0), (0,10). All values = 1.0.
        let triangles = vec![0u32, 1, 2];
        let base_xy: Vec<f32> = vec![0.0, 0.0, 10.0, 0.0, 0.0, 10.0];
        let keyframe = Keyframe {
            time_seconds: 0.0,
            z: vec![0.0; 3],
            color: vec![1.0; 3],
            flow_uv: None,
        };
        let vp = make_viewport();
        let buf = rasterise_keyframe(
            &triangles,
            &base_xy,
            &keyframe,
            &vp,
            &VIRIDIS,
            0.0,
            1.0,
            [0, 0, 0, 0],
        );

        // Top-left pixel maps to world (0, 10) — corner of triangle (inside).
        assert_eq!(alpha_at(&buf, 0, 0), 255, "(0, 10) corner should be filled");
        // Bottom-right corner of the raster maps to world (10, 0) — corner of triangle.
        assert_eq!(alpha_at(&buf, 9, 9), 255, "(10, 0) corner should be filled");
        // Top-right pixel maps to world (10, 10) — outside the triangle.
        assert_eq!(
            alpha_at(&buf, 9, 0),
            0,
            "(10, 10) outside should be transparent"
        );
    }

    #[test]
    fn nan_color_skips_pixels() {
        let triangles = vec![0u32, 1, 2];
        let base_xy: Vec<f32> = vec![0.0, 0.0, 10.0, 0.0, 0.0, 10.0];
        let keyframe = Keyframe {
            time_seconds: 0.0,
            z: vec![0.0; 3],
            color: vec![f64::NAN, f64::NAN, f64::NAN],
            flow_uv: None,
        };
        let vp = make_viewport();
        let buf = rasterise_keyframe(
            &triangles,
            &base_xy,
            &keyframe,
            &vp,
            &VIRIDIS,
            0.0,
            1.0,
            [10, 20, 30, 40],
        );
        // Every pixel should still be the background.
        assert_eq!(alpha_at(&buf, 0, 0), 40);
        assert_eq!(alpha_at(&buf, 5, 5), 40);
    }

    #[test]
    fn colormap_value_picks_correct_lut_entry() {
        // Triangle covering the whole raster; all vertices have value = vmax (1.0).
        let triangles = vec![0u32, 1, 2, 0, 2, 3];
        let base_xy: Vec<f32> = vec![0.0, 0.0, 10.0, 0.0, 10.0, 10.0, 0.0, 10.0];
        let keyframe = Keyframe {
            time_seconds: 0.0,
            z: vec![0.0; 4],
            color: vec![1.0; 4],
            flow_uv: None,
        };
        let vp = make_viewport();
        let buf = rasterise_keyframe(
            &triangles,
            &base_xy,
            &keyframe,
            &vp,
            &VIRIDIS,
            0.0,
            1.0,
            [0, 0, 0, 0],
        );
        // All-1.0 should pick VIRIDIS[255] (yellow). Sample a centre pixel.
        let idx = ((5 * buf.width + 5) * 4) as usize;
        assert_eq!(&buf.pixels[idx..idx + 3], &VIRIDIS[255][..]);
    }
}
