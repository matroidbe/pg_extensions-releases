//! World-coords ↔ pixel-coords mapping for the 2D raster renderer.
//!
//! Two responsibilities:
//!   1. Parse a `POLYGON((minx miny, maxx miny, ...))` WKT bbox into a
//!      `WorldBbox` (the same shape callers pass into `pgx.fetch_mesh`),
//!      OR compute the bbox from an `AssembledMesh` when the caller
//!      didn't supply one.
//!   2. Provide an affine `Viewport` that maps world coords (X, Y) to
//!      pixel coords (i, j) — with Y flipped (image rows go top→bottom
//!      while world Y goes south→north).
//!
//! Coordinates stay in the dataset's native CRS — there's no
//! reprojection in v1. The caller is responsible for matching the
//! CRS in the GetMap request to the mesh's `srid`.

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WorldBbox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl WorldBbox {
    pub fn width(&self) -> f64 {
        self.max_x - self.min_x
    }

    pub fn height(&self) -> f64 {
        self.max_y - self.min_y
    }

    pub fn is_degenerate(&self) -> bool {
        !(self.width() > 0.0 && self.height() > 0.0)
    }

    /// Compute an axis-aligned bbox from an `(x, y)` interleaved vertex
    /// buffer like `AssembledMesh::base_xy`.
    pub fn from_xy(base_xy: &[f32]) -> Option<Self> {
        if base_xy.len() < 2 || !base_xy.len().is_multiple_of(2) {
            return None;
        }
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        let mut i = 0;
        while i < base_xy.len() {
            let x = base_xy[i] as f64;
            let y = base_xy[i + 1] as f64;
            if x.is_finite() {
                min_x = min_x.min(x);
                max_x = max_x.max(x);
            }
            if y.is_finite() {
                min_y = min_y.min(y);
                max_y = max_y.max(y);
            }
            i += 2;
        }
        if !(min_x.is_finite() && min_y.is_finite()) {
            return None;
        }
        Some(WorldBbox {
            min_x,
            min_y,
            max_x,
            max_y,
        })
    }

    /// Convert a `POLYGON((x0 y0, x1 y1, ...))` WKT string into a
    /// rectangular bbox by taking the min/max over its vertices.
    /// Lenient — accepts any polygon ring, computes its envelope.
    pub fn from_wkt(wkt: &str) -> Option<Self> {
        let lower = wkt.trim().to_ascii_uppercase();
        // Find the first parenthesised coord list.
        let open = lower.find('(')?;
        // Strip down to the inner coord list of the first ring.
        let inner_start = lower[open + 1..].find('(').map(|p| open + 1 + p + 1)?;
        let inner_end = lower[inner_start..].find(')').map(|p| inner_start + p)?;
        let coords = &wkt[inner_start..inner_end];
        let mut min_x = f64::INFINITY;
        let mut max_x = f64::NEG_INFINITY;
        let mut min_y = f64::INFINITY;
        let mut max_y = f64::NEG_INFINITY;
        for pair in coords.split(',') {
            let mut it = pair.split_ascii_whitespace();
            let x = it.next()?.parse::<f64>().ok()?;
            let y = it.next()?.parse::<f64>().ok()?;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        if !min_x.is_finite() {
            return None;
        }
        Some(WorldBbox {
            min_x,
            min_y,
            max_x,
            max_y,
        })
    }
}

/// World-to-pixel affine. World (X, Y) → pixel (col, row) with row 0
/// at the top of the image (Y flipped).
#[derive(Debug, Clone, Copy)]
pub struct Viewport {
    pub width: u32,
    pub height: u32,
    pub bbox: WorldBbox,
    sx: f64, // pixels per world unit (X)
    sy: f64, // pixels per world unit (Y) — positive, flip applied separately
}

impl Viewport {
    pub fn new(bbox: WorldBbox, width: u32, height: u32) -> Self {
        let sx = if bbox.width() > 0.0 {
            (width as f64) / bbox.width()
        } else {
            1.0
        };
        let sy = if bbox.height() > 0.0 {
            (height as f64) / bbox.height()
        } else {
            1.0
        };
        Viewport {
            width,
            height,
            bbox,
            sx,
            sy,
        }
    }

    /// World (X, Y) → fractional pixel (col, row).
    #[inline]
    pub fn world_to_pixel(&self, x: f64, y: f64) -> (f64, f64) {
        let col = (x - self.bbox.min_x) * self.sx;
        // Y axis flipped: world max_y is row 0, world min_y is row height.
        let row = (self.bbox.max_y - y) * self.sy;
        (col, row)
    }

    /// Inverse — fractional pixel (col, row) → world (X, Y).
    #[inline]
    #[allow(dead_code)] // forward-compat: GetFeatureInfo will need this in v2.
    pub fn pixel_to_world(&self, col: f64, row: f64) -> (f64, f64) {
        let x = self.bbox.min_x + col / self.sx;
        let y = self.bbox.max_y - row / self.sy;
        (x, y)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_wkt_basic_polygon() {
        let bbox = WorldBbox::from_wkt("POLYGON((0 0, 100 0, 100 50, 0 50, 0 0))").unwrap();
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.min_y, 0.0);
        assert_eq!(bbox.max_x, 100.0);
        assert_eq!(bbox.max_y, 50.0);
    }

    #[test]
    fn from_wkt_handles_case_and_spaces() {
        let bbox =
            WorldBbox::from_wkt("polygon ((  50.5 20.0 , 150 20, 150 80, 50.5 80, 50.5 20))")
                .unwrap();
        assert_eq!(bbox.min_x, 50.5);
        assert_eq!(bbox.max_y, 80.0);
    }

    #[test]
    fn from_wkt_rejects_garbage() {
        assert!(WorldBbox::from_wkt("LINESTRING(0 0, 1 1)").is_none());
        assert!(WorldBbox::from_wkt("POLYGON(())").is_none());
        assert!(WorldBbox::from_wkt("nonsense").is_none());
    }

    #[test]
    fn from_xy_computes_envelope() {
        let xy: [f32; 8] = [10.0, 20.0, 30.0, 5.0, -1.0, 40.0, 15.0, 25.0];
        let bbox = WorldBbox::from_xy(&xy).unwrap();
        assert_eq!(bbox.min_x, -1.0);
        assert_eq!(bbox.max_x, 30.0);
        assert_eq!(bbox.min_y, 5.0);
        assert_eq!(bbox.max_y, 40.0);
    }

    #[test]
    fn viewport_round_trip() {
        // 100m x 50m extent rendered at 200 x 100 pixels.
        let bbox = WorldBbox {
            min_x: 0.0,
            min_y: 0.0,
            max_x: 100.0,
            max_y: 50.0,
        };
        let vp = Viewport::new(bbox, 200, 100);
        // World (0, 50) → top-left pixel (0, 0)
        let (c, r) = vp.world_to_pixel(0.0, 50.0);
        assert!((c - 0.0).abs() < 1e-9);
        assert!((r - 0.0).abs() < 1e-9);
        // World (100, 0) → bottom-right pixel (200, 100)
        let (c, r) = vp.world_to_pixel(100.0, 0.0);
        assert!((c - 200.0).abs() < 1e-9);
        assert!((r - 100.0).abs() < 1e-9);
        // Round trip the centre
        let (x, y) = vp.pixel_to_world(100.0, 50.0);
        assert!((x - 50.0).abs() < 1e-9);
        assert!((y - 25.0).abs() < 1e-9);
    }

    #[test]
    fn viewport_zero_extent_does_not_divide_by_zero() {
        let bbox = WorldBbox {
            min_x: 5.0,
            min_y: 5.0,
            max_x: 5.0,
            max_y: 5.0,
        };
        let vp = Viewport::new(bbox, 100, 100);
        // No panic; degenerate viewport returns col=row=0 for the point.
        let (c, r) = vp.world_to_pixel(5.0, 5.0);
        assert_eq!(c, 0.0);
        assert_eq!(r, 0.0);
    }
}
