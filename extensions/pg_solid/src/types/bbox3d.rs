use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::ffi::CStr;

#[derive(Debug, Clone, PostgresType, Serialize, Deserialize)]
#[inoutfuncs]
pub struct BBox3D {
    pub xmin: f32,
    pub ymin: f32,
    pub zmin: f32,
    pub xmax: f32,
    pub ymax: f32,
    pub zmax: f32,
}

impl BBox3D {
    pub fn new(xmin: f32, ymin: f32, zmin: f32, xmax: f32, ymax: f32, zmax: f32) -> Self {
        Self {
            xmin,
            ymin,
            zmin,
            xmax,
            ymax,
            zmax,
        }
    }

    pub fn from_array(vals: [f32; 6]) -> Self {
        Self::new(vals[0], vals[1], vals[2], vals[3], vals[4], vals[5])
    }

    pub fn overlaps(&self, other: &BBox3D) -> bool {
        self.xmin <= other.xmax
            && self.xmax >= other.xmin
            && self.ymin <= other.ymax
            && self.ymax >= other.ymin
            && self.zmin <= other.zmax
            && self.zmax >= other.zmin
    }

    /// Merge two bounding boxes into their union.
    pub fn union(&self, other: &BBox3D) -> BBox3D {
        BBox3D::new(
            self.xmin.min(other.xmin),
            self.ymin.min(other.ymin),
            self.zmin.min(other.zmin),
            self.xmax.max(other.xmax),
            self.ymax.max(other.ymax),
            self.zmax.max(other.zmax),
        )
    }

    /// Volume of the bounding box.
    pub fn bbox_volume(&self) -> f64 {
        (self.xmax - self.xmin) as f64
            * (self.ymax - self.ymin) as f64
            * (self.zmax - self.zmin) as f64
    }

    /// Volume enlargement if `other` were added to this bbox.
    pub fn enlargement(&self, other: &BBox3D) -> f64 {
        self.union(other).bbox_volume() - self.bbox_volume()
    }

    /// Equality check with f32 epsilon tolerance.
    pub fn same(&self, other: &BBox3D) -> bool {
        (self.xmin - other.xmin).abs() < f32::EPSILON
            && (self.ymin - other.ymin).abs() < f32::EPSILON
            && (self.zmin - other.zmin).abs() < f32::EPSILON
            && (self.xmax - other.xmax).abs() < f32::EPSILON
            && (self.ymax - other.ymax).abs() < f32::EPSILON
            && (self.zmax - other.zmax).abs() < f32::EPSILON
    }

    /// Minimum distance between two AABBs. Returns 0 if they overlap.
    pub fn min_distance(&self, other: &BBox3D) -> f64 {
        let dx = (self.xmin.max(other.xmin) - self.xmax.min(other.xmax)).max(0.0) as f64;
        let dy = (self.ymin.max(other.ymin) - self.ymax.min(other.ymax)).max(0.0) as f64;
        let dz = (self.zmin.max(other.zmin) - self.zmax.min(other.zmax)).max(0.0) as f64;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }

    /// Check if a point is inside this AABB.
    pub fn contains_point(&self, x: f64, y: f64, z: f64) -> bool {
        x >= self.xmin as f64
            && x <= self.xmax as f64
            && y >= self.ymin as f64
            && y <= self.ymax as f64
            && z >= self.zmin as f64
            && z <= self.zmax as f64
    }
}

impl InOutFuncs for BBox3D {
    // Format: BOX3D(xmin ymin zmin, xmax ymax zmax)
    fn input(input: &CStr) -> Self {
        let s = input.to_str().unwrap_or("");
        let s = s.trim();
        let inner = s
            .strip_prefix("BOX3D(")
            .and_then(|s| s.strip_suffix(')'))
            .unwrap_or_else(|| {
                pgrx::error!(
                    "invalid BOX3D format, expected: BOX3D(xmin ymin zmin, xmax ymax zmax)"
                )
            });

        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            pgrx::error!("invalid BOX3D format: expected two coordinate groups separated by comma");
        }

        let min_parts: Vec<f32> = parts[0]
            .split_whitespace()
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| pgrx::error!("invalid number in BOX3D: {s}"))
            })
            .collect();
        let max_parts: Vec<f32> = parts[1]
            .split_whitespace()
            .map(|s| {
                s.parse()
                    .unwrap_or_else(|_| pgrx::error!("invalid number in BOX3D: {s}"))
            })
            .collect();

        if min_parts.len() != 3 || max_parts.len() != 3 {
            pgrx::error!("BOX3D requires exactly 3 coordinates per corner");
        }

        Self::new(
            min_parts[0],
            min_parts[1],
            min_parts[2],
            max_parts[0],
            max_parts[1],
            max_parts[2],
        )
    }

    fn output(&self, buffer: &mut pgrx::StringInfo) {
        buffer.push_str(&format!(
            "BOX3D({} {} {}, {} {} {})",
            format_f32(self.xmin),
            format_f32(self.ymin),
            format_f32(self.zmin),
            format_f32(self.xmax),
            format_f32(self.ymax),
            format_f32(self.zmax),
        ));
    }
}

fn format_f32(v: f32) -> String {
    if v.fract() == 0.0 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox_overlaps() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b = BBox3D::new(5.0, 5.0, 5.0, 15.0, 15.0, 15.0);
        assert!(a.overlaps(&b));
        assert!(b.overlaps(&a));
    }

    #[test]
    fn test_bbox_no_overlap() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b = BBox3D::new(20.0, 20.0, 20.0, 30.0, 30.0, 30.0);
        assert!(!a.overlaps(&b));
    }

    #[test]
    fn test_bbox_union() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b = BBox3D::new(5.0, 5.0, 5.0, 20.0, 20.0, 20.0);
        let u = a.union(&b);
        assert_eq!(u.xmin, 0.0);
        assert_eq!(u.ymin, 0.0);
        assert_eq!(u.zmin, 0.0);
        assert_eq!(u.xmax, 20.0);
        assert_eq!(u.ymax, 20.0);
        assert_eq!(u.zmax, 20.0);
    }

    #[test]
    fn test_bbox_volume() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 20.0, 30.0);
        assert!((a.bbox_volume() - 6000.0).abs() < 1e-6);
    }

    #[test]
    fn test_bbox_enlargement() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        // Same box, no enlargement
        assert!((a.enlargement(&b)).abs() < 1e-6);

        let c = BBox3D::new(10.0, 10.0, 10.0, 20.0, 20.0, 20.0);
        // Union is (0,0,0,20,20,20) = 8000, original is 1000
        assert!((a.enlargement(&c) - 7000.0).abs() < 1e-6);
    }

    #[test]
    fn test_bbox_same() {
        let a = BBox3D::new(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        let b = BBox3D::new(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
        assert!(a.same(&b));

        let c = BBox3D::new(1.0, 2.0, 3.0, 4.0, 5.0, 7.0);
        assert!(!a.same(&c));
    }

    #[test]
    fn test_bbox_min_distance() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        let b = BBox3D::new(20.0, 0.0, 0.0, 30.0, 10.0, 10.0);
        // Gap of 10 along X axis
        assert!((a.min_distance(&b) - 10.0).abs() < 1e-6);

        // Overlapping boxes → distance 0
        let c = BBox3D::new(5.0, 5.0, 5.0, 15.0, 15.0, 15.0);
        assert!((a.min_distance(&c)).abs() < 1e-6);
    }

    #[test]
    fn test_bbox_contains_point() {
        let a = BBox3D::new(0.0, 0.0, 0.0, 10.0, 10.0, 10.0);
        assert!(a.contains_point(5.0, 5.0, 5.0));
        assert!(a.contains_point(0.0, 0.0, 0.0)); // on boundary
        assert!(!a.contains_point(11.0, 5.0, 5.0)); // outside
        assert!(!a.contains_point(-1.0, 5.0, 5.0)); // outside
    }
}
