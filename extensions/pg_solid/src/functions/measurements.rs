use pgrx::prelude::*;

use crate::ffi;
use crate::types::bbox3d::BBox3D;
use crate::types::point3d::Point3D;
use crate::types::solid::Solid;
use crate::types::vec3d::Vec3D;

/// Volume in mm³. Reads from pre-computed header — zero OCCT cost.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_volume(s: Solid) -> f64 {
    s.header.volume
}

/// Surface area in mm². Reads from pre-computed header — zero OCCT cost.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_surface_area(s: Solid) -> f64 {
    s.header.surface_area
}

/// Axis-aligned bounding box. Reads from pre-computed header — zero OCCT cost.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_bbox(s: Solid) -> BBox3D {
    s.header.bbox3d()
}

/// Center of mass. Computed from B-Rep via OCCT.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_centroid(s: Solid) -> Point3D {
    let shape = s.to_occt_shape().unwrap_or_else(|e| {
        pgrx::error!("solid_centroid: {e}");
    });
    let [cx, cy, cz] = ffi::props::centroid(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_centroid: {e}");
    });
    Point3D::new(cx, cy, cz)
}

/// Number of B-Rep faces. Computed from B-Rep via OCCT.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_face_count(s: Solid) -> i32 {
    let shape = s.to_occt_shape().unwrap_or_else(|e| {
        pgrx::error!("solid_face_count: {e}");
    });
    ffi::props::face_count(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_face_count: {e}");
    })
}

/// Check if the solid geometry is valid.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_is_valid(s: Solid) -> bool {
    let shape = s.to_occt_shape().unwrap_or_else(|e| {
        pgrx::error!("solid_is_valid: {e}");
    });
    ffi::props::is_valid(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_is_valid: {e}");
    })
}

/// Oriented bounding box as text. Reads from pre-computed header — zero OCCT cost.
/// Format: OBB(cx cy cz, xx xy xz, yx yy yz, hx hy hz)
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_obb(s: Solid) -> String {
    let c = &s.header.obb_center;
    let x = &s.header.obb_x_axis;
    let y = &s.header.obb_y_axis;
    let h = &s.header.obb_half_size;
    format!(
        "OBB({} {} {}, {} {} {}, {} {} {}, {} {} {})",
        c[0], c[1], c[2], x[0], x[1], x[2], y[0], y[1], y[2], h[0], h[1], h[2]
    )
}

/// OBB volume (8 * hx * hy * hz). Reads from pre-computed header — zero OCCT cost.
/// Always <= AABB volume; equal for axis-aligned shapes.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_obb_volume(s: Solid) -> f64 {
    s.header.obb_volume()
}

/// Dimensions (length, width, height) from OBB, sorted descending.
/// Reads from pre-computed header — zero OCCT cost.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_dimensions(s: Solid) -> Vec3D {
    let h = &s.header.obb_half_size;
    let mut dims = [2.0 * h[0], 2.0 * h[1], 2.0 * h[2]];
    dims.sort_by(|a, b| b.partial_cmp(a).unwrap_or(std::cmp::Ordering::Equal));
    Vec3D::new(dims[0], dims[1], dims[2])
}

/// Human-readable validity diagnosis.
/// Returns "OK" for valid shapes, or "INVALID: N face error(s) ..." for invalid ones.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_check(s: Solid) -> String {
    let shape = s.to_occt_shape().unwrap_or_else(|e| {
        pgrx::error!("solid_check: {e}");
    });
    ffi::props::check(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_check: {e}");
    })
}

/// Shared face area between two touching solids (mm²).
/// Uses BRepAlgoAPI_Section to find intersection edges, builds faces, computes area.
/// Returns 0 if solids don't share a boundary face.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_shared_face_area(a: Solid, b: Solid) -> f64 {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_shared_face_area: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_shared_face_area: {e}"));
    ffi::boolean::section_area(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_shared_face_area: {e}"))
}
