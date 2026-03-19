use pgrx::prelude::*;

use crate::ffi;
use crate::types::point3d::Point3D;
use crate::types::solid::Solid;

/// Minimum distance between two solids (mm).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_distance(a: Solid, b: Solid) -> f64 {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_distance: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_distance: {e}"));
    ffi::spatial::distance(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_distance: {e}"))
}

/// Do two solids intersect (share any point)?
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_intersects(a: Solid, b: Solid) -> bool {
    // Fast rejection via pre-computed AABB
    let bbox_a = a.header.bbox3d();
    let bbox_b = b.header.bbox3d();
    if !bbox_a.overlaps(&bbox_b) {
        return false;
    }
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_intersects: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_intersects: {e}"));
    ffi::spatial::intersects(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_intersects: {e}"))
}

/// Does the solid contain the given point?
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_contains_point(s: Solid, pt: Point3D) -> bool {
    // Fast rejection via pre-computed AABB
    let bbox = s.header.bbox3d();
    if !bbox.contains_point(pt.x, pt.y, pt.z) {
        return false;
    }
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_contains_point: {e}"));
    ffi::spatial::point_in_solid(&shape, pt.x, pt.y, pt.z)
        .unwrap_or_else(|e| pgrx::error!("solid_contains_point: {e}"))
}

/// Are two solids within a given clearance distance?
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_within_clearance(a: Solid, b: Solid, distance: f64) -> bool {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_within_clearance: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_within_clearance: {e}"));
    let dist = ffi::spatial::distance(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_within_clearance: {e}"));
    dist < distance
}

/// Does solid A fully contain solid B?
/// Uses boolean intersection: volume(common(A,B)) ≈ volume(B).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_contains(a: Solid, b: Solid) -> bool {
    // Fast rejection: A's bbox must fully contain B's bbox
    let bbox_a = a.header.bbox3d();
    let bbox_b = b.header.bbox3d();
    if bbox_b.xmin < bbox_a.xmin
        || bbox_b.ymin < bbox_a.ymin
        || bbox_b.zmin < bbox_a.zmin
        || bbox_b.xmax > bbox_a.xmax
        || bbox_b.ymax > bbox_a.ymax
        || bbox_b.zmax > bbox_a.zmax
    {
        return false;
    }

    let vol_b = b.header.volume;
    if vol_b < 1e-10 {
        return false;
    }

    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_contains: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_contains: {e}"));
    let intersection = ffi::boolean::common(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_contains: {e}"));
    let vol_common =
        ffi::props::volume(&intersection).unwrap_or_else(|e| pgrx::error!("solid_contains: {e}"));
    (vol_common - vol_b).abs() / vol_b < 0.01
}

/// Do two solids touch at their boundary without overlapping?
/// True when distance ≈ 0 AND boolean intersection has zero volume.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_touches(a: Solid, b: Solid) -> bool {
    // Fast rejection: bboxes must overlap or touch
    let bbox_a = a.header.bbox3d();
    let bbox_b = b.header.bbox3d();
    if !bbox_a.overlaps(&bbox_b) {
        return false;
    }

    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_touches: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_touches: {e}"));

    // Must be in contact
    let dist = ffi::spatial::distance(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_touches: {e}"));
    if dist > 1e-6 {
        return false;
    }

    // But no volume overlap
    let intersection = ffi::boolean::common(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_touches: {e}"));
    let vol =
        ffi::props::volume(&intersection).unwrap_or_else(|e| pgrx::error!("solid_touches: {e}"));
    vol.abs() < 1e-6
}

/// Does the host solid contain the centroid of the guest solid? (BIM hosting)
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_hosts(host: Solid, guest: Solid) -> bool {
    let shape_guest = guest
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_hosts: {e}"));
    let [cx, cy, cz] =
        ffi::props::centroid(&shape_guest).unwrap_or_else(|e| pgrx::error!("solid_hosts: {e}"));

    // Fast rejection: centroid outside host bbox
    let bbox = host.header.bbox3d();
    if !bbox.contains_point(cx, cy, cz) {
        return false;
    }

    let shape_host = host
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_hosts: {e}"));
    ffi::spatial::point_in_solid(&shape_host, cx, cy, cz)
        .unwrap_or_else(|e| pgrx::error!("solid_hosts: {e}"))
}
