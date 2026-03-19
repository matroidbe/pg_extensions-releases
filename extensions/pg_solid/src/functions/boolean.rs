use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

/// Boolean union of two solids.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_union(a: Solid, b: Solid) -> Solid {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_union: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_union: {e}"));
    let result =
        ffi::boolean::fuse(&shape_a, &shape_b).unwrap_or_else(|e| pgrx::error!("solid_union: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_union: {e}"))
}

/// Boolean difference: a minus b.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_difference(a: Solid, b: Solid) -> Solid {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_difference: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_difference: {e}"));
    let result = ffi::boolean::cut(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_difference: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_difference: {e}"))
}

/// Boolean intersection of two solids.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_intersection(a: Solid, b: Solid) -> Solid {
    let shape_a = a
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_intersection: {e}"));
    let shape_b = b
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_intersection: {e}"));
    let result = ffi::boolean::common(&shape_a, &shape_b)
        .unwrap_or_else(|e| pgrx::error!("solid_intersection: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_intersection: {e}"))
}

/// Heal/fix a solid geometry (fix edges, shells, wires).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_heal(s: Solid) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_heal: {e}"));
    let healed = ffi::boolean::heal(&shape).unwrap_or_else(|e| pgrx::error!("solid_heal: {e}"));
    Solid::from_occt_shape(&healed).unwrap_or_else(|e| pgrx::error!("solid_heal: {e}"))
}

/// Offset (expand/shrink) a solid by a distance.
/// Positive distance expands, negative shrinks.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_offset(s: Solid, distance: f64) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_offset: {e}"));
    let result = ffi::boolean::offset(&shape, distance)
        .unwrap_or_else(|e| pgrx::error!("solid_offset: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_offset: {e}"))
}

/// Cross-section: slice a solid with a plane defined by origin point and normal direction.
/// Returns the 2D cross-section as a Solid (compound of planar faces).
/// Useful for floor plans (slice at Z height) or section views.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_slice(
    s: Solid,
    ox: default!(f64, 0.0),
    oy: default!(f64, 0.0),
    oz: default!(f64, 0.0),
    nx: default!(f64, 0.0),
    ny: default!(f64, 0.0),
    nz: default!(f64, 1.0),
) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_slice: {e}"));
    let result = ffi::boolean::slice(&shape, ox, oy, oz, nx, ny, nz)
        .unwrap_or_else(|e| pgrx::error!("solid_slice: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_slice: {e}"))
}

/// Project a 3D solid to 2D edges using hidden line removal.
/// Direction (dx, dy, dz) is the viewing direction (default: top-down Z).
/// Returns a compound of visible edge outlines as a Solid.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_2d(
    s: Solid,
    dx: default!(f64, 0.0),
    dy: default!(f64, 0.0),
    dz: default!(f64, 1.0),
) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_2d: {e}"));
    let result = ffi::boolean::project_2d(&shape, dx, dy, dz)
        .unwrap_or_else(|e| pgrx::error!("solid_to_2d: {e}"));
    Solid::from_occt_shape(&result).unwrap_or_else(|e| pgrx::error!("solid_to_2d: {e}"))
}

/// Extrude a rectangular profile along a direction vector (IFC-style linear sweep).
/// Creates a width x height rectangle on the XY plane, then sweeps along (dx, dy, dz).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_from_ifc_extrusion(width: f64, height: f64, dx: f64, dy: f64, dz: f64) -> Solid {
    let result = ffi::boolean::extrude(width, height, dx, dy, dz)
        .unwrap_or_else(|e| pgrx::error!("solid_from_ifc_extrusion: {e}"));
    Solid::from_occt_shape(&result)
        .unwrap_or_else(|e| pgrx::error!("solid_from_ifc_extrusion: {e}"))
}
