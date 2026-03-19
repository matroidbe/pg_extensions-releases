use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

/// Move solid by offset. Returns new solid with recomputed header.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_translate(s: Solid, dx: f64, dy: f64, dz: f64) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"));
    let moved = ffi::transform::translate(&shape, dx, dy, dz)
        .unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"));
    Solid::from_occt_shape(&moved).unwrap_or_else(|e| pgrx::error!("solid_translate: {e}"))
}

/// Rotate solid around axis through origin. Angle in degrees.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_rotate(s: Solid, axis_x: f64, axis_y: f64, axis_z: f64, angle_deg: f64) -> Solid {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"));
    let angle_rad = angle_deg.to_radians();
    let rotated = ffi::transform::rotate(&shape, axis_x, axis_y, axis_z, angle_rad)
        .unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"));
    Solid::from_occt_shape(&rotated).unwrap_or_else(|e| pgrx::error!("solid_rotate: {e}"))
}

/// Apply 4x3 affine transformation matrix (12 floats, row-major: [R|t]).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_transform(s: Solid, matrix: Vec<f64>) -> Solid {
    if matrix.len() != 12 {
        pgrx::error!(
            "solid_transform: matrix must have exactly 12 elements (3x4 row-major), got {}",
            matrix.len()
        );
    }
    let mat: [f64; 12] = matrix.try_into().unwrap();
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"));
    let transformed = ffi::transform::transform(&shape, &mat)
        .unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"));
    Solid::from_occt_shape(&transformed).unwrap_or_else(|e| pgrx::error!("solid_transform: {e}"))
}
