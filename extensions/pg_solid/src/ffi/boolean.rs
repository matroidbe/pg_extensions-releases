use std::os::raw::c_void;

use super::{last_error, OcctShape};

/// Boolean union (fuse) of two shapes.
pub fn fuse(shape1: &OcctShape, shape2: &OcctShape) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_fuse(shape1.ptr(), shape2.ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Boolean difference (cut) — shape1 minus shape2.
pub fn cut(shape1: &OcctShape, shape2: &OcctShape) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_cut(shape1.ptr(), shape2.ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Boolean intersection (common) of two shapes.
pub fn common(shape1: &OcctShape, shape2: &OcctShape) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_common(shape1.ptr(), shape2.ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Shape healing — fix edges, shells, wires.
pub fn heal(shape: &OcctShape) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_heal(shape.ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Offset (expand/shrink) a shape by distance.
pub fn offset(shape: &OcctShape, distance: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_offset(shape.ptr(), distance, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Section area between two shapes (shared face area for touching solids).
pub fn section_area(shape1: &OcctShape, shape2: &OcctShape) -> Result<f64, String> {
    let mut area = 0.0f64;
    let rc = unsafe { super::occt_shape_section_area(shape1.ptr(), shape2.ptr(), &mut area) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(area)
}

/// Cross-section: slice a shape with a plane defined by origin + normal.
/// Returns a compound of faces on the cutting plane.
pub fn slice(
    shape: &OcctShape,
    ox: f64,
    oy: f64,
    oz: f64,
    nx: f64,
    ny: f64,
    nz: f64,
) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_slice(shape.ptr(), ox, oy, oz, nx, ny, nz, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// 2D projection of a shape along a direction using hidden line removal.
/// Returns a compound of 2D edges (visible outlines).
pub fn project_2d(shape: &OcctShape, dx: f64, dy: f64, dz: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_project_2d(shape.ptr(), dx, dy, dz, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Linear extrusion: create a rectangle (width x height) on XY plane, sweep along (dx, dy, dz).
pub fn extrude(width: f64, height: f64, dx: f64, dy: f64, dz: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_extrude(width, height, dx, dy, dz, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}
