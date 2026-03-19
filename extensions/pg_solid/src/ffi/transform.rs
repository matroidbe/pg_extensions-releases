use std::os::raw::c_void;

use super::{last_error, OcctShape};

pub fn translate(shape: &OcctShape, dx: f64, dy: f64, dz: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_translate(shape.ptr(), dx, dy, dz, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

pub fn rotate(
    shape: &OcctShape,
    ax: f64,
    ay: f64,
    az: f64,
    angle_rad: f64,
) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_rotate(shape.ptr(), ax, ay, az, angle_rad, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

pub fn transform(shape: &OcctShape, matrix: &[f64; 12]) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_transform(shape.ptr(), matrix.as_ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}
