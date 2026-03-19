use super::{last_error, OcctShape};
use std::os::raw::c_void;

pub fn make_box(dx: f64, dy: f64, dz: f64) -> Result<OcctShape, String> {
    let mut ptr: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_make_box(dx, dy, dz, &mut ptr) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape { ptr })
}

pub fn make_cylinder(radius: f64, height: f64) -> Result<OcctShape, String> {
    let mut ptr: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_make_cylinder(radius, height, &mut ptr) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape { ptr })
}

pub fn make_sphere(radius: f64) -> Result<OcctShape, String> {
    let mut ptr: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_make_sphere(radius, &mut ptr) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape { ptr })
}

pub fn make_cone(r1: f64, r2: f64, height: f64) -> Result<OcctShape, String> {
    let mut ptr: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_make_cone(r1, r2, height, &mut ptr) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape { ptr })
}
