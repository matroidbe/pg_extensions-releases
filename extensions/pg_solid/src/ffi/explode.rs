use std::os::raw::c_void;

use super::{last_error, OcctShape};

/// Count the number of TopAbs_SOLID children in a shape.
/// For a simple solid, returns 1. For a compound, returns the number of solid bodies.
pub fn solid_count(shape: &OcctShape) -> Result<i32, String> {
    let mut count = 0i32;
    let rc = unsafe { super::occt_shape_solid_count(shape.ptr(), &mut count) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(count)
}

/// Extract the Nth solid child (0-indexed) as a new OcctShape.
pub fn solid_at(shape: &OcctShape, index: i32) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_shape_solid_at(shape.ptr(), index, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}
