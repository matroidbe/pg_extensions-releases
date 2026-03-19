use super::{last_error, OcctShape};

pub fn distance(shape1: &OcctShape, shape2: &OcctShape) -> Result<f64, String> {
    let mut dist = 0.0f64;
    let rc = unsafe { super::occt_shape_distance(shape1.ptr(), shape2.ptr(), &mut dist) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(dist)
}

pub fn intersects(shape1: &OcctShape, shape2: &OcctShape) -> Result<bool, String> {
    let mut result = 0i32;
    let rc = unsafe { super::occt_shape_intersects(shape1.ptr(), shape2.ptr(), &mut result) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(result != 0)
}

pub fn point_in_solid(shape: &OcctShape, x: f64, y: f64, z: f64) -> Result<bool, String> {
    let mut result = 0i32;
    let rc = unsafe { super::occt_point_in_solid(shape.ptr(), x, y, z, &mut result) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(result != 0)
}
