use super::{last_error, OcctShape};

/// Compute axis-aligned bounding box.
pub fn bbox(shape: &OcctShape) -> Result<[f32; 6], String> {
    let mut vals = [0.0f32; 6];
    let rc = unsafe {
        super::occt_shape_bbox(
            shape.ptr(),
            &mut vals[0],
            &mut vals[1],
            &mut vals[2],
            &mut vals[3],
            &mut vals[4],
            &mut vals[5],
        )
    };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(vals)
}

/// Compute volume (mm³).
pub fn volume(shape: &OcctShape) -> Result<f64, String> {
    let mut v = 0.0f64;
    let rc = unsafe { super::occt_shape_volume(shape.ptr(), &mut v) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(v)
}

/// Compute total surface area (mm²).
pub fn surface_area(shape: &OcctShape) -> Result<f64, String> {
    let mut a = 0.0f64;
    let rc = unsafe { super::occt_shape_surface_area(shape.ptr(), &mut a) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(a)
}

/// Compute center of mass.
pub fn centroid(shape: &OcctShape) -> Result<[f64; 3], String> {
    let mut cx = 0.0f64;
    let mut cy = 0.0f64;
    let mut cz = 0.0f64;
    let rc = unsafe { super::occt_shape_centroid(shape.ptr(), &mut cx, &mut cy, &mut cz) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok([cx, cy, cz])
}

/// Count B-Rep faces.
pub fn face_count(shape: &OcctShape) -> Result<i32, String> {
    let mut count = 0i32;
    let rc = unsafe { super::occt_shape_face_count(shape.ptr(), &mut count) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(count)
}

/// OBB data returned from OCCT: center, X/Y axis directions, half-sizes.
pub struct ObbData {
    pub center: [f64; 3],
    pub x_axis: [f64; 3],
    pub y_axis: [f64; 3],
    pub half_size: [f64; 3],
}

/// Compute oriented bounding box.
pub fn obb(shape: &OcctShape) -> Result<ObbData, String> {
    let (mut cx, mut cy, mut cz) = (0.0f64, 0.0f64, 0.0f64);
    let (mut xx, mut xy, mut xz) = (0.0f64, 0.0f64, 0.0f64);
    let (mut yx, mut yy, mut yz) = (0.0f64, 0.0f64, 0.0f64);
    let (mut hx, mut hy, mut hz) = (0.0f64, 0.0f64, 0.0f64);
    let rc = unsafe {
        super::occt_shape_obb(
            shape.ptr(),
            &mut cx,
            &mut cy,
            &mut cz,
            &mut xx,
            &mut xy,
            &mut xz,
            &mut yx,
            &mut yy,
            &mut yz,
            &mut hx,
            &mut hy,
            &mut hz,
        )
    };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(ObbData {
        center: [cx, cy, cz],
        x_axis: [xx, xy, xz],
        y_axis: [yx, yy, yz],
        half_size: [hx, hy, hz],
    })
}

/// Shape check — returns human-readable diagnostic string.
pub fn check(shape: &OcctShape) -> Result<String, String> {
    let mut msg_ptr: *mut std::os::raw::c_char = std::ptr::null_mut();
    let mut msg_len: usize = 0;
    let rc = unsafe { super::occt_shape_check(shape.ptr(), &mut msg_ptr, &mut msg_len) };
    if rc != 0 {
        return Err(last_error());
    }
    let result = if msg_ptr.is_null() {
        "unknown".to_string()
    } else {
        let s = unsafe { std::ffi::CStr::from_ptr(msg_ptr) }
            .to_string_lossy()
            .into_owned();
        unsafe { super::occt_buffer_free(msg_ptr as *mut u8) };
        s
    };
    Ok(result)
}

/// Check shape validity.
pub fn is_valid(shape: &OcctShape) -> Result<bool, String> {
    let mut valid = 0i32;
    let rc = unsafe { super::occt_shape_is_valid(shape.ptr(), &mut valid) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(valid != 0)
}
