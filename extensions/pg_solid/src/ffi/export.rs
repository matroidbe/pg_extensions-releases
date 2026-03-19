use super::{last_error, OcctBuffer, OcctShape};

/// Tessellate shape and export as binary STL bytes.
pub fn to_stl(
    shape: &OcctShape,
    linear_deflection: f64,
    angular_deflection: f64,
) -> Result<Vec<u8>, String> {
    let mut data_ptr: *mut u8 = std::ptr::null_mut();
    let mut data_len: usize = 0;
    let rc = unsafe {
        super::occt_shape_to_stl(
            shape.ptr(),
            linear_deflection,
            angular_deflection,
            &mut data_ptr,
            &mut data_len,
        )
    };
    if rc != 0 {
        return Err(last_error());
    }
    let buf = OcctBuffer {
        ptr: data_ptr,
        len: data_len,
    };
    Ok(buf.as_slice().to_vec())
}
