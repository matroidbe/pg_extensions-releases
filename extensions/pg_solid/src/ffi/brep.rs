use super::{last_error, OcctBuffer, OcctShape};
use std::os::raw::c_void;

/// Deserialize BRep bytes into an OCCT shape.
pub fn read(brep_data: &[u8]) -> Result<OcctShape, String> {
    let mut shape_ptr: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_brep_read(brep_data.as_ptr(), brep_data.len(), &mut shape_ptr) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape { ptr: shape_ptr })
}

/// Serialize an OCCT shape to BRep bytes.
pub fn write(shape: &OcctShape) -> Result<Vec<u8>, String> {
    let mut data_ptr: *mut u8 = std::ptr::null_mut();
    let mut data_len: usize = 0;
    let rc = unsafe { super::occt_brep_write(shape.ptr(), &mut data_ptr, &mut data_len) };
    if rc != 0 {
        return Err(last_error());
    }
    let buf = OcctBuffer {
        ptr: data_ptr,
        len: data_len,
    };
    Ok(buf.as_slice().to_vec())
}
