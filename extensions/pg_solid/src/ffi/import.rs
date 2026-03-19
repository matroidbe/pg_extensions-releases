use std::ffi::CString;
use std::os::raw::c_void;

use super::{last_error, OcctBuffer, OcctShape};

/// Read a STEP file from a byte buffer, returning an OcctShape.
pub fn from_step(data: &[u8]) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_step_read(data.as_ptr(), data.len(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Write an OcctShape to STEP format, returning the bytes.
pub fn to_step(shape: &OcctShape) -> Result<Vec<u8>, String> {
    let mut data_ptr: *mut u8 = std::ptr::null_mut();
    let mut data_len: usize = 0;
    let rc = unsafe { super::occt_step_write(shape.ptr(), &mut data_ptr, &mut data_len) };
    if rc != 0 {
        return Err(last_error());
    }
    let buf = OcctBuffer {
        ptr: data_ptr,
        len: data_len,
    };
    Ok(buf.as_slice().to_vec())
}

/// Read an IGES file from a byte buffer, returning an OcctShape.
pub fn from_iges(data: &[u8]) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_iges_read(data.as_ptr(), data.len(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Read a STEP file from a filesystem path.
pub fn from_step_file(path: &str) -> Result<OcctShape, String> {
    let c_path = CString::new(path).map_err(|_| "invalid path (contains null byte)".to_string())?;
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_step_read_file(c_path.as_ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Read an IGES file from a filesystem path.
pub fn from_iges_file(path: &str) -> Result<OcctShape, String> {
    let c_path = CString::new(path).map_err(|_| "invalid path (contains null byte)".to_string())?;
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { super::occt_iges_read_file(c_path.as_ptr(), &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Write an OcctShape to IGES format, returning the bytes.
pub fn to_iges(shape: &OcctShape) -> Result<Vec<u8>, String> {
    let mut data_ptr: *mut u8 = std::ptr::null_mut();
    let mut data_len: usize = 0;
    let rc = unsafe { super::occt_iges_write(shape.ptr(), &mut data_ptr, &mut data_len) };
    if rc != 0 {
        return Err(last_error());
    }
    let buf = OcctBuffer {
        ptr: data_ptr,
        len: data_len,
    };
    Ok(buf.as_slice().to_vec())
}

/// Convert an IFC file to geometry using the built-in Rust parser + OCCT.
/// Parses the IFC STEP file, extracts all building element geometry,
/// and returns a compound shape containing all solids.
pub fn from_ifc_file(path: &str) -> Result<OcctShape, String> {
    let data = std::fs::read(path).map_err(|e| format!("failed to read IFC file: {e}"))?;

    let parse_result =
        crate::ifc::parser::parse(&data).map_err(|e| format!("IFC parse error: {e}"))?;

    let store = crate::ifc::entity::EntityStore::new(parse_result.entities);
    let model = crate::ifc::semantic::IfcSemanticModel::from_store(store);

    let elements = model.elements();
    let mut shapes: Vec<OcctShape> = Vec::new();

    for element in &elements {
        let entity = match model.store().get(element.entity_id) {
            Some(e) => e,
            None => continue,
        };
        match crate::ifc::geometry::element_to_shape(model.store(), entity) {
            Ok(Some(shape)) => shapes.push(shape),
            Ok(None) => {}
            Err(_) => {} // Skip elements with unsupported geometry
        }
    }

    if shapes.is_empty() {
        return Err("no geometry found in IFC file".into());
    }

    if shapes.len() == 1 {
        return Ok(shapes.into_iter().next().unwrap());
    }

    // Compound all shapes
    let refs: Vec<&OcctShape> = shapes.iter().collect();
    super::ifc_geom::make_compound(&refs)
}
