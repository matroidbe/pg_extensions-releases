use std::os::raw::c_void;

use super::{last_error, OcctShape};

extern "C" {
    fn occt_make_rectangle_face(width: f64, height: f64, face_out: *mut *mut c_void) -> i32;
    fn occt_make_circle_face(radius: f64, face_out: *mut *mut c_void) -> i32;
    fn occt_make_polygon_face(points: *const f64, npoints: i32, face_out: *mut *mut c_void) -> i32;
    fn occt_extrude_face(
        face: *mut c_void,
        dx: f64,
        dy: f64,
        dz: f64,
        out: *mut *mut c_void,
    ) -> i32;
    fn occt_make_faceted_solid(
        all_points: *const f64,
        face_starts: *const i32,
        face_sizes: *const i32,
        nfaces: i32,
        out: *mut *mut c_void,
    ) -> i32;
    fn occt_make_compound(shapes: *mut *mut c_void, nshapes: i32, out: *mut *mut c_void) -> i32;
}

/// Create a centered rectangular face on the XY plane.
pub fn make_rectangle_face(width: f64, height: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { occt_make_rectangle_face(width, height, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Create a circular face centered at origin on XY plane.
pub fn make_circle_face(radius: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { occt_make_circle_face(radius, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Create a face from a closed polygon defined by 3D points.
pub fn make_polygon_face(points: &[[f64; 3]]) -> Result<OcctShape, String> {
    let flat: Vec<f64> = points.iter().flat_map(|p| p.iter().copied()).collect();
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { occt_make_polygon_face(flat.as_ptr(), points.len() as i32, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Extrude a face along a direction vector to create a solid.
pub fn extrude_face(face: &OcctShape, dx: f64, dy: f64, dz: f64) -> Result<OcctShape, String> {
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { occt_extrude_face(face.ptr(), dx, dy, dz, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Build a solid from a faceted boundary (list of polygon faces).
pub fn make_faceted_solid(faces: &[Vec<[f64; 3]>]) -> Result<OcctShape, String> {
    let mut all_points: Vec<f64> = Vec::new();
    let mut face_starts: Vec<i32> = Vec::new();
    let mut face_sizes: Vec<i32> = Vec::new();
    let mut point_offset = 0i32;

    for face in faces {
        face_starts.push(point_offset);
        face_sizes.push(face.len() as i32);
        for pt in face {
            all_points.extend_from_slice(pt);
        }
        point_offset += face.len() as i32;
    }

    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe {
        occt_make_faceted_solid(
            all_points.as_ptr(),
            face_starts.as_ptr(),
            face_sizes.as_ptr(),
            faces.len() as i32,
            &mut out,
        )
    };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}

/// Assemble multiple shapes into a compound.
pub fn make_compound(shapes: &[&OcctShape]) -> Result<OcctShape, String> {
    let mut ptrs: Vec<*mut c_void> = shapes.iter().map(|s| s.ptr()).collect();
    let mut out: *mut c_void = std::ptr::null_mut();
    let rc = unsafe { occt_make_compound(ptrs.as_mut_ptr(), shapes.len() as i32, &mut out) };
    if rc != 0 {
        return Err(last_error());
    }
    Ok(OcctShape::from_ptr(out))
}
