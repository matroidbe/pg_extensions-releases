pub mod boolean;
pub mod brep;
pub mod explode;
pub mod export;
pub mod ifc_geom;
pub mod import;
pub mod mesh;
pub mod primitives;
pub mod props;
pub mod spatial;
pub mod transform;

use std::ffi::CStr;
use std::os::raw::c_void;

extern "C" {
    // Error handling
    fn occt_last_error() -> *const std::os::raw::c_char;

    // Shape lifecycle
    fn occt_shape_free(shape: *mut c_void);
    fn occt_buffer_free(buf: *mut u8);

    // BRep serialization
    fn occt_brep_read(data: *const u8, len: usize, shape_out: *mut *mut c_void) -> i32;
    fn occt_brep_write(shape: *mut c_void, data_out: *mut *mut u8, len_out: *mut usize) -> i32;

    // Bounding box
    fn occt_shape_bbox(
        shape: *mut c_void,
        xmin: *mut f32,
        ymin: *mut f32,
        zmin: *mut f32,
        xmax: *mut f32,
        ymax: *mut f32,
        zmax: *mut f32,
    ) -> i32;

    // Properties
    fn occt_shape_volume(shape: *mut c_void, volume_out: *mut f64) -> i32;
    fn occt_shape_surface_area(shape: *mut c_void, area_out: *mut f64) -> i32;
    fn occt_shape_centroid(shape: *mut c_void, cx: *mut f64, cy: *mut f64, cz: *mut f64) -> i32;
    fn occt_shape_face_count(shape: *mut c_void, count_out: *mut i32) -> i32;
    fn occt_shape_is_valid(shape: *mut c_void, valid_out: *mut i32) -> i32;

    // Compound explode
    pub(crate) fn occt_shape_solid_count(shape: *mut c_void, count_out: *mut i32) -> i32;
    pub(crate) fn occt_shape_solid_at(shape: *mut c_void, index: i32, out: *mut *mut c_void)
        -> i32;

    // Primitives
    fn occt_make_box(dx: f64, dy: f64, dz: f64, shape_out: *mut *mut c_void) -> i32;
    fn occt_make_cylinder(radius: f64, height: f64, shape_out: *mut *mut c_void) -> i32;
    fn occt_make_sphere(radius: f64, shape_out: *mut *mut c_void) -> i32;
    fn occt_make_cone(r1: f64, r2: f64, height: f64, shape_out: *mut *mut c_void) -> i32;

    // Spatial predicates
    pub(crate) fn occt_shape_distance(
        shape1: *mut c_void,
        shape2: *mut c_void,
        dist_out: *mut f64,
    ) -> i32;
    pub(crate) fn occt_shape_intersects(
        shape1: *mut c_void,
        shape2: *mut c_void,
        result_out: *mut i32,
    ) -> i32;
    pub(crate) fn occt_point_in_solid(
        shape: *mut c_void,
        x: f64,
        y: f64,
        z: f64,
        result_out: *mut i32,
    ) -> i32;

    // OBB
    pub(crate) fn occt_shape_obb(
        shape: *mut c_void,
        cx: *mut f64,
        cy: *mut f64,
        cz: *mut f64,
        xx: *mut f64,
        xy: *mut f64,
        xz: *mut f64,
        yx: *mut f64,
        yy: *mut f64,
        yz: *mut f64,
        hx: *mut f64,
        hy: *mut f64,
        hz: *mut f64,
    ) -> i32;

    // Transforms
    pub(crate) fn occt_shape_translate(
        shape: *mut c_void,
        dx: f64,
        dy: f64,
        dz: f64,
        out: *mut *mut c_void,
    ) -> i32;
    pub(crate) fn occt_shape_rotate(
        shape: *mut c_void,
        ax: f64,
        ay: f64,
        az: f64,
        angle_rad: f64,
        out: *mut *mut c_void,
    ) -> i32;
    pub(crate) fn occt_shape_transform(
        shape: *mut c_void,
        matrix: *const f64,
        out: *mut *mut c_void,
    ) -> i32;

    // Boolean operations
    pub(crate) fn occt_shape_fuse(
        shape1: *mut c_void,
        shape2: *mut c_void,
        out: *mut *mut c_void,
    ) -> i32;
    pub(crate) fn occt_shape_cut(
        shape1: *mut c_void,
        shape2: *mut c_void,
        out: *mut *mut c_void,
    ) -> i32;
    pub(crate) fn occt_shape_common(
        shape1: *mut c_void,
        shape2: *mut c_void,
        out: *mut *mut c_void,
    ) -> i32;

    // Shape healing
    pub(crate) fn occt_shape_heal(shape: *mut c_void, out: *mut *mut c_void) -> i32;

    // STL export
    pub(crate) fn occt_shape_to_stl(
        shape: *mut c_void,
        linear_deflection: f64,
        angular_deflection: f64,
        data_out: *mut *mut u8,
        len_out: *mut usize,
    ) -> i32;

    // Extrusion (rectangle profile on XY plane)
    pub(crate) fn occt_shape_extrude(
        width: f64,
        height: f64,
        dx: f64,
        dy: f64,
        dz: f64,
        out: *mut *mut c_void,
    ) -> i32;

    // STEP import/export
    pub(crate) fn occt_step_read(data: *const u8, len: usize, shape_out: *mut *mut c_void) -> i32;
    pub(crate) fn occt_step_write(
        shape: *mut c_void,
        data_out: *mut *mut u8,
        len_out: *mut usize,
    ) -> i32;

    // IGES import/export
    pub(crate) fn occt_iges_read(data: *const u8, len: usize, shape_out: *mut *mut c_void) -> i32;
    pub(crate) fn occt_iges_write(
        shape: *mut c_void,
        data_out: *mut *mut u8,
        len_out: *mut usize,
    ) -> i32;

    // File-based import (reads directly from disk path)
    pub(crate) fn occt_step_read_file(
        filepath: *const std::os::raw::c_char,
        shape_out: *mut *mut c_void,
    ) -> i32;
    pub(crate) fn occt_iges_read_file(
        filepath: *const std::os::raw::c_char,
        shape_out: *mut *mut c_void,
    ) -> i32;

    // Shape check (diagnostic string)
    pub(crate) fn occt_shape_check(
        shape: *mut c_void,
        msg_out: *mut *mut std::os::raw::c_char,
        msg_len: *mut usize,
    ) -> i32;

    // Offset shape (expand/shrink)
    pub(crate) fn occt_shape_offset(
        shape: *mut c_void,
        distance: f64,
        out: *mut *mut c_void,
    ) -> i32;

    // Section area (shared face area between two touching solids)
    pub(crate) fn occt_shape_section_area(
        shape1: *mut c_void,
        shape2: *mut c_void,
        area_out: *mut f64,
    ) -> i32;

    // Slice: cross-section with a plane
    pub(crate) fn occt_shape_slice(
        shape: *mut c_void,
        ox: f64,
        oy: f64,
        oz: f64,
        nx: f64,
        ny: f64,
        nz: f64,
        out: *mut *mut c_void,
    ) -> i32;

    // 2D projection via hidden line removal
    pub(crate) fn occt_shape_project_2d(
        shape: *mut c_void,
        dx: f64,
        dy: f64,
        dz: f64,
        out: *mut *mut c_void,
    ) -> i32;

    // Mesh extraction (raw arrays for format serialization in Rust)
    pub(crate) fn occt_mesh_extract(
        shape: *mut c_void,
        linear_deflection: f64,
        angular_deflection: f64,
        positions_out: *mut *mut f32,
        position_count: *mut usize,
        normals_out: *mut *mut f32,
        normal_count: *mut usize,
        indices_out: *mut *mut u32,
        index_count: *mut usize,
    ) -> i32;
    pub(crate) fn occt_mesh_free(positions: *mut f32, normals: *mut f32, indices: *mut u32);
}

fn last_error() -> String {
    unsafe {
        let ptr = occt_last_error();
        if ptr.is_null() {
            "unknown OCCT error".to_string()
        } else {
            CStr::from_ptr(ptr).to_string_lossy().into_owned()
        }
    }
}

/// RAII wrapper for OCCT shape pointers.
pub struct OcctShape {
    ptr: *mut c_void,
}

impl OcctShape {
    pub fn from_ptr(ptr: *mut c_void) -> Self {
        Self { ptr }
    }

    pub fn ptr(&self) -> *mut c_void {
        self.ptr
    }
}

impl Drop for OcctShape {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { occt_shape_free(self.ptr) };
        }
    }
}

/// RAII wrapper for buffers allocated by OCCT wrapper.
pub struct OcctBuffer {
    ptr: *mut u8,
    len: usize,
}

impl OcctBuffer {
    pub fn as_slice(&self) -> &[u8] {
        if self.ptr.is_null() || self.len == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}

impl Drop for OcctBuffer {
    fn drop(&mut self) {
        if !self.ptr.is_null() {
            unsafe { occt_buffer_free(self.ptr) };
        }
    }
}
