use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

#[pg_extern(immutable, parallel_safe)]
pub fn solid_from_brep(brep_bytes: &[u8]) -> Solid {
    let shape = ffi::brep::read(brep_bytes).unwrap_or_else(|e| {
        pgrx::error!("solid_from_brep: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_from_brep: {e}");
    })
}

#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_brep(s: Solid) -> Vec<u8> {
    s.brep_bytes
}

#[pg_extern(immutable, parallel_safe)]
pub fn solid_box(dx: f64, dy: f64, dz: f64) -> Solid {
    let shape = ffi::primitives::make_box(dx, dy, dz).unwrap_or_else(|e| {
        pgrx::error!("solid_box: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_box: {e}");
    })
}

#[pg_extern(immutable, parallel_safe)]
pub fn solid_cylinder(radius: f64, height: f64) -> Solid {
    let shape = ffi::primitives::make_cylinder(radius, height).unwrap_or_else(|e| {
        pgrx::error!("solid_cylinder: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_cylinder: {e}");
    })
}

#[pg_extern(immutable, parallel_safe)]
pub fn solid_sphere(radius: f64) -> Solid {
    let shape = ffi::primitives::make_sphere(radius).unwrap_or_else(|e| {
        pgrx::error!("solid_sphere: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_sphere: {e}");
    })
}

#[pg_extern(immutable, parallel_safe)]
pub fn solid_cone(r1: f64, r2: f64, height: f64) -> Solid {
    let shape = ffi::primitives::make_cone(r1, r2, height).unwrap_or_else(|e| {
        pgrx::error!("solid_cone: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_cone: {e}");
    })
}

/// Import a solid from STEP file data (bytea).
#[pg_extern(immutable, parallel_safe)]
pub fn solid_from_step(step_bytes: &[u8]) -> Solid {
    let shape = ffi::import::from_step(step_bytes).unwrap_or_else(|e| {
        pgrx::error!("solid_from_step: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_from_step: {e}");
    })
}

/// Import a solid from IGES file data (bytea).
#[pg_extern(immutable, parallel_safe)]
pub fn solid_from_iges(iges_bytes: &[u8]) -> Solid {
    let shape = ffi::import::from_iges(iges_bytes).unwrap_or_else(|e| {
        pgrx::error!("solid_from_iges: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_from_iges: {e}");
    })
}

/// Import a solid from a STEP file on disk.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_from_step_file(filepath: &str) -> Solid {
    let shape = ffi::import::from_step_file(filepath).unwrap_or_else(|e| {
        pgrx::error!("solid_from_step_file: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_from_step_file: {e}");
    })
}

/// Import a solid from an IGES file on disk.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_from_iges_file(filepath: &str) -> Solid {
    let shape = ffi::import::from_iges_file(filepath).unwrap_or_else(|e| {
        pgrx::error!("solid_from_iges_file: {e}");
    });
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| {
        pgrx::error!("solid_from_iges_file: {e}");
    })
}
