use pgrx::prelude::*;

use crate::ffi;
use crate::types::solid::Solid;

/// Export solid as binary STL (bytea).
/// linear_deflection controls tessellation accuracy (smaller = finer mesh).
/// angular_deflection controls angular tolerance in radians.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_stl(
    s: Solid,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_stl: {e}"));
    ffi::export::to_stl(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_to_stl: {e}"))
}

/// Export solid as STEP file data (bytea).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_step(s: Solid) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_step: {e}"));
    ffi::import::to_step(&shape).unwrap_or_else(|e| pgrx::error!("solid_to_step: {e}"))
}

/// Export solid as IGES file data (bytea).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_iges(s: Solid) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_iges: {e}"));
    ffi::import::to_iges(&shape).unwrap_or_else(|e| pgrx::error!("solid_to_iges: {e}"))
}

/// Export solid as Wavefront OBJ (bytea).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_obj(
    s: Solid,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_obj: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_to_obj: {e}"));
    mesh.to_obj()
}

/// Export solid as glTF Binary v2 (bytea).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_glb(
    s: Solid,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_glb: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_to_glb: {e}"));
    mesh.to_glb()
}

/// Export solid as USD ASCII (bytea).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_to_usda(
    s: Solid,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> Vec<u8> {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_usda: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_to_usda: {e}"));
    mesh.to_usda()
}

/// Import an IFC file from disk, converting to Solid via ifcopenshell.
/// Requires Python 3 and ifcopenshell to be installed on the system.
#[pg_extern(strict)]
pub fn solid_from_ifc_file(filepath: &str) -> Solid {
    let shape = ffi::import::from_ifc_file(filepath)
        .unwrap_or_else(|e| pgrx::error!("solid_from_ifc_file: {e}"));
    Solid::from_occt_shape(&shape).unwrap_or_else(|e| pgrx::error!("solid_from_ifc_file: {e}"))
}

/// Export solid as OBJ file to disk.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_export_obj(
    s: Solid,
    filepath: &str,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> String {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_export_obj: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_export_obj: {e}"));
    let data = mesh.to_obj();
    std::fs::write(filepath, &data).unwrap_or_else(|e| pgrx::error!("solid_export_obj: {e}"));
    filepath.to_string()
}

/// Export solid as GLB file to disk.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_export_glb(
    s: Solid,
    filepath: &str,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> String {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_export_glb: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_export_glb: {e}"));
    let data = mesh.to_glb();
    std::fs::write(filepath, &data).unwrap_or_else(|e| pgrx::error!("solid_export_glb: {e}"));
    filepath.to_string()
}

/// Export solid as USDA file to disk.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_export_usda(
    s: Solid,
    filepath: &str,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
) -> String {
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_export_usda: {e}"));
    let mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_export_usda: {e}"));
    let data = mesh.to_usda();
    std::fs::write(filepath, &data).unwrap_or_else(|e| pgrx::error!("solid_export_usda: {e}"));
    filepath.to_string()
}
