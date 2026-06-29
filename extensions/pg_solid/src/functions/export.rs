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

/// Export solid as glTF Binary v2 (bytea). `target_srid` is written into
/// `asset.extras.srid`. Default (4326) matches `solid_georeference`.
///
/// If `georef_path` is set, pg_solid parses the IFC file and applies the
/// resolved affine to the *tessellated vertex buffer* — booleans,
/// fusion, offsets, etc. run in the solid's native (metre-scale) frame
/// where OCCT tolerances behave; only the final degrees / ECEF /
/// projected-CRS coordinates touch the float buffer. This is the
/// recommended path for "fuse many IFC elements, then drop the result
/// on a map": call `solid_agg_union(solid)` in local metres, then
/// `solid_to_glb(..., georef_path => 'file.ifc')`.
#[pg_extern(immutable, parallel_safe)]
#[allow(clippy::too_many_arguments)]
pub fn solid_to_glb(
    s: Option<Solid>,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
    target_srid: default!(i32, 4326),
    georef_path: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    let s = s.unwrap_or_else(|| pgrx::error!("solid_to_glb: solid is NULL"));
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_to_glb: {e}"));
    let mut mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_to_glb: {e}"));
    if let Some(path) = georef_path {
        let mat = crate::ifc::georef::build_affine_for_file(
            path,
            target_srid,
            "solid_to_glb",
            crate::functions::transforms::tangent_plane_affine,
            crate::functions::transforms::ecef_affine,
        );
        mesh.apply_affine(&mat);
    }
    mesh.to_glb(target_srid)
}

/// Emit one glTF Binary v2 (GLB) carrying many independent meshes — one
/// per input solid, no boolean fusion. Each input becomes a separate
/// glTF node + mesh under scene 0; `names[i]` (if supplied) lands in
/// `nodes[i].name` so a viewer can map a picked node back to e.g. an
/// IfcGlobalId.
///
/// This is the right path for "show me every wall of this building":
///   - `solid_agg_union` (BRepAlgoAPI_Fuse) drops elements when they
///     share faces / touch corners, especially at degree scale;
///     `solids_to_multi_glb` never runs an OCCT boolean, so every
///     input row is rendered.
///   - Tessellation runs in each solid's native frame; if `georef_path`
///     is set the per-mesh vertex buffer is transformed at emit time.
///
/// `solids` array NULLs are skipped. `names` may be shorter than
/// `solids` — missing entries fall back to anonymous nodes.
#[pg_extern(immutable, parallel_safe)]
#[allow(clippy::too_many_arguments)]
pub fn solids_to_multi_glb(
    solids: Option<Vec<Option<Solid>>>,
    names: default!(Option<Vec<Option<String>>>, "NULL"),
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
    target_srid: default!(i32, 4326),
    georef_path: default!(Option<&str>, "NULL"),
) -> Vec<u8> {
    let solids =
        solids.unwrap_or_else(|| pgrx::error!("solids_to_multi_glb: solids array is NULL"));
    let affine = georef_path.map(|p| {
        crate::ifc::georef::build_affine_for_file(
            p,
            target_srid,
            "solids_to_multi_glb",
            crate::functions::transforms::tangent_plane_affine,
            crate::functions::transforms::ecef_affine,
        )
    });

    let mut meshes: Vec<ffi::mesh::MeshData> = Vec::with_capacity(solids.len());
    let mut emitted_names: Vec<Option<String>> = Vec::with_capacity(solids.len());

    for (idx, opt) in solids.into_iter().enumerate() {
        let Some(solid) = opt else { continue };
        let shape = solid
            .to_occt_shape()
            .unwrap_or_else(|e| pgrx::error!("solids_to_multi_glb: solid[{idx}]: {e}"));
        let mut mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
            .unwrap_or_else(|e| pgrx::error!("solids_to_multi_glb: solid[{idx}]: {e}"));
        if mesh.vertex_count == 0 {
            continue;
        }
        if let Some(mat) = affine.as_ref() {
            mesh.apply_affine(mat);
        }
        let name = names
            .as_ref()
            .and_then(|v| v.get(idx))
            .and_then(|n| n.clone());
        meshes.push(mesh);
        emitted_names.push(name);
    }

    if meshes.is_empty() {
        pgrx::error!("solids_to_multi_glb: no non-NULL solids with renderable geometry");
    }

    ffi::mesh::write_multi_glb(&meshes, &emitted_names, target_srid)
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

/// Export solid as GLB file to disk. Same signature as `solid_to_glb`
/// plus the destination `filepath`. See `solid_to_glb` for the role of
/// `target_srid` and `georef_path`.
#[pg_extern(immutable, parallel_safe)]
#[allow(clippy::too_many_arguments)]
pub fn solid_export_glb(
    s: Option<Solid>,
    filepath: &str,
    linear_deflection: default!(f64, 0.1),
    angular_deflection: default!(f64, 0.5),
    target_srid: default!(i32, 4326),
    georef_path: default!(Option<&str>, "NULL"),
) -> String {
    let s = s.unwrap_or_else(|| pgrx::error!("solid_export_glb: solid is NULL"));
    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_export_glb: {e}"));
    let mut mesh = ffi::mesh::MeshData::extract(&shape, linear_deflection, angular_deflection)
        .unwrap_or_else(|e| pgrx::error!("solid_export_glb: {e}"));
    if let Some(p) = georef_path {
        let mat = crate::ifc::georef::build_affine_for_file(
            p,
            target_srid,
            "solid_export_glb",
            crate::functions::transforms::tangent_plane_affine,
            crate::functions::transforms::ecef_affine,
        );
        mesh.apply_affine(&mat);
    }
    let data = mesh.to_glb(target_srid);
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

/// Axis-aligned bounding-box footprint at z = zmin as a closed WKT
/// `POLYGON Z ((x y z, ...))`. Caller pairs it with PostGIS:
///   `ST_GeomFromText(solid_footprint_wkt(s), <srid>)`
/// to obtain a `geometry(PolygonZ, srid)`. pg_solid itself has no PostGIS
/// dependency.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_footprint_wkt(s: Solid) -> String {
    let b = s.header.bbox;
    let xmin = b[0] as f64;
    let ymin = b[1] as f64;
    let zmin = b[2] as f64;
    let xmax = b[3] as f64;
    let ymax = b[4] as f64;
    format!(
        "POLYGON Z (({xmin} {ymin} {zmin}, {xmax} {ymin} {zmin}, {xmax} {ymax} {zmin}, {xmin} {ymax} {zmin}, {xmin} {ymin} {zmin}))"
    )
}

/// Axis-aligned bounding box as a closed WKT `POLYGON Z ((x y z, ...))` at
/// the bbox mid-Z. Pair with `ST_GeomFromText(..., srid)` for PostGIS.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_bbox_wkt(s: Solid) -> String {
    let b = s.header.bbox;
    let xmin = b[0] as f64;
    let ymin = b[1] as f64;
    let xmax = b[3] as f64;
    let ymax = b[4] as f64;
    let z = 0.5 * (b[2] as f64 + b[5] as f64);
    format!(
        "POLYGON Z (({xmin} {ymin} {z}, {xmax} {ymin} {z}, {xmax} {ymax} {z}, {xmin} {ymax} {z}, {xmin} {ymin} {z}))"
    )
}
