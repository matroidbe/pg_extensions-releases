#![allow(clippy::type_complexity)]
use pgrx::prelude::*;

use crate::ffi;
use crate::ifc;
use crate::types::solid::Solid;

/// Parse an IFC file and return all building elements with geometry and metadata.
/// Each row includes the element's global ID, IFC type, name, material, storey,
/// a 3D solid geometry (if convertible), and property sets as JSON.
#[pg_extern(immutable, strict)]
pub fn ifc_elements(
    filepath: &str,
) -> TableIterator<
    'static,
    (
        name!(global_id, String),
        name!(ifc_type, String),
        name!(name, Option<String>),
        name!(material, Option<String>),
        name!(storey, Option<String>),
        name!(solid, Option<Solid>),
        name!(properties, Option<pgrx::JsonB>),
    ),
> {
    let data =
        std::fs::read(filepath).unwrap_or_else(|e| pgrx::error!("ifc_elements: read file: {e}"));

    let parse_result =
        ifc::parser::parse(&data).unwrap_or_else(|e| pgrx::error!("ifc_elements: parse: {e}"));

    let store = ifc::entity::EntityStore::new(parse_result.entities);
    let model = ifc::semantic::IfcSemanticModel::from_store(store);

    let elements = model.elements();
    let mut rows = Vec::with_capacity(elements.len());

    for element in &elements {
        // Convert geometry — skip failures with a warning
        let solid = match ifc::geometry::element_to_shape(
            model.store(),
            element_entity(model.store(), element),
        ) {
            Ok(Some(shape)) => match Solid::from_occt_shape(&shape) {
                Ok(s) => Some(s),
                Err(e) => {
                    pgrx::warning!(
                        "ifc_elements: solid conversion failed for {}: {e}",
                        element.global_id
                    );
                    None
                }
            },
            Ok(None) => None,
            Err(e) => {
                pgrx::warning!(
                    "ifc_elements: geometry failed for {}: {e}",
                    element.global_id
                );
                None
            }
        };

        let properties = element.properties.as_ref().map(|v| pgrx::JsonB(v.clone()));

        rows.push((
            element.global_id.clone(),
            element.ifc_type.clone(),
            element.name.clone(),
            element.material.clone(),
            element.storey.clone(),
            solid,
            properties,
        ));
    }

    TableIterator::new(rows)
}

/// Parse an IFC file and return all relationship edges.
/// Each row represents a directional relationship between two IFC entities.
#[pg_extern(immutable, strict)]
pub fn ifc_relationships(
    filepath: &str,
) -> TableIterator<
    'static,
    (
        name!(source_id, String),
        name!(target_id, String),
        name!(rel_type, String),
        name!(ordinal, Option<i32>),
        name!(properties, Option<pgrx::JsonB>),
    ),
> {
    let data = std::fs::read(filepath)
        .unwrap_or_else(|e| pgrx::error!("ifc_relationships: read file: {e}"));

    let parse_result =
        ifc::parser::parse(&data).unwrap_or_else(|e| pgrx::error!("ifc_relationships: parse: {e}"));

    let store = ifc::entity::EntityStore::new(parse_result.entities);
    let model = ifc::semantic::IfcSemanticModel::from_store(store);

    let edges = model.relationships();
    let rows: Vec<_> = edges
        .into_iter()
        .map(|edge| {
            let properties = edge.properties.map(pgrx::JsonB);
            (
                edge.source_id,
                edge.target_id,
                edge.rel_type,
                edge.ordinal,
                properties,
            )
        })
        .collect();

    TableIterator::new(rows)
}

/// Parse an IFC file and return the spatial structure hierarchy.
/// Returns the building hierarchy: Project → Site → Building → Storey.
#[pg_extern(immutable, strict)]
pub fn ifc_spatial_structure(
    filepath: &str,
) -> TableIterator<
    'static,
    (
        name!(global_id, String),
        name!(ifc_type, String),
        name!(name, Option<String>),
        name!(parent_global_id, Option<String>),
        name!(elevation, Option<f64>),
    ),
> {
    let data = std::fs::read(filepath)
        .unwrap_or_else(|e| pgrx::error!("ifc_spatial_structure: read file: {e}"));

    let parse_result = ifc::parser::parse(&data)
        .unwrap_or_else(|e| pgrx::error!("ifc_spatial_structure: parse: {e}"));

    let store = ifc::entity::EntityStore::new(parse_result.entities);
    let model = ifc::semantic::IfcSemanticModel::from_store(store);

    let nodes = model.spatial_structure();
    let rows: Vec<_> = nodes
        .into_iter()
        .map(|node| {
            (
                node.global_id,
                node.ifc_type,
                node.name,
                node.parent_global_id,
                node.elevation,
            )
        })
        .collect();

    TableIterator::new(rows)
}

/// Helper: get the raw IfcEntity for an IfcElement so we can pass it to geometry conversion.
fn element_entity<'a>(
    store: &'a ifc::entity::EntityStore,
    element: &ifc::semantic::IfcElement,
) -> &'a ifc::entity::IfcEntity {
    store
        .get(element.entity_id)
        .unwrap_or_else(|| pgrx::error!("ifc_elements: entity {} not found", element.entity_id))
}

/// Extract every `IfcSite` from a file with its WGS84 latitude / longitude
/// (decimal degrees, north / east positive) and ellipsoidal elevation in metres.
/// Coordinates come from `IfcSite.RefLatitude` / `RefLongitude` (compound
/// `(deg, min, sec[, millionths])` lists) and `RefElevation`. NULLs surface as
/// SQL NULLs — older IFC2x3 files often omit these fields.
#[pg_extern(immutable, strict)]
pub fn ifc_site_location(
    filepath: &str,
) -> TableIterator<
    'static,
    (
        name!(global_id, String),
        name!(name, Option<String>),
        name!(lat, Option<f64>),
        name!(lon, Option<f64>),
        name!(elevation, Option<f64>),
    ),
> {
    let data = std::fs::read(filepath)
        .unwrap_or_else(|e| pgrx::error!("ifc_site_location: read file: {e}"));
    let parse_result =
        ifc::parser::parse(&data).unwrap_or_else(|e| pgrx::error!("ifc_site_location: parse: {e}"));
    let store = ifc::entity::EntityStore::new(parse_result.entities);

    let rows: Vec<_> = ifc::georef::extract_site_locations(&store)
        .into_iter()
        .map(|s| (s.global_id, s.name, s.lat, s.lon, s.elevation))
        .collect();
    TableIterator::new(rows)
}

/// Return the IFC4 `IfcMapConversion` row (zero or one) describing how the
/// model's local coordinate system maps to a projected CRS — eastings /
/// northings / orthogonal height plus planar rotation (degrees) and uniform
/// scale. `target_crs` is the EPSG-style identifier from the referenced
/// `IfcProjectedCRS` (e.g. `EPSG:32631`).
#[pg_extern(immutable, strict)]
pub fn ifc_map_conversion(
    filepath: &str,
) -> TableIterator<
    'static,
    (
        name!(eastings, f64),
        name!(northings, f64),
        name!(orthogonal_height, f64),
        name!(rotation_deg, f64),
        name!(scale, f64),
        name!(source_crs, Option<String>),
        name!(target_crs, Option<String>),
    ),
> {
    let data = std::fs::read(filepath)
        .unwrap_or_else(|e| pgrx::error!("ifc_map_conversion: read file: {e}"));
    let parse_result = ifc::parser::parse(&data)
        .unwrap_or_else(|e| pgrx::error!("ifc_map_conversion: parse: {e}"));
    let store = ifc::entity::EntityStore::new(parse_result.entities);

    let rows: Vec<_> = ifc::georef::extract_map_conversion(&store)
        .into_iter()
        .map(|mc| {
            (
                mc.eastings,
                mc.northings,
                mc.orthogonal_height,
                mc.rotation_deg(),
                mc.scale,
                mc.source_crs_name.clone(),
                mc.target_crs_name.clone(),
            )
        })
        .collect();
    TableIterator::new(rows)
}

/// Georeference an IFC solid into `target_srid`.
///
///   - **4326** (default): geographic. Anchors at `IfcSite.RefLatitude` /
///     `RefLongitude` (most files have these). Local metres become tiny
///     degree offsets via the tangent-plane approximation, so vertex
///     magnitudes stay single-precision-safe — suitable for direct glTF /
///     web-viewer use. Honours any `IfcMapConversion` rotation + scale.
///   - **4978**: geocentric ECEF metres. Same anchor as 4326. Use only
///     with a precision-preserving renderer.
///   - **Any other SRID**: must match the file's `IfcProjectedCRS` (e.g.
///     32631 for `EPSG:32631`). Applies `IfcMapConversion` verbatim; output
///     is metres in that projected CRS. Errors if the file has no
///     `IfcMapConversion` or if the SRID disagrees with `IfcProjectedCRS`.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_georeference(s: Solid, filepath: &str, target_srid: default!(i32, 4326)) -> Solid {
    let mat = ifc::georef::build_affine_for_file(
        filepath,
        target_srid,
        "solid_georeference",
        crate::functions::transforms::tangent_plane_affine,
        crate::functions::transforms::ecef_affine,
    );

    let shape = s
        .to_occt_shape()
        .unwrap_or_else(|e| pgrx::error!("solid_georeference: {e}"));
    let transformed = ffi::transform::transform(&shape, &mat)
        .unwrap_or_else(|e| pgrx::error!("solid_georeference: {e}"));
    Solid::from_occt_shape(&transformed).unwrap_or_else(|e| pgrx::error!("solid_georeference: {e}"))
}
