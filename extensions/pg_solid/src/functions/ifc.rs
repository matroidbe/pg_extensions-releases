#![allow(clippy::type_complexity)]
use pgrx::prelude::*;

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
