use std::collections::HashMap;

use super::entity::{EntityStore, IfcEntity, IfcValue};

/// A building element extracted from the IFC model.
#[derive(Debug)]
#[allow(dead_code)]
pub struct IfcElement {
    pub entity_id: u64,
    pub global_id: String,
    pub ifc_type: String,
    pub name: Option<String>,
    pub material: Option<String>,
    pub storey: Option<String>,
    pub properties: Option<serde_json::Value>,
    /// ID of the IfcProductDefinitionShape (attr 6 of IfcProduct)
    pub representation_id: Option<u64>,
    /// ID of the IfcLocalPlacement (attr 5 of IfcProduct)
    pub placement_id: Option<u64>,
}

/// A spatial structure node.
#[derive(Debug)]
pub struct SpatialNode {
    pub global_id: String,
    pub ifc_type: String,
    pub name: Option<String>,
    pub parent_global_id: Option<String>,
    pub elevation: Option<f64>,
}

/// A relationship edge.
#[derive(Debug)]
pub struct RelEdge {
    pub source_id: String,
    pub target_id: String,
    pub rel_type: String,
    pub ordinal: Option<i32>,
    pub properties: Option<serde_json::Value>,
}

/// IFC product types (subtypes of IfcProduct that we extract as elements).
const PRODUCT_TYPES: &[&str] = &[
    "IFCWALL",
    "IFCWALLSTANDARDCASE",
    "IFCSLAB",
    "IFCCOLUMN",
    "IFCBEAM",
    "IFCDOOR",
    "IFCWINDOW",
    "IFCSTAIR",
    "IFCROOF",
    "IFCPLATE",
    "IFCMEMBER",
    "IFCRAILING",
    "IFCCURTAINWALL",
    "IFCCOVERING",
    "IFCFURNISHINGELEMENT",
    "IFCBUILDINGELEMENTPROXY",
    "IFCOPENINGELEMENT",
    "IFCFLOWSEGMENT",
    "IFCFLOWTERMINAL",
    "IFCFLOWFITTING",
    "IFCFLOWCONTROLLER",
    "IFCDISTRIBUTIONELEMENT",
    "IFCSTAIRFLIGHT",
    "IFCRAMP",
    "IFCRAMPFLIGHT",
    "IFCFOOTING",
    "IFCPILE",
    "IFCREINFORCINGBAR",
    "IFCREINFORCINGMESH",
    "IFCTENDON",
    "IFCSPACE",
];

/// Spatial structure types (hierarchy).
const SPATIAL_TYPES: &[&str] = &[
    "IFCPROJECT",
    "IFCSITE",
    "IFCBUILDING",
    "IFCBUILDINGSTOREY",
    "IFCSPACE",
];

/// High-level IFC semantic model.
pub struct IfcSemanticModel {
    store: EntityStore,
    /// element entity_id -> storey name
    element_storeys: HashMap<u64, String>,
    /// element entity_id -> material name
    element_materials: HashMap<u64, String>,
    /// element entity_id -> property sets JSON
    element_properties: HashMap<u64, serde_json::Value>,
    /// spatial entity_id -> parent entity_id
    spatial_parents: HashMap<u64, u64>,
}

impl IfcSemanticModel {
    pub fn from_store(store: EntityStore) -> Self {
        let element_storeys = build_storey_index(&store);
        let element_materials = build_material_index(&store);
        let element_properties = build_property_index(&store);
        let spatial_parents = build_spatial_parent_index(&store);

        Self {
            store,
            element_storeys,
            element_materials,
            element_properties,
            spatial_parents,
        }
    }

    pub fn store(&self) -> &EntityStore {
        &self.store
    }

    /// Extract all building elements.
    pub fn elements(&self) -> Vec<IfcElement> {
        let mut result = Vec::new();

        for type_name in PRODUCT_TYPES {
            for entity in self.store.by_type(type_name) {
                let global_id = match self.store.global_id(entity) {
                    Some(id) => id,
                    None => continue,
                };

                // Convert IFCWALL -> IfcWall
                let ifc_type = to_mixed_case(&entity.type_name);
                let name = self.store.name(entity);
                let material = self.element_materials.get(&entity.id).cloned();
                let storey = self.element_storeys.get(&entity.id).cloned();
                let properties = self.element_properties.get(&entity.id).cloned();

                // IfcProduct: attr 5 = ObjectPlacement, attr 6 = Representation
                let placement_id = entity.attributes.get(5).and_then(|v| v.as_ref());
                let representation_id = entity.attributes.get(6).and_then(|v| v.as_ref());

                result.push(IfcElement {
                    entity_id: entity.id,
                    global_id,
                    ifc_type,
                    name,
                    material,
                    storey,
                    properties,
                    representation_id,
                    placement_id,
                });
            }
        }

        result
    }

    /// Extract spatial structure hierarchy.
    pub fn spatial_structure(&self) -> Vec<SpatialNode> {
        let mut result = Vec::new();

        for type_name in SPATIAL_TYPES {
            for entity in self.store.by_type(type_name) {
                let global_id = match self.store.global_id(entity) {
                    Some(id) => id,
                    None => continue,
                };

                let ifc_type = to_mixed_case(&entity.type_name);
                let name = self.store.name(entity);

                // Look up parent via spatial_parents index
                let parent_global_id = self
                    .spatial_parents
                    .get(&entity.id)
                    .and_then(|parent_id| self.store.get(*parent_id))
                    .and_then(|parent| self.store.global_id(parent));

                // Elevation for IfcBuildingStorey (attr 9 in IFC2x3, attr 8 in some schemas)
                let elevation = if entity.type_name == "IFCBUILDINGSTOREY" {
                    // Try indices 9, 8 for elevation
                    entity
                        .attributes
                        .get(9)
                        .and_then(|v| v.as_f64())
                        .or_else(|| entity.attributes.get(8).and_then(|v| v.as_f64()))
                } else {
                    None
                };

                result.push(SpatialNode {
                    global_id,
                    ifc_type,
                    name,
                    parent_global_id,
                    elevation,
                });
            }
        }

        result
    }

    /// Extract all relationships as edges.
    pub fn relationships(&self) -> Vec<RelEdge> {
        let mut edges = Vec::new();

        // IfcRelContainedInSpatialStructure: attr 4 = RelatedElements (list), attr 5 = RelatingStructure
        for rel in self.store.by_type("IFCRELCONTAINEDINSPATIALSTRUCTURE") {
            let source_id = attr_global_id(&self.store, rel, 5);
            let targets = attr_ref_list(rel, 4);
            if let Some(src) = source_id {
                for (ordinal, target_ref) in targets.iter().enumerate() {
                    if let Some(tgt) = self
                        .store
                        .get(*target_ref)
                        .and_then(|e| self.store.global_id(e))
                    {
                        edges.push(RelEdge {
                            source_id: src.clone(),
                            target_id: tgt,
                            rel_type: "IfcRelContainedInSpatialStructure".into(),
                            ordinal: Some(ordinal as i32),
                            properties: None,
                        });
                    }
                }
            }
        }

        // IfcRelAggregates: attr 4 = RelatingObject, attr 5 = RelatedObjects (list)
        for rel in self.store.by_type("IFCRELAGGREGATES") {
            let source_id = attr_global_id(&self.store, rel, 4);
            let targets = attr_ref_list(rel, 5);
            if let Some(src) = source_id {
                for (ordinal, target_ref) in targets.iter().enumerate() {
                    if let Some(tgt) = self
                        .store
                        .get(*target_ref)
                        .and_then(|e| self.store.global_id(e))
                    {
                        edges.push(RelEdge {
                            source_id: src.clone(),
                            target_id: tgt,
                            rel_type: "IfcRelAggregates".into(),
                            ordinal: Some(ordinal as i32),
                            properties: None,
                        });
                    }
                }
            }
        }

        // IfcRelVoidsElement: attr 4 = RelatingBuildingElement, attr 5 = RelatedOpeningElement
        for rel in self.store.by_type("IFCRELVOIDSELEMENT") {
            let source_id = attr_global_id(&self.store, rel, 4);
            let target_id = attr_global_id(&self.store, rel, 5);
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                edges.push(RelEdge {
                    source_id: src,
                    target_id: tgt,
                    rel_type: "IfcRelVoidsElement".into(),
                    ordinal: None,
                    properties: None,
                });
            }
        }

        // IfcRelFillsElement: attr 4 = RelatingOpeningElement, attr 5 = RelatedBuildingElement
        for rel in self.store.by_type("IFCRELFILLSELEMENT") {
            let source_id = attr_global_id(&self.store, rel, 4);
            let target_id = attr_global_id(&self.store, rel, 5);
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                edges.push(RelEdge {
                    source_id: src,
                    target_id: tgt,
                    rel_type: "IfcRelFillsElement".into(),
                    ordinal: None,
                    properties: None,
                });
            }
        }

        // IfcRelConnectsElements: attr 5 = RelatingElement, attr 6 = RelatedElement
        for rel in self.store.by_type("IFCRELCONNECTSELEMENTS") {
            let source_id = attr_global_id(&self.store, rel, 5);
            let target_id = attr_global_id(&self.store, rel, 6);
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                edges.push(RelEdge {
                    source_id: src,
                    target_id: tgt,
                    rel_type: "IfcRelConnectsElements".into(),
                    ordinal: None,
                    properties: None,
                });
            }
        }

        // IfcRelConnectsPathElements: attr 5 = RelatingElement, attr 6 = RelatedElement
        for rel in self.store.by_type("IFCRELCONNECTSPATHELEMENTS") {
            let source_id = attr_global_id(&self.store, rel, 5);
            let target_id = attr_global_id(&self.store, rel, 6);
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                edges.push(RelEdge {
                    source_id: src,
                    target_id: tgt,
                    rel_type: "IfcRelConnectsPathElements".into(),
                    ordinal: None,
                    properties: None,
                });
            }
        }

        // IfcRelAssociatesMaterial: attr 4 = RelatedObjects (list), attr 5 = RelatingMaterial
        for rel in self.store.by_type("IFCRELASSOCIATESMATERIAL") {
            let material_name = rel
                .attributes
                .get(5)
                .and_then(|v| v.as_ref())
                .and_then(|id| resolve_material_name(&self.store, id));
            let targets = attr_ref_list(rel, 4);
            let props = material_name
                .as_ref()
                .map(|m| serde_json::json!({"material": m}));
            for target_ref in &targets {
                if let Some(tgt) = self
                    .store
                    .get(*target_ref)
                    .and_then(|e| self.store.global_id(e))
                {
                    edges.push(RelEdge {
                        source_id: format!("material:{target_ref}"),
                        target_id: tgt,
                        rel_type: "IfcRelAssociatesMaterial".into(),
                        ordinal: None,
                        properties: props.clone(),
                    });
                }
            }
        }

        // IfcRelSpaceBoundary: attr 4 = RelatingSpace, attr 5 = RelatedBuildingElement
        for rel in self.store.by_type("IFCRELSPACEBOUNDARY") {
            let source_id = attr_global_id(&self.store, rel, 4);
            let target_id = attr_global_id(&self.store, rel, 5);
            if let (Some(src), Some(tgt)) = (source_id, target_id) {
                edges.push(RelEdge {
                    source_id: src,
                    target_id: tgt,
                    rel_type: "IfcRelSpaceBoundary".into(),
                    ordinal: None,
                    properties: None,
                });
            }
        }

        // IfcRelDefinesByProperties: attr 4 = RelatedObjects, attr 5 = RelatingPropertyDefinition
        for rel in self.store.by_type("IFCRELDEFINESBYPROPERTIES") {
            let pset_ref = rel.attributes.get(5).and_then(|v| v.as_ref());
            let pset_name = pset_ref
                .and_then(|id| self.store.get(id))
                .and_then(|e| self.store.name(e))
                .unwrap_or_default();
            let targets = attr_ref_list(rel, 4);
            let props = Some(serde_json::json!({"property_set": pset_name}));
            for target_ref in &targets {
                if let Some(tgt) = self
                    .store
                    .get(*target_ref)
                    .and_then(|e| self.store.global_id(e))
                {
                    edges.push(RelEdge {
                        source_id: format!("pset:{}", pset_ref.unwrap_or(0)),
                        target_id: tgt,
                        rel_type: "IfcRelDefinesByProperties".into(),
                        ordinal: None,
                        properties: props.clone(),
                    });
                }
            }
        }

        edges
    }
}

// ── Index builders ──────────────────────────────────────────────────

/// Build element_id -> storey name index from IfcRelContainedInSpatialStructure.
fn build_storey_index(store: &EntityStore) -> HashMap<u64, String> {
    let mut index = HashMap::new();
    for rel in store.by_type("IFCRELCONTAINEDINSPATIALSTRUCTURE") {
        // attr 5 = RelatingStructure
        let structure_id = match rel.attributes.get(5).and_then(|v| v.as_ref()) {
            Some(id) => id,
            None => continue,
        };
        let structure = match store.get(structure_id) {
            Some(e) => e,
            None => continue,
        };
        // Check it's a storey
        if structure.type_name != "IFCBUILDINGSTOREY" {
            continue;
        }
        let storey_name = store
            .name(structure)
            .unwrap_or_else(|| format!("#{structure_id}"));

        // attr 4 = RelatedElements (list of refs)
        for element_ref in attr_ref_list(rel, 4) {
            index.insert(element_ref, storey_name.clone());
        }
    }
    index
}

/// Build element_id -> material name index from IfcRelAssociatesMaterial.
fn build_material_index(store: &EntityStore) -> HashMap<u64, String> {
    let mut index = HashMap::new();
    for rel in store.by_type("IFCRELASSOCIATESMATERIAL") {
        // attr 5 = RelatingMaterial
        let material_id = match rel.attributes.get(5).and_then(|v| v.as_ref()) {
            Some(id) => id,
            None => continue,
        };
        let material_name = match resolve_material_name(store, material_id) {
            Some(name) => name,
            None => continue,
        };
        // attr 4 = RelatedObjects (list of refs)
        for element_ref in attr_ref_list(rel, 4) {
            index.insert(element_ref, material_name.clone());
        }
    }
    index
}

/// Resolve material name from IfcMaterial, IfcMaterialLayerSetUsage, IfcMaterialLayerSet, etc.
fn resolve_material_name(store: &EntityStore, material_id: u64) -> Option<String> {
    let entity = store.get(material_id)?;
    match entity.type_name.as_str() {
        "IFCMATERIAL" => {
            // attr 0 = Name
            entity
                .attributes
                .first()
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        }
        "IFCMATERIALLAYERSETUSAGE" => {
            // attr 0 = ForLayerSet (ref to IfcMaterialLayerSet)
            let set_id = entity.attributes.first().and_then(|v| v.as_ref())?;
            resolve_material_name(store, set_id)
        }
        "IFCMATERIALLAYERSET" => {
            // attr 0 = MaterialLayers (list of IfcMaterialLayer refs)
            // attr 1 = LayerSetName (optional)
            if let Some(name) = entity.attributes.get(1).and_then(|v| v.as_str()) {
                return Some(name.to_string());
            }
            // Fall back to first layer's material
            let layers = entity.attributes.first().and_then(|v| v.as_list())?;
            for layer_val in layers {
                if let Some(layer_id) = layer_val.as_ref() {
                    if let Some(layer) = store.get(layer_id) {
                        // IfcMaterialLayer: attr 0 = Material (ref)
                        if let Some(mat_id) = layer.attributes.first().and_then(|v| v.as_ref()) {
                            if let Some(name) = resolve_material_name(store, mat_id) {
                                return Some(name);
                            }
                        }
                    }
                }
            }
            None
        }
        "IFCMATERIALLIST" => {
            // attr 0 = Materials (list of IfcMaterial refs)
            let materials = entity.attributes.first().and_then(|v| v.as_list())?;
            let names: Vec<String> = materials
                .iter()
                .filter_map(|v| v.as_ref())
                .filter_map(|id| resolve_material_name(store, id))
                .collect();
            if names.is_empty() {
                None
            } else {
                Some(names.join("; "))
            }
        }
        _ => None,
    }
}

/// Build element_id -> property sets JSON from IfcRelDefinesByProperties.
fn build_property_index(store: &EntityStore) -> HashMap<u64, serde_json::Value> {
    let mut index: HashMap<u64, serde_json::Map<String, serde_json::Value>> = HashMap::new();

    for rel in store.by_type("IFCRELDEFINESBYPROPERTIES") {
        // attr 5 = RelatingPropertyDefinition (IfcPropertySet)
        let pset_id = match rel.attributes.get(5).and_then(|v| v.as_ref()) {
            Some(id) => id,
            None => continue,
        };
        let pset = match store.get(pset_id) {
            Some(e) => e,
            None => continue,
        };

        if pset.type_name != "IFCPROPERTYSET" {
            continue;
        }

        let pset_name = store
            .name(pset)
            .unwrap_or_else(|| format!("PropertySet_{pset_id}"));

        // IfcPropertySet: attr 4 = HasProperties (list of refs)
        let props_json = extract_property_set_values(store, pset);

        // Apply to all related objects
        let element_refs = attr_ref_list(rel, 4);
        for element_ref in element_refs {
            let entry = index.entry(element_ref).or_default();
            entry.insert(pset_name.clone(), props_json.clone());
        }
    }

    index
        .into_iter()
        .map(|(id, map)| (id, serde_json::Value::Object(map)))
        .collect()
}

/// Extract property values from an IfcPropertySet into JSON.
fn extract_property_set_values(store: &EntityStore, pset: &IfcEntity) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    let prop_refs = match pset.attributes.get(4).and_then(|v| v.as_list()) {
        Some(list) => list,
        None => return serde_json::Value::Object(map),
    };

    for prop_val in prop_refs {
        let prop_id = match prop_val.as_ref() {
            Some(id) => id,
            None => continue,
        };
        let prop = match store.get(prop_id) {
            Some(e) => e,
            None => continue,
        };

        if prop.type_name != "IFCPROPERTYSINGLEVALUE" {
            continue;
        }

        // IfcPropertySingleValue: attr 0 = Name, attr 2 = NominalValue
        let prop_name = match prop.attributes.first().and_then(|v| v.as_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        let prop_value = prop
            .attributes
            .get(2)
            .map(ifc_value_to_json)
            .unwrap_or(serde_json::Value::Null);
        map.insert(prop_name, prop_value);
    }

    serde_json::Value::Object(map)
}

/// Convert an IfcValue to a JSON value.
fn ifc_value_to_json(value: &IfcValue) -> serde_json::Value {
    match value {
        IfcValue::String(s) => serde_json::Value::String(s.clone()),
        IfcValue::Integer(v) => serde_json::json!(*v),
        IfcValue::Real(v) => serde_json::json!(*v),
        IfcValue::Bool(v) => serde_json::json!(*v),
        IfcValue::Enum(s) => serde_json::Value::String(s.clone()),
        IfcValue::Null | IfcValue::Derived => serde_json::Value::Null,
        IfcValue::Typed(_, inner) => ifc_value_to_json(inner),
        IfcValue::List(items) => {
            serde_json::Value::Array(items.iter().map(ifc_value_to_json).collect())
        }
        IfcValue::Ref(id) => serde_json::json!(format!("#{id}")),
    }
}

/// Build spatial parent index from IfcRelAggregates.
fn build_spatial_parent_index(store: &EntityStore) -> HashMap<u64, u64> {
    let mut index = HashMap::new();
    for rel in store.by_type("IFCRELAGGREGATES") {
        // attr 4 = RelatingObject (parent)
        let parent_id = match rel.attributes.get(4).and_then(|v| v.as_ref()) {
            Some(id) => id,
            None => continue,
        };
        // attr 5 = RelatedObjects (list of children)
        for child_ref in attr_ref_list(rel, 5) {
            index.insert(child_ref, parent_id);
        }
    }
    index
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Get global_id of an entity referenced at a given attribute index.
fn attr_global_id(store: &EntityStore, entity: &IfcEntity, attr_index: usize) -> Option<String> {
    let ref_id = entity.attributes.get(attr_index).and_then(|v| v.as_ref())?;
    let target = store.get(ref_id)?;
    store.global_id(target)
}

/// Get list of entity references from a list attribute.
fn attr_ref_list(entity: &IfcEntity, attr_index: usize) -> Vec<u64> {
    entity
        .attributes
        .get(attr_index)
        .and_then(|v| v.as_list())
        .map(|list| list.iter().filter_map(|v| v.as_ref()).collect())
        .unwrap_or_default()
}

/// Convert IFCWALL -> IfcWall, IFCBUILDINGSTOREY -> IfcBuildingStorey
fn to_mixed_case(upper: &str) -> String {
    if !upper.starts_with("IFC") {
        return upper.to_string();
    }
    let mut result = String::with_capacity(upper.len());
    result.push_str("Ifc");
    let rest = &upper[3..];
    let mut capitalize_next = true;
    for ch in rest.chars() {
        if capitalize_next {
            result.push(ch.to_ascii_uppercase());
            capitalize_next = false;
        } else {
            result.push(ch.to_ascii_lowercase());
        }
        // Capitalize after known word boundaries in IFC type names
        // We detect word boundaries by looking at transitions in the original ALL-CAPS name
        // For simplicity, just lowercase everything after "Ifc" since the original is all-caps
    }
    // The simple lowercasing gives "Ifcwall" instead of "IfcWall". Use a lookup instead.
    ifc_type_display_name(upper)
}

/// Map uppercase IFC type names to proper mixed-case display names.
fn ifc_type_display_name(upper: &str) -> String {
    match upper {
        "IFCWALL" => "IfcWall",
        "IFCWALLSTANDARDCASE" => "IfcWallStandardCase",
        "IFCSLAB" => "IfcSlab",
        "IFCCOLUMN" => "IfcColumn",
        "IFCBEAM" => "IfcBeam",
        "IFCDOOR" => "IfcDoor",
        "IFCWINDOW" => "IfcWindow",
        "IFCSTAIR" => "IfcStair",
        "IFCSTAIRFLIGHT" => "IfcStairFlight",
        "IFCROOF" => "IfcRoof",
        "IFCPLATE" => "IfcPlate",
        "IFCMEMBER" => "IfcMember",
        "IFCRAILING" => "IfcRailing",
        "IFCCURTAINWALL" => "IfcCurtainWall",
        "IFCCOVERING" => "IfcCovering",
        "IFCFURNISHINGELEMENT" => "IfcFurnishingElement",
        "IFCBUILDINGELEMENTPROXY" => "IfcBuildingElementProxy",
        "IFCOPENINGELEMENT" => "IfcOpeningElement",
        "IFCFLOWSEGMENT" => "IfcFlowSegment",
        "IFCFLOWTERMINAL" => "IfcFlowTerminal",
        "IFCFLOWFITTING" => "IfcFlowFitting",
        "IFCFLOWCONTROLLER" => "IfcFlowController",
        "IFCDISTRIBUTIONELEMENT" => "IfcDistributionElement",
        "IFCRAMP" => "IfcRamp",
        "IFCRAMPFLIGHT" => "IfcRampFlight",
        "IFCFOOTING" => "IfcFooting",
        "IFCPILE" => "IfcPile",
        "IFCREINFORCINGBAR" => "IfcReinforcingBar",
        "IFCREINFORCINGMESH" => "IfcReinforcingMesh",
        "IFCTENDON" => "IfcTendon",
        "IFCSPACE" => "IfcSpace",
        "IFCPROJECT" => "IfcProject",
        "IFCSITE" => "IfcSite",
        "IFCBUILDING" => "IfcBuilding",
        "IFCBUILDINGSTOREY" => "IfcBuildingStorey",
        _ => {
            // Fallback: Ifc + lowercase rest
            if let Some(rest) = upper.strip_prefix("IFC") {
                let mut s = String::from("Ifc");
                let mut first = true;
                for ch in rest.chars() {
                    if first {
                        s.push(ch.to_ascii_uppercase());
                        first = false;
                    } else {
                        s.push(ch.to_ascii_lowercase());
                    }
                }
                return s;
            }
            return upper.to_string();
        }
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ifc::parser;

    const TEST_IFC: &[u8] = b"ISO-10303-21;
HEADER;
FILE_SCHEMA(('IFC2X3'));
ENDSEC;
DATA;
#1 = IFCPROJECT('1abc', $, 'Test Project', $, $, $, $, $, $);
#2 = IFCSITE('2abc', $, 'Test Site', $, $, $, $, $, $, $, $, $, $, $);
#3 = IFCBUILDING('3abc', $, 'Test Building', $, $, $, $, $, $, $, $, $);
#4 = IFCBUILDINGSTOREY('4abc', $, 'Ground Floor', $, $, $, $, $, .ELEMENT., 0.0);
#10 = IFCRELAGGREGATES('r1', $, $, $, #1, (#2));
#11 = IFCRELAGGREGATES('r2', $, $, $, #2, (#3));
#12 = IFCRELAGGREGATES('r3', $, $, $, #3, (#4));
#20 = IFCCARTESIANPOINT((0.0, 0.0, 0.0));
#21 = IFCDIRECTION((0.0, 0.0, 1.0));
#22 = IFCDIRECTION((1.0, 0.0, 0.0));
#23 = IFCAXIS2PLACEMENT3D(#20, #21, #22);
#24 = IFCLOCALPLACEMENT($, #23);
#30 = IFCRECTANGLEPROFILEDEF(.AREA., 'RectProfile', $, 6000.0, 200.0);
#31 = IFCEXTRUDEDAREASOLID(#30, #23, #21, 3000.0);
#32 = IFCSHAPEREPRESENTATION($, 'Body', 'SweptSolid', (#31));
#33 = IFCPRODUCTDEFINITIONSHAPE($, $, (#32));
#50 = IFCWALL('5abc', $, 'Wall-01', $, $, #24, #33, $);
#60 = IFCRELCONTAINEDINSPATIALSTRUCTURE('r4', $, $, $, (#50), #4);
#70 = IFCMATERIAL('Concrete');
#71 = IFCRELASSOCIATESMATERIAL('r5', $, $, $, (#50), #70);
#80 = IFCPROPERTYSINGLEVALUE('FireRating', $, IFCLABEL('2HR'), $);
#81 = IFCPROPERTYSINGLEVALUE('LoadBearing', $, IFCBOOLEAN(.T.), $);
#82 = IFCPROPERTYSET('ps1', $, 'Pset_WallCommon', $, (#80, #81));
#83 = IFCRELDEFINESBYPROPERTIES('r6', $, $, $, (#50), #82);
ENDSEC;
END-ISO-10303-21;";

    fn build_model() -> IfcSemanticModel {
        let result = parser::parse(TEST_IFC).unwrap();
        let store = super::super::entity::EntityStore::new(result.entities);
        IfcSemanticModel::from_store(store)
    }

    #[test]
    fn test_spatial_structure() {
        let model = build_model();
        let nodes = model.spatial_structure();
        assert!(
            nodes.len() >= 4,
            "should have project, site, building, storey"
        );
        let types: Vec<&str> = nodes.iter().map(|n| n.ifc_type.as_str()).collect();
        assert!(types.contains(&"IfcProject"));
        assert!(types.contains(&"IfcSite"));
        assert!(types.contains(&"IfcBuilding"));
        assert!(types.contains(&"IfcBuildingStorey"));
    }

    #[test]
    fn test_spatial_parent() {
        let model = build_model();
        let nodes = model.spatial_structure();
        let storey = nodes
            .iter()
            .find(|n| n.ifc_type == "IfcBuildingStorey")
            .unwrap();
        assert_eq!(storey.parent_global_id.as_deref(), Some("3abc")); // building
    }

    #[test]
    fn test_element_extraction() {
        let model = build_model();
        let elements = model.elements();
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0].ifc_type, "IfcWall");
        assert_eq!(elements[0].global_id, "5abc");
        assert_eq!(elements[0].name.as_deref(), Some("Wall-01"));
    }

    #[test]
    fn test_element_material() {
        let model = build_model();
        let elements = model.elements();
        let wall = &elements[0];
        assert_eq!(wall.material.as_deref(), Some("Concrete"));
    }

    #[test]
    fn test_element_storey() {
        let model = build_model();
        let elements = model.elements();
        let wall = &elements[0];
        assert_eq!(wall.storey.as_deref(), Some("Ground Floor"));
    }

    #[test]
    fn test_element_properties() {
        let model = build_model();
        let elements = model.elements();
        let wall = &elements[0];
        let props = wall.properties.as_ref().unwrap();
        let pset = &props["Pset_WallCommon"];
        assert_eq!(pset["FireRating"], "2HR");
        assert_eq!(pset["LoadBearing"], true);
    }

    #[test]
    fn test_element_has_geometry_refs() {
        let model = build_model();
        let elements = model.elements();
        let wall = &elements[0];
        assert!(wall.representation_id.is_some());
        assert!(wall.placement_id.is_some());
    }

    #[test]
    fn test_relationships() {
        let model = build_model();
        let edges = model.relationships();
        assert!(!edges.is_empty());

        let rel_types: Vec<&str> = edges.iter().map(|e| e.rel_type.as_str()).collect();
        assert!(rel_types.contains(&"IfcRelAggregates"));
        assert!(rel_types.contains(&"IfcRelContainedInSpatialStructure"));
        assert!(rel_types.contains(&"IfcRelAssociatesMaterial"));
        assert!(rel_types.contains(&"IfcRelDefinesByProperties"));
    }

    #[test]
    fn test_containment_edge() {
        let model = build_model();
        let edges = model.relationships();
        let containment = edges
            .iter()
            .find(|e| e.rel_type == "IfcRelContainedInSpatialStructure")
            .unwrap();
        assert_eq!(containment.source_id, "4abc"); // storey
        assert_eq!(containment.target_id, "5abc"); // wall
    }

    #[test]
    fn test_to_mixed_case() {
        assert_eq!(to_mixed_case("IFCWALL"), "IfcWall");
        assert_eq!(to_mixed_case("IFCBUILDINGSTOREY"), "IfcBuildingStorey");
        assert_eq!(to_mixed_case("IFCWALLSTANDARDCASE"), "IfcWallStandardCase");
    }
}
