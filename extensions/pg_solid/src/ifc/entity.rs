use std::collections::HashMap;

/// A single IFC entity instance parsed from the STEP file.
#[derive(Debug, Clone)]
pub struct IfcEntity {
    pub id: u64,
    pub type_name: String,
    pub attributes: Vec<IfcValue>,
}

/// Any IFC attribute value.
#[derive(Debug, Clone, PartialEq)]
pub enum IfcValue {
    Ref(u64),
    String(String),
    Integer(i64),
    Real(f64),
    Bool(bool),
    Enum(String),
    Null,
    Derived,
    List(Vec<IfcValue>),
    Typed(String, Box<IfcValue>),
}

impl IfcValue {
    pub fn as_ref(&self) -> Option<u64> {
        match self {
            IfcValue::Ref(id) => Some(*id),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            IfcValue::String(s) => Some(s),
            IfcValue::Typed(_, inner) => inner.as_str(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            IfcValue::Real(v) => Some(*v),
            IfcValue::Integer(v) => Some(*v as f64),
            IfcValue::Typed(_, inner) => inner.as_f64(),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            IfcValue::Integer(v) => Some(*v),
            IfcValue::Typed(_, inner) => inner.as_i64(),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            IfcValue::Bool(v) => Some(*v),
            IfcValue::Enum(s) => match s.as_str() {
                "T" | "TRUE" => Some(true),
                "F" | "FALSE" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn as_enum(&self) -> Option<&str> {
        match self {
            IfcValue::Enum(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&[IfcValue]> {
        match self {
            IfcValue::List(v) => Some(v),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, IfcValue::Null)
    }
}

/// Entity store with lookup by ID and type.
pub struct EntityStore {
    entities: HashMap<u64, IfcEntity>,
    /// Reverse index: uppercase type_name -> list of entity IDs
    type_index: HashMap<String, Vec<u64>>,
}

impl EntityStore {
    pub fn new(entities: HashMap<u64, IfcEntity>) -> Self {
        let mut type_index: HashMap<String, Vec<u64>> = HashMap::new();
        for (id, ent) in &entities {
            type_index
                .entry(ent.type_name.to_uppercase())
                .or_default()
                .push(*id);
        }
        Self {
            entities,
            type_index,
        }
    }

    pub fn get(&self, id: u64) -> Option<&IfcEntity> {
        self.entities.get(&id)
    }

    pub fn by_type(&self, type_name: &str) -> Vec<&IfcEntity> {
        self.type_index
            .get(&type_name.to_uppercase())
            .map(|ids| ids.iter().filter_map(|id| self.entities.get(id)).collect())
            .unwrap_or_default()
    }

    pub fn by_type_prefix(&self, prefix: &str) -> Vec<&IfcEntity> {
        let prefix_upper = prefix.to_uppercase();
        self.type_index
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix_upper))
            .flat_map(|(_, ids)| ids.iter().filter_map(|id| self.entities.get(id)))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.entities.len()
    }

    /// Get the GlobalId (attribute 0) from an IfcRoot-derived entity.
    pub fn global_id(&self, entity: &IfcEntity) -> Option<String> {
        entity
            .attributes
            .first()
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    /// Get the Name (attribute 2) from an IfcRoot-derived entity.
    pub fn name(&self, entity: &IfcEntity) -> Option<String> {
        entity
            .attributes
            .get(2)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ifc_value_conversions() {
        assert_eq!(IfcValue::Real(3.14).as_f64(), Some(3.14));
        assert_eq!(IfcValue::Integer(42).as_i64(), Some(42));
        assert_eq!(IfcValue::Integer(42).as_f64(), Some(42.0));
        assert_eq!(IfcValue::Bool(true).as_bool(), Some(true));
        assert_eq!(IfcValue::Null.as_f64(), None);
        assert!(IfcValue::Null.is_null());
        assert_eq!(IfcValue::Enum("ELEMENT".into()).as_enum(), Some("ELEMENT"));
        assert_eq!(IfcValue::Enum("T".into()).as_bool(), Some(true));
        assert_eq!(IfcValue::Enum("F".into()).as_bool(), Some(false));
    }

    #[test]
    fn test_ifc_value_string() {
        assert_eq!(IfcValue::String("hello".into()).as_str(), Some("hello"));
        assert_eq!(IfcValue::Real(1.0).as_str(), None);
    }

    #[test]
    fn test_ifc_value_typed() {
        let v = IfcValue::Typed("IFCLABEL".into(), Box::new(IfcValue::String("Wall".into())));
        assert_eq!(v.as_str(), Some("Wall"));
    }

    #[test]
    fn test_ifc_value_list() {
        let v = IfcValue::List(vec![IfcValue::Real(1.0), IfcValue::Real(2.0)]);
        assert_eq!(v.as_list().unwrap().len(), 2);
    }

    #[test]
    fn test_ifc_value_ref() {
        assert_eq!(IfcValue::Ref(123).as_ref(), Some(123));
        assert_eq!(IfcValue::Null.as_ref(), None);
    }

    #[test]
    fn test_entity_store_by_type() {
        let mut entities = HashMap::new();
        entities.insert(
            1,
            IfcEntity {
                id: 1,
                type_name: "IFCWALL".into(),
                attributes: vec![],
            },
        );
        entities.insert(
            2,
            IfcEntity {
                id: 2,
                type_name: "IFCSLAB".into(),
                attributes: vec![],
            },
        );
        entities.insert(
            3,
            IfcEntity {
                id: 3,
                type_name: "IFCWALL".into(),
                attributes: vec![],
            },
        );
        let store = EntityStore::new(entities);
        assert_eq!(store.by_type("IFCWALL").len(), 2);
        assert_eq!(store.by_type("IFCSLAB").len(), 1);
        assert_eq!(store.by_type("IfcWall").len(), 2); // case-insensitive
        assert_eq!(store.by_type("IFCBEAM").len(), 0);
        assert_eq!(store.len(), 3);
    }

    #[test]
    fn test_entity_store_by_type_prefix() {
        let mut entities = HashMap::new();
        entities.insert(
            1,
            IfcEntity {
                id: 1,
                type_name: "IFCRELCONTAINEDINSPATIALSTRUCTURE".into(),
                attributes: vec![],
            },
        );
        entities.insert(
            2,
            IfcEntity {
                id: 2,
                type_name: "IFCRELAGGREGATES".into(),
                attributes: vec![],
            },
        );
        entities.insert(
            3,
            IfcEntity {
                id: 3,
                type_name: "IFCWALL".into(),
                attributes: vec![],
            },
        );
        let store = EntityStore::new(entities);
        assert_eq!(store.by_type_prefix("IFCREL").len(), 2);
    }

    #[test]
    fn test_entity_store_global_id_and_name() {
        let entity = IfcEntity {
            id: 1,
            type_name: "IFCWALL".into(),
            attributes: vec![
                IfcValue::String("0abc123".into()),
                IfcValue::Null,
                IfcValue::String("Wall-01".into()),
            ],
        };
        let mut entities = HashMap::new();
        entities.insert(1, entity);
        let store = EntityStore::new(entities);
        let ent = store.get(1).unwrap();
        assert_eq!(store.global_id(ent), Some("0abc123".to_string()));
        assert_eq!(store.name(ent), Some("Wall-01".to_string()));
    }
}
