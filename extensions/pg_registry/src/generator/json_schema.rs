//! Generate JSON Schema from PostgreSQL table structure

use pgrx::prelude::*;
use serde_json::{json, Map, Value};

/// Column information from information_schema
#[derive(Debug)]
struct ColumnInfo {
    column_name: String,
    data_type: String,
    udt_name: String,
    is_nullable: bool,
    character_maximum_length: Option<i32>,
}

/// Parse table name into schema and table parts
fn parse_table_name(table_name: &str) -> (String, String) {
    if let Some(dot_pos) = table_name.find('.') {
        let schema = &table_name[..dot_pos];
        let table = &table_name[dot_pos + 1..];
        (schema.to_string(), table.to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    }
}

/// Get column information from information_schema
fn get_columns(schema_name: &str, table_name: &str) -> Vec<ColumnInfo> {
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT
                    column_name::text,
                    data_type::text,
                    udt_name::text,
                    is_nullable::text,
                    character_maximum_length::integer
                FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2
                ORDER BY ordinal_position
                "#,
                None,
                &[schema_name.into(), table_name.into()],
            )
            .expect("Failed to query columns");

        result
            .into_iter()
            .filter_map(|row| {
                // pgrx uses 1-based ordinal access
                let column_name: String = row.get(1).ok().flatten()?;
                let data_type: String = row.get(2).ok().flatten()?;
                let udt_name: String = row.get(3).ok().flatten()?;
                let is_nullable_str: String = row.get(4).ok().flatten()?;
                let character_maximum_length: Option<i32> = row.get(5).ok().flatten();

                Some(ColumnInfo {
                    column_name,
                    data_type,
                    udt_name,
                    is_nullable: is_nullable_str.to_uppercase() == "YES",
                    character_maximum_length,
                })
            })
            .collect()
    })
}

/// Map PostgreSQL data type to JSON Schema type
fn pg_type_to_json_schema(col: &ColumnInfo) -> Value {
    let base_schema = match col.data_type.as_str() {
        // String types
        "text" | "character" | "character varying" | "char" | "varchar" | "name" => {
            if let Some(max_len) = col.character_maximum_length {
                json!({"type": "string", "maxLength": max_len})
            } else {
                json!({"type": "string"})
            }
        }

        // Integer types
        "integer" | "int" | "int4" | "smallint" | "int2" | "bigint" | "int8" | "serial"
        | "bigserial" | "smallserial" => json!({"type": "integer"}),

        // Floating point types
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal" | "money" => {
            json!({"type": "number"})
        }

        // Boolean
        "boolean" | "bool" => json!({"type": "boolean"}),

        // UUID
        "uuid" => json!({"type": "string", "format": "uuid"}),

        // Date/Time types
        "date" => json!({"type": "string", "format": "date"}),
        "time" | "time without time zone" | "time with time zone" | "timetz" => {
            json!({"type": "string", "format": "time"})
        }
        "timestamp"
        | "timestamp without time zone"
        | "timestamp with time zone"
        | "timestamptz" => {
            json!({"type": "string", "format": "date-time"})
        }
        "interval" => json!({"type": "string"}),

        // JSON types
        "json" | "jsonb" => json!({"type": "object"}),

        // Binary types
        "bytea" => json!({"type": "string", "contentEncoding": "base64"}),

        // Array types
        "ARRAY" => {
            // Use udt_name to determine element type (e.g., "_text" for text[])
            let element_type = match col.udt_name.as_str() {
                "_text" | "_varchar" | "_char" | "_name" => json!({"type": "string"}),
                "_int4" | "_int2" | "_int8" | "_serial" => json!({"type": "integer"}),
                "_float4" | "_float8" | "_numeric" => json!({"type": "number"}),
                "_bool" => json!({"type": "boolean"}),
                "_uuid" => json!({"type": "string", "format": "uuid"}),
                "_json" | "_jsonb" => json!({"type": "object"}),
                _ => json!({}), // Unknown array element type
            };
            json!({"type": "array", "items": element_type})
        }

        // Network types
        "inet" | "cidr" | "macaddr" | "macaddr8" => json!({"type": "string"}),

        // Geometric types
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            json!({"type": "string"})
        }

        // Range types
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            json!({"type": "object"})
        }

        // XML
        "xml" => json!({"type": "string"}),

        // User-defined or unknown types
        _ => {
            // Check if it's an array by udt_name prefix
            if col.udt_name.starts_with('_') {
                json!({"type": "array", "items": {}})
            } else {
                json!({"type": "string"})
            }
        }
    };

    // Handle nullability
    if col.is_nullable {
        if let Some(obj) = base_schema.as_object() {
            let mut nullable_schema = obj.clone();
            if let Some(type_val) = nullable_schema.get("type").cloned() {
                nullable_schema.insert("type".to_string(), json!([type_val, "null"]));
            }
            Value::Object(nullable_schema)
        } else {
            base_schema
        }
    } else {
        base_schema
    }
}

/// Generate JSON Schema from a PostgreSQL table
pub fn generate_from_table(table_name: &str) -> pgrx::JsonB {
    let (schema_name, table) = parse_table_name(table_name);

    // Check if table exists
    let table_exists = Spi::get_one_with_args::<bool>(
        r#"
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )
        "#,
        &[schema_name.clone().into(), table.clone().into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if !table_exists {
        pgrx::error!("Table '{}' does not exist", table_name);
    }

    let columns = get_columns(&schema_name, &table);

    if columns.is_empty() {
        pgrx::error!("Table '{}' has no columns", table_name);
    }

    // Build properties and required arrays
    let mut properties = Map::new();
    let mut required = Vec::new();

    for col in &columns {
        let col_schema = pg_type_to_json_schema(col);
        properties.insert(col.column_name.clone(), col_schema);

        if !col.is_nullable {
            required.push(Value::String(col.column_name.clone()));
        }
    }

    // Build the final schema
    let mut schema = Map::new();
    schema.insert(
        "$schema".to_string(),
        json!("https://json-schema.org/draft/2020-12/schema"),
    );
    schema.insert("type".to_string(), json!("object"));
    schema.insert("title".to_string(), json!(table_name));
    schema.insert("properties".to_string(), Value::Object(properties));

    if !required.is_empty() {
        schema.insert("required".to_string(), Value::Array(required));
    }

    pgrx::JsonB(Value::Object(schema))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("public.orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_pg_type_to_json_schema_string() {
        let col = ColumnInfo {
            column_name: "name".to_string(),
            data_type: "text".to_string(),
            udt_name: "text".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "string"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_varchar_with_length() {
        let col = ColumnInfo {
            column_name: "name".to_string(),
            data_type: "character varying".to_string(),
            udt_name: "varchar".to_string(),
            is_nullable: false,
            character_maximum_length: Some(100),
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "string", "maxLength": 100}));
    }

    #[test]
    fn test_pg_type_to_json_schema_integer() {
        let col = ColumnInfo {
            column_name: "id".to_string(),
            data_type: "integer".to_string(),
            udt_name: "int4".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "integer"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_nullable() {
        let col = ColumnInfo {
            column_name: "description".to_string(),
            data_type: "text".to_string(),
            udt_name: "text".to_string(),
            is_nullable: true,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": ["string", "null"]}));
    }

    #[test]
    fn test_pg_type_to_json_schema_uuid() {
        let col = ColumnInfo {
            column_name: "id".to_string(),
            data_type: "uuid".to_string(),
            udt_name: "uuid".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "string", "format": "uuid"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_timestamp() {
        let col = ColumnInfo {
            column_name: "created_at".to_string(),
            data_type: "timestamp with time zone".to_string(),
            udt_name: "timestamptz".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "string", "format": "date-time"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_jsonb() {
        let col = ColumnInfo {
            column_name: "metadata".to_string(),
            data_type: "jsonb".to_string(),
            udt_name: "jsonb".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "object"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_text_array() {
        let col = ColumnInfo {
            column_name: "tags".to_string(),
            data_type: "ARRAY".to_string(),
            udt_name: "_text".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(
            schema,
            json!({"type": "array", "items": {"type": "string"}})
        );
    }

    #[test]
    fn test_pg_type_to_json_schema_integer_array() {
        let col = ColumnInfo {
            column_name: "ids".to_string(),
            data_type: "ARRAY".to_string(),
            udt_name: "_int4".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(
            schema,
            json!({"type": "array", "items": {"type": "integer"}})
        );
    }

    #[test]
    fn test_pg_type_to_json_schema_bytea() {
        let col = ColumnInfo {
            column_name: "data".to_string(),
            data_type: "bytea".to_string(),
            udt_name: "bytea".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(
            schema,
            json!({"type": "string", "contentEncoding": "base64"})
        );
    }

    #[test]
    fn test_pg_type_to_json_schema_boolean() {
        let col = ColumnInfo {
            column_name: "active".to_string(),
            data_type: "boolean".to_string(),
            udt_name: "bool".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "boolean"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_numeric() {
        let col = ColumnInfo {
            column_name: "price".to_string(),
            data_type: "numeric".to_string(),
            udt_name: "numeric".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "number"}));
    }

    #[test]
    fn test_pg_type_to_json_schema_date() {
        let col = ColumnInfo {
            column_name: "birth_date".to_string(),
            data_type: "date".to_string(),
            udt_name: "date".to_string(),
            is_nullable: false,
            character_maximum_length: None,
        };
        let schema = pg_type_to_json_schema(&col);
        assert_eq!(schema, json!({"type": "string", "format": "date"}));
    }
}
