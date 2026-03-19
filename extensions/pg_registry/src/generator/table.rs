//! Generate PostgreSQL tables from JSON Schema
//!
//! This module provides the reverse mapping of json_schema.rs - it takes
//! a JSON Schema definition and creates a corresponding PostgreSQL table.

use pgrx::prelude::*;
use serde_json::Value;

/// Property info extracted from JSON Schema
#[derive(Debug)]
struct PropertyInfo {
    name: String,
    pg_type: String,
    is_nullable: bool,
}

/// Parse a fully qualified table name into schema and table parts
fn parse_table_name(table_name: &str) -> (String, String) {
    if let Some(dot_pos) = table_name.find('.') {
        let schema = &table_name[..dot_pos];
        let table = &table_name[dot_pos + 1..];
        (schema.to_string(), table.to_string())
    } else {
        ("public".to_string(), table_name.to_string())
    }
}

/// Map a JSON Schema property definition to a PostgreSQL type
///
/// This is the reverse of pg_type_to_json_schema in json_schema.rs
fn json_schema_to_pg_type(prop_schema: &Value) -> (String, bool) {
    // Handle nullable types: {"type": ["string", "null"]} or {"type": "string"}
    let (base_type, is_nullable) = match prop_schema.get("type") {
        Some(Value::Array(types)) => {
            let has_null = types.iter().any(|t| t.as_str() == Some("null"));
            let non_null_type = types
                .iter()
                .find(|t| t.as_str() != Some("null"))
                .and_then(|t| t.as_str())
                .unwrap_or("string");
            (non_null_type, has_null)
        }
        Some(Value::String(t)) => (t.as_str(), false),
        _ => ("string", true), // Default to nullable TEXT for unknown schemas
    };

    // Get format if present
    let format = prop_schema
        .get("format")
        .and_then(|f| f.as_str())
        .unwrap_or("");

    // Get contentEncoding if present (for bytea)
    let content_encoding = prop_schema
        .get("contentEncoding")
        .and_then(|e| e.as_str())
        .unwrap_or("");

    // Map to PostgreSQL type
    let pg_type = match (base_type, format, content_encoding) {
        // String with formats
        ("string", "uuid", _) => "UUID".to_string(),
        ("string", "date", _) => "DATE".to_string(),
        ("string", "date-time", _) => "TIMESTAMPTZ".to_string(),
        ("string", "time", _) => "TIME".to_string(),
        ("string", _, "base64") => "BYTEA".to_string(),
        ("string", _, _) => {
            // Check for maxLength to use VARCHAR
            if let Some(max_len) = prop_schema.get("maxLength").and_then(|m| m.as_i64()) {
                format!("VARCHAR({})", max_len)
            } else {
                "TEXT".to_string()
            }
        }

        // Numeric types
        ("integer", _, _) => "BIGINT".to_string(),
        ("number", _, _) => "NUMERIC".to_string(),

        // Boolean
        ("boolean", _, _) => "BOOLEAN".to_string(),

        // Object (JSON)
        ("object", _, _) => "JSONB".to_string(),

        // Array types
        ("array", _, _) => {
            // Get items schema to determine element type
            if let Some(items) = prop_schema.get("items") {
                let (element_type, _) = json_schema_to_pg_type(items);
                format!("{}[]", element_type)
            } else {
                "JSONB".to_string() // Fallback for arrays without items schema
            }
        }

        // Default fallback
        _ => "TEXT".to_string(),
    };

    (pg_type, is_nullable)
}

/// Extract property information from a JSON Schema
fn extract_properties(schema: &Value) -> Vec<PropertyInfo> {
    let properties = match schema.get("properties") {
        Some(Value::Object(props)) => props,
        _ => return vec![],
    };

    let required: Vec<&str> = schema
        .get("required")
        .and_then(|r| r.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    properties
        .iter()
        .map(|(name, prop_schema)| {
            let (pg_type, schema_nullable) = json_schema_to_pg_type(prop_schema);
            // A property is nullable if:
            // 1. It's not in the "required" array, OR
            // 2. Its type includes "null"
            let is_nullable = !required.contains(&name.as_str()) || schema_nullable;

            PropertyInfo {
                name: name.clone(),
                pg_type,
                is_nullable,
            }
        })
        .collect()
}

/// Build a CREATE TABLE SQL statement from a JSON Schema
///
/// # Arguments
/// * `table_name` - Fully qualified table name (e.g., "public.orders")
/// * `schema` - JSON Schema definition
/// * `add_id_column` - If true, adds an `id BIGSERIAL PRIMARY KEY` column
///
/// # Returns
/// The CREATE TABLE SQL statement
pub fn build_create_table_sql(table_name: &str, schema: &Value, add_id_column: bool) -> String {
    let properties = extract_properties(schema);

    if properties.is_empty() {
        pgrx::error!(
            "Cannot create table: JSON Schema has no properties defined for '{}'",
            table_name
        );
    }

    let mut columns: Vec<String> = Vec::new();

    // Add auto-increment ID column if requested (for stream mode)
    if add_id_column {
        columns.push("id BIGSERIAL PRIMARY KEY".to_string());
    }

    // Add columns from schema properties
    for prop in &properties {
        let null_constraint = if prop.is_nullable { "" } else { " NOT NULL" };
        columns.push(format!(
            "{} {}{}",
            quote_identifier(&prop.name),
            prop.pg_type,
            null_constraint
        ));
    }

    format!(
        "CREATE TABLE {} (\n    {}\n)",
        table_name,
        columns.join(",\n    ")
    )
}

/// Quote a PostgreSQL identifier (column name, table name, etc.)
fn quote_identifier(name: &str) -> String {
    // Check if the name needs quoting (contains special chars or is a reserved word)
    let needs_quoting = !name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        || name
            .chars()
            .next()
            .map(|c| c.is_ascii_digit())
            .unwrap_or(false)
        || is_reserved_word(name);

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Check if a name is a PostgreSQL reserved word
fn is_reserved_word(name: &str) -> bool {
    // Common reserved words - not exhaustive but covers most cases
    const RESERVED: &[&str] = &[
        "all",
        "and",
        "any",
        "array",
        "as",
        "asc",
        "begin",
        "between",
        "by",
        "case",
        "check",
        "column",
        "constraint",
        "create",
        "cross",
        "current",
        "default",
        "delete",
        "desc",
        "distinct",
        "drop",
        "else",
        "end",
        "exists",
        "false",
        "for",
        "foreign",
        "from",
        "full",
        "group",
        "having",
        "in",
        "index",
        "inner",
        "insert",
        "into",
        "is",
        "join",
        "key",
        "left",
        "like",
        "limit",
        "not",
        "null",
        "offset",
        "on",
        "or",
        "order",
        "outer",
        "primary",
        "references",
        "right",
        "select",
        "set",
        "table",
        "then",
        "to",
        "true",
        "union",
        "unique",
        "update",
        "using",
        "values",
        "when",
        "where",
        "with",
    ];
    RESERVED.contains(&name.to_lowercase().as_str())
}

/// Create a PostgreSQL table from a JSON Schema
///
/// # Arguments
/// * `table_name` - Fully qualified table name (e.g., "public.orders")
/// * `schema` - JSON Schema definition as JSONB
/// * `add_id_column` - If true, adds an `id BIGSERIAL PRIMARY KEY` column
///
/// # Returns
/// True if the table was created successfully
pub fn create_table(table_name: &str, schema: &Value, add_id_column: bool) -> bool {
    let (schema_name, table) = parse_table_name(table_name);

    // Check if table already exists
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

    if table_exists {
        pgrx::error!("Table '{}' already exists", table_name);
    }

    // Build and execute CREATE TABLE statement
    let create_sql = build_create_table_sql(table_name, schema, add_id_column);

    Spi::run(&create_sql).expect("Failed to create table");

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
    fn test_json_schema_to_pg_type_string() {
        let schema = json!({"type": "string"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TEXT");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_nullable_string() {
        let schema = json!({"type": ["string", "null"]});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TEXT");
        assert!(nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_varchar() {
        let schema = json!({"type": "string", "maxLength": 100});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "VARCHAR(100)");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_uuid() {
        let schema = json!({"type": "string", "format": "uuid"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "UUID");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_timestamp() {
        let schema = json!({"type": "string", "format": "date-time"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TIMESTAMPTZ");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_date() {
        let schema = json!({"type": "string", "format": "date"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "DATE");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_integer() {
        let schema = json!({"type": "integer"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "BIGINT");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_number() {
        let schema = json!({"type": "number"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "NUMERIC");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_boolean() {
        let schema = json!({"type": "boolean"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "BOOLEAN");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_object() {
        let schema = json!({"type": "object"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "JSONB");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_bytea() {
        let schema = json!({"type": "string", "contentEncoding": "base64"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "BYTEA");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_text_array() {
        let schema = json!({"type": "array", "items": {"type": "string"}});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TEXT[]");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_integer_array() {
        let schema = json!({"type": "array", "items": {"type": "integer"}});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "BIGINT[]");
        assert!(!nullable);
    }

    #[test]
    fn test_build_create_table_sql_simple() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        });

        let sql = build_create_table_sql("public.users", &schema, false);

        assert!(sql.contains("CREATE TABLE public.users"));
        assert!(sql.contains("name TEXT NOT NULL"));
        assert!(sql.contains("age BIGINT")); // age is nullable (not in required)
    }

    #[test]
    fn test_build_create_table_sql_with_id() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        let sql = build_create_table_sql("public.users", &schema, true);

        assert!(sql.contains("id BIGSERIAL PRIMARY KEY"));
        assert!(sql.contains("name TEXT NOT NULL"));
    }

    #[test]
    fn test_build_create_table_sql_complex() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string", "format": "uuid"},
                "email": {"type": "string", "maxLength": 255},
                "created_at": {"type": "string", "format": "date-time"},
                "metadata": {"type": "object"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "active": {"type": "boolean"}
            },
            "required": ["id", "email", "active"]
        });

        let sql = build_create_table_sql("public.users", &schema, false);

        assert!(sql.contains("id UUID NOT NULL"));
        assert!(sql.contains("email VARCHAR(255) NOT NULL"));
        assert!(sql.contains("created_at TIMESTAMPTZ")); // nullable
        assert!(sql.contains("metadata JSONB")); // nullable
        assert!(sql.contains("tags TEXT[]")); // nullable
        assert!(sql.contains("active BOOLEAN NOT NULL"));
    }

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("name"), "name");
        assert_eq!(quote_identifier("user_id"), "user_id");
    }

    #[test]
    fn test_quote_identifier_reserved() {
        assert_eq!(quote_identifier("table"), "\"table\"");
        assert_eq!(quote_identifier("select"), "\"select\"");
        assert_eq!(quote_identifier("order"), "\"order\"");
    }

    #[test]
    fn test_quote_identifier_special_chars() {
        assert_eq!(quote_identifier("My Column"), "\"My Column\"");
        assert_eq!(quote_identifier("123abc"), "\"123abc\"");
    }

    #[test]
    fn test_is_reserved_word() {
        assert!(is_reserved_word("table"));
        assert!(is_reserved_word("TABLE"));
        assert!(is_reserved_word("select"));
        assert!(!is_reserved_word("users"));
        assert!(!is_reserved_word("customer_id"));
    }
}
