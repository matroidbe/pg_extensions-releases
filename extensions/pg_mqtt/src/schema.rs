//! Schema registry for pg_mqtt
//!
//! Provides JSON Schema management for typed MQTT topics. Schemas can be registered
//! and bound to topics for validation at publish time.
//!
//! Also provides:
//! - Table generation from JSON Schema definitions
//! - JSON Schema generation from existing PostgreSQL tables
//! - Convenience functions for creating typed topics in one call

use pgrx::prelude::*;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

// =============================================================================
// Table Generation from JSON Schema
// =============================================================================

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
        _ => ("string", true),
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
        ("string", "uuid", _) => "UUID".to_string(),
        ("string", "date", _) => "DATE".to_string(),
        ("string", "date-time", _) => "TIMESTAMPTZ".to_string(),
        ("string", "time", _) => "TIME".to_string(),
        ("string", _, "base64") => "BYTEA".to_string(),
        ("string", _, _) => {
            if let Some(max_len) = prop_schema.get("maxLength").and_then(|m| m.as_i64()) {
                format!("VARCHAR({})", max_len)
            } else {
                "TEXT".to_string()
            }
        }
        ("integer", _, _) => "BIGINT".to_string(),
        ("number", _, _) => "NUMERIC".to_string(),
        ("boolean", _, _) => "BOOLEAN".to_string(),
        ("object", _, _) => "JSONB".to_string(),
        ("array", _, _) => {
            if let Some(items) = prop_schema.get("items") {
                let (element_type, _) = json_schema_to_pg_type(items);
                format!("{}[]", element_type)
            } else {
                "JSONB".to_string()
            }
        }
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
fn build_create_table_sql(table_name: &str, schema: &Value, add_id_column: bool) -> String {
    let properties = extract_properties(schema);

    if properties.is_empty() {
        pgrx::error!(
            "Cannot create table: JSON Schema has no properties defined for '{}'",
            table_name
        );
    }

    let mut columns: Vec<String> = Vec::new();

    if add_id_column {
        columns.push("id BIGSERIAL PRIMARY KEY".to_string());
    }

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

/// Create a PostgreSQL table from a JSON Schema definition
fn create_table_from_schema(table_name: &str, schema: &Value, add_id_column: bool) -> bool {
    let (schema_name, table) = parse_table_name(table_name);

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

    let create_sql = build_create_table_sql(table_name, schema, add_id_column);
    Spi::run(&create_sql).expect("Failed to create table");

    true
}

// =============================================================================
// JSON Schema Generation from PostgreSQL Table
// =============================================================================

/// Column information from information_schema
#[derive(Debug)]
struct ColumnInfo {
    column_name: String,
    data_type: String,
    udt_name: String,
    is_nullable: bool,
    character_maximum_length: Option<i32>,
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
        "text" | "character" | "character varying" | "char" | "varchar" | "name" => {
            if let Some(max_len) = col.character_maximum_length {
                json!({"type": "string", "maxLength": max_len})
            } else {
                json!({"type": "string"})
            }
        }
        "integer" | "int" | "int4" | "smallint" | "int2" | "bigint" | "int8" | "serial"
        | "bigserial" | "smallserial" => json!({"type": "integer"}),
        "real" | "float4" | "double precision" | "float8" | "numeric" | "decimal" | "money" => {
            json!({"type": "number"})
        }
        "boolean" | "bool" => json!({"type": "boolean"}),
        "uuid" => json!({"type": "string", "format": "uuid"}),
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
        "json" | "jsonb" => json!({"type": "object"}),
        "bytea" => json!({"type": "string", "contentEncoding": "base64"}),
        "ARRAY" => {
            let element_type = match col.udt_name.as_str() {
                "_text" | "_varchar" | "_char" | "_name" => json!({"type": "string"}),
                "_int4" | "_int2" | "_int8" | "_serial" => json!({"type": "integer"}),
                "_float4" | "_float8" | "_numeric" => json!({"type": "number"}),
                "_bool" => json!({"type": "boolean"}),
                "_uuid" => json!({"type": "string", "format": "uuid"}),
                "_json" | "_jsonb" => json!({"type": "object"}),
                _ => json!({}),
            };
            json!({"type": "array", "items": element_type})
        }
        "inet" | "cidr" | "macaddr" | "macaddr8" => json!({"type": "string"}),
        "point" | "line" | "lseg" | "box" | "path" | "polygon" | "circle" => {
            json!({"type": "string"})
        }
        "int4range" | "int8range" | "numrange" | "tsrange" | "tstzrange" | "daterange" => {
            json!({"type": "object"})
        }
        "xml" => json!({"type": "string"}),
        _ => {
            if col.udt_name.starts_with('_') {
                json!({"type": "array", "items": {}})
            } else {
                json!({"type": "string"})
            }
        }
    };

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
fn generate_schema_from_table(table_name: &str) -> pgrx::JsonB {
    let (schema_name, table) = parse_table_name(table_name);

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

    let mut properties = Map::new();
    let mut required = Vec::new();

    for col in &columns {
        // Skip auto-generated id columns
        if col.column_name == "id" && col.data_type == "bigint" {
            continue;
        }
        let col_schema = pg_type_to_json_schema(col);
        properties.insert(col.column_name.clone(), col_schema);

        if !col.is_nullable {
            required.push(Value::String(col.column_name.clone()));
        }
    }

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

// =============================================================================
// Schema Registry Functions
// =============================================================================

/// Compute SHA256 fingerprint of a schema definition
fn compute_fingerprint(schema_def: &serde_json::Value) -> String {
    let normalized = serde_json::to_string(schema_def).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(normalized.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Get the next version for a subject
fn get_next_version(subject: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT COALESCE(MAX(version), 0) + 1 FROM pgmqtt.schemas WHERE subject = $1",
        &[subject.into()],
    )
    .unwrap_or(Some(1))
    .unwrap_or(1)
}

/// Register a new JSON Schema
#[pg_extern]
pub fn mqtt_register_schema(
    subject: &str,
    schema_def: pgrx::JsonB,
    description: Option<&str>,
) -> i32 {
    let fingerprint = compute_fingerprint(&schema_def.0);
    let version = get_next_version(subject);

    // Check if this exact schema already exists for this subject
    let existing_id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgmqtt.schemas WHERE subject = $1 AND fingerprint = $2",
        &[subject.into(), fingerprint.clone().into()],
    )
    .ok()
    .flatten();

    if let Some(id) = existing_id {
        return id;
    }

    let schema_json = serde_json::to_string(&schema_def.0).expect("Failed to serialize schema");

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgmqtt.schemas (subject, version, schema_def, fingerprint, description)
        VALUES ($1, $2, $3::jsonb, $4, $5)
        RETURNING id
        "#,
        &[
            subject.into(),
            version.into(),
            schema_json.into(),
            fingerprint.into(),
            description.into(),
        ],
    )
    .expect("Failed to insert schema")
    .expect("No ID returned from insert")
}

/// Get a schema by ID
#[pg_extern]
pub fn mqtt_get_schema(schema_id: i32) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgmqtt.schemas WHERE id = $1",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
}

/// Get the latest schema for a subject
#[pg_extern]
pub fn mqtt_get_latest_schema(subject: &str) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgmqtt.schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
        &[subject.into()],
    )
    .ok()
    .flatten()
}

/// Get the schema bound to a topic
#[pg_extern]
pub fn mqtt_get_topic_schema(topic_name: &str) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT s.schema_def
        FROM pgmqtt.topic_schemas ts
        JOIN pgmqtt.schemas s ON ts.schema_id = s.id
        WHERE ts.topic = $1
        "#,
        &[topic_name.into()],
    )
    .ok()
    .flatten()
}

/// Bind a schema to a topic for validation
#[pg_extern]
pub fn mqtt_bind_schema(
    topic: &str,
    schema_id: i32,
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    if validation_mode != "STRICT" && validation_mode != "LOG" {
        pgrx::error!(
            "validation_mode must be 'STRICT' or 'LOG', got '{}'",
            validation_mode
        );
    }

    let schema_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgmqtt.schemas WHERE id = $1)",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if !schema_exists {
        pgrx::error!("Schema with id {} does not exist", schema_id);
    }

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgmqtt.topic_schemas (topic, schema_id, validation_mode)
        VALUES ($1, $2, $3)
        ON CONFLICT (topic)
        DO UPDATE SET schema_id = EXCLUDED.schema_id, validation_mode = EXCLUDED.validation_mode
        RETURNING id
        "#,
        &[topic.into(), schema_id.into(), validation_mode.into()],
    )
    .expect("Failed to bind schema to topic")
    .expect("No ID returned from upsert")
}

/// Unbind a schema from a topic
#[pg_extern]
pub fn mqtt_unbind_schema(topic: &str) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgmqtt.topic_schemas
            WHERE topic = $1
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[topic.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

/// Drop a schema by ID
#[pg_extern]
pub fn mqtt_drop_schema(schema_id: i32) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgmqtt.schemas
            WHERE id = $1
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[schema_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

// =============================================================================
// Table / Schema Utility Functions
// =============================================================================

/// Create a PostgreSQL table from a JSON Schema definition
#[pg_extern]
pub fn mqtt_table_from_schema(
    table_name: &str,
    schema_def: pgrx::JsonB,
    add_id_column: default!(bool, "true"),
) -> bool {
    create_table_from_schema(table_name, &schema_def.0, add_id_column)
}

/// Generate a JSON Schema from an existing PostgreSQL table
#[pg_extern]
pub fn mqtt_schema_from_table(table_name: &str) -> pgrx::JsonB {
    generate_schema_from_table(table_name)
}

// =============================================================================
// Typed Topic Convenience Functions
// =============================================================================

/// Convert an MQTT topic to a safe PostgreSQL table name
fn topic_to_table_name(topic: &str) -> String {
    let safe_name: String = topic
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    // Remove leading underscores and consecutive underscores
    let cleaned: String = safe_name
        .split('_')
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    format!("pgmqtt.{}", cleaned)
}

/// Create a typed topic with schema, table, and binding in one call
///
/// This convenience function:
/// 1. Creates a PostgreSQL table from the JSON Schema
/// 2. Registers the schema in the registry
/// 3. Binds the schema to the topic with the table as source
///
/// # Arguments
/// * `topic` - The MQTT topic name (e.g., "sensors/temperature")
/// * `schema_def` - JSON Schema defining the message structure
/// * `table_name` - Optional table name (defaults to auto-generated from topic)
/// * `validation_mode` - "STRICT" or "LOG" (default: "STRICT")
///
/// # Returns
/// The topic schema binding ID
#[pg_extern]
pub fn mqtt_create_typed_topic(
    topic: &str,
    schema_def: pgrx::JsonB,
    table_name: default!(Option<&str>, "NULL"),
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    if validation_mode != "STRICT" && validation_mode != "LOG" {
        pgrx::error!(
            "validation_mode must be 'STRICT' or 'LOG', got '{}'",
            validation_mode
        );
    }

    // Determine table name
    let actual_table_name = match table_name {
        Some(name) => {
            if name.contains('.') {
                name.to_string()
            } else {
                format!("pgmqtt.{}", name)
            }
        }
        None => topic_to_table_name(topic),
    };

    // 1. Create table from schema (with id column for ordering)
    create_table_from_schema(&actual_table_name, &schema_def.0, true);

    // 2. Register the schema
    let subject = format!("{}-value", topic);
    let schema_id = mqtt_register_schema(
        &subject,
        schema_def,
        Some(&format!("Schema for MQTT topic {}", topic)),
    );

    // 3. Bind schema to topic with source_table
    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgmqtt.topic_schemas (topic, schema_id, source_table, validation_mode)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (topic)
        DO UPDATE SET schema_id = EXCLUDED.schema_id,
                      source_table = EXCLUDED.source_table,
                      validation_mode = EXCLUDED.validation_mode
        RETURNING id
        "#,
        &[
            topic.into(),
            schema_id.into(),
            actual_table_name.into(),
            validation_mode.into(),
        ],
    )
    .expect("Failed to create typed topic")
    .expect("No ID returned")
}

/// Create a typed topic from an existing PostgreSQL table
///
/// This convenience function:
/// 1. Generates a JSON Schema from the table structure
/// 2. Registers the schema in the registry
/// 3. Binds the schema to the topic with the table as source
#[pg_extern]
pub fn mqtt_create_typed_topic_from_table(
    topic: &str,
    table_name: &str,
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    if validation_mode != "STRICT" && validation_mode != "LOG" {
        pgrx::error!(
            "validation_mode must be 'STRICT' or 'LOG', got '{}'",
            validation_mode
        );
    }

    let qualified_table_name = if table_name.contains('.') {
        table_name.to_string()
    } else {
        format!("public.{}", table_name)
    };

    // 1. Generate schema from table
    let schema_def = generate_schema_from_table(&qualified_table_name);

    // 2. Register the schema
    let subject = format!("{}-value", topic);
    let schema_id = mqtt_register_schema(
        &subject,
        schema_def,
        Some(&format!(
            "Schema generated from table {}",
            qualified_table_name
        )),
    );

    // 3. Bind schema to topic with source_table
    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgmqtt.topic_schemas (topic, schema_id, source_table, validation_mode)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (topic)
        DO UPDATE SET schema_id = EXCLUDED.schema_id,
                      source_table = EXCLUDED.source_table,
                      validation_mode = EXCLUDED.validation_mode
        RETURNING id
        "#,
        &[
            topic.into(),
            schema_id.into(),
            qualified_table_name.into(),
            validation_mode.into(),
        ],
    )
    .expect("Failed to create typed topic from table")
    .expect("No ID returned")
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_fingerprint_deterministic() {
        let schema =
            serde_json::json!({"type": "object", "properties": {"name": {"type": "string"}}});
        let fp1 = compute_fingerprint(&schema);
        let fp2 = compute_fingerprint(&schema);
        assert_eq!(fp1, fp2);
        assert_eq!(fp1.len(), 64); // SHA256 hex is 64 chars
    }

    #[test]
    fn test_compute_fingerprint_different_schemas() {
        let s1 = serde_json::json!({"type": "object"});
        let s2 = serde_json::json!({"type": "array"});
        assert_ne!(compute_fingerprint(&s1), compute_fingerprint(&s2));
    }

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("pgmqtt.sensors");
        assert_eq!(schema, "pgmqtt");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("sensors");
        assert_eq!(schema, "public");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_topic_to_table_name() {
        assert_eq!(
            topic_to_table_name("sensors/temperature"),
            "pgmqtt.sensors_temperature"
        );
        assert_eq!(
            topic_to_table_name("home/room1/temp"),
            "pgmqtt.home_room1_temp"
        );
        assert_eq!(topic_to_table_name("simple"), "pgmqtt.simple");
    }

    #[test]
    fn test_quote_identifier_simple() {
        assert_eq!(quote_identifier("name"), "name");
        assert_eq!(quote_identifier("my_column"), "my_column");
    }

    #[test]
    fn test_quote_identifier_reserved() {
        assert_eq!(quote_identifier("select"), "\"select\"");
        assert_eq!(quote_identifier("table"), "\"table\"");
    }

    #[test]
    fn test_quote_identifier_starts_with_digit() {
        assert_eq!(quote_identifier("1col"), "\"1col\"");
    }

    #[test]
    fn test_json_schema_to_pg_type_string() {
        let schema = serde_json::json!({"type": "string"});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TEXT");
        assert!(!nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_number() {
        let schema = serde_json::json!({"type": "number"});
        let (pg_type, _) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "NUMERIC");
    }

    #[test]
    fn test_json_schema_to_pg_type_integer() {
        let schema = serde_json::json!({"type": "integer"});
        let (pg_type, _) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "BIGINT");
    }

    #[test]
    fn test_json_schema_to_pg_type_nullable() {
        let schema = serde_json::json!({"type": ["string", "null"]});
        let (pg_type, nullable) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "TEXT");
        assert!(nullable);
    }

    #[test]
    fn test_json_schema_to_pg_type_uuid() {
        let schema = serde_json::json!({"type": "string", "format": "uuid"});
        let (pg_type, _) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "UUID");
    }

    #[test]
    fn test_json_schema_to_pg_type_varchar() {
        let schema = serde_json::json!({"type": "string", "maxLength": 255});
        let (pg_type, _) = json_schema_to_pg_type(&schema);
        assert_eq!(pg_type, "VARCHAR(255)");
    }

    #[test]
    fn test_extract_properties() {
        let schema = serde_json::json!({
            "type": "object",
            "properties": {
                "temp": {"type": "number"},
                "unit": {"type": "string"}
            },
            "required": ["temp"]
        });
        let props = extract_properties(&schema);
        assert_eq!(props.len(), 2);

        let temp = props.iter().find(|p| p.name == "temp").unwrap();
        assert_eq!(temp.pg_type, "NUMERIC");
        assert!(!temp.is_nullable);

        let unit = props.iter().find(|p| p.name == "unit").unwrap();
        assert_eq!(unit.pg_type, "TEXT");
        assert!(unit.is_nullable); // not in required
    }
}
