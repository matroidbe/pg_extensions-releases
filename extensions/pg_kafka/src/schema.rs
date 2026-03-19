//! Schema registry for pg_kafka
//!
//! Provides JSON Schema management for typed topics. Schemas can be registered
//! and bound to topics for validation at produce time.
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
fn build_create_table_sql(table_name: &str, schema: &Value, add_id_column: bool) -> String {
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
fn generate_schema_from_table(table_name: &str) -> pgrx::JsonB {
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
        "SELECT COALESCE(MAX(version), 0) + 1 FROM pgkafka.schemas WHERE subject = $1",
        &[subject.into()],
    )
    .unwrap_or(Some(1))
    .unwrap_or(1)
}

/// Register a new JSON Schema
///
/// # Arguments
/// * `subject` - Subject name (e.g., "orders-value")
/// * `schema_def` - The JSON Schema definition
/// * `description` - Optional human-readable description
///
/// # Returns
/// The schema ID of the newly registered schema
#[pg_extern]
pub fn register_schema(subject: &str, schema_def: pgrx::JsonB, description: Option<&str>) -> i32 {
    let fingerprint = compute_fingerprint(&schema_def.0);
    let version = get_next_version(subject);

    // Check if this exact schema already exists for this subject
    let existing_id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgkafka.schemas WHERE subject = $1 AND fingerprint = $2",
        &[subject.into(), fingerprint.clone().into()],
    )
    .ok()
    .flatten();

    if let Some(id) = existing_id {
        return id;
    }

    // Insert new schema
    let schema_json = serde_json::to_string(&schema_def.0).expect("Failed to serialize schema");

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgkafka.schemas (subject, version, schema_def, fingerprint, description)
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
pub fn get_schema(schema_id: i32) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgkafka.schemas WHERE id = $1",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
}

/// Get the latest schema for a subject
#[pg_extern]
pub fn get_latest_schema(subject: &str) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT schema_def FROM pgkafka.schemas WHERE subject = $1 ORDER BY version DESC LIMIT 1",
        &[subject.into()],
    )
    .ok()
    .flatten()
}

/// Get the schema bound to a topic
#[pg_extern]
pub fn get_topic_schema(
    topic_name: &str,
    schema_type: default!(&str, "'value'"),
) -> Option<pgrx::JsonB> {
    Spi::get_one_with_args::<pgrx::JsonB>(
        r#"
        SELECT s.schema_def
        FROM pgkafka.topic_schemas ts
        JOIN pgkafka.schemas s ON ts.schema_id = s.id
        WHERE ts.topic_name = $1 AND ts.schema_type = $2
        "#,
        &[topic_name.into(), schema_type.into()],
    )
    .ok()
    .flatten()
}

/// Bind a schema to a topic for validation
///
/// # Arguments
/// * `topic_name` - The Kafka topic name
/// * `schema_id` - The schema ID to bind
/// * `schema_type` - "key" or "value" (default: "value")
/// * `validation_mode` - "STRICT" or "LOG" (default: "STRICT")
///
/// # Returns
/// The binding ID
#[pg_extern]
pub fn bind_schema_to_topic(
    topic_name: &str,
    schema_id: i32,
    schema_type: default!(&str, "'value'"),
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    // Validate schema_type
    if schema_type != "key" && schema_type != "value" {
        pgrx::error!(
            "schema_type must be 'key' or 'value', got '{}'",
            schema_type
        );
    }

    // Validate validation_mode
    if validation_mode != "STRICT" && validation_mode != "LOG" {
        pgrx::error!(
            "validation_mode must be 'STRICT' or 'LOG', got '{}'",
            validation_mode
        );
    }

    // Verify schema exists
    let schema_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgkafka.schemas WHERE id = $1)",
        &[schema_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false);

    if !schema_exists {
        pgrx::error!("Schema with id {} does not exist", schema_id);
    }

    // Upsert binding
    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgkafka.topic_schemas (topic_name, schema_type, schema_id, validation_mode)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (topic_name, schema_type)
        DO UPDATE SET schema_id = EXCLUDED.schema_id, validation_mode = EXCLUDED.validation_mode
        RETURNING id
        "#,
        &[
            topic_name.into(),
            schema_type.into(),
            schema_id.into(),
            validation_mode.into(),
        ],
    )
    .expect("Failed to bind schema to topic")
    .expect("No ID returned from upsert")
}

/// Unbind a schema from a topic
#[pg_extern]
pub fn unbind_schema_from_topic(topic_name: &str, schema_type: default!(&str, "'value'")) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgkafka.topic_schemas
            WHERE topic_name = $1 AND schema_type = $2
            RETURNING 1
        )
        SELECT COUNT(*) FROM deleted
        "#,
        &[topic_name.into(), schema_type.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0);

    deleted > 0
}

/// Drop a schema by ID
#[pg_extern]
pub fn drop_schema(schema_id: i32) -> bool {
    let deleted = Spi::get_one_with_args::<i64>(
        r#"
        WITH deleted AS (
            DELETE FROM pgkafka.schemas
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
// Convenience Functions for Typed Topics
// =============================================================================

/// Create a typed topic with schema, table, and topic binding in one call
///
/// This convenience function:
/// 1. Creates a PostgreSQL table from the JSON Schema
/// 2. Registers the schema in the schema registry
/// 3. Creates a Kafka topic backed by the table
/// 4. Enables writes on the topic
/// 5. Binds the schema to the topic for validation
///
/// # Arguments
/// * `topic_name` - The Kafka topic name (also used as table name if not specified)
/// * `schema_def` - JSON Schema defining the message structure
/// * `table_name` - Optional table name (defaults to topic_name)
/// * `write_mode` - "stream" (append-only) or "table" (upsert by key) (default: "stream")
/// * `key_column` - Column name for the Kafka message key (required for table mode)
/// * `validation_mode` - "STRICT" or "LOG" (default: "STRICT")
///
/// # Returns
/// The topic ID
#[pg_extern]
pub fn create_typed_topic(
    topic_name: &str,
    schema_def: pgrx::JsonB,
    table_name: default!(Option<&str>, "NULL"),
    write_mode: default!(&str, "'stream'"),
    key_column: default!(Option<&str>, "NULL"),
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    // Validate write_mode
    if write_mode != "stream" && write_mode != "table" {
        pgrx::error!(
            "write_mode must be 'stream' or 'table', got '{}'",
            write_mode
        );
    }

    // Table mode requires key_column
    if write_mode == "table" && key_column.is_none() {
        pgrx::error!("key_column is required for table mode");
    }

    // Determine table name
    let actual_table_name = table_name.unwrap_or(topic_name);
    let qualified_table_name = if actual_table_name.contains('.') {
        actual_table_name.to_string()
    } else {
        format!("public.{}", actual_table_name)
    };

    // 1. Create the table from the schema (add id column for stream mode offset tracking)
    let add_id_column = write_mode == "stream";
    create_table_from_schema(&qualified_table_name, &schema_def.0, add_id_column);

    // 2. Register the schema
    let subject = format!("{}-value", topic_name);
    let schema_id = register_schema(
        &subject,
        schema_def,
        Some(&format!("Schema for topic {}", topic_name)),
    );

    // 3. Create the topic from the table
    // For stream mode, use "id" as offset column (auto-generated)
    // For table mode, we need to add an id column for offset tracking
    let offset_column = if write_mode == "stream" {
        "id"
    } else {
        // For table mode, add id column if not present
        let (schema_name, table) = parse_table_name(&qualified_table_name);
        let has_id = Spi::get_one_with_args::<bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = 'id'
            )
            "#,
            &[schema_name.clone().into(), table.clone().into()],
        )
        .ok()
        .flatten()
        .unwrap_or(false);

        if !has_id {
            // Add id column for offset tracking
            Spi::run(&format!(
                "ALTER TABLE {} ADD COLUMN id BIGSERIAL UNIQUE",
                qualified_table_name
            ))
            .expect("Failed to add id column");
        }
        "id"
    };

    // Determine key_column_str for topic creation
    let key_column_str = key_column;

    // Build the value expression for the topic (serialize row as JSON)
    let value_expr = format!("row_to_json({})::text", actual_table_name);

    let topic_id = crate::topic_writes::create_topic_from_table(
        topic_name,
        &qualified_table_name,
        offset_column,
        None,              // value_column
        Some(&value_expr), // value_expr
        key_column_str,    // key_column
        None,              // key_expr
        None,              // timestamp_column
    );

    // 4. Enable writes on the topic
    // For table mode, we need kafka_offset_column for tracking
    let kafka_offset_column = if write_mode == "table" {
        // Add kafka_offset column if not present
        let (schema_name, table) = parse_table_name(&qualified_table_name);
        let has_kafka_offset = Spi::get_one_with_args::<bool>(
            r#"
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = 'kafka_offset'
            )
            "#,
            &[schema_name.into(), table.into()],
        )
        .ok()
        .flatten()
        .unwrap_or(false);

        if !has_kafka_offset {
            Spi::run(&format!(
                "ALTER TABLE {} ADD COLUMN kafka_offset BIGINT",
                qualified_table_name
            ))
            .expect("Failed to add kafka_offset column");
        }
        Some("kafka_offset")
    } else {
        None
    };

    crate::topic_writes::enable_topic_writes(
        topic_name,
        write_mode,
        key_column_str,
        kafka_offset_column,
    );

    // 5. Bind the schema to the topic
    bind_schema_to_topic(topic_name, schema_id, "value", validation_mode);

    topic_id
}

/// Create a typed topic from an existing PostgreSQL table
///
/// This convenience function:
/// 1. Generates a JSON Schema from the table structure
/// 2. Registers the schema in the schema registry
/// 3. Creates a Kafka topic backed by the table
/// 4. Enables writes on the topic
/// 5. Binds the schema to the topic for validation
///
/// # Arguments
/// * `topic_name` - The Kafka topic name
/// * `table_name` - The existing PostgreSQL table
/// * `offset_column` - Column to use as Kafka offset (must be BIGSERIAL or have unique constraint)
/// * `write_mode` - "stream" (append-only) or "table" (upsert by key) (default: "stream")
/// * `key_column` - Column name for the Kafka message key (required for table mode)
/// * `kafka_offset_column` - Column to store Kafka offset for table mode (optional)
/// * `validation_mode` - "STRICT" or "LOG" (default: "STRICT")
///
/// # Returns
/// The topic ID
#[pg_extern]
pub fn create_typed_topic_from_table(
    topic_name: &str,
    table_name: &str,
    offset_column: &str,
    write_mode: default!(&str, "'stream'"),
    key_column: default!(Option<&str>, "NULL"),
    kafka_offset_column: default!(Option<&str>, "NULL"),
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    // Validate write_mode
    if write_mode != "stream" && write_mode != "table" {
        pgrx::error!(
            "write_mode must be 'stream' or 'table', got '{}'",
            write_mode
        );
    }

    // Table mode requires key_column
    if write_mode == "table" && key_column.is_none() {
        pgrx::error!("key_column is required for table mode");
    }

    // Get qualified table name
    let qualified_table_name = if table_name.contains('.') {
        table_name.to_string()
    } else {
        format!("public.{}", table_name)
    };

    // Get the simple table name (without schema) for value_expr
    let simple_table_name = if let Some(pos) = table_name.rfind('.') {
        &table_name[pos + 1..]
    } else {
        table_name
    };

    // 1. Generate schema from the table
    let schema_def = generate_schema_from_table(&qualified_table_name);

    // 2. Register the schema
    let subject = format!("{}-value", topic_name);
    let schema_id = register_schema(
        &subject,
        schema_def,
        Some(&format!(
            "Schema generated from table {}",
            qualified_table_name
        )),
    );

    // 3. Create the topic from the table
    let value_expr = format!("row_to_json({})::text", simple_table_name);

    let topic_id = crate::topic_writes::create_topic_from_table(
        topic_name,
        &qualified_table_name,
        offset_column,
        None,              // value_column
        Some(&value_expr), // value_expr
        key_column,        // key_column
        None,              // key_expr
        None,              // timestamp_column
    );

    // 4. Enable writes on the topic
    crate::topic_writes::enable_topic_writes(
        topic_name,
        write_mode,
        key_column,
        kafka_offset_column,
    );

    // 5. Bind the schema to the topic
    bind_schema_to_topic(topic_name, schema_id, "value", validation_mode);

    topic_id
}

/// Generate a JSON Schema from an existing PostgreSQL table
///
/// # Arguments
/// * `table_name` - The fully qualified table name (e.g., "public.orders")
///
/// # Returns
/// The generated JSON Schema as JSONB
#[pg_extern]
pub fn schema_from_table(table_name: &str) -> pgrx::JsonB {
    generate_schema_from_table(table_name)
}

/// Create a PostgreSQL table from a JSON Schema definition
///
/// # Arguments
/// * `table_name` - The fully qualified table name (e.g., "public.orders")
/// * `schema_def` - The JSON Schema definition
/// * `add_id_column` - If true, adds an `id BIGSERIAL PRIMARY KEY` column (default: true)
///
/// # Returns
/// True if the table was created successfully
#[pg_extern]
pub fn table_from_schema(
    table_name: &str,
    schema_def: pgrx::JsonB,
    add_id_column: default!(bool, "true"),
) -> bool {
    create_table_from_schema(table_name, &schema_def.0, add_id_column)
}

// Unit tests are in the integration test file (tests/kafka_client_test.rs)
// to avoid linker issues with pgrx when running `cargo test`.
// The functions here are tested via E2E tests that call the SQL functions.
