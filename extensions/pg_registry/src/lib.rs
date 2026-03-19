//! pg_registry - Schema registry for PostgreSQL extensions
//!
//! This extension provides JSON Schema management for pg_kafka and other
//! extensions. It stores schemas, auto-generates schemas from PostgreSQL
//! tables, and validates data against registered schemas.

#![allow(unexpected_cfgs)]

use pgrx::prelude::*;

mod generator;
mod schema;
mod validator;

pgrx::pg_module_magic!();

// =============================================================================
// Extension Documentation
// =============================================================================

/// Returns the extension documentation (README.md) as a string.
/// This enables runtime discovery of extension capabilities via pg_mcp.
#[pg_extern]
fn extension_docs() -> &'static str {
    include_str!("../README.md")
}

// Bootstrap tables - runs first during CREATE EXTENSION
pgrx::extension_sql!(
    r#"
-- Core schema storage table
CREATE TABLE pgregistry.schemas (
    id SERIAL PRIMARY KEY,
    subject TEXT NOT NULL,
    version INT NOT NULL DEFAULT 1,
    schema_def JSONB NOT NULL,
    fingerprint TEXT NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (subject, version),
    UNIQUE (subject, fingerprint)
);

CREATE INDEX idx_schemas_subject ON pgregistry.schemas(subject);
CREATE INDEX idx_schemas_fingerprint ON pgregistry.schemas(fingerprint);

-- Schema-to-topic binding table
CREATE TABLE pgregistry.topic_schemas (
    id SERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    schema_type TEXT NOT NULL CHECK (schema_type IN ('key', 'value')),
    schema_id INT NOT NULL REFERENCES pgregistry.schemas(id) ON DELETE CASCADE,
    validation_mode TEXT NOT NULL DEFAULT 'STRICT' CHECK (validation_mode IN ('STRICT', 'LOG')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_name, schema_type)
);

CREATE INDEX idx_topic_schemas_topic ON pgregistry.topic_schemas(topic_name);

-- Table-to-topic binding table for bidirectional Kafka integration
CREATE TABLE pgregistry.table_topic_bindings (
    id SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL UNIQUE,
    schema_id INT REFERENCES pgregistry.schemas(id) ON DELETE SET NULL,
    kafka_topic TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('stream', 'table')),
    key_column TEXT,           -- For table mode: which column is the primary key (from Kafka key)
    offset_column TEXT,        -- For stream mode: auto-increment column for offsets
    kafka_offset_column TEXT,  -- For table mode: column tracking latest kafka offset
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_table_topic_bindings_topic ON pgregistry.table_topic_bindings(kafka_topic);
"#,
    name = "bootstrap_tables",
    bootstrap
);

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
fn register_schema(subject: &str, schema_def: pgrx::JsonB, description: Option<&str>) -> i32 {
    schema::storage::register_schema(subject, schema_def, description)
}

/// Generate JSON Schema from a PostgreSQL table structure
///
/// Introspects the table's columns and generates a JSON Schema that
/// describes the expected structure of records from that table.
///
/// # Arguments
/// * `table_name` - Fully qualified table name (e.g., "public.orders")
///
/// # Returns
/// A JSON Schema definition as JSONB
#[pg_extern]
fn generate_schema_from_table(table_name: &str) -> pgrx::JsonB {
    generator::json_schema::generate_from_table(table_name)
}

/// Register a schema generated from a PostgreSQL table
///
/// Combines `generate_schema_from_table` and `register_schema` into one call.
///
/// # Arguments
/// * `subject` - Subject name for the schema
/// * `table_name` - Fully qualified table name
/// * `description` - Optional description
///
/// # Returns
/// The schema ID of the newly registered schema
#[pg_extern]
fn register_schema_from_table(subject: &str, table_name: &str, description: Option<&str>) -> i32 {
    let schema_def = generator::json_schema::generate_from_table(table_name);
    schema::storage::register_schema(subject, schema_def, description)
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
fn bind_schema_to_topic(
    topic_name: &str,
    schema_id: i32,
    schema_type: default!(&str, "'value'"),
    validation_mode: default!(&str, "'STRICT'"),
) -> i32 {
    schema::storage::bind_schema_to_topic(topic_name, schema_id, schema_type, validation_mode)
}

/// Unbind a schema from a topic
///
/// # Arguments
/// * `topic_name` - The Kafka topic name
/// * `schema_type` - "key" or "value" (default: "value")
///
/// # Returns
/// True if a binding was removed, false if none existed
#[pg_extern]
fn unbind_schema_from_topic(topic_name: &str, schema_type: default!(&str, "'value'")) -> bool {
    schema::storage::unbind_schema_from_topic(topic_name, schema_type)
}

/// Validate JSON data against a schema
///
/// # Arguments
/// * `schema_id` - The schema ID to validate against
/// * `data` - The JSON data to validate
///
/// # Returns
/// True if valid, false otherwise
#[pg_extern]
fn validate(schema_id: i32, data: pgrx::JsonB) -> bool {
    validator::jsonschema::validate(schema_id, data)
}

/// Validate JSON data against a topic's bound schema
///
/// # Arguments
/// * `topic_name` - The topic name
/// * `data` - The JSON data to validate
/// * `schema_type` - "key" or "value" (default: "value")
///
/// # Returns
/// True if valid (or no schema bound), false if invalid
#[pg_extern]
fn validate_for_topic(
    topic_name: &str,
    data: pgrx::JsonB,
    schema_type: default!(&str, "'value'"),
) -> bool {
    validator::jsonschema::validate_for_topic(topic_name, data, schema_type)
}

/// Get a schema by ID
///
/// # Arguments
/// * `schema_id` - The schema ID
///
/// # Returns
/// The schema definition as JSONB, or NULL if not found
#[pg_extern]
fn get_schema(schema_id: i32) -> Option<pgrx::JsonB> {
    schema::storage::get_schema(schema_id)
}

/// Get the latest schema for a subject
///
/// # Arguments
/// * `subject` - The subject name
///
/// # Returns
/// The schema definition as JSONB, or NULL if not found
#[pg_extern]
fn get_latest_schema(subject: &str) -> Option<pgrx::JsonB> {
    schema::storage::get_latest_schema(subject)
}

/// Get the schema bound to a topic
///
/// # Arguments
/// * `topic_name` - The topic name
/// * `schema_type` - "key" or "value" (default: "value")
///
/// # Returns
/// The schema definition as JSONB, or NULL if no schema is bound
#[pg_extern]
fn get_topic_schema(
    topic_name: &str,
    schema_type: default!(&str, "'value'"),
) -> Option<pgrx::JsonB> {
    schema::storage::get_topic_schema(topic_name, schema_type)
}

/// Drop a schema by ID
///
/// # Arguments
/// * `schema_id` - The schema ID to drop
///
/// # Returns
/// True if deleted, false if not found
#[pg_extern]
fn drop_schema(schema_id: i32) -> bool {
    schema::storage::drop_schema(schema_id)
}

// ============================================================================
// Table Creation and Binding Functions
// ============================================================================

/// Create a PostgreSQL table from a registered schema
///
/// Generates a CREATE TABLE statement from the JSON Schema definition
/// and executes it. The table will have columns matching the schema properties.
///
/// # Arguments
/// * `table_name` - Fully qualified table name (e.g., "public.orders")
/// * `schema_subject` - Subject name of the registered schema
/// * `add_id_column` - If true, adds an `id BIGSERIAL PRIMARY KEY` column (default: true)
///
/// # Returns
/// True if the table was created successfully
#[pg_extern]
fn create_table_from_schema(
    table_name: &str,
    schema_subject: &str,
    add_id_column: default!(bool, true),
) -> bool {
    // Get the schema
    let schema_def = schema::storage::get_latest_schema(schema_subject)
        .unwrap_or_else(|| pgrx::error!("Schema '{}' not found", schema_subject));

    // Create the table
    generator::table::create_table(table_name, &schema_def.0, add_id_column)
}

/// Bind a table to a Kafka topic for bidirectional data flow
///
/// Creates a binding that allows:
/// - SQL INSERTs into the table to be readable via Kafka consumers
/// - Kafka produces to be INSERTed/UPSERTed into the table
///
/// # Arguments
/// * `table_name` - Fully qualified table name (e.g., "public.orders")
/// * `kafka_topic` - The Kafka topic name
/// * `mode` - "stream" (append-only) or "table" (upsert by key), default: "stream"
///
/// # Returns
/// The binding ID
///
/// # Notes
/// - For "stream" mode: Every Kafka message is INSERTed as a new row
/// - For "table" mode: Kafka key = primary key, duplicates UPDATE existing rows
#[pg_extern]
fn bind_table_to_topic(
    table_name: &str,
    kafka_topic: &str,
    mode: default!(&str, "'stream'"),
) -> i32 {
    schema::storage::bind_table_to_topic(table_name, kafka_topic, mode)
}

/// Unbind a table from its Kafka topic
///
/// # Arguments
/// * `table_name` - The table name
///
/// # Returns
/// True if a binding was removed, false if none existed
#[pg_extern]
fn unbind_table_from_topic(table_name: &str) -> bool {
    schema::storage::unbind_table_from_topic(table_name)
}

/// Get the binding info for a table
///
/// # Arguments
/// * `table_name` - The table name
///
/// # Returns
/// Binding info as JSON, or NULL if no binding exists
#[pg_extern]
fn get_table_binding(table_name: &str) -> Option<pgrx::JsonB> {
    schema::storage::get_table_binding(table_name)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_register_and_get_schema() {
        let schema_def = pgrx::JsonB(serde_json::json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            }
        }));

        let schema_id = crate::register_schema("test-subject", schema_def, Some("Test schema"));

        let retrieved = crate::get_schema(schema_id);
        assert!(retrieved.is_some());

        let retrieved_def = retrieved.unwrap();
        assert_eq!(retrieved_def.0["type"], "object");
    }

    #[pg_test]
    fn test_get_latest_schema() {
        let schema_def1 = pgrx::JsonB(serde_json::json!({"type": "object", "version": 1}));
        let schema_def2 = pgrx::JsonB(serde_json::json!({"type": "object", "version": 2}));

        crate::register_schema("versioned-subject", schema_def1, None);
        crate::register_schema("versioned-subject", schema_def2, None);

        let latest = crate::get_latest_schema("versioned-subject");
        assert!(latest.is_some());

        let schema = latest.unwrap();
        assert_eq!(schema.0["version"], 2);
    }

    #[pg_test]
    fn test_validate_valid_data() {
        let schema_def = pgrx::JsonB(serde_json::json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        }));

        let schema_id = crate::register_schema("validation-test", schema_def, None);

        let valid_data = pgrx::JsonB(serde_json::json!({"name": "Alice", "age": 30}));
        assert!(crate::validate(schema_id, valid_data));
    }

    #[pg_test]
    fn test_validate_invalid_data() {
        let schema_def = pgrx::JsonB(serde_json::json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        }));

        let schema_id = crate::register_schema("validation-test-invalid", schema_def, None);

        // Missing required field
        let invalid_data = pgrx::JsonB(serde_json::json!({"age": 30}));
        assert!(!crate::validate(schema_id, invalid_data));
    }

    #[pg_test]
    fn test_bind_and_validate_for_topic() {
        let schema_def = pgrx::JsonB(serde_json::json!({
            "type": "object",
            "properties": {
                "order_id": {"type": "integer"}
            },
            "required": ["order_id"]
        }));

        let schema_id = crate::register_schema("orders-value", schema_def, None);
        crate::bind_schema_to_topic("orders", schema_id, "value", "STRICT");

        let valid_data = pgrx::JsonB(serde_json::json!({"order_id": 123}));
        assert!(crate::validate_for_topic("orders", valid_data, "value"));

        let invalid_data = pgrx::JsonB(serde_json::json!({"name": "test"}));
        assert!(!crate::validate_for_topic("orders", invalid_data, "value"));
    }

    #[pg_test]
    fn test_drop_schema() {
        let schema_def = pgrx::JsonB(serde_json::json!({"type": "string"}));
        let schema_id = crate::register_schema("to-delete", schema_def, None);

        assert!(crate::get_schema(schema_id).is_some());
        assert!(crate::drop_schema(schema_id));
        assert!(crate::get_schema(schema_id).is_none());
        assert!(!crate::drop_schema(schema_id)); // Already deleted
    }

    #[pg_test]
    fn test_create_table_from_schema() {
        // Register a schema
        let schema_def = pgrx::JsonB(serde_json::json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "email": {"type": "string", "maxLength": 255},
                "age": {"type": "integer"}
            },
            "required": ["name", "email"]
        }));

        crate::register_schema("users-value", schema_def, Some("User schema"));

        // Create table from schema
        let result =
            crate::create_table_from_schema("public.test_users_from_schema", "users-value", true);
        assert!(result);

        // Verify table was created with correct columns
        let has_id = Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'test_users_from_schema' AND column_name = 'id')"
        ).unwrap().unwrap_or(false);
        assert!(has_id, "Table should have id column");

        let has_name = Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'test_users_from_schema' AND column_name = 'name')"
        ).unwrap().unwrap_or(false);
        assert!(has_name, "Table should have name column");

        let has_email = Spi::get_one::<bool>(
            "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'test_users_from_schema' AND column_name = 'email')"
        ).unwrap().unwrap_or(false);
        assert!(has_email, "Table should have email column");

        // Clean up
        Spi::run("DROP TABLE public.test_users_from_schema").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_bind_table_to_topic_stream_mode() {
        // Create a test table with serial column
        Spi::run(
            "CREATE TABLE public.test_stream_events (
                id BIGSERIAL PRIMARY KEY,
                event_type TEXT NOT NULL,
                payload JSONB
            )",
        )
        .expect("Failed to create test table");

        // Bind table to topic in stream mode
        let binding_id = crate::bind_table_to_topic(
            "public.test_stream_events",
            "stream-events-topic",
            "stream",
        );
        assert!(binding_id > 0);

        // Verify binding was created
        let binding = crate::get_table_binding("public.test_stream_events");
        assert!(binding.is_some());

        let binding_json = binding.unwrap();
        assert_eq!(binding_json.0["mode"], "stream");
        assert_eq!(binding_json.0["kafka_topic"], "stream-events-topic");

        // Clean up
        crate::unbind_table_from_topic("public.test_stream_events");
        Spi::run("DROP TABLE public.test_stream_events").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_bind_table_to_topic_table_mode() {
        // Create a test table with primary key and kafka_offset column
        Spi::run(
            "CREATE TABLE public.test_customers (
                customer_id TEXT PRIMARY KEY,
                name TEXT,
                email TEXT,
                kafka_offset BIGINT
            )",
        )
        .expect("Failed to create test table");

        // Bind table to topic in table mode
        let binding_id =
            crate::bind_table_to_topic("public.test_customers", "customers-topic", "table");
        assert!(binding_id > 0);

        // Verify binding was created
        let binding = crate::get_table_binding("public.test_customers");
        assert!(binding.is_some());

        let binding_json = binding.unwrap();
        assert_eq!(binding_json.0["mode"], "table");
        assert_eq!(binding_json.0["key_column"], "customer_id");
        assert_eq!(binding_json.0["kafka_offset_column"], "kafka_offset");

        // Clean up
        crate::unbind_table_from_topic("public.test_customers");
        Spi::run("DROP TABLE public.test_customers").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_unbind_table_from_topic() {
        // Create and bind a table
        Spi::run(
            "CREATE TABLE public.test_unbind (
                id BIGSERIAL PRIMARY KEY,
                data TEXT
            )",
        )
        .expect("Failed to create test table");

        crate::bind_table_to_topic("public.test_unbind", "unbind-topic", "stream");

        // Verify binding exists
        assert!(crate::get_table_binding("public.test_unbind").is_some());

        // Unbind
        let result = crate::unbind_table_from_topic("public.test_unbind");
        assert!(result);

        // Verify binding is gone
        assert!(crate::get_table_binding("public.test_unbind").is_none());

        // Clean up
        Spi::run("DROP TABLE public.test_unbind").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_bind_table_requires_serial_for_stream() {
        // Create a table without serial column
        Spi::run(
            "CREATE TABLE public.test_no_serial (
                name TEXT NOT NULL,
                value INT
            )",
        )
        .expect("Failed to create test table");

        // Binding in stream mode should fail
        let result = std::panic::catch_unwind(|| {
            crate::bind_table_to_topic("public.test_no_serial", "no-serial-topic", "stream")
        });
        assert!(
            result.is_err(),
            "Should fail without serial column for stream mode"
        );

        // Clean up
        Spi::run("DROP TABLE public.test_no_serial").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_bind_table_requires_pk_for_table_mode() {
        // Create a table without primary key
        Spi::run(
            "CREATE TABLE public.test_no_pk (
                id BIGSERIAL,
                name TEXT NOT NULL
            )",
        )
        .expect("Failed to create test table");

        // Binding in table mode should fail
        let result = std::panic::catch_unwind(|| {
            crate::bind_table_to_topic("public.test_no_pk", "no-pk-topic", "table")
        });
        assert!(
            result.is_err(),
            "Should fail without primary key for table mode"
        );

        // Clean up
        Spi::run("DROP TABLE public.test_no_pk").expect("Failed to drop table");
    }
}

/// This module is required by `cargo pgrx test` invocations.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
