//! pg_kafka - Kafka protocol server backed by PostgreSQL tables
//!
//! This extension implements a subset of the Kafka protocol, allowing
//! standard Kafka clients to produce and consume messages stored in
//! PostgreSQL tables.
//!
//! The TCP server runs as a PostgreSQL background worker, starting
//! automatically when PostgreSQL starts.

// Allow pgrx macro cfg values and functions with many parameters
#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]

use pgrx::prelude::*;

mod admin;
mod config;
mod produce;
mod protocol;
mod schema;
mod server;
mod storage;
mod topic_admin;
mod topic_writes;
mod validation;
mod worker;

// Re-export public API
pub use admin::status;
pub use produce::produce;
pub use schema::{
    bind_schema_to_topic, create_typed_topic, create_typed_topic_from_table, drop_schema,
    get_latest_schema, get_schema, get_topic_schema, register_schema, schema_from_table,
    table_from_schema, unbind_schema_from_topic,
};
pub use topic_admin::{create_topic, drop_topic};
pub use topic_writes::{create_topic_from_table, disable_topic_writes, enable_topic_writes};
pub use worker::{_PG_init, pg_kafka_worker_main};

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

// Include our schema tables SQL - runs first (bootstrap)
pgrx::extension_sql!(
    r#"
-- Topic registry
CREATE TABLE pgkafka.topics (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source_table TEXT,
    offset_column TEXT,
    value_column TEXT,
    value_expr TEXT,
    key_column TEXT,
    key_expr TEXT,
    headers_column TEXT,
    headers_expr TEXT,
    timestamp_column TEXT,
    timestamp_expr TEXT,
    -- Write support for source-backed topics
    writable BOOLEAN NOT NULL DEFAULT FALSE,
    write_mode TEXT CHECK (write_mode IS NULL OR write_mode IN ('stream', 'table')),
    write_key_column TEXT,           -- For table mode: column to match Kafka key
    kafka_offset_column TEXT,        -- For table mode: column to track Kafka offset
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    config JSONB NOT NULL DEFAULT '{}',
    CONSTRAINT chk_value_source CHECK (
        source_table IS NULL OR (value_column IS NOT NULL OR value_expr IS NOT NULL)
    ),
    CONSTRAINT chk_offset_required CHECK (
        source_table IS NULL OR offset_column IS NOT NULL
    ),
    CONSTRAINT chk_value_exclusive CHECK (
        NOT (value_column IS NOT NULL AND value_expr IS NOT NULL)
    ),
    CONSTRAINT chk_key_exclusive CHECK (
        NOT (key_column IS NOT NULL AND key_expr IS NOT NULL)
    ),
    CONSTRAINT chk_write_mode_source CHECK (
        write_mode IS NULL OR source_table IS NOT NULL
    ),
    CONSTRAINT chk_table_mode_key CHECK (
        write_mode != 'table' OR write_key_column IS NOT NULL
    )
);

-- Messages table (for native topics)
CREATE TABLE pgkafka.messages (
    offset_id BIGSERIAL,
    topic_id INT NOT NULL REFERENCES pgkafka.topics(id) ON DELETE CASCADE,
    key BYTEA,
    key_text TEXT,
    key_json JSONB,
    value BYTEA NOT NULL,
    value_text TEXT,
    value_json JSONB,
    headers JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic_id, offset_id)
);

CREATE INDEX idx_messages_topic_offset ON pgkafka.messages (topic_id, offset_id);

-- Consumer groups
CREATE TABLE pgkafka.consumer_groups (
    id SERIAL PRIMARY KEY,
    group_id TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'Empty',
    generation_id INT NOT NULL DEFAULT 0,
    protocol TEXT,
    leader_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Consumer group members
CREATE TABLE pgkafka.consumer_group_members (
    id SERIAL PRIMARY KEY,
    group_id INT NOT NULL REFERENCES pgkafka.consumer_groups(id) ON DELETE CASCADE,
    member_id TEXT NOT NULL,
    client_id TEXT,
    client_host TEXT,
    session_timeout_ms INT NOT NULL DEFAULT 30000,
    rebalance_timeout_ms INT NOT NULL DEFAULT 60000,
    assignment BYTEA,
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (group_id, member_id)
);

-- Consumer offsets
CREATE TABLE pgkafka.consumer_offsets (
    group_id INT NOT NULL REFERENCES pgkafka.consumer_groups(id) ON DELETE CASCADE,
    topic_id INT NOT NULL REFERENCES pgkafka.topics(id) ON DELETE CASCADE,
    partition INT NOT NULL DEFAULT 0,
    committed_offset BIGINT NOT NULL,
    metadata TEXT,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (group_id, topic_id, partition)
);

-- Aggressive autovacuum for high-write message table
ALTER TABLE pgkafka.messages SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_analyze_scale_factor = 0.005
);

-- Schema registry (for typed topics)
CREATE TABLE pgkafka.schemas (
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

CREATE INDEX idx_schemas_subject ON pgkafka.schemas(subject);
CREATE INDEX idx_schemas_fingerprint ON pgkafka.schemas(fingerprint);

-- Schema-to-topic binding for validation
CREATE TABLE pgkafka.topic_schemas (
    id SERIAL PRIMARY KEY,
    topic_name TEXT NOT NULL,
    schema_type TEXT NOT NULL CHECK (schema_type IN ('key', 'value')),
    schema_id INT NOT NULL REFERENCES pgkafka.schemas(id) ON DELETE CASCADE,
    validation_mode TEXT NOT NULL DEFAULT 'STRICT' CHECK (validation_mode IN ('STRICT', 'LOG')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (topic_name, schema_type)
);

CREATE INDEX idx_topic_schemas_topic ON pgkafka.topic_schemas(topic_name);
"#,
    name = "bootstrap_tables",
    bootstrap
);

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_status() {
        let status = crate::status();
        assert!(status.0.is_object());
    }

    #[pg_test]
    fn test_produce_json_payload() {
        // Create a test topic
        let topic_id =
            crate::create_topic("test-json-topic", None, None, None, None, None, None, None);
        assert!(topic_id > 0, "Failed to create topic");

        // Produce a JSON message
        let json_value = br#"{"name":"test","count":42}"#.to_vec();
        let json_key = b"user-123".to_vec();
        let offset = crate::produce("test-json-topic", json_value, Some(json_key), None);
        assert!(offset > 0, "Failed to produce message");

        // Verify value_text is populated
        let value_text = Spi::get_one::<String>(&format!(
            "SELECT value_text FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(value_text.is_ok());
        assert!(
            value_text.unwrap().is_some(),
            "value_text should be populated"
        );

        // Verify value_json is populated
        let json_result = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT value_json FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(json_result.is_ok());
        let json_value = json_result.unwrap();
        assert!(json_value.is_some(), "value_json should be populated");

        // Verify key_text is populated
        let key_text = Spi::get_one::<String>(&format!(
            "SELECT key_text FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(key_text.is_ok());
        assert_eq!(key_text.unwrap(), Some("user-123".to_string()));

        // Verify key_json is NULL (not valid JSON)
        let key_json = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT key_json FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(key_json.is_ok());
        assert!(
            key_json.unwrap().is_none(),
            "key_json should be NULL for non-JSON key"
        );

        // Clean up
        crate::drop_topic("test-json-topic", true);
    }

    #[pg_test]
    fn test_produce_binary_payload() {
        // Create a test topic
        let topic_id = crate::create_topic(
            "test-binary-topic",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(topic_id > 0, "Failed to create topic");

        // Produce a binary (non-UTF8) message
        let binary_value = vec![0xff, 0xfe, 0x00, 0x01];
        let offset = crate::produce("test-binary-topic", binary_value, None, None);
        assert!(offset > 0, "Failed to produce message");

        // Verify value_text is NULL (invalid UTF-8)
        let value_text = Spi::get_one::<String>(&format!(
            "SELECT value_text FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(value_text.is_ok());
        assert!(
            value_text.unwrap().is_none(),
            "value_text should be NULL for binary data"
        );

        // Verify value_json is NULL
        let value_json = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT value_json FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(value_json.is_ok());
        assert!(
            value_json.unwrap().is_none(),
            "value_json should be NULL for binary data"
        );

        // Clean up
        crate::drop_topic("test-binary-topic", true);
    }

    #[pg_test]
    fn test_produce_text_non_json_payload() {
        // Create a test topic
        let topic_id =
            crate::create_topic("test-text-topic", None, None, None, None, None, None, None);
        assert!(topic_id > 0, "Failed to create topic");

        // Produce a plain text (non-JSON) message
        let text_value = b"Hello, World!".to_vec();
        let offset = crate::produce("test-text-topic", text_value, None, None);
        assert!(offset > 0, "Failed to produce message");

        // Verify value_text is populated
        let value_text = Spi::get_one::<String>(&format!(
            "SELECT value_text FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(value_text.is_ok());
        assert_eq!(value_text.unwrap(), Some("Hello, World!".to_string()));

        // Verify value_json is NULL (not valid JSON)
        let value_json = Spi::get_one::<pgrx::JsonB>(&format!(
            "SELECT value_json FROM pgkafka.messages WHERE offset_id = {}",
            offset
        ));
        assert!(value_json.is_ok());
        assert!(
            value_json.unwrap().is_none(),
            "value_json should be NULL for non-JSON text"
        );

        // Clean up
        crate::drop_topic("test-text-topic", true);
    }

    #[pg_test]
    fn test_query_json_payload() {
        // Create a test topic
        let topic_id =
            crate::create_topic("test-query-topic", None, None, None, None, None, None, None);
        assert!(topic_id > 0, "Failed to create topic");

        // Produce multiple JSON messages
        crate::produce(
            "test-query-topic",
            br#"{"type":"order","id":1}"#.to_vec(),
            None,
            None,
        );
        crate::produce(
            "test-query-topic",
            br#"{"type":"user","id":2}"#.to_vec(),
            None,
            None,
        );
        crate::produce(
            "test-query-topic",
            br#"{"type":"order","id":3}"#.to_vec(),
            None,
            None,
        );

        // Query using JSON operators
        let count = Spi::get_one::<i64>(
            "SELECT COUNT(*) FROM pgkafka.messages m
             JOIN pgkafka.topics t ON m.topic_id = t.id
             WHERE t.name = 'test-query-topic' AND value_json->>'type' = 'order'",
        );
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), Some(2), "Should find 2 order messages");

        // Clean up
        crate::drop_topic("test-query-topic", true);
    }

    #[pg_test]
    fn test_create_topic_from_table() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_orders (
            id BIGSERIAL PRIMARY KEY,
            customer_id TEXT NOT NULL,
            total DECIMAL(10,2),
            status TEXT,
            created_at TIMESTAMPTZ DEFAULT now()
        )",
        )
        .expect("Failed to create test_orders table");

        // Insert some test data
        Spi::run(
            "INSERT INTO test_orders (customer_id, total, status) VALUES
            ('cust-1', 100.00, 'pending'),
            ('cust-2', 250.50, 'shipped'),
            ('cust-1', 75.25, 'delivered')",
        )
        .expect("Failed to insert test data");

        // Create a topic from the table using value_expr (row_to_json)
        let topic_id = crate::create_topic_from_table(
            "orders-topic",
            "public.test_orders",
            "id",
            None,                                   // value_column
            Some("row_to_json(test_orders)::text"), // value_expr
            Some("customer_id"),                    // key_column
            None,                                   // key_expr
            Some("created_at"),                     // timestamp_column
        );
        assert!(topic_id > 0, "Failed to create topic from table");

        // Verify topic was created with correct mappings
        let source_table = Spi::get_one::<String>(
            "SELECT source_table FROM pgkafka.topics WHERE name = 'orders-topic'",
        );
        assert_eq!(
            source_table.unwrap(),
            Some("public.test_orders".to_string())
        );

        let offset_col = Spi::get_one::<String>(
            "SELECT offset_column FROM pgkafka.topics WHERE name = 'orders-topic'",
        );
        assert_eq!(offset_col.unwrap(), Some("id".to_string()));

        // Clean up
        crate::drop_topic("orders-topic", true);
        Spi::run("DROP TABLE test_orders").expect("Failed to drop test_orders");
    }

    #[pg_test]
    fn test_create_topic_from_table_validation_errors() {
        // Test: Table does not exist
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "nonexistent-topic",
                "public.nonexistent_table",
                "id",
                Some("data"),
                None,
                None,
                None,
                None,
            )
        });
        assert!(result.is_err(), "Should fail for nonexistent table");

        // Create a table without primary key for testing
        Spi::run(
            "CREATE TABLE test_no_pk (
            id INT,
            data TEXT
        )",
        )
        .expect("Failed to create test_no_pk table");

        // Test: Offset column without unique constraint
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "no-pk-topic",
                "public.test_no_pk",
                "id",
                Some("data"),
                None,
                None,
                None,
                None,
            )
        });
        assert!(
            result.is_err(),
            "Should fail when offset column lacks unique constraint"
        );

        // Create a table with UUID primary key
        Spi::run(
            "CREATE TABLE test_uuid_pk (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            data TEXT
        )",
        )
        .expect("Failed to create test_uuid_pk table");

        // Test: Offset column with non-integer type
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "uuid-pk-topic",
                "public.test_uuid_pk",
                "id",
                Some("data"),
                None,
                None,
                None,
                None,
            )
        });
        assert!(
            result.is_err(),
            "Should fail when offset column is not integer type"
        );

        // Clean up
        Spi::run("DROP TABLE test_no_pk").expect("Failed to drop test_no_pk");
        Spi::run("DROP TABLE test_uuid_pk").expect("Failed to drop test_uuid_pk");
    }

    #[pg_test]
    fn test_create_topic_from_table_value_validation() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_value_validation (
            id BIGSERIAL PRIMARY KEY,
            data TEXT
        )",
        )
        .expect("Failed to create test table");

        // Test: Neither value_column nor value_expr provided
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "no-value-topic",
                "public.test_value_validation",
                "id",
                None, // value_column
                None, // value_expr
                None,
                None,
                None,
            )
        });
        assert!(
            result.is_err(),
            "Should fail when neither value_column nor value_expr is provided"
        );

        // Test: Both value_column and value_expr provided
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "both-value-topic",
                "public.test_value_validation",
                "id",
                Some("data"),       // value_column
                Some("data::text"), // value_expr
                None,
                None,
                None,
            )
        });
        assert!(
            result.is_err(),
            "Should fail when both value_column and value_expr are provided"
        );

        // Clean up
        Spi::run("DROP TABLE test_value_validation").expect("Failed to drop test table");
    }

    #[pg_test]
    fn test_source_backed_topic_is_read_only() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_readonly (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB
        )",
        )
        .expect("Failed to create test_readonly table");

        // Create topic from table
        let topic_id = crate::create_topic_from_table(
            "readonly-topic",
            "public.test_readonly",
            "id",
            Some("payload"),
            None,
            None,
            None,
            None,
        );
        assert!(topic_id > 0, "Failed to create topic from table");

        // Try to produce to the source-backed topic - should fail
        let result = std::panic::catch_unwind(|| {
            crate::produce(
                "readonly-topic",
                br#"{"test": "data"}"#.to_vec(),
                None,
                None,
            )
        });
        assert!(
            result.is_err(),
            "Should fail to produce to source-backed topic"
        );

        // Clean up
        crate::drop_topic("readonly-topic", true);
        Spi::run("DROP TABLE test_readonly").expect("Failed to drop test_readonly");
    }

    #[pg_test]
    fn test_source_backed_topic_with_value_column() {
        // Create a source table with a payload column
        Spi::run(
            "CREATE TABLE test_events (
            event_id BIGSERIAL PRIMARY KEY,
            event_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT now()
        )",
        )
        .expect("Failed to create test_events table");

        // Insert test data
        Spi::run(
            "INSERT INTO test_events (event_type, payload) VALUES
            ('user.created', '{\"user_id\": 1, \"name\": \"Alice\"}'),
            ('order.placed', '{\"order_id\": 100, \"amount\": 50.00}'),
            ('user.updated', '{\"user_id\": 1, \"name\": \"Alice Smith\"}')",
        )
        .expect("Failed to insert test data");

        // Create topic using value_column
        let topic_id = crate::create_topic_from_table(
            "events-topic",
            "public.test_events",
            "event_id",
            Some("payload"),    // value_column
            None,               // value_expr
            Some("event_type"), // key_column
            None,               // key_expr
            Some("created_at"), // timestamp_column
        );
        assert!(topic_id > 0, "Failed to create topic from table");

        // Verify topic configuration
        let value_column = Spi::get_one::<String>(
            "SELECT value_column FROM pgkafka.topics WHERE name = 'events-topic'",
        );
        assert_eq!(value_column.unwrap(), Some("payload".to_string()));

        // Clean up
        crate::drop_topic("events-topic", true);
        Spi::run("DROP TABLE test_events").expect("Failed to drop test_events");
    }

    #[pg_test]
    fn test_timestamp_column_validation_valid_types() {
        // Create tables with different valid timestamp column types
        Spi::run(
            "CREATE TABLE test_ts_timestamptz (
                id BIGSERIAL PRIMARY KEY,
                data TEXT,
                ts TIMESTAMPTZ DEFAULT now()
            )",
        )
        .expect("Failed to create table");

        Spi::run(
            "CREATE TABLE test_ts_timestamp (
                id BIGSERIAL PRIMARY KEY,
                data TEXT,
                ts TIMESTAMP DEFAULT now()
            )",
        )
        .expect("Failed to create table");

        Spi::run(
            "CREATE TABLE test_ts_bigint (
                id BIGSERIAL PRIMARY KEY,
                data TEXT,
                ts_epoch BIGINT DEFAULT (EXTRACT(EPOCH FROM now()) * 1000)::bigint
            )",
        )
        .expect("Failed to create table");

        // Test: TIMESTAMPTZ should work
        let topic_id = crate::create_topic_from_table(
            "ts-timestamptz-topic",
            "public.test_ts_timestamptz",
            "id",
            Some("data"),
            None,
            None,
            None,
            Some("ts"),
        );
        assert!(topic_id > 0, "Should succeed with TIMESTAMPTZ column");

        // Test: TIMESTAMP should work
        let topic_id = crate::create_topic_from_table(
            "ts-timestamp-topic",
            "public.test_ts_timestamp",
            "id",
            Some("data"),
            None,
            None,
            None,
            Some("ts"),
        );
        assert!(topic_id > 0, "Should succeed with TIMESTAMP column");

        // Test: BIGINT should work (epoch milliseconds)
        let topic_id = crate::create_topic_from_table(
            "ts-bigint-topic",
            "public.test_ts_bigint",
            "id",
            Some("data"),
            None,
            None,
            None,
            Some("ts_epoch"),
        );
        assert!(topic_id > 0, "Should succeed with BIGINT column");

        // Clean up
        crate::drop_topic("ts-timestamptz-topic", true);
        crate::drop_topic("ts-timestamp-topic", true);
        crate::drop_topic("ts-bigint-topic", true);
        Spi::run("DROP TABLE test_ts_timestamptz").expect("Failed to drop table");
        Spi::run("DROP TABLE test_ts_timestamp").expect("Failed to drop table");
        Spi::run("DROP TABLE test_ts_bigint").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_timestamp_column_validation_invalid_type() {
        // Create a table with an invalid timestamp column type
        Spi::run(
            "CREATE TABLE test_ts_invalid (
                id BIGSERIAL PRIMARY KEY,
                data TEXT,
                name TEXT
            )",
        )
        .expect("Failed to create table");

        // Test: TEXT column should fail for timestamp
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "ts-invalid-topic",
                "public.test_ts_invalid",
                "id",
                Some("data"),
                None,
                None,
                None,
                Some("name"), // TEXT type, not valid for timestamp
            )
        });
        assert!(
            result.is_err(),
            "Should fail when timestamp column is not a temporal type"
        );

        // Clean up
        Spi::run("DROP TABLE test_ts_invalid").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_timestamp_column_not_found() {
        // Create a table without the specified timestamp column
        Spi::run(
            "CREATE TABLE test_ts_missing (
                id BIGSERIAL PRIMARY KEY,
                data TEXT
            )",
        )
        .expect("Failed to create table");

        // Test: Non-existent column should fail
        let result = std::panic::catch_unwind(|| {
            crate::create_topic_from_table(
                "ts-missing-topic",
                "public.test_ts_missing",
                "id",
                Some("data"),
                None,
                None,
                None,
                Some("created_at"), // Column doesn't exist
            )
        });
        assert!(
            result.is_err(),
            "Should fail when timestamp column does not exist"
        );

        // Clean up
        Spi::run("DROP TABLE test_ts_missing").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_enable_topic_writes_stream_mode() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_write_stream (
                id BIGSERIAL PRIMARY KEY,
                data TEXT
            )",
        )
        .expect("Failed to create table");

        // Create topic from table
        let topic_id = crate::create_topic_from_table(
            "write-stream-topic",
            "public.test_write_stream",
            "id",
            Some("data"),
            None,
            None,
            None,
            None,
        );
        assert!(topic_id > 0, "Failed to create topic from table");

        // Enable stream mode writes
        let result = crate::enable_topic_writes("write-stream-topic", "stream", None, None);
        assert!(result, "Should enable writes");

        // Verify settings
        let writable = Spi::get_one::<bool>(
            "SELECT writable FROM pgkafka.topics WHERE name = 'write-stream-topic'",
        );
        assert_eq!(writable.unwrap(), Some(true));

        let write_mode = Spi::get_one::<String>(
            "SELECT write_mode FROM pgkafka.topics WHERE name = 'write-stream-topic'",
        );
        assert_eq!(write_mode.unwrap(), Some("stream".to_string()));

        // Clean up
        crate::drop_topic("write-stream-topic", true);
        Spi::run("DROP TABLE test_write_stream").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_enable_topic_writes_table_mode() {
        // Create a source table with key and offset columns
        Spi::run(
            "CREATE TABLE test_write_table (
                customer_id TEXT PRIMARY KEY,
                name TEXT,
                kafka_offset BIGINT
            )",
        )
        .expect("Failed to create table");

        // Add a unique constraint on a serial column for offset
        Spi::run("ALTER TABLE test_write_table ADD COLUMN id BIGSERIAL UNIQUE")
            .expect("Failed to add id column");

        // Create topic from table using id as offset
        let topic_id = crate::create_topic_from_table(
            "write-table-topic",
            "public.test_write_table",
            "id",
            Some("name"),
            None,
            Some("customer_id"),
            None,
            None,
        );
        assert!(topic_id > 0, "Failed to create topic from table");

        // Enable table mode writes
        let result = crate::enable_topic_writes(
            "write-table-topic",
            "table",
            Some("customer_id"),
            Some("kafka_offset"),
        );
        assert!(result, "Should enable writes");

        // Verify settings
        let writable = Spi::get_one::<bool>(
            "SELECT writable FROM pgkafka.topics WHERE name = 'write-table-topic'",
        );
        assert_eq!(writable.unwrap(), Some(true));

        let write_mode = Spi::get_one::<String>(
            "SELECT write_mode FROM pgkafka.topics WHERE name = 'write-table-topic'",
        );
        assert_eq!(write_mode.unwrap(), Some("table".to_string()));

        let key_col = Spi::get_one::<String>(
            "SELECT write_key_column FROM pgkafka.topics WHERE name = 'write-table-topic'",
        );
        assert_eq!(key_col.unwrap(), Some("customer_id".to_string()));

        let offset_col = Spi::get_one::<String>(
            "SELECT kafka_offset_column FROM pgkafka.topics WHERE name = 'write-table-topic'",
        );
        assert_eq!(offset_col.unwrap(), Some("kafka_offset".to_string()));

        // Clean up
        crate::drop_topic("write-table-topic", true);
        Spi::run("DROP TABLE test_write_table").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_enable_topic_writes_table_mode_requires_key_column() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_table_mode_key (
                id BIGSERIAL PRIMARY KEY,
                data TEXT
            )",
        )
        .expect("Failed to create table");

        // Create topic from table
        crate::create_topic_from_table(
            "table-mode-key-topic",
            "public.test_table_mode_key",
            "id",
            Some("data"),
            None,
            None,
            None,
            None,
        );

        // Try to enable table mode without key_column - should fail
        let result = std::panic::catch_unwind(|| {
            crate::enable_topic_writes("table-mode-key-topic", "table", None, Some("kafka_offset"))
        });
        assert!(
            result.is_err(),
            "Should fail when key_column is not provided for table mode"
        );

        // Clean up
        crate::drop_topic("table-mode-key-topic", true);
        Spi::run("DROP TABLE test_table_mode_key").expect("Failed to drop table");
    }

    #[pg_test]
    fn test_enable_topic_writes_native_topic_fails() {
        // Create a native topic (not source-backed)
        let topic_id = crate::create_topic(
            "native-write-topic",
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        assert!(topic_id > 0, "Failed to create native topic");

        // Try to enable writes on native topic - should fail
        let result = std::panic::catch_unwind(|| {
            crate::enable_topic_writes("native-write-topic", "stream", None, None)
        });
        assert!(
            result.is_err(),
            "Should fail to enable writes on native topic"
        );

        // Clean up
        crate::drop_topic("native-write-topic", true);
    }

    #[pg_test]
    fn test_disable_topic_writes() {
        // Create a source table
        Spi::run(
            "CREATE TABLE test_disable_write (
                id BIGSERIAL PRIMARY KEY,
                data TEXT
            )",
        )
        .expect("Failed to create table");

        // Create topic and enable writes
        crate::create_topic_from_table(
            "disable-write-topic",
            "public.test_disable_write",
            "id",
            Some("data"),
            None,
            None,
            None,
            None,
        );
        crate::enable_topic_writes("disable-write-topic", "stream", None, None);

        // Verify writes are enabled
        let writable = Spi::get_one::<bool>(
            "SELECT writable FROM pgkafka.topics WHERE name = 'disable-write-topic'",
        );
        assert_eq!(writable.unwrap(), Some(true));

        // Disable writes
        let result = crate::disable_topic_writes("disable-write-topic");
        assert!(result, "Should disable writes");

        // Verify settings are cleared
        let writable = Spi::get_one::<bool>(
            "SELECT writable FROM pgkafka.topics WHERE name = 'disable-write-topic'",
        );
        assert_eq!(writable.unwrap(), Some(false));

        let write_mode = Spi::get_one::<String>(
            "SELECT write_mode FROM pgkafka.topics WHERE name = 'disable-write-topic'",
        );
        assert!(write_mode.unwrap().is_none());

        // Clean up
        crate::drop_topic("disable-write-topic", true);
        Spi::run("DROP TABLE test_disable_write").expect("Failed to drop table");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Setup code for tests
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
