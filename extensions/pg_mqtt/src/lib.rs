//! pg_mqtt - MQTT 5.0 broker backed by PostgreSQL tables
//!
//! This extension implements an MQTT 5.0 broker that stores messages in PostgreSQL
//! tables. Standard MQTT clients can connect and publish/subscribe to topics.
//!
//! # Architecture
//!
//! ```text
//! MQTT Clients (Paho, Mosquitto, etc.)
//!         | TCP Protocol (port 1883)
//!         v
//! +-------------------------------------+
//! | pg_mqtt Extension (Rust + pgrx)    |
//! +-------------------------------------+
//! | TCP Server Layer (tokio)           |
//! +-------------------------------------+
//! | Protocol Handler (MQTT 5.0)        |
//! +-------------------------------------+
//! | Storage Layer (SPI Bridge)         |
//! +-------------------------------------+
//! | PostgreSQL Tables (pgmqtt schema)  |
//! +-------------------------------------+
//! ```

use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::time::Duration;

pub mod protocol;
pub mod schema;
pub mod server;
pub mod storage;
pub mod validation;

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

// GUC settings
static PG_MQTT_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);
static PG_MQTT_PORT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1883);
static PG_MQTT_WORKER_COUNT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(4);
static PG_MQTT_DATABASE: pgrx::GucSetting<Option<std::ffi::CString>> =
    pgrx::GucSetting::<Option<std::ffi::CString>>::new(None);

// Default host for binding
const DEFAULT_HOST: &str = "0.0.0.0";
const DEFAULT_DATABASE: &str = "postgres";

extension_sql!(
    r#"
    -- Messages table (one row per published message)
    CREATE TABLE IF NOT EXISTS messages (
        message_id BIGSERIAL PRIMARY KEY,
        topic TEXT NOT NULL,
        payload BYTEA,
        qos SMALLINT DEFAULT 0,
        retain BOOLEAN DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Index for topic lookups
    CREATE INDEX IF NOT EXISTS idx_messages_topic ON messages(topic);
    CREATE INDEX IF NOT EXISTS idx_messages_created_at ON messages(created_at);

    -- Subscriptions table
    CREATE TABLE IF NOT EXISTS subscriptions (
        subscription_id BIGSERIAL PRIMARY KEY,
        client_id TEXT NOT NULL,
        topic_filter TEXT NOT NULL,
        qos SMALLINT DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(client_id, topic_filter)
    );

    -- Index for subscription lookups
    CREATE INDEX IF NOT EXISTS idx_subscriptions_client_id ON subscriptions(client_id);

    -- Client sessions table
    CREATE TABLE IF NOT EXISTS sessions (
        client_id TEXT PRIMARY KEY,
        connected_at TIMESTAMPTZ DEFAULT NOW(),
        last_seen TIMESTAMPTZ DEFAULT NOW()
    );

    -- Retained messages table (one per topic)
    CREATE TABLE IF NOT EXISTS retained (
        topic TEXT PRIMARY KEY,
        payload BYTEA,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Schema registry (for typed topics)
    CREATE TABLE IF NOT EXISTS schemas (
        id SERIAL PRIMARY KEY,
        subject TEXT NOT NULL,
        version INT NOT NULL DEFAULT 1,
        schema_def JSONB NOT NULL,
        fingerprint TEXT NOT NULL,
        description TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE(subject, version),
        UNIQUE(subject, fingerprint)
    );
    CREATE INDEX IF NOT EXISTS idx_schemas_subject ON schemas(subject);

    -- Schema binding + optional typed table backing
    CREATE TABLE IF NOT EXISTS topic_schemas (
        id SERIAL PRIMARY KEY,
        topic TEXT NOT NULL UNIQUE,
        schema_id INT NOT NULL REFERENCES schemas(id) ON DELETE CASCADE,
        source_table TEXT,
        validation_mode TEXT NOT NULL DEFAULT 'STRICT'
            CHECK (validation_mode IN ('STRICT', 'LOG')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS idx_topic_schemas_topic ON topic_schemas(topic);
    "#,
    name = "bootstrap",
    bootstrap
);

/// Initialize the extension and register background workers
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    // Register GUC settings
    pgrx::GucRegistry::define_bool_guc(
        c"pg_mqtt.enabled",
        c"Enable the MQTT broker",
        c"When true, the MQTT broker background workers will start on server startup.",
        &PG_MQTT_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_mqtt.port",
        c"MQTT broker port",
        c"The TCP port on which the MQTT broker listens for connections.",
        &PG_MQTT_PORT,
        1,
        65535,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_mqtt.worker_count",
        c"Number of MQTT workers",
        c"The number of background workers handling MQTT connections.",
        &PG_MQTT_WORKER_COUNT,
        1,
        32,
        pgrx::GucContext::Sighup, // Use Sighup so extension can be loaded dynamically for tests
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_mqtt.database",
        c"Database for pg_mqtt to connect to",
        c"The database where pg_mqtt extension is installed. Defaults to 'postgres'.",
        &PG_MQTT_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    // Register N background workers
    // NOTE: Always register workers here, check enabled flag inside worker_main.
    // This matches pg_kafka's pattern and avoids issues with shared memory cleanup
    // when workers are conditionally registered.
    let worker_count = PG_MQTT_WORKER_COUNT.get();
    pgrx::log!("pg_mqtt: registering {} background worker(s)", worker_count);

    for i in 0..worker_count {
        BackgroundWorkerBuilder::new(&format!("pg_mqtt worker {}", i))
            .set_function("pg_mqtt_worker_main")
            .set_library("pg_mqtt")
            .set_argument(Some(pg_sys::Datum::from(i as i64)))
            .enable_shmem_access(None)
            .enable_spi_access()
            .set_start_time(BgWorkerStartTime::RecoveryFinished)
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }
}

/// Background worker entry point
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_mqtt_worker_main(arg: pg_sys::Datum) {
    let worker_id = arg.value() as i32;

    // Set up signal handlers for SIGHUP and SIGTERM
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Connect to the database for SPI access
    let db_setting = PG_MQTT_DATABASE.get();
    let database = db_setting
        .as_ref()
        .and_then(|s| s.to_str().ok())
        .unwrap_or(DEFAULT_DATABASE);
    BackgroundWorker::connect_worker_to_spi(Some(database), None);

    pgrx::log!(
        "pg_mqtt worker {}: started, pid={}",
        worker_id,
        std::process::id()
    );

    // Check if enabled — workers always get registered but exit early if disabled
    if !PG_MQTT_ENABLED.get() {
        pgrx::log!(
            "pg_mqtt worker {}: disabled via pg_mqtt.enabled=false",
            worker_id
        );
        return;
    }

    // Get configuration
    let port = PG_MQTT_PORT.get() as u16;

    pgrx::log!(
        "pg_mqtt worker {}: starting TCP server on {}:{}",
        worker_id,
        DEFAULT_HOST,
        port
    );

    // Run server with SPI bridge polling loop
    if let Err(e) = crate::server::run_server(DEFAULT_HOST, port, worker_id) {
        pgrx::log!("pg_mqtt worker {}: server error: {}", worker_id, e);
    }

    pgrx::log!("pg_mqtt worker {}: shutting down", worker_id);
}

/// SQL function to check if the MQTT broker is running
#[pg_extern]
fn mqtt_status() -> String {
    let enabled = PG_MQTT_ENABLED.get();
    let port = PG_MQTT_PORT.get();
    let workers = PG_MQTT_WORKER_COUNT.get();

    if enabled {
        format!(
            "MQTT broker: running on {}:{} with {} worker(s)",
            DEFAULT_HOST, port, workers
        )
    } else {
        "MQTT broker: disabled".to_string()
    }
}

/// SQL function to publish a message to a topic
///
/// If a schema is bound to the topic, the payload is validated against it.
/// If the topic has a backing table (typed topic), the payload is decomposed
/// into the table's columns instead of being stored in pgmqtt.messages.
#[pg_extern]
fn mqtt_publish(topic: &str, payload: &str) -> i64 {
    // Check if a schema is bound to this topic
    let binding: Result<Option<(String, Option<String>, String)>, _> = Spi::connect(|client| {
        let result = client.select(
            r#"SELECT s.schema_def::text, ts.source_table, ts.validation_mode
               FROM pgmqtt.topic_schemas ts
               JOIN pgmqtt.schemas s ON ts.schema_id = s.id
               WHERE ts.topic = $1"#,
            None,
            &[topic.into()],
        )?;

        let row = match result.into_iter().next() {
            Some(r) => r,
            None => return Ok::<_, spi::Error>(None),
        };

        let schema_text: Option<String> = row.get(1)?;
        let source_table: Option<String> = row.get(2)?;
        let mode: Option<String> = row.get(3)?;

        Ok(Some((
            schema_text.unwrap_or_default(),
            source_table,
            mode.unwrap_or_else(|| "STRICT".to_string()),
        )))
    });

    if let Ok(Some((schema_text, source_table, validation_mode))) = binding {
        // Parse schema
        let schema_def: serde_json::Value = serde_json::from_str(&schema_text)
            .unwrap_or_else(|e| pgrx::error!("Invalid schema definition: {}", e));

        // Parse payload as JSON
        let payload_json: serde_json::Value = match serde_json::from_str(payload) {
            Ok(v) => v,
            Err(e) => {
                let msg = format!("Payload is not valid JSON: {}", e);
                if validation_mode == "STRICT" {
                    pgrx::error!("{}", msg);
                } else {
                    pgrx::warning!("{}", msg);
                }
                return insert_to_messages(topic, payload);
            }
        };

        // Validate against schema
        match jsonschema::validator_for(&schema_def) {
            Ok(validator) => {
                if !validator.is_valid(&payload_json) {
                    let errors: Vec<String> = validator
                        .iter_errors(&payload_json)
                        .map(|e| format!("{}", e))
                        .collect();
                    let msg = format!(
                        "Payload does not conform to schema for topic '{}': {}",
                        topic,
                        errors.join("; ")
                    );
                    if validation_mode == "STRICT" {
                        pgrx::error!("{}", msg);
                    } else {
                        pgrx::warning!("{}", msg);
                    }
                }
            }
            Err(e) => {
                pgrx::warning!("Failed to compile schema: {}", e);
            }
        }

        // If typed topic (has source_table), decompose into backing table
        if let Some(ref table) = source_table {
            return insert_to_source_table(table, &payload_json);
        }
    }

    // Default: insert into pgmqtt.messages
    insert_to_messages(topic, payload)
}

/// Insert a message into pgmqtt.messages (untyped path)
fn insert_to_messages(topic: &str, payload: &str) -> i64 {
    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgmqtt.messages (topic, payload, qos, retain)
         VALUES ($1, $2::bytea, 0, false)
         RETURNING message_id",
        &[topic.into(), payload.into()],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to publish message: no message_id returned"),
        Err(e) => pgrx::error!("Failed to publish message: {}", e),
    }
}

/// Insert a JSON payload into a typed topic's backing table.
///
/// Uses json_populate_record to decompose JSON into table columns automatically.
/// Excludes the `id` column so the BIGSERIAL default generates it.
fn insert_to_source_table(source_table: &str, json_value: &serde_json::Value) -> i64 {
    if !json_value.is_object() {
        pgrx::error!("Payload must be a JSON object for typed topics");
    }

    let json_text = serde_json::to_string(json_value).unwrap_or_default();

    // Get non-id column names so the BIGSERIAL id gets its default value
    let columns: Vec<String> = Spi::connect(|client| {
        let mut cols = Vec::new();
        let table = client.select(
            &format!(
                "SELECT column_name::text FROM information_schema.columns
                 WHERE table_schema || '.' || table_name = '{}'
                   AND column_name != 'id'
                 ORDER BY ordinal_position",
                source_table.replace('\'', "''")
            ),
            None,
            &[],
        )?;
        for row in table {
            if let Ok(Some(col)) = row.get::<String>(1) {
                cols.push(col);
            }
        }
        Ok::<_, spi::Error>(cols)
    })
    .unwrap_or_default();

    if columns.is_empty() {
        pgrx::error!(
            "Could not find columns for typed topic table '{}'",
            source_table
        );
    }

    let col_list = columns.join(", ");
    let query = format!(
        "INSERT INTO {} ({}) SELECT {} FROM json_populate_record(null::{}, $1::json) RETURNING id",
        source_table, col_list, col_list, source_table
    );

    let result = Spi::get_one_with_args::<i64>(&query, &[json_text.into()]);

    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("Failed to insert into typed topic table: no id returned"),
        Err(e) => pgrx::error!("Failed to insert into typed topic table: {}", e),
    }
}

/// SQL function to get recent messages from a topic
#[pg_extern]
fn mqtt_messages(
    topic_pattern: &str,
    limit: default!(i32, 10),
) -> TableIterator<
    'static,
    (
        name!(message_id, i64),
        name!(topic, String),
        name!(payload, String),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let mut results = Vec::new();

        let table = client.select(
            &format!(
                "SELECT message_id, topic, encode(payload, 'escape')::text, created_at
                 FROM pgmqtt.messages
                 WHERE topic LIKE '{}'
                 ORDER BY created_at DESC
                 LIMIT {}",
                topic_pattern.replace('\'', "''"),
                limit
            ),
            None,
            &[],
        )?;

        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let topic: String = row.get(2)?.unwrap_or_default();
            let payload: String = row.get(3)?.unwrap_or_default();
            let created_at: pgrx::datum::TimestampWithTimeZone = row.get(4)?.unwrap_or_else(|| {
                pgrx::datum::TimestampWithTimeZone::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as i64,
                )
                .unwrap()
            });

            results.push((id, topic, payload, created_at));
        }

        Ok::<_, spi::Error>(results)
    })
    .unwrap_or_default();

    TableIterator::new(rows)
}

/// SQL function to count subscriptions
#[pg_extern]
fn mqtt_subscription_count() -> i64 {
    Spi::get_one("SELECT COUNT(*)::bigint FROM pgmqtt.subscriptions")
        .unwrap_or(Some(0))
        .unwrap_or(0)
}

/// SQL function to list active sessions
#[pg_extern]
fn mqtt_sessions() -> TableIterator<
    'static,
    (
        name!(client_id, String),
        name!(connected_at, pgrx::datum::TimestampWithTimeZone),
        name!(last_seen, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    let rows: Vec<_> = Spi::connect(|client| {
        let mut results = Vec::new();

        let table = client.select(
            "SELECT client_id, connected_at, last_seen FROM pgmqtt.sessions ORDER BY connected_at DESC",
            None,
            &[],
        )?;

        for row in table {
            let client_id: String = row.get(1)?.unwrap_or_default();
            let connected_at: pgrx::datum::TimestampWithTimeZone = row.get(2)?.unwrap_or_else(|| {
                pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap()
            });
            let last_seen: pgrx::datum::TimestampWithTimeZone = row.get(3)?.unwrap_or_else(|| {
                pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap()
            });

            results.push((client_id, connected_at, last_seen));
        }

        Ok::<_, spi::Error>(results)
    })
    .unwrap_or_default();

    TableIterator::new(rows)
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_mqtt_status() {
        let status = crate::mqtt_status();
        assert!(status.contains("MQTT broker"));
    }

    #[pg_test]
    fn test_mqtt_publish() {
        // Create schema and tables first
        Spi::run("CREATE SCHEMA IF NOT EXISTS pgmqtt").unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgmqtt.messages (
                message_id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                payload BYTEA,
                qos SMALLINT DEFAULT 0,
                retain BOOLEAN DEFAULT false,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )",
        )
        .unwrap();

        let id = crate::mqtt_publish("test/topic", "hello world");
        assert!(id > 0);
    }

    #[pg_test]
    fn test_topic_wildcard_matching() {
        use crate::storage::spi_client::topic_matches;

        // Exact match
        assert!(topic_matches("sensors/room1/temp", "sensors/room1/temp"));
        assert!(!topic_matches("sensors/room1/temp", "sensors/room2/temp"));

        // Single-level wildcard
        assert!(topic_matches("sensors/+/temp", "sensors/room1/temp"));
        assert!(topic_matches("sensors/+/temp", "sensors/room2/temp"));
        assert!(!topic_matches("sensors/+/temp", "sensors/room1/humidity"));

        // Multi-level wildcard
        assert!(topic_matches("sensors/#", "sensors"));
        assert!(topic_matches("sensors/#", "sensors/room1"));
        assert!(topic_matches("sensors/#", "sensors/room1/temp"));
    }

    /// Helper: bootstrap schema tables for tests
    fn bootstrap_schema_tables() {
        Spi::run("CREATE SCHEMA IF NOT EXISTS pgmqtt").unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgmqtt.messages (
                message_id BIGSERIAL PRIMARY KEY,
                topic TEXT NOT NULL,
                payload BYTEA,
                qos SMALLINT DEFAULT 0,
                retain BOOLEAN DEFAULT false,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgmqtt.schemas (
                id SERIAL PRIMARY KEY,
                subject TEXT NOT NULL,
                version INT NOT NULL DEFAULT 1,
                schema_def JSONB NOT NULL,
                fingerprint TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                UNIQUE(subject, version),
                UNIQUE(subject, fingerprint)
            )",
        )
        .unwrap();
        Spi::run(
            "CREATE TABLE IF NOT EXISTS pgmqtt.topic_schemas (
                id SERIAL PRIMARY KEY,
                topic TEXT NOT NULL UNIQUE,
                schema_id INT NOT NULL REFERENCES pgmqtt.schemas(id) ON DELETE CASCADE,
                source_table TEXT,
                validation_mode TEXT NOT NULL DEFAULT 'STRICT'
                    CHECK (validation_mode IN ('STRICT', 'LOG')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .unwrap();
    }

    #[pg_test]
    fn test_register_and_get_schema() {
        bootstrap_schema_tables();

        let schema_def =
            r#"{"type":"object","properties":{"temp":{"type":"number"}},"required":["temp"]}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "test-sensor",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        assert!(schema_id > 0);

        // Retrieve by ID
        let retrieved = crate::schema::mqtt_get_schema(schema_id);
        assert!(retrieved.is_some());

        // Retrieve latest by subject
        let latest = crate::schema::mqtt_get_latest_schema("test-sensor");
        assert!(latest.is_some());
    }

    #[pg_test]
    fn test_register_schema_idempotent() {
        bootstrap_schema_tables();

        let schema_def = r#"{"type":"object","properties":{"val":{"type":"integer"}}}"#;
        let id1 = crate::schema::mqtt_register_schema(
            "idempotent-test",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        // Same schema + same subject = same version returned
        let id2 = crate::schema::mqtt_register_schema(
            "idempotent-test",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        assert_eq!(id1, id2);
    }

    #[pg_test]
    fn test_bind_and_unbind_schema() {
        bootstrap_schema_tables();

        let schema_def = r#"{"type":"object","properties":{"temp":{"type":"number"}}}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "bind-test",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );

        let binding_id = crate::schema::mqtt_bind_schema("sensors/bind-test", schema_id, "STRICT");
        assert!(binding_id > 0);

        // Verify we can get topic schema
        let topic_schema = crate::schema::mqtt_get_topic_schema("sensors/bind-test");
        assert!(topic_schema.is_some());

        // Unbind
        let unbound = crate::schema::mqtt_unbind_schema("sensors/bind-test");
        assert!(unbound);

        // Verify it's gone
        let topic_schema = crate::schema::mqtt_get_topic_schema("sensors/bind-test");
        assert!(topic_schema.is_none());
    }

    #[pg_test]
    fn test_publish_no_schema_any_payload() {
        bootstrap_schema_tables();

        // Without any schema bound, any payload should succeed (backward compat)
        let id = crate::mqtt_publish("untyped/topic", "any payload at all");
        assert!(id > 0);
    }

    #[pg_test]
    fn test_publish_valid_payload_strict() {
        bootstrap_schema_tables();

        let schema_def =
            r#"{"type":"object","properties":{"temp":{"type":"number"}},"required":["temp"]}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "strict-valid",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        crate::schema::mqtt_bind_schema("sensors/strict-valid", schema_id, "STRICT");

        // Valid payload should succeed
        let id = crate::mqtt_publish("sensors/strict-valid", r#"{"temp": 22.5}"#);
        assert!(id > 0);
    }

    #[pg_test]
    #[should_panic(expected = "does not conform to schema")]
    fn test_publish_invalid_payload_strict() {
        bootstrap_schema_tables();

        let schema_def =
            r#"{"type":"object","properties":{"temp":{"type":"number"}},"required":["temp"]}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "strict-invalid",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        crate::schema::mqtt_bind_schema("sensors/strict-invalid", schema_id, "STRICT");

        // Missing required field "temp" — should panic in STRICT mode
        crate::mqtt_publish("sensors/strict-invalid", r#"{"humidity": 50}"#);
    }

    #[pg_test]
    fn test_publish_invalid_payload_log_mode() {
        bootstrap_schema_tables();

        let schema_def =
            r#"{"type":"object","properties":{"temp":{"type":"number"}},"required":["temp"]}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "log-mode",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        crate::schema::mqtt_bind_schema("sensors/log-mode", schema_id, "LOG");

        // Invalid payload in LOG mode should succeed (with warning)
        let id = crate::mqtt_publish("sensors/log-mode", r#"{"humidity": 50}"#);
        assert!(id > 0);
    }

    #[pg_test]
    fn test_create_typed_topic() {
        bootstrap_schema_tables();

        let schema_def = r#"{"type":"object","properties":{"temp":{"type":"number"},"unit":{"type":"string"}},"required":["temp"]}"#;
        let schema_id = crate::schema::mqtt_create_typed_topic(
            "sensors/typed-test",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
            "STRICT",
        );
        assert!(schema_id > 0);

        // Verify backing table was created
        let table_exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables
             WHERE table_schema = 'pgmqtt' AND table_name = 'sensors_typed_test')",
        )
        .unwrap()
        .unwrap_or(false);
        assert!(table_exists, "backing table should exist");

        // Publish to typed topic — should insert into backing table
        let id = crate::mqtt_publish("sensors/typed-test", r#"{"temp": 22.5, "unit": "C"}"#);
        assert!(id > 0);

        // Verify data in backing table
        let temp =
            Spi::get_one::<f64>("SELECT temp::float8 FROM pgmqtt.sensors_typed_test WHERE id = 1")
                .unwrap();
        assert!(temp.is_some());
    }

    #[pg_test]
    fn test_create_typed_topic_from_table() {
        bootstrap_schema_tables();

        // Create a table first
        Spi::run(
            "CREATE TABLE pgmqtt.readings_test (
                id BIGSERIAL PRIMARY KEY,
                sensor TEXT NOT NULL,
                value NUMERIC NOT NULL
            )",
        )
        .unwrap();

        let schema_id = crate::schema::mqtt_create_typed_topic_from_table(
            "readings/test",
            "pgmqtt.readings_test",
            "STRICT",
        );
        assert!(schema_id > 0);

        // Publish to typed topic
        let id = crate::mqtt_publish("readings/test", r#"{"sensor": "DHT22", "value": 23.1}"#);
        assert!(id > 0);

        // Verify in backing table
        let sensor =
            Spi::get_one::<String>("SELECT sensor FROM pgmqtt.readings_test WHERE id = 1").unwrap();
        assert_eq!(sensor, Some("DHT22".to_string()));
    }

    #[pg_test]
    fn test_drop_schema_cascades() {
        bootstrap_schema_tables();

        let schema_def = r#"{"type":"object","properties":{"x":{"type":"number"}}}"#;
        let schema_id = crate::schema::mqtt_register_schema(
            "cascade-test",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            None,
        );
        crate::schema::mqtt_bind_schema("sensors/cascade", schema_id, "STRICT");

        // Verify binding exists
        assert!(crate::schema::mqtt_get_topic_schema("sensors/cascade").is_some());

        // Drop schema — should cascade to remove binding
        let dropped = crate::schema::mqtt_drop_schema(schema_id);
        assert!(dropped);

        // Binding should be gone
        assert!(crate::schema::mqtt_get_topic_schema("sensors/cascade").is_none());
    }

    #[pg_test]
    fn test_table_from_schema() {
        bootstrap_schema_tables();

        let schema_def = r#"{"type":"object","properties":{"name":{"type":"string"},"age":{"type":"integer"}},"required":["name"]}"#;
        let created = crate::schema::mqtt_table_from_schema(
            "pgmqtt.test_table_gen",
            pgrx::JsonB(serde_json::from_str(schema_def).unwrap()),
            true,
        );
        assert!(created);

        // Verify table was created with correct columns
        let col_count = Spi::get_one::<i64>(
            "SELECT COUNT(*)::bigint FROM information_schema.columns
             WHERE table_schema = 'pgmqtt' AND table_name = 'test_table_gen'",
        )
        .unwrap()
        .unwrap_or(0);
        // id + name + age = 3
        assert_eq!(col_count, 3);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // Setup code for tests
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // Don't use shared_preload_libraries in tests — background workers
        // bind TCP ports and interfere with the pgrx test lifecycle.
        // SQL functions are tested directly without the MQTT server.
        vec![]
    }
}
