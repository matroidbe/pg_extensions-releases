//! Integration tests for pg_streaming
//!
//! These tests exercise end-to-end pipeline behavior against a real PostgreSQL
//! instance with pg_kafka and pg_streaming installed.
//!
//! Prerequisites:
//!   - Run ./test.sh which installs extensions and starts PostgreSQL
//!   - pg_kafka + pg_streaming extensions loaded
//!   - Database "pg_streaming" exists with both extensions
//!
//! Run with: cargo test --tests -- --test-threads=1

mod common;

use common::*;
use std::time::Duration;

/// Processing timeout: how long to wait for background workers to process records
const PROCESSING_TIMEOUT: Duration = Duration::from_secs(15);

// =============================================================================
// Pipeline Lifecycle
// =============================================================================

#[test]
fn test_create_pipeline() {
    skip_if_not_running!();

    cleanup_pipeline("it_create_test");

    let id = query_one(
        "SELECT pgstreams.create_pipeline('it_create_test', '{
            \"input\": {\"kafka\": {\"topic\": \"orders\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap()
    .unwrap();
    assert!(id.parse::<i32>().unwrap() > 0, "pipeline ID should be > 0");

    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_create_test'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "created");

    cleanup_pipeline("it_create_test");
}

#[test]
fn test_start_stop_lifecycle() {
    skip_if_not_running!();

    cleanup_pipeline("it_lifecycle");
    ensure_topic("orders");

    execute(
        "SELECT pgstreams.create_pipeline('it_lifecycle', '{
            \"input\": {\"kafka\": {\"topic\": \"orders\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    // Start
    execute("SELECT pgstreams.start('it_lifecycle')").unwrap();
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_lifecycle'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running");

    // Stop
    execute("SELECT pgstreams.stop('it_lifecycle')").unwrap();
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_lifecycle'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "stopped");

    // Restart from stopped
    execute("SELECT pgstreams.start('it_lifecycle')").unwrap();
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_lifecycle'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running");

    cleanup_pipeline("it_lifecycle");
}

#[test]
fn test_restart_pipeline() {
    skip_if_not_running!();

    cleanup_pipeline("it_restart");
    ensure_topic("orders");

    execute(
        "SELECT pgstreams.create_pipeline('it_restart', '{
            \"input\": {\"kafka\": {\"topic\": \"orders\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_restart')").unwrap();
    execute("SELECT pgstreams.restart('it_restart')").unwrap();

    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_restart'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running");

    cleanup_pipeline("it_restart");
}

#[test]
fn test_drop_pipeline() {
    skip_if_not_running!();

    cleanup_pipeline("it_drop");

    execute(
        "SELECT pgstreams.create_pipeline('it_drop', '{
            \"input\": {\"kafka\": {\"topic\": \"orders\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.drop_pipeline('it_drop')").unwrap();

    let count =
        query_one("SELECT count(*)::bigint FROM pgstreams.pipelines WHERE name = 'it_drop'")
            .unwrap()
            .unwrap();
    assert_eq!(count, "0");
}

#[test]
fn test_pipeline_version_tracking() {
    skip_if_not_running!();

    cleanup_pipeline("it_versioned");

    execute(
        "SELECT pgstreams.create_pipeline('it_versioned', '{
            \"input\": {\"kafka\": {\"topic\": \"t\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    let version = query_one(
        "SELECT version FROM pgstreams.pipeline_versions pv
         JOIN pgstreams.pipelines p ON p.id = pv.pipeline_id
         WHERE p.name = 'it_versioned'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(version, "1");

    // Update should create version 2
    execute(
        "SELECT pgstreams.update_pipeline('it_versioned', '{
            \"input\": {\"kafka\": {\"topic\": \"t2\"}},
            \"pipeline\": {\"processors\": [{\"filter\": \"true\"}]},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .expect("update_pipeline should succeed");

    let max_version = query_one(
        "SELECT max(version) FROM pgstreams.pipeline_versions pv
         JOIN pgstreams.pipelines p ON p.id = pv.pipeline_id
         WHERE p.name = 'it_versioned'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(max_version, "2");

    cleanup_pipeline("it_versioned");
}

#[test]
fn test_start_from_failed_state() {
    skip_if_not_running!();

    cleanup_pipeline("it_failed");
    ensure_topic("t");

    execute(
        "SELECT pgstreams.create_pipeline('it_failed', '{
            \"input\": {\"kafka\": {\"topic\": \"t\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    // Manually set to failed
    execute(
        "UPDATE pgstreams.pipelines SET state = 'failed', error = 'test error'
         WHERE name = 'it_failed'",
    )
    .unwrap();

    // Should recover from failed state
    execute("SELECT pgstreams.start('it_failed')").unwrap();
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_failed'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running");

    // Error should be cleared
    let error =
        query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_failed'").unwrap();
    assert!(error.is_none(), "error should be cleared after start");

    cleanup_pipeline("it_failed");
}

// =============================================================================
// Observability
// =============================================================================

#[test]
fn test_status_function() {
    skip_if_not_running!();

    cleanup_pipeline("it_status");

    execute(
        "SELECT pgstreams.create_pipeline('it_status', '{
            \"input\": {\"kafka\": {\"topic\": \"t\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();

    let rows = query_all(
        "SELECT name::text, state::text FROM pgstreams.status() WHERE name = 'it_status'",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "it_status");
    assert_eq!(rows[0][1], "created");

    cleanup_pipeline("it_status");
}

#[test]
fn test_errors_function() {
    skip_if_not_running!();

    // Insert a test error directly
    execute(
        "INSERT INTO pgstreams.error_log (pipeline, processor, error, record)
         VALUES ('it_error_pipe', 'filter', 'bad expression', '{\"key\": 1}'::jsonb)",
    )
    .unwrap();

    let rows =
        query_all("SELECT pipeline::text, error::text FROM pgstreams.errors('it_error_pipe', 10)")
            .unwrap();
    assert!(!rows.is_empty());
    assert_eq!(rows[0][0], "it_error_pipe");
    assert_eq!(rows[0][1], "bad expression");

    // Cleanup
    let _ = execute("DELETE FROM pgstreams.error_log WHERE pipeline = 'it_error_pipe'");
}

#[test]
fn test_lag_function() {
    skip_if_not_running!();

    // lag() should return without error (other tests may have running pipelines)
    let result = query_one("SELECT count(*)::bigint FROM pgstreams.lag()");
    assert!(result.is_ok(), "lag() should not error");
    let count: i64 = result.unwrap().unwrap().parse().unwrap();
    assert!(count >= 0, "lag() count should be >= 0");
}

#[test]
fn test_trace_function() {
    skip_if_not_running!();

    // trace() should work for nonexistent pipeline
    let count = query_one("SELECT count(*)::bigint FROM pgstreams.trace('nonexistent')")
        .unwrap()
        .unwrap();
    assert_eq!(count.parse::<i64>().unwrap(), 0);
}

// =============================================================================
// Table-to-Table Pipeline: Filter + Mapping
// =============================================================================

#[test]
fn test_table_to_table_filter_and_map() {
    skip_if_not_running!();

    // Cleanup any leftover state
    cleanup_pipeline("it_t2t_filter");
    cleanup_table("it_raw_events");
    cleanup_table("it_processed_events");

    // Create source and target tables
    execute(
        "CREATE TABLE it_raw_events (
            id          BIGSERIAL PRIMARY KEY,
            event_type  TEXT NOT NULL,
            payload     JSONB NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .unwrap();

    execute(
        "CREATE TABLE it_processed_events (
            id           BIGSERIAL PRIMARY KEY,
            event_type   TEXT,
            user_id      TEXT,
            action       TEXT,
            processed_at TIMESTAMPTZ DEFAULT now()
        )",
    )
    .unwrap();

    // Create pipeline: filter out heartbeats, extract fields
    execute(
        "SELECT pgstreams.create_pipeline('it_t2t_filter', $$
        {
            \"input\": {
                \"table\": {
                    \"name\": \"public.it_raw_events\",
                    \"offset_column\": \"id\",
                    \"poll\": \"1s\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"filter\": \"event_type != 'heartbeat'\"},
                    {\"mapping\": {
                        \"event_type\": \"event_type\",
                        \"user_id\":    \"payload->>'user_id'\",
                        \"action\":     \"payload->>'action'\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_processed_events\",
                    \"mode\": \"append\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_t2t_filter')").unwrap();

    // Insert test data
    execute(
        "INSERT INTO it_raw_events (event_type, payload) VALUES
            ('click',     '{\"user_id\": \"u-1\", \"action\": \"view\"}'),
            ('heartbeat', '{\"status\": \"ok\"}'),
            ('signup',    '{\"user_id\": \"u-2\", \"action\": \"register\"}'),
            ('click',     '{\"user_id\": \"u-1\", \"action\": \"purchase\"}'),
            ('heartbeat', '{\"status\": \"ok\"}')",
    )
    .unwrap();

    // Wait for processing: 3 non-heartbeat events
    wait_for_row_count("it_processed_events", 3, PROCESSING_TIMEOUT)
        .expect("expected 3 processed events");

    // Verify correct number of rows
    let count = query_one("SELECT count(*)::bigint FROM it_processed_events")
        .unwrap()
        .unwrap();
    assert_eq!(count, "3", "should have 3 events (2 heartbeats filtered)");

    // Verify no heartbeat events passed through
    let heartbeats = query_one(
        "SELECT count(*)::bigint FROM it_processed_events WHERE event_type = 'heartbeat'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(heartbeats, "0", "heartbeats should be filtered out");

    // Verify field extraction
    let rows = query_all("SELECT event_type, user_id, action FROM it_processed_events ORDER BY id")
        .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][0], "click");
    assert_eq!(rows[0][1], "u-1");
    assert_eq!(rows[0][2], "view");
    assert_eq!(rows[1][0], "signup");
    assert_eq!(rows[1][1], "u-2");
    assert_eq!(rows[1][2], "register");
    assert_eq!(rows[2][0], "click");
    assert_eq!(rows[2][1], "u-1");
    assert_eq!(rows[2][2], "purchase");

    // Cleanup
    cleanup_pipeline("it_t2t_filter");
    cleanup_table("it_raw_events");
    cleanup_table("it_processed_events");
}

// =============================================================================
// Table-to-Table Pipeline: Upsert mode
// =============================================================================

#[test]
fn test_table_to_table_upsert() {
    skip_if_not_running!();

    cleanup_pipeline("it_t2t_upsert");
    cleanup_table("it_sensor_readings");
    cleanup_table("it_sensor_latest");

    // Create source table (sensor readings)
    execute(
        "CREATE TABLE it_sensor_readings (
            id          BIGSERIAL PRIMARY KEY,
            device_id   TEXT NOT NULL,
            temperature NUMERIC(5,2),
            humidity    NUMERIC(5,2),
            read_at     TIMESTAMPTZ NOT NULL DEFAULT now()
        )",
    )
    .unwrap();

    // Create target table (latest state per device)
    execute(
        "CREATE TABLE it_sensor_latest (
            device_id    TEXT PRIMARY KEY,
            temperature  NUMERIC(5,2),
            humidity     NUMERIC(5,2),
            last_reading TEXT
        )",
    )
    .unwrap();

    // Create pipeline: upsert latest reading per device
    execute(
        "SELECT pgstreams.create_pipeline('it_t2t_upsert', $$
        {
            \"input\": {
                \"table\": {
                    \"name\": \"public.it_sensor_readings\",
                    \"offset_column\": \"id\",
                    \"poll\": \"1s\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"mapping\": {
                        \"device_id\":    \"device_id\",
                        \"temperature\":  \"temperature\",
                        \"humidity\":     \"humidity\",
                        \"last_reading\": \"read_at::text\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_sensor_latest\",
                    \"mode\": \"upsert\",
                    \"key\": \"device_id\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_t2t_upsert')").unwrap();

    // Insert first batch of readings
    execute(
        "INSERT INTO it_sensor_readings (device_id, temperature, humidity, read_at) VALUES
            ('sensor-001', 22.5, 45.0, '2025-03-15 10:00:00+00'),
            ('sensor-002', 28.3, 60.2, '2025-03-15 10:00:00+00')",
    )
    .unwrap();

    // Wait for first batch
    wait_for_row_count("it_sensor_latest", 2, PROCESSING_TIMEOUT)
        .expect("expected 2 sensor records");

    // Insert updates for sensor-001
    execute(
        "INSERT INTO it_sensor_readings (device_id, temperature, humidity, read_at) VALUES
            ('sensor-001', 24.0, 43.0, '2025-03-15 10:10:00+00'),
            ('sensor-003', 18.0, 70.0, '2025-03-15 10:05:00+00')",
    )
    .unwrap();

    // Wait for 3 unique devices
    wait_for_row_count("it_sensor_latest", 3, PROCESSING_TIMEOUT)
        .expect("expected 3 sensor records");

    // sensor-001 should have the latest reading (24.0, not 22.5)
    let temp =
        query_one("SELECT temperature::text FROM it_sensor_latest WHERE device_id = 'sensor-001'")
            .unwrap()
            .unwrap();
    assert_eq!(temp, "24.00", "sensor-001 should have latest temperature");

    // Should be exactly 3 devices (upsert, not append)
    let count = query_one("SELECT count(*)::bigint FROM it_sensor_latest")
        .unwrap()
        .unwrap();
    assert_eq!(count, "3", "should have exactly 3 devices after upsert");

    // Cleanup
    cleanup_pipeline("it_t2t_upsert");
    cleanup_table("it_sensor_readings");
    cleanup_table("it_sensor_latest");
}

// =============================================================================
// Kafka-backed Pipeline: Filter + Mapping (typed topics)
// =============================================================================

#[test]
fn test_kafka_filter_and_map() {
    skip_if_not_running!();

    cleanup_pipeline("it_kafka_filter");
    cleanup_topic("it_orders");
    cleanup_topic("it_high_value_out");

    // Create typed topics
    execute(
        "SELECT pgkafka.create_typed_topic('it_orders', '{
            \"type\": \"object\",
            \"properties\": {
                \"order_id\":    {\"type\": \"string\"},
                \"customer_id\": {\"type\": \"integer\"},
                \"amount\":      {\"type\": \"number\"},
                \"region\":      {\"type\": \"string\"}
            },
            \"required\": [\"order_id\", \"customer_id\", \"amount\"]
        }'::jsonb)",
    )
    .unwrap();

    execute(
        "SELECT pgkafka.create_typed_topic('it_high_value_out', '{
            \"type\": \"object\",
            \"properties\": {
                \"order_id\":    {\"type\": \"string\"},
                \"customer_id\": {\"type\": \"integer\"},
                \"amount\":      {\"type\": \"number\"}
            }
        }'::jsonb)",
    )
    .unwrap();

    // Insert test data into the typed topic table
    execute(
        "INSERT INTO it_orders (order_id, customer_id, amount, region) VALUES
            ('ORD-001', 1,  750.00, 'US'),
            ('ORD-002', 2,  120.00, 'EU'),
            ('ORD-003', 3, 1200.00, 'EU'),
            ('ORD-004', 1,   49.99, 'US'),
            ('ORD-005', 4, 5000.00, 'APAC')",
    )
    .unwrap();

    // Create pipeline: filter high-value orders (> 500), map to output
    execute(
        "SELECT pgstreams.create_pipeline('it_kafka_filter', $$
        {
            \"input\": {
                \"kafka\": {
                    \"topic\": \"it_orders\",
                    \"group\": \"it-high-value-filter\",
                    \"start\": \"earliest\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"filter\": \"amount > 500\"},
                    {\"mapping\": {
                        \"order_id\":    \"order_id\",
                        \"customer_id\": \"customer_id\",
                        \"amount\":      \"amount\"
                    }}
                ]
            },
            \"output\": {
                \"kafka\": {
                    \"topic\": \"it_high_value_out\",
                    \"key\": \"order_id\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_kafka_filter')").unwrap();

    // Wait for output: 3 orders above 500 (ORD-001=750, ORD-003=1200, ORD-005=5000)
    wait_for_row_count("it_high_value_out", 3, PROCESSING_TIMEOUT)
        .expect("expected 3 high-value orders");

    // Verify only high-value orders passed through
    let low_count = query_one("SELECT count(*)::bigint FROM it_high_value_out WHERE amount <= 500")
        .unwrap()
        .unwrap();
    assert_eq!(low_count, "0", "no orders <= 500 should pass filter");

    let total = query_one("SELECT count(*)::bigint FROM it_high_value_out")
        .unwrap()
        .unwrap();
    assert_eq!(total, "3", "exactly 3 high-value orders expected");

    // Cleanup
    cleanup_pipeline("it_kafka_filter");
    cleanup_topic("it_orders");
    cleanup_topic("it_high_value_out");
}

// =============================================================================
// Kafka-backed Pipeline: SQL Enrichment
// =============================================================================

#[test]
fn test_kafka_sql_enrichment() {
    skip_if_not_running!();

    cleanup_pipeline("it_enrich");
    cleanup_topic("it_enrich_orders");
    cleanup_table("it_customers");
    cleanup_table("it_enriched_out");

    // Create reference table
    execute(
        "CREATE TABLE it_customers (
            customer_id INT PRIMARY KEY,
            name        TEXT NOT NULL,
            tier        TEXT NOT NULL DEFAULT 'standard'
        )",
    )
    .unwrap();

    execute(
        "INSERT INTO it_customers VALUES
            (1, 'Acme Corp',  'platinum'),
            (2, 'Globex Inc', 'gold'),
            (3, 'Initech',    'standard')",
    )
    .unwrap();

    // Create input typed topic
    execute(
        "SELECT pgkafka.create_typed_topic('it_enrich_orders', '{
            \"type\": \"object\",
            \"properties\": {
                \"order_id\":    {\"type\": \"string\"},
                \"customer_id\": {\"type\": \"integer\"},
                \"amount\":      {\"type\": \"number\"}
            },
            \"required\": [\"order_id\", \"customer_id\", \"amount\"]
        }'::jsonb)",
    )
    .unwrap();

    // Create output table
    execute(
        "CREATE TABLE it_enriched_out (
            id            BIGSERIAL PRIMARY KEY,
            order_id      TEXT,
            customer_id   INT,
            customer_name TEXT,
            customer_tier TEXT,
            amount        NUMERIC(10,2),
            enriched_at   TIMESTAMPTZ DEFAULT now()
        )",
    )
    .unwrap();

    // Insert orders: customer 99 doesn't exist, should be skipped
    execute(
        "INSERT INTO it_enrich_orders (order_id, customer_id, amount) VALUES
            ('ORD-100', 1,  350.00),
            ('ORD-101', 2,  199.00),
            ('ORD-102', 3,   89.97),
            ('ORD-103', 99, 500.00)",
    )
    .unwrap();

    // Create enrichment pipeline
    execute(
        "SELECT pgstreams.create_pipeline('it_enrich', $$
        {
            \"input\": {
                \"kafka\": {
                    \"topic\": \"it_enrich_orders\",
                    \"group\": \"it-enricher\",
                    \"start\": \"earliest\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {
                        \"sql\": {
                            \"query\": \"SELECT name, tier FROM it_customers WHERE customer_id = $1\",
                            \"args\": [\"_batch.customer_id\"],
                            \"result_map\": {
                                \"customer_name\": \"name\",
                                \"customer_tier\": \"tier\"
                            },
                            \"on_empty\": \"skip\"
                        }
                    },
                    {\"mapping\": {
                        \"order_id\":      \"order_id\",
                        \"customer_id\":   \"customer_id\",
                        \"customer_name\": \"_original->>'customer_name'\",
                        \"customer_tier\": \"_original->>'customer_tier'\",
                        \"amount\":        \"amount\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_enriched_out\",
                    \"mode\": \"append\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_enrich')").unwrap();

    // Wait: 3 enriched orders (customer 99 skipped)
    wait_for_row_count("it_enriched_out", 3, PROCESSING_TIMEOUT)
        .expect("expected 3 enriched orders");

    // Verify enrichment
    let rows = query_all(
        "SELECT order_id, customer_name, customer_tier
         FROM it_enriched_out ORDER BY order_id",
    )
    .unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], "Acme Corp");
    assert_eq!(rows[0][2], "platinum");
    assert_eq!(rows[1][1], "Globex Inc");
    assert_eq!(rows[1][2], "gold");
    assert_eq!(rows[2][1], "Initech");
    assert_eq!(rows[2][2], "standard");

    // Verify customer 99 was skipped
    let count = query_one("SELECT count(*)::bigint FROM it_enriched_out")
        .unwrap()
        .unwrap();
    assert_eq!(count, "3", "customer 99 should be skipped");

    // Cleanup
    cleanup_pipeline("it_enrich");
    cleanup_topic("it_enrich_orders");
    cleanup_table("it_customers");
    cleanup_table("it_enriched_out");
}

// =============================================================================
// Kafka-backed Pipeline: Unbounded Aggregation
// =============================================================================

#[test]
fn test_kafka_unbounded_aggregation() {
    skip_if_not_running!();

    cleanup_pipeline("it_agg_category");
    cleanup_topic("it_agg_orders");
    cleanup_table("it_category_dashboard");
    // Clean up aggregate state table from previous runs
    let _ = execute("DROP TABLE IF EXISTS pgstreams.it_agg_category CASCADE");

    // Create input typed topic
    execute(
        "SELECT pgkafka.create_typed_topic('it_agg_orders', '{
            \"type\": \"object\",
            \"properties\": {
                \"order_id\":  {\"type\": \"string\"},
                \"category\":  {\"type\": \"string\"},
                \"amount\":    {\"type\": \"number\"}
            },
            \"required\": [\"order_id\", \"category\", \"amount\"]
        }'::jsonb)",
    )
    .unwrap();

    // Create output dashboard table (columns match aggregate output: group_key + agg columns + updated_at)
    execute(
        "CREATE TABLE it_category_dashboard (
            group_key       TEXT PRIMARY KEY,
            total_revenue   NUMERIC,
            order_count     BIGINT,
            updated_at      TIMESTAMPTZ
        )",
    )
    .unwrap();

    // Insert test data
    execute(
        "INSERT INTO it_agg_orders (order_id, category, amount) VALUES
            ('ORD-001', 'hardware', 150.00),
            ('ORD-002', 'software', 299.00),
            ('ORD-003', 'hardware',  75.00),
            ('ORD-004', 'software', 499.00),
            ('ORD-005', 'hardware', 200.00)",
    )
    .unwrap();

    // Create aggregation pipeline
    execute(
        "SELECT pgstreams.create_pipeline('it_agg_category', $$
        {
            \"input\": {
                \"kafka\": {
                    \"topic\": \"it_agg_orders\",
                    \"group\": \"it-sales-agg\",
                    \"start\": \"earliest\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {
                        \"aggregate\": {
                            \"group_by\": \"category\",
                            \"columns\": {
                                \"total_revenue\": \"sum(amount)\",
                                \"order_count\":   \"count(*)\"
                            },
                            \"state_table\": \"it_agg_category\",
                            \"emit\": \"updated_rows\"
                        }
                    }
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_category_dashboard\",
                    \"mode\": \"upsert\",
                    \"key\": \"group_key\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_agg_category')").unwrap();

    // Wait for 2 categories in dashboard
    wait_for_row_count("it_category_dashboard", 2, PROCESSING_TIMEOUT)
        .expect("expected 2 categories in dashboard");

    // Verify aggregation
    let rows = query_all(
        "SELECT group_key, total_revenue::numeric::text, order_count::text
         FROM it_category_dashboard ORDER BY group_key",
    )
    .unwrap();

    assert_eq!(rows.len(), 2);

    // hardware: 150 + 75 + 200 = 425, 3 orders
    assert_eq!(rows[0][0], "hardware");
    let hw_rev: f64 = rows[0][1].parse().unwrap();
    assert!(
        (hw_rev - 425.0).abs() < 0.01,
        "hardware revenue: {}",
        hw_rev
    );
    assert_eq!(rows[0][2], "3");

    // software: 299 + 499 = 798, 2 orders
    assert_eq!(rows[1][0], "software");
    let sw_rev: f64 = rows[1][1].parse().unwrap();
    assert!(
        (sw_rev - 798.0).abs() < 0.01,
        "software revenue: {}",
        sw_rev
    );
    assert_eq!(rows[1][2], "2");

    // Cleanup
    cleanup_pipeline("it_agg_category");
    cleanup_topic("it_agg_orders");
    cleanup_table("it_category_dashboard");
    // State table created by aggregation
    let _ = execute("DROP TABLE IF EXISTS pgstreams.it_agg_category CASCADE");
}

// =============================================================================
// Bootstrap: Verify schema tables exist
// =============================================================================

#[test]
fn test_bootstrap_tables_exist() {
    skip_if_not_running!();

    let tables = [
        "pgstreams.pipelines",
        "pgstreams.pipeline_versions",
        "pgstreams.state_tables",
        "pgstreams.resources",
        "pgstreams.connector_offsets",
        "pgstreams.error_log",
        "pgstreams.late_events",
        "pgstreams.metrics",
    ];

    for table in &tables {
        let count = query_one(&format!("SELECT count(*)::bigint FROM {}", table));
        assert!(
            count.is_ok(),
            "bootstrap table {} should exist and be queryable",
            table
        );
    }
}

// =============================================================================
// Pipeline with multiple processors chained
// =============================================================================

#[test]
fn test_processor_chain() {
    skip_if_not_running!();

    cleanup_pipeline("it_chain");
    cleanup_table("it_chain_source");
    cleanup_table("it_chain_target");

    execute(
        "CREATE TABLE it_chain_source (
            id          BIGSERIAL PRIMARY KEY,
            event_type  TEXT NOT NULL,
            amount      NUMERIC(10,2) NOT NULL,
            region      TEXT NOT NULL
        )",
    )
    .unwrap();

    execute(
        "CREATE TABLE it_chain_target (
            id           BIGSERIAL PRIMARY KEY,
            event_type   TEXT,
            amount_usd   NUMERIC(10,2),
            region       TEXT
        )",
    )
    .unwrap();

    // Chain: filter (region = 'US') → mapping (rename amount to amount_usd)
    execute(
        "SELECT pgstreams.create_pipeline('it_chain', $$
        {
            \"input\": {
                \"table\": {
                    \"name\": \"public.it_chain_source\",
                    \"offset_column\": \"id\",
                    \"poll\": \"1s\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"filter\": \"region = 'US'\"},
                    {\"mapping\": {
                        \"event_type\": \"event_type\",
                        \"amount_usd\": \"amount\",
                        \"region\":     \"region\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_chain_target\",
                    \"mode\": \"append\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_chain')").unwrap();

    execute(
        "INSERT INTO it_chain_source (event_type, amount, region) VALUES
            ('sale',   100.00, 'US'),
            ('sale',   200.00, 'EU'),
            ('refund',  50.00, 'US'),
            ('sale',   300.00, 'APAC'),
            ('sale',   150.00, 'US')",
    )
    .unwrap();

    // Only 3 US events should pass
    wait_for_row_count("it_chain_target", 3, PROCESSING_TIMEOUT).expect("expected 3 US events");

    let count = query_one("SELECT count(*)::bigint FROM it_chain_target")
        .unwrap()
        .unwrap();
    assert_eq!(count, "3");

    // Verify all are US
    let non_us = query_one("SELECT count(*)::bigint FROM it_chain_target WHERE region != 'US'")
        .unwrap()
        .unwrap();
    assert_eq!(non_us, "0");

    // Cleanup
    cleanup_pipeline("it_chain");
    cleanup_table("it_chain_source");
    cleanup_table("it_chain_target");
}

// =============================================================================
// Pipeline incremental processing: new data after pipeline starts
// =============================================================================

#[test]
fn test_incremental_processing() {
    skip_if_not_running!();

    cleanup_pipeline("it_incremental");
    cleanup_table("it_incr_source");
    cleanup_table("it_incr_target");

    execute(
        "CREATE TABLE it_incr_source (
            id      BIGSERIAL PRIMARY KEY,
            message TEXT NOT NULL
        )",
    )
    .unwrap();

    execute(
        "CREATE TABLE it_incr_target (
            id      BIGSERIAL PRIMARY KEY,
            message TEXT
        )",
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_incremental', $$
        {
            \"input\": {
                \"table\": {
                    \"name\": \"public.it_incr_source\",
                    \"offset_column\": \"id\",
                    \"poll\": \"1s\"
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"mapping\": {
                        \"message\": \"message\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {
                    \"name\": \"public.it_incr_target\",
                    \"mode\": \"append\"
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_incremental')").unwrap();

    // First batch
    execute("INSERT INTO it_incr_source (message) VALUES ('batch1-a'), ('batch1-b')").unwrap();
    wait_for_row_count("it_incr_target", 2, PROCESSING_TIMEOUT)
        .expect("expected 2 rows after first batch");

    // Second batch (incremental — should pick up from where it left off)
    execute("INSERT INTO it_incr_source (message) VALUES ('batch2-a'), ('batch2-b'), ('batch2-c')")
        .unwrap();
    wait_for_row_count("it_incr_target", 5, PROCESSING_TIMEOUT)
        .expect("expected 5 rows after second batch");

    let count = query_one("SELECT count(*)::bigint FROM it_incr_target")
        .unwrap()
        .unwrap();
    assert_eq!(count, "5", "all 5 messages should be processed");

    // Cleanup
    cleanup_pipeline("it_incremental");
    cleanup_table("it_incr_source");
    cleanup_table("it_incr_target");
}

// =============================================================================
// Modbus Pipelines (in-process mock Modbus TCP server)
// ======================================================================
/// Modbus source → table sink: the pipeline polls the mock device and
/// lands decoded snapshots in a Postgres table.
#[test]
fn test_modbus_to_table_pipeline() {
    skip_if_not_running!();

    cleanup_pipeline("it_modbus_src");
    cleanup_table("it_modbus_readings");

    // Mock PLC: f32 21.5 (ABCD) at holding 100, coil 12 on.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let device = common::modbus_mock::MockDevice::default();
    device.set_holding(100, &[0x41AC, 0x0000]);
    device.set_coil(12, true);
    common::modbus_mock::spawn_on(&rt, device, 15502);

    execute(
        "CREATE TABLE it_modbus_readings (
            id          BIGSERIAL PRIMARY KEY,
            temperature DOUBLE PRECISION,
            running     BOOLEAN,
            ts          TEXT
        )",
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_modbus_src', $$
        {
            \"input\": {
                \"modbus\": {
                    \"host\": \"127.0.0.1\",
                    \"port\": 15502,
                    \"poll\": \"500ms\",
                    \"reads\": [
                        {\"name\": \"temperature\", \"kind\": \"holding\", \"address\": 100, \"data_type\": \"f32\"},
                        {\"name\": \"running\", \"kind\": \"coil\", \"address\": 12}
                    ]
                }
            },
            \"pipeline\": {
                \"processors\": [
                    {\"mapping\": {
                        \"temperature\": \"(value_json->>'temperature')::double precision\",
                        \"running\":     \"(value_json->>'running')::boolean\",
                        \"ts\":          \"value_json->>'ts'\"
                    }}
                ]
            },
            \"output\": {
                \"table\": {\"name\": \"public.it_modbus_readings\", \"mode\": \"append\"}
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_modbus_src')").unwrap();

    // At 500ms poll, several snapshots should land quickly.
    wait_for_row_count("it_modbus_readings", 2, PROCESSING_TIMEOUT)
        .expect("expected at least 2 modbus snapshots");

    let rows =
        query_all("SELECT temperature::text, running::text, ts FROM it_modbus_readings LIMIT 1")
            .unwrap();
    assert_eq!(rows[0][0], "21.5");
    assert_eq!(rows[0][1], "true");
    assert!(!rows[0][2].is_empty(), "ts should be set");

    cleanup_pipeline("it_modbus_src");
    cleanup_table("it_modbus_readings");
}

/// Table source → Modbus sink: rows inserted in Postgres become register
/// and coil writes on the mock device.
#[test]
fn test_table_to_modbus_pipeline() {
    skip_if_not_running!();

    cleanup_pipeline("it_modbus_sink");
    cleanup_table("it_plc_commands");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let device = common::modbus_mock::MockDevice::default();
    common::modbus_mock::spawn_on(&rt, device.clone(), 15503);

    execute(
        "CREATE TABLE it_plc_commands (
            id       BIGSERIAL PRIMARY KEY,
            setpoint DOUBLE PRECISION NOT NULL,
            enable   BOOLEAN NOT NULL
        )",
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_modbus_sink', $$
        {
            \"input\": {
                \"table\": {
                    \"name\": \"public.it_plc_commands\",
                    \"offset_column\": \"id\",
                    \"poll\": \"500ms\"
                }
            },
            \"pipeline\": {\"processors\": []},
            \"output\": {
                \"modbus\": {
                    \"host\": \"127.0.0.1\",
                    \"port\": 15503,
                    \"writes\": [
                        {\"field\": \"setpoint\", \"kind\": \"holding\", \"address\": 40, \"data_type\": \"f32\"},
                        {\"field\": \"enable\", \"kind\": \"coil\", \"address\": 12}
                    ]
                }
            }
        }
        $$::jsonb)",
    )
    .unwrap();

    execute("SELECT pgstreams.start('it_modbus_sink')").unwrap();
    execute("INSERT INTO it_plc_commands (setpoint, enable) VALUES (21.5, true)").unwrap();

    // Wait for the write to land on the device: f32 21.5 = 0x41AC0000.
    let start = std::time::Instant::now();
    while start.elapsed() < PROCESSING_TIMEOUT {
        if device.holding_at(40) == 0x41AC && device.holding_at(41) == 0x0000 && device.coil_at(12)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert_eq!(
        device.holding_at(40),
        0x41AC,
        "setpoint high word should be written"
    );
    assert_eq!(
        device.holding_at(41),
        0x0000,
        "setpoint low word should be written"
    );
    assert!(device.coil_at(12), "enable coil should be written");

    cleanup_pipeline("it_modbus_sink");
    cleanup_table("it_plc_commands");
}

/// Invalid modbus configs are rejected at create time (validate.rs path).
#[test]
fn test_modbus_invalid_config_rejected_at_create() {
    skip_if_not_running!();

    cleanup_pipeline("it_modbus_bad");

    let result = execute(
        "SELECT pgstreams.create_pipeline('it_modbus_bad', $$
        {
            \"input\": {
                \"modbus\": {
                    \"host\": \"127.0.0.1\",
                    \"reads\": [
                        {\"name\": \"dup\", \"kind\": \"coil\", \"address\": 0},
                        {\"name\": \"dup\", \"kind\": \"coil\", \"address\": 1}
                    ]
                }
            },
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }
        $$::jsonb)",
    );

    // The generic client error hides the message detail; rejection itself +
    // no pipeline row is the contract. (Unit test
    // test_validate_modbus_input_deep_validation_runs asserts the message.)
    result.expect_err("duplicate tag names should be rejected");
    let count =
        query_one("SELECT count(*)::bigint FROM pgstreams.pipelines WHERE name = 'it_modbus_bad'")
            .unwrap()
            .unwrap();
    assert_eq!(count, "0", "rejected pipeline must not be stored");
    cleanup_pipeline("it_modbus_bad");
}

// =============================================================================
// Siemens S7 Pipelines (require a real PLC or snap7 server container;
// set S7_TEST_HOST to enable, e.g. S7_TEST_HOST=192.168.0.100)
// =============================================================================

/// S7 source → table sink against a real PLC / snap7 server.
/// Run with: S7_TEST_HOST=<ip> cargo test --tests -- --ignored s7
#[test]
#[ignore = "requires S7_TEST_HOST pointing at a PLC or snap7 server"]
fn test_s7_to_table_pipeline() {
    skip_if_not_running!();
    let Ok(host) = std::env::var("S7_TEST_HOST") else {
        eprintln!("SKIPPED: S7_TEST_HOST not set");
        return;
    };

    cleanup_pipeline("it_s7_src");
    cleanup_table("it_s7_readings");

    execute("CREATE TABLE it_s7_readings (id BIGSERIAL PRIMARY KEY, speed INT, ts TEXT)").unwrap();

    execute(&format!(
        "SELECT pgstreams.create_pipeline('it_s7_src', $$
        {{
            \"input\": {{
                \"s7\": {{
                    \"host\": \"{host}\",
                    \"model\": \"s7-1200\",
                    \"poll\": \"500ms\",
                    \"reads\": [
                        {{\"name\": \"speed\", \"area\": \"db\", \"db\": 1, \"offset\": 0, \"type\": \"int\"}}
                    ]
                }}
            }},
            \"pipeline\": {{
                \"processors\": [
                    {{\"mapping\": {{
                        \"speed\": \"(value_json->>'speed')::int\",
                        \"ts\":    \"value_json->>'ts'\"
                    }}}}
                ]
            }},
            \"output\": {{
                \"table\": {{\"name\": \"public.it_s7_readings\", \"mode\": \"append\"}}
            }}
        }}
        $$::jsonb)",
    ))
    .unwrap();

    execute("SELECT pgstreams.start('it_s7_src')").unwrap();
    wait_for_row_count("it_s7_readings", 2, PROCESSING_TIMEOUT)
        .expect("expected at least 2 s7 snapshots");

    cleanup_pipeline("it_s7_src");
    cleanup_table("it_s7_readings");
}

/// Table source → S7 sink against a real PLC / snap7 server.
#[test]
#[ignore = "requires S7_TEST_HOST pointing at a PLC or snap7 server"]
fn test_table_to_s7_pipeline() {
    skip_if_not_running!();
    let Ok(host) = std::env::var("S7_TEST_HOST") else {
        eprintln!("SKIPPED: S7_TEST_HOST not set");
        return;
    };

    cleanup_pipeline("it_s7_sink");
    cleanup_table("it_s7_commands");

    execute("CREATE TABLE it_s7_commands (id BIGSERIAL PRIMARY KEY, setpoint DOUBLE PRECISION)")
        .unwrap();

    execute(&format!(
        "SELECT pgstreams.create_pipeline('it_s7_sink', $$
        {{
            \"input\": {{
                \"table\": {{
                    \"name\": \"public.it_s7_commands\",
                    \"offset_column\": \"id\",
                    \"poll\": \"500ms\"
                }}
            }},
            \"pipeline\": {{\"processors\": []}},
            \"output\": {{
                \"s7\": {{
                    \"host\": \"{host}\",
                    \"model\": \"s7-1200\",
                    \"writes\": [
                        {{\"field\": \"setpoint\", \"area\": \"db\", \"db\": 1, \"offset\": 4, \"type\": \"real\"}}
                    ]
                }}
            }}
        }}
        $$::jsonb)",
    ))
    .unwrap();

    execute("SELECT pgstreams.start('it_s7_sink')").unwrap();
    execute("INSERT INTO it_s7_commands (setpoint) VALUES (21.5)").unwrap();

    // No programmatic read-back here — verify on the PLC/snap7 side.
    // The pipeline not entering 'failed' state within the timeout is the
    // automated assertion.
    std::thread::sleep(Duration::from_secs(3));
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_s7_sink'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running", "s7 sink pipeline should stay running");

    cleanup_pipeline("it_s7_sink");
    cleanup_table("it_s7_commands");
}

// =============================================================================
// call output connector
//
// Mirrors the test plan in design/pg_streaming/call-sink.md. These have to be
// integration tests: the sink runs inside the executor background worker, and a
// #[pg_test] rolls its transaction back so a worker could never observe it.
// =============================================================================

/// Drop the fixtures a call-sink test creates.
fn cleanup_call_fixtures(pipeline: &str, extra_fns: &[&str]) {
    cleanup_pipeline(pipeline);
    cleanup_table("it_call_src");
    cleanup_table("it_call_landed");
    cleanup_table("it_call_dl");
    for f in extra_fns {
        let _ = execute(&format!("DROP FUNCTION IF EXISTS {} CASCADE", f));
    }
    let _ = execute(&format!(
        "DELETE FROM pgstreams.error_log WHERE pipeline = '{}'",
        pipeline
    ));
}

/// Source table + landing table shared by the call-sink tests.
fn create_call_source() {
    execute(
        "CREATE TABLE it_call_src (
            id BIGSERIAL PRIMARY KEY,
            n  INT NOT NULL
        )",
    )
    .unwrap();
}

/// A function that raises on every third record → 2/3 land, 1/3 are rejected.
/// One poison record must not discard the whole batch.
#[test]
fn test_call_sink_isolates_poison_records() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_isolate", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int % 3 = 0 THEN
                   RAISE EXCEPTION 'poison record %', rec->>'n' USING ERRCODE = '22000';
               END IF;
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int);
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_isolate', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"on_record_error\": \"dead_letter\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_isolate')").unwrap();

    execute("INSERT INTO it_call_src (n) SELECT generate_series(1, 9)").unwrap();

    // 1,2,4,5,7,8 land; 3,6,9 raise.
    wait_for_row_count("it_call_landed", 6, PROCESSING_TIMEOUT)
        .expect("expected 6 records to land past the poison ones");

    let count = query_one("SELECT count(*)::bigint FROM it_call_landed")
        .unwrap()
        .unwrap();
    assert_eq!(count, "6", "the good records must survive the poison ones");

    let landed = query_one("SELECT string_agg(n::text, ',' ORDER BY n) FROM it_call_landed")
        .unwrap()
        .unwrap();
    assert_eq!(landed, "1,2,4,5,7,8");

    // The three failures reach the error log with their SQLSTATE.
    wait_for(
        "3 rejected records in error_log",
        "SELECT count(*)::bigint FROM pgstreams.error_log WHERE pipeline = 'it_call_isolate'",
        "3",
        PROCESSING_TIMEOUT,
    )
    .expect("expected 3 error_log rows");

    let sample = query_one(
        "SELECT error FROM pgstreams.error_log \
         WHERE pipeline = 'it_call_isolate' ORDER BY id LIMIT 1",
    )
    .unwrap()
    .unwrap();
    assert!(
        sample.contains("poison record") && sample.contains("22000"),
        "error_log should carry the message and SQLSTATE, got: {}",
        sample
    );

    // The offset advances past the dead-lettered records — otherwise the
    // pipeline would spin on record 3 forever.
    wait_for(
        "offset advanced past all 9 records",
        "SELECT offset_value FROM pgstreams.connector_offsets \
         WHERE pipeline = 'it_call_isolate' AND connector = 'table_input'",
        "9",
        PROCESSING_TIMEOUT,
    )
    .expect("offset should advance past dead-lettered records");

    cleanup_call_fixtures("it_call_isolate", &["it_call_ingest(jsonb)"]);
}

/// Rejected records reach the pipeline's dead-letter output, not just the log.
#[test]
fn test_call_sink_routes_to_dead_letter_output() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_dlq", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    execute("CREATE TABLE it_call_dl (id BIGINT, n INT, offset_id BIGINT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int = 2 THEN RAISE EXCEPTION 'reject %', rec->>'n'; END IF;
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int);
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_dlq', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {
                \"processors\": [],
                \"dead_letter\": {\"table\": {
                    \"name\": \"public.it_call_dl\",
                    \"mode\": \"append\"
                }}
            },
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"on_record_error\": \"dead_letter\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_dlq')").unwrap();

    execute("INSERT INTO it_call_src (n) VALUES (1), (2), (3)").unwrap();

    wait_for_row_count("it_call_landed", 2, PROCESSING_TIMEOUT)
        .expect("expected records 1 and 3 to land");
    wait_for_row_count("it_call_dl", 1, PROCESSING_TIMEOUT)
        .expect("expected record 2 in the dead-letter table");

    // The record reaches the dead-letter sink with its shape intact, so an
    // ordinary table sink keeps working.
    let dl_n = query_one("SELECT n FROM it_call_dl").unwrap().unwrap();
    assert_eq!(dl_n, "2");

    let dl_count = query_one("SELECT count(*)::bigint FROM it_call_dl")
        .unwrap()
        .unwrap();
    assert_eq!(
        dl_count, "1",
        "only the failed record should be dead-lettered"
    );

    cleanup_call_fixtures("it_call_dlq", &["it_call_ingest(jsonb)"]);
}

/// `skip` drops failed records without touching the dead-letter output.
#[test]
fn test_call_sink_skip_does_not_dead_letter() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_skip", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    execute("CREATE TABLE it_call_dl (id BIGINT, n INT, offset_id BIGINT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int = 2 THEN RAISE EXCEPTION 'reject %', rec->>'n'; END IF;
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int);
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_skip', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {
                \"processors\": [],
                \"dead_letter\": {\"table\": {
                    \"name\": \"public.it_call_dl\",
                    \"mode\": \"append\"
                }}
            },
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"on_record_error\": \"skip\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_skip')").unwrap();

    execute("INSERT INTO it_call_src (n) VALUES (1), (2), (3)").unwrap();

    wait_for_row_count("it_call_landed", 2, PROCESSING_TIMEOUT)
        .expect("expected records 1 and 3 to land");
    // Give the sink room to have written a dead letter if it were going to.
    std::thread::sleep(Duration::from_secs(3));

    let dl_count = query_one("SELECT count(*)::bigint FROM it_call_dl")
        .unwrap()
        .unwrap();
    assert_eq!(
        dl_count, "0",
        "skip must not write to the dead-letter output"
    );

    // It is still logged, so a skipped record is not invisible.
    let logged = query_one(
        "SELECT count(*)::bigint FROM pgstreams.error_log WHERE pipeline = 'it_call_skip'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(logged, "1", "skipped records should still reach error_log");

    cleanup_call_fixtures("it_call_skip", &["it_call_ingest(jsonb)"]);
}

/// `set_config` is applied before each call and outside the per-record
/// subtransaction, so a rolled-back record must not strip the setting from the
/// record that follows it.
#[test]
fn test_call_sink_set_config_survives_rollback() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_setcfg", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT, role TEXT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int = 1 THEN RAISE EXCEPTION 'boom'; END IF;
               INSERT INTO it_call_landed
               VALUES ((rec->>'n')::int, current_setting('app.user_role'));
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_setcfg', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"set_config\": {\"app.user_role\": \"ingest_service\"},
                \"on_record_error\": \"skip\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_setcfg')").unwrap();

    // Record 1 raises and rolls back; records 2 and 3 must still see the role.
    execute("INSERT INTO it_call_src (n) VALUES (1), (2), (3)").unwrap();

    wait_for_row_count("it_call_landed", 2, PROCESSING_TIMEOUT)
        .expect("expected records 2 and 3 to land");

    let roles = query_one("SELECT string_agg(DISTINCT role, ',') FROM it_call_landed")
        .unwrap()
        .unwrap();
    assert_eq!(
        roles, "ingest_service",
        "set_config must survive the rolled-back record"
    );

    cleanup_call_fixtures("it_call_setcfg", &["it_call_ingest(jsonb)"]);
}

/// `args` forms: the bare word `record`, a field expression, and a literal.
#[test]
fn test_call_sink_arg_forms() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_args", &["it_call_ingest(jsonb, int, text)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (payload jsonb, n INT, source TEXT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb, num int, src text)
           RETURNS void LANGUAGE sql AS $fn$
               INSERT INTO it_call_landed VALUES (rec, num, src)
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_args', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\", \"n * 10\", \"'stream'\"],
                \"on_record_error\": \"dead_letter\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_args')").unwrap();

    execute("INSERT INTO it_call_src (n) VALUES (7)").unwrap();

    wait_for_row_count("it_call_landed", 1, PROCESSING_TIMEOUT).expect("expected 1 record to land");

    let rows = query_all("SELECT (payload->>'n')::int, n, source FROM it_call_landed").unwrap();
    assert_eq!(rows.len(), 1);
    // `record` — the whole record as jsonb
    assert_eq!(rows[0][0], "7");
    // a field expression, evaluated against the batch CTE
    assert_eq!(rows[0][1], "70");
    // a literal
    assert_eq!(rows[0][2], "stream");

    cleanup_call_fixtures("it_call_args", &["it_call_ingest(jsonb, int, text)"]);
}

/// A function that does not exist fails at compile time (pipeline start), not
/// once per record.
#[test]
fn test_call_sink_missing_function_fails_pipeline() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_missing", &[]);

    create_call_source();
    execute(
        "SELECT pgstreams.create_pipeline('it_call_missing', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {\"function\": \"public.it_call_nope\"}}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_missing')").unwrap();

    wait_for(
        "pipeline marked failed for a missing function",
        "SELECT state FROM pgstreams.pipelines WHERE name = 'it_call_missing'",
        "failed",
        PROCESSING_TIMEOUT,
    )
    .expect("pipeline should fail to compile against a missing function");

    let err = query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_call_missing'")
        .unwrap()
        .unwrap();
    assert!(
        err.contains("does not exist"),
        "expected a resolution error, got: {}",
        err
    );

    cleanup_call_fixtures("it_call_missing", &[]);
}

/// A function name that is not a plain identifier is rejected at create time.
#[test]
fn test_call_sink_rejects_non_identifier_function() {
    skip_if_not_running!();
    cleanup_pipeline("it_call_inject");

    let result = execute(
        "SELECT pgstreams.create_pipeline('it_call_inject', $$
        {
            \"input\": {\"kafka\": {\"topic\": \"orders\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {\"function\": \"f(1); DROP TABLE t; --\"}}
        }
        $$::jsonb)",
    );
    let err = result.expect_err("a non-identifier function name must be rejected");
    assert!(
        err.contains("unquoted identifier"),
        "expected a validation error, got: {}",
        err
    );

    cleanup_pipeline("it_call_inject");
}

/// `on_record_error: fail` takes no *per-record* subtransaction, so the first
/// raise discards the whole batch — records written earlier in the same batch
/// roll back with it. This is the cheap, no-isolation path; a poison record
/// stalls the pipeline rather than being routed around.
///
/// One subtransaction still wraps the batch, so the failure comes back as an
/// Err that fails the pipeline once with the reason recorded, rather than
/// escaping `write()` and restarting the executor into the same batch forever.
#[test]
fn test_call_sink_fail_aborts_the_batch() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_fail", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int = 2 THEN RAISE EXCEPTION 'hard fail'; END IF;
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int);
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_fail', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"on_record_error\": \"fail\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_fail')").unwrap();

    // Record 1 would succeed on its own; record 2 raises.
    execute("INSERT INTO it_call_src (n) VALUES (1), (2), (3)").unwrap();

    // The pipeline fails cleanly rather than the worker dying and retrying.
    wait_for(
        "pipeline marked failed by the poison record",
        "SELECT state FROM pgstreams.pipelines WHERE name = 'it_call_fail'",
        "failed",
        PROCESSING_TIMEOUT,
    )
    .expect("on_record_error: fail should fail the pipeline, not crash the worker");

    let landed = query_one("SELECT count(*)::bigint FROM it_call_landed")
        .unwrap()
        .unwrap();
    assert_eq!(
        landed, "0",
        "fail aborts the batch — record 1 must roll back with record 2"
    );

    // Nothing was committed, so the offset never advanced.
    let offset = query_one(
        "SELECT count(*)::bigint FROM pgstreams.connector_offsets \
         WHERE pipeline = 'it_call_fail' AND offset_value > 0",
    )
    .unwrap()
    .unwrap();
    assert_eq!(offset, "0", "a failed batch must not commit its offset");

    // The reason is recorded on the pipeline rather than only in the server log.
    let err = query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_call_fail'")
        .unwrap()
        .unwrap();
    assert!(
        err.contains("hard fail"),
        "the recorded error should carry the function's message, got: {}",
        err
    );

    cleanup_call_fixtures("it_call_fail", &["it_call_ingest(jsonb)"]);

    // The executor was never killed, so the next test needs no settle time —
    // but prove it is still alive before moving on.
    let alive = query_one(
        "SELECT count(*)::bigint FROM pg_stat_activity \
                           WHERE backend_type LIKE 'pg_streaming executor%'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        alive, "4",
        "all executors should still be running after a failed batch"
    );
}

/// A dead-letter table that cannot accept the record must not take the
/// pipeline down. Before the write was wrapped in a subtransaction, the failed
/// INSERT raised a PostgreSQL error that was not a Rust `Err`, so it escaped
/// `write()`, killed the executor tick, and the worker restarted straight back
/// into the same poison record — an endless crash-loop that also starved every
/// other pipeline on that executor.
#[test]
fn test_call_sink_survives_a_broken_dead_letter_sink() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_baddlq", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    // Deliberately missing the `n` and `offset_id` columns the record carries,
    // so every dead-letter INSERT fails.
    execute("CREATE TABLE it_call_dl (unrelated TEXT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE plpgsql AS $fn$
           BEGIN
               IF (rec->>'n')::int = 2 THEN RAISE EXCEPTION 'reject %', rec->>'n'; END IF;
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int);
           END;
           $fn$;"#,
    )
    .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_call_baddlq', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {
                \"processors\": [],
                \"dead_letter\": {\"table\": {
                    \"name\": \"public.it_call_dl\",
                    \"mode\": \"append\"
                }}
            },
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"on_record_error\": \"dead_letter\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_baddlq')").unwrap();

    execute("INSERT INTO it_call_src (n) VALUES (1), (2), (3)").unwrap();

    // The good records still land despite the dead-letter sink being broken.
    wait_for_row_count("it_call_landed", 2, PROCESSING_TIMEOUT)
        .expect("records 1 and 3 must land even though dead-lettering record 2 fails");

    // The rejected record is not lost — error_log still has it.
    wait_for(
        "record 2 in error_log",
        "SELECT count(*)::bigint FROM pgstreams.error_log WHERE pipeline = 'it_call_baddlq'",
        "1",
        PROCESSING_TIMEOUT,
    )
    .expect("the rejected record should still reach error_log");

    // The offset advanced past all three, so the pipeline is not stuck.
    wait_for(
        "offset advanced past all 3 records",
        "SELECT offset_value FROM pgstreams.connector_offsets \
         WHERE pipeline = 'it_call_baddlq' AND connector = 'table_input'",
        "3",
        PROCESSING_TIMEOUT,
    )
    .expect("offset should advance despite the dead-letter failure");

    // And the pipeline is still running — not failed, not crash-looping.
    let state = query_one("SELECT state FROM pgstreams.pipelines WHERE name = 'it_call_baddlq'")
        .unwrap()
        .unwrap();
    assert_eq!(state, "running", "the pipeline must survive a broken DLQ");

    // The executor is still healthy: more input keeps flowing.
    execute("INSERT INTO it_call_src (n) VALUES (4)").unwrap();
    wait_for_row_count("it_call_landed", 3, PROCESSING_TIMEOUT)
        .expect("the executor must keep processing after a dead-letter failure");

    cleanup_call_fixtures("it_call_baddlq", &["it_call_ingest(jsonb)"]);
}

/// A `set_config` the server rejects fails the pipeline once, with the reason
/// recorded, instead of raising past `write()` and restarting the executor.
#[test]
fn test_call_sink_bad_set_config_fails_pipeline_cleanly() {
    skip_if_not_running!();
    cleanup_call_fixtures("it_call_badcfg", &["it_call_ingest(jsonb)"]);

    create_call_source();
    execute("CREATE TABLE it_call_landed (n INT)").unwrap();
    execute(
        r#"CREATE FUNCTION it_call_ingest(rec jsonb) RETURNS void LANGUAGE sql AS $fn$
               INSERT INTO it_call_landed VALUES ((rec->>'n')::int)
           $fn$;"#,
    )
    .unwrap();

    // work_mem is a real GUC, so this passes name validation, but 'not a size'
    // is not a value it accepts — set_config raises at run time.
    execute(
        "SELECT pgstreams.create_pipeline('it_call_badcfg', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_call_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"call\": {
                \"function\": \"public.it_call_ingest\",
                \"args\": [\"record\"],
                \"set_config\": {\"work_mem\": \"not a size\"}
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_call_badcfg')").unwrap();

    execute("INSERT INTO it_call_src (n) VALUES (1)").unwrap();

    wait_for(
        "pipeline marked failed for a rejected set_config",
        "SELECT state FROM pgstreams.pipelines WHERE name = 'it_call_badcfg'",
        "failed",
        PROCESSING_TIMEOUT,
    )
    .expect("a rejected set_config should fail the pipeline, not crash the worker");

    let err = query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_call_badcfg'")
        .unwrap()
        .unwrap();
    assert!(
        err.contains("set_config"),
        "the recorded error should name set_config, got: {}",
        err
    );

    cleanup_call_fixtures("it_call_badcfg", &["it_call_ingest(jsonb)"]);
}

// =============================================================================
// Output failures must not crash-loop the executor
// =============================================================================

/// A sink whose write fails — the single most ordinary failure there is, a
/// CHECK constraint the records violate — used to raise a PostgreSQL error that
/// was not a Rust `Err`. It escaped `process_batch`, killed the executor tick,
/// and the worker restarted into the same batch every 5s forever, while the
/// pipeline still reported `running` and recorded no error at all.
///
/// This is not specific to the `call` sink; it applied to every output
/// connector. Now the write is wrapped in a subtransaction, so the failure
/// fails the pipeline once, with the reason visible in `pgstreams.pipelines`.
#[test]
fn test_failing_output_fails_pipeline_instead_of_crash_looping() {
    skip_if_not_running!();

    cleanup_pipeline("it_badoutput");
    cleanup_table("it_badoutput_src");
    cleanup_table("it_badoutput_tgt");

    execute("CREATE TABLE it_badoutput_src (id BIGSERIAL PRIMARY KEY, n INT)").unwrap();
    execute("CREATE TABLE it_badoutput_tgt (id BIGINT, n INT CHECK (n < 100), offset_id BIGINT)")
        .unwrap();

    execute(
        "SELECT pgstreams.create_pipeline('it_badoutput', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_badoutput_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"table\": {
                \"name\": \"public.it_badoutput_tgt\",
                \"mode\": \"append\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_badoutput')").unwrap();

    // 500 violates the CHECK, so the sink's INSERT raises.
    execute("INSERT INTO it_badoutput_src (n) VALUES (500)").unwrap();

    wait_for(
        "pipeline marked failed by the rejecting sink",
        "SELECT state FROM pgstreams.pipelines WHERE name = 'it_badoutput'",
        "failed",
        PROCESSING_TIMEOUT,
    )
    .expect("a raising output should fail the pipeline, not crash-loop the executor");

    let err = query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_badoutput'")
        .unwrap()
        .unwrap();
    assert!(
        err.contains("check constraint") || err.contains("violates"),
        "the recorded error should name the constraint, got: {}",
        err
    );

    // The executors are all still alive — nothing restarted.
    let alive = query_one(
        "SELECT count(*)::bigint FROM pg_stat_activity \
         WHERE backend_type LIKE 'pg_streaming executor%'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        alive, "4",
        "no executor should have died over a rejected write"
    );

    // And the executor still works: a second pipeline runs fine afterwards.
    cleanup_pipeline("it_badoutput");
    execute("DELETE FROM it_badoutput_src").unwrap();
    execute(
        "SELECT pgstreams.create_pipeline('it_badoutput2', $$
        {
            \"input\": {\"table\": {
                \"name\": \"public.it_badoutput_src\",
                \"offset_column\": \"id\",
                \"poll\": \"1s\"
            }},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"table\": {
                \"name\": \"public.it_badoutput_tgt\",
                \"mode\": \"append\"
            }}
        }
        $$::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_badoutput2')").unwrap();
    execute("INSERT INTO it_badoutput_src (n) VALUES (7)").unwrap();

    wait_for_row_count("it_badoutput_tgt", 1, PROCESSING_TIMEOUT)
        .expect("the executor must still process good records afterwards");

    cleanup_pipeline("it_badoutput2");
    cleanup_table("it_badoutput_src");
    cleanup_table("it_badoutput_tgt");
}

/// A pipeline naming a Kafka topic that does not exist must say exactly that.
///
/// The lookup was written `SELECT id FROM pgkafka.topics WHERE name = $1`, and
/// `Spi::get_one` on zero rows fails with "SpiTupleTable positioned before the
/// start" instead of yielding Ok(None) — so the "topic not found" arm was
/// unreachable and a typo'd topic was reported as though pg_kafka were not
/// installed.
#[test]
fn test_missing_kafka_topic_reports_not_found() {
    skip_if_not_running!();

    cleanup_pipeline("it_missing_topic");

    execute(
        "SELECT pgstreams.create_pipeline('it_missing_topic', '{
            \"input\": {\"kafka\": {\"topic\": \"no_such_topic_xyz\"}},
            \"pipeline\": {\"processors\": []},
            \"output\": {\"drop\": {}}
        }'::jsonb)",
    )
    .unwrap();
    execute("SELECT pgstreams.start('it_missing_topic')").unwrap();

    wait_for(
        "pipeline marked failed for a missing topic",
        "SELECT state FROM pgstreams.pipelines WHERE name = 'it_missing_topic'",
        "failed",
        PROCESSING_TIMEOUT,
    )
    .expect("a missing topic should fail the pipeline");

    let err = query_one("SELECT error FROM pgstreams.pipelines WHERE name = 'it_missing_topic'")
        .unwrap()
        .unwrap();
    assert!(
        err.contains("not found in pgkafka.topics"),
        "expected a not-found message, got: {}",
        err
    );
    assert!(
        !err.contains("positioned before the start"),
        "SPI internals leaked into the message: {}",
        err
    );

    cleanup_pipeline("it_missing_topic");
}
