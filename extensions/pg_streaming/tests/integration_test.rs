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
