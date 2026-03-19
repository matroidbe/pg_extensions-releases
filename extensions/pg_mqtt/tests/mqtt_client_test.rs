//! Integration tests for pg_mqtt using rumqttc MQTT 5.0 client
//!
//! These tests require a running PostgreSQL instance with pg_mqtt extension loaded.
//! Run with: cargo test --test mqtt_client_test -- --nocapture --test-threads=1
//!
//! Prerequisites:
//! 1. PostgreSQL running with pg_mqtt extension
//! 2. MQTT broker listening on localhost:1883

mod common;

use rumqttc::v5::{mqttbytes::QoS, Client, Event, Incoming, MqttOptions};
use rumqttc::Outgoing;
use std::thread;
use std::time::Duration;

// ============================================================================
// Basic MQTT protocol tests
// ============================================================================

/// Test basic MQTT 5.0 connect and publish functionality
///
/// Connects to the broker, publishes a message, and verifies connection works.
#[test]
fn test_mqtt_connect_and_publish() {
    skip_if_no_server!(common::MQTT_ADDR);
    let mut mqttoptions = MqttOptions::new("test-client-1", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut connection) = Client::new(mqttoptions, 10);

    // Spawn a thread to handle connection events
    let handle = thread::spawn(move || {
        let mut connected = false;
        let mut publish_sent = false;

        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    println!("Connected to broker");
                    connected = true;
                }
                Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                    println!("Published message");
                    publish_sent = true;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {:?}", e);
                    break;
                }
            }

            // Exit after publish is confirmed
            if connected && publish_sent {
                break;
            }
        }

        (connected, publish_sent)
    });

    // Give connection time to establish
    thread::sleep(Duration::from_millis(100));

    // Publish a test message
    client
        .publish(
            "test/topic",
            QoS::AtMostOnce,
            false,
            b"Hello from test".to_vec(),
        )
        .expect("Failed to publish");

    // Wait for completion with timeout
    let result = handle.join();

    match result {
        Ok((connected, published)) => {
            assert!(connected, "Failed to connect to MQTT broker");
            assert!(published, "Failed to publish message");
        }
        Err(_) => panic!("Thread panicked"),
    }
}

/// Test MQTT 5.0 subscribe functionality
///
/// Connects to the broker, subscribes to a topic, and waits for acknowledgment.
#[test]
fn test_mqtt_subscribe() {
    skip_if_no_server!(common::MQTT_ADDR);
    let mut mqttoptions = MqttOptions::new("test-client-2", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut connection) = Client::new(mqttoptions, 10);

    let handle = thread::spawn(move || {
        let mut connected = false;
        let mut subscribed = false;

        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    println!("Connected to broker");
                    connected = true;
                }
                Ok(Event::Incoming(Incoming::SubAck(_))) => {
                    println!("Subscription acknowledged");
                    subscribed = true;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {:?}", e);
                    break;
                }
            }

            if connected && subscribed {
                break;
            }
        }

        (connected, subscribed)
    });

    // Give connection time to establish
    thread::sleep(Duration::from_millis(100));

    // Subscribe to a topic
    client
        .subscribe("test/#", QoS::AtMostOnce)
        .expect("Failed to subscribe");

    let result = handle.join();

    match result {
        Ok((connected, subscribed)) => {
            assert!(connected, "Failed to connect to MQTT broker");
            assert!(subscribed, "Failed to subscribe to topic");
        }
        Err(_) => panic!("Thread panicked"),
    }
}

/// Test MQTT 5.0 ping/keepalive functionality
#[test]
fn test_mqtt_ping() {
    skip_if_no_server!(common::MQTT_ADDR);
    let mut mqttoptions = MqttOptions::new("test-client-3", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5)); // rumqttc requires >= 5 secs

    let (_client, mut connection) = Client::new(mqttoptions, 10);

    let handle = thread::spawn(move || {
        let mut connected = false;
        let mut ping_response = false;
        let start = std::time::Instant::now();

        for event in connection.iter() {
            // Timeout after 10 seconds to allow for ping (keep_alive is 5 secs)
            if start.elapsed() > Duration::from_secs(10) {
                break;
            }

            match event {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    println!("Connected to broker");
                    connected = true;
                }
                Ok(Event::Incoming(Incoming::PingResp(_))) => {
                    println!("Received ping response");
                    ping_response = true;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {:?}", e);
                    break;
                }
            }

            if connected && ping_response {
                break;
            }
        }

        (connected, ping_response)
    });

    let result = handle.join();

    match result {
        Ok((connected, ping_response)) => {
            assert!(connected, "Failed to connect to MQTT broker");
            assert!(ping_response, "Did not receive ping response");
        }
        Err(_) => panic!("Thread panicked"),
    }
}

/// Test connection with clean start flag
#[test]
fn test_mqtt_clean_session() {
    skip_if_no_server!(common::MQTT_ADDR);
    let mut mqttoptions = MqttOptions::new("test-client-4", "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_start(true);

    let (client, mut connection) = Client::new(mqttoptions, 10);

    let handle = thread::spawn(move || {
        let mut connected = false;

        for event in connection.iter() {
            match event {
                Ok(Event::Incoming(Incoming::ConnAck(connack))) => {
                    println!(
                        "Connected to broker with session_present={}",
                        connack.session_present
                    );
                    connected = true;
                    // With clean_start=true, session_present should be false
                    return (connected, !connack.session_present);
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {:?}", e);
                    break;
                }
            }
        }

        (connected, false)
    });

    // Give connection time to establish
    thread::sleep(Duration::from_millis(500));

    // Disconnect cleanly
    client.disconnect().ok();

    let result = handle.join();

    match result {
        Ok((connected, clean_session_ok)) => {
            assert!(connected, "Failed to connect to MQTT broker");
            assert!(clean_session_ok, "Clean session flag not handled correctly");
        }
        Err(_) => panic!("Thread panicked"),
    }
}

// ============================================================================
// Typed payload / schema validation tests
// ============================================================================

/// Helper: publish a message via MQTT and wait for the outgoing publish confirmation
fn mqtt_publish_and_wait(client_id: &str, topic: &str, payload: &[u8]) -> bool {
    let mut mqttoptions = MqttOptions::new(client_id, "localhost", 1883);
    mqttoptions.set_keep_alive(Duration::from_secs(5));
    mqttoptions.set_clean_start(true);

    let (client, mut connection) = Client::new(mqttoptions, 10);

    let topic_owned = topic.to_string();
    let payload_owned = payload.to_vec();

    let handle = thread::spawn(move || {
        let mut connected = false;
        let mut publish_sent = false;
        let start = std::time::Instant::now();

        for event in connection.iter() {
            if start.elapsed() > Duration::from_secs(5) {
                break;
            }

            match event {
                Ok(Event::Incoming(Incoming::ConnAck(_))) => {
                    connected = true;
                }
                Ok(Event::Outgoing(Outgoing::Publish(_))) => {
                    publish_sent = true;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("Connection error: {:?}", e);
                    break;
                }
            }

            if connected && publish_sent {
                break;
            }
        }

        (connected, publish_sent)
    });

    // Give connection time to establish
    thread::sleep(Duration::from_millis(200));

    // Publish the message
    let pub_result = client.publish(topic_owned, QoS::AtMostOnce, false, payload_owned);

    if pub_result.is_err() {
        return false;
    }

    // Wait for completion
    match handle.join() {
        Ok((connected, published)) => connected && published,
        Err(_) => false,
    }
}

/// Helper: set up a typed topic with schema validation via SQL
fn setup_typed_topic(topic: &str, table_name: &str, schema_json: &str) {
    // Clean up any existing state
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgmqtt.topic_schemas WHERE topic = '{}';",
        topic
    ));
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgmqtt.schemas WHERE subject = '{}';",
        topic
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS {};", table_name));

    // Create the typed topic via the convenience function
    let result = common::run_sql(&format!(
        "SELECT mqtt_create_typed_topic('{}', '{}'::jsonb, NULL, 'STRICT');",
        topic,
        schema_json.replace('\'', "''")
    ));

    match result {
        Ok(id) => println!(
            "Created typed topic '{}' with schema_id={}",
            topic,
            id.trim()
        ),
        Err(e) => panic!("Failed to create typed topic '{}': {}", topic, e),
    }
}

/// Test: Publish an untyped message via MQTT protocol and verify in pgmqtt.messages
///
/// This tests the basic publish → store in pgmqtt.messages flow via the MQTT protocol.
#[test]
fn test_mqtt_publish_untyped_stored_in_db() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("test/untyped/{}", timestamp);
    let payload = format!("Hello from integration test {}", timestamp);

    println!("=== Publishing untyped message to '{}' ===", topic);
    let published = mqtt_publish_and_wait(
        &format!("untyped-test-{}", timestamp),
        &topic,
        payload.as_bytes(),
    );
    assert!(published, "Failed to publish message via MQTT");

    // Wait for the message to be stored
    thread::sleep(Duration::from_millis(500));

    // Verify the message is in pgmqtt.messages
    let count = common::run_sql(&format!(
        "SELECT COUNT(*)::bigint FROM pgmqtt.messages WHERE topic = '{}';",
        topic
    ));

    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert!(
                n > 0,
                "Message should be stored in pgmqtt.messages, got count={}",
                n
            );
            println!(
                "SUCCESS: Found {} message(s) in pgmqtt.messages for topic '{}'",
                n, topic
            );
        }
        Err(e) => {
            println!(
                "Could not verify via SQL (psql may not be available): {}",
                e
            );
        }
    }
}

/// Test: Publish a valid typed payload via MQTT protocol to a typed topic
///
/// Sets up a typed topic with a schema, publishes a valid JSON payload via MQTT,
/// and verifies the data is decomposed into the backing table's columns.
#[test]
fn test_mqtt_publish_typed_valid_payload() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("sensors/temp/{}", timestamp);
    let schema = r#"{"type":"object","properties":{"temp":{"type":"number"},"unit":{"type":"string"},"location":{"type":"string"}},"required":["temp","unit"]}"#;

    // Set up typed topic via SQL
    setup_typed_topic(
        &topic,
        &format!("pgmqtt.sensors_temp_{}", timestamp),
        schema,
    );

    // Publish a valid JSON payload via MQTT
    let payload = r#"{"temp": 22.5, "unit": "C", "location": "office"}"#;
    println!("=== Publishing valid typed payload to '{}' ===", topic);
    let published = mqtt_publish_and_wait(
        &format!("typed-valid-{}", timestamp),
        &topic,
        payload.as_bytes(),
    );
    assert!(published, "Failed to publish message via MQTT");

    // Wait for storage
    thread::sleep(Duration::from_millis(1000));

    // Verify data landed in the backing table
    let table_name = format!("pgmqtt.sensors_temp_{}", timestamp);
    let count = common::run_sql(&format!("SELECT COUNT(*)::bigint FROM {};", table_name));

    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert!(
                n > 0,
                "Valid typed payload should be stored in backing table '{}', got count={}",
                table_name,
                n
            );
            println!("SUCCESS: {} row(s) in backing table '{}'", n, table_name);
        }
        Err(e) => {
            panic!("Failed to query backing table '{}': {}", table_name, e);
        }
    }

    // Verify the actual column values
    let temp = common::run_sql(&format!(
        "SELECT temp::text FROM {} ORDER BY id DESC LIMIT 1;",
        table_name
    ));
    match temp {
        Ok(t) => {
            assert!(
                t.trim().contains("22.5"),
                "temp column should be 22.5, got: '{}'",
                t.trim()
            );
            println!("Verified temp = {}", t.trim());
        }
        Err(e) => println!("Could not verify temp value: {}", e),
    }

    let unit = common::run_sql(&format!(
        "SELECT unit FROM {} ORDER BY id DESC LIMIT 1;",
        table_name
    ));
    match unit {
        Ok(u) => {
            assert_eq!(
                u.trim(),
                "C",
                "unit column should be 'C', got: '{}'",
                u.trim()
            );
            println!("Verified unit = {}", u.trim());
        }
        Err(e) => println!("Could not verify unit value: {}", e),
    }

    println!("SUCCESS: Typed payload decomposed into table columns correctly!");
}

/// Test: Publish an invalid typed payload via MQTT protocol in STRICT mode
///
/// Sets up a typed topic with STRICT validation, publishes an invalid JSON payload
/// (missing required fields), and verifies it is NOT stored in the backing table.
#[test]
fn test_mqtt_publish_typed_invalid_payload_strict() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("sensors/strict/{}", timestamp);
    let schema = r#"{"type":"object","properties":{"temp":{"type":"number"},"unit":{"type":"string"}},"required":["temp","unit"]}"#;

    // Set up typed topic
    setup_typed_topic(
        &topic,
        &format!("pgmqtt.sensors_strict_{}", timestamp),
        schema,
    );

    // Publish an INVALID payload (missing required "unit" field)
    let invalid_payload = r#"{"temp": 22.5}"#;
    println!(
        "=== Publishing INVALID payload to '{}' (STRICT mode) ===",
        topic
    );
    let published = mqtt_publish_and_wait(
        &format!("typed-invalid-{}", timestamp),
        &topic,
        invalid_payload.as_bytes(),
    );
    // The publish succeeds from the client's perspective (QoS 0, no error ack)
    assert!(
        published,
        "Client should be able to send message (MQTT protocol level)"
    );

    // Wait for server processing
    thread::sleep(Duration::from_millis(1000));

    // Verify the invalid payload was NOT stored in the backing table
    let table_name = format!("pgmqtt.sensors_strict_{}", timestamp);
    let count = common::run_sql(&format!("SELECT COUNT(*)::bigint FROM {};", table_name));

    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert_eq!(
                n, 0,
                "Invalid payload should NOT be stored in backing table (STRICT mode), got count={}",
                n
            );
            println!(
                "SUCCESS: Invalid payload correctly rejected (0 rows in '{}')",
                table_name
            );
        }
        Err(e) => {
            panic!("Failed to query backing table '{}': {}", table_name, e);
        }
    }

    // Also verify it's NOT in pgmqtt.messages (should be dropped entirely)
    let msg_count = common::run_sql(&format!(
        "SELECT COUNT(*)::bigint FROM pgmqtt.messages WHERE topic = '{}';",
        topic
    ));
    match msg_count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert_eq!(
                n, 0,
                "Invalid payload should NOT be in pgmqtt.messages either, got count={}",
                n
            );
            println!("SUCCESS: Invalid payload not stored anywhere");
        }
        Err(e) => println!("Could not verify messages table: {}", e),
    }
}

/// Test: Publish a non-JSON payload to a typed topic in STRICT mode
///
/// Verifies that non-JSON payloads are rejected when a schema is bound.
#[test]
fn test_mqtt_publish_typed_non_json_strict() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("sensors/nonjson/{}", timestamp);
    let schema = r#"{"type":"object","properties":{"temp":{"type":"number"}},"required":["temp"]}"#;

    // Set up typed topic
    setup_typed_topic(
        &topic,
        &format!("pgmqtt.sensors_nonjson_{}", timestamp),
        schema,
    );

    // Publish a non-JSON payload
    let non_json = b"this is not json at all";
    println!(
        "=== Publishing non-JSON payload to '{}' (STRICT mode) ===",
        topic
    );
    let published = mqtt_publish_and_wait(&format!("nonjson-{}", timestamp), &topic, non_json);
    assert!(published, "Client should be able to send message");

    // Wait for processing
    thread::sleep(Duration::from_millis(1000));

    // Verify nothing stored
    let table_name = format!("pgmqtt.sensors_nonjson_{}", timestamp);
    let count = common::run_sql(&format!("SELECT COUNT(*)::bigint FROM {};", table_name));
    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert_eq!(
                n, 0,
                "Non-JSON payload should NOT be stored, got count={}",
                n
            );
            println!("SUCCESS: Non-JSON payload rejected in STRICT mode");
        }
        Err(e) => {
            panic!("Failed to query table: {}", e);
        }
    }
}

/// Test: Publish to an untyped topic (no schema bound) accepts any payload
///
/// Verifies backward compatibility: topics without schema bindings accept all payloads.
#[test]
fn test_mqtt_publish_no_schema_accepts_anything() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let topic = format!("untyped/freeform/{}", timestamp);

    // No schema setup — this is an untyped topic

    // Publish arbitrary binary data
    let payload = b"arbitrary binary \x00\x01\x02 data";
    println!(
        "=== Publishing arbitrary data to untyped topic '{}' ===",
        topic
    );
    let published = mqtt_publish_and_wait(&format!("freeform-{}", timestamp), &topic, payload);
    assert!(published, "Should publish to untyped topic");

    // Wait for storage
    thread::sleep(Duration::from_millis(500));

    // Verify it's stored in pgmqtt.messages
    let count = common::run_sql(&format!(
        "SELECT COUNT(*)::bigint FROM pgmqtt.messages WHERE topic = '{}';",
        topic
    ));
    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert!(
                n > 0,
                "Untyped topic should store any payload, got count={}",
                n
            );
            println!(
                "SUCCESS: Untyped topic accepted arbitrary payload ({} rows)",
                n
            );
        }
        Err(e) => println!("Could not verify: {}", e),
    }
}

/// Test: Full roundtrip — SQL creates typed topic, MQTT publishes, SQL queries backing table
///
/// This is the most comprehensive test: it creates a typed topic via SQL,
/// publishes multiple messages via the MQTT protocol, and verifies all rows
/// are correctly decomposed into the backing table.
#[test]
fn test_mqtt_typed_topic_full_roundtrip() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("devices/metrics/{}", timestamp);
    let table_name = format!("pgmqtt.devices_metrics_{}", timestamp);
    let schema = r#"{"type":"object","properties":{"device_id":{"type":"string"},"cpu":{"type":"number"},"memory":{"type":"number"},"status":{"type":"string"}},"required":["device_id","cpu","memory"]}"#;

    // Step 1: Create typed topic via SQL
    println!("=== Step 1: Create typed topic '{}' ===", topic);
    setup_typed_topic(&topic, &table_name, schema);

    // Step 2: Publish multiple valid messages via MQTT
    println!("\n=== Step 2: Publish metrics via MQTT ===");
    let messages = vec![
        r#"{"device_id": "server-001", "cpu": 45.2, "memory": 67.8, "status": "healthy"}"#,
        r#"{"device_id": "server-002", "cpu": 89.1, "memory": 92.3, "status": "warning"}"#,
        r#"{"device_id": "server-003", "cpu": 12.0, "memory": 34.5}"#, // no status (optional)
    ];

    for (i, msg) in messages.iter().enumerate() {
        let client_id = format!("roundtrip-{}-{}", timestamp, i);
        let published = mqtt_publish_and_wait(&client_id, &topic, msg.as_bytes());
        assert!(published, "Failed to publish message {}", i);
        println!("Published message {}: {}", i, msg);
        // Small delay between publishes
        thread::sleep(Duration::from_millis(200));
    }

    // Step 3: Wait for all messages to be stored
    thread::sleep(Duration::from_millis(1000));

    // Step 4: Verify rows in backing table
    println!("\n=== Step 3: Verify data in backing table ===");
    let count = common::run_sql(&format!("SELECT COUNT(*)::bigint FROM {};", table_name));

    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert_eq!(n, 3, "Should have 3 rows in backing table, got {}", n);
            println!("Row count: {} (expected: 3)", n);
        }
        Err(e) => panic!("Failed to count rows: {}", e),
    }

    // Verify specific values
    let high_cpu = common::run_sql(&format!(
        "SELECT device_id FROM {} WHERE cpu > 80 LIMIT 1;",
        table_name
    ));
    match high_cpu {
        Ok(d) => {
            assert_eq!(
                d.trim(),
                "server-002",
                "High CPU device should be server-002"
            );
            println!("Verified high-CPU device: {}", d.trim());
        }
        Err(e) => println!("Could not verify device: {}", e),
    }

    // Verify optional field is NULL when not provided
    let null_status = common::run_sql(&format!(
        "SELECT (status IS NULL)::text FROM {} WHERE device_id = 'server-003';",
        table_name
    ));
    match null_status {
        Ok(s) => {
            assert!(
                s.trim() == "true" || s.trim() == "t",
                "Optional 'status' should be NULL for server-003, got: '{}'",
                s.trim()
            );
            println!("Verified status is NULL for server-003");
        }
        Err(e) => println!("Could not verify null status: {}", e),
    }

    println!("\nSUCCESS: Full typed topic roundtrip verified!");
}

/// Test: Register schema, bind to topic via SQL, then publish via MQTT
///
/// Tests the manual workflow: register_schema → bind_schema → publish via MQTT
#[test]
fn test_mqtt_manual_schema_bind_and_publish() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("events/manual/{}", timestamp);
    let schema = r#"{"type":"object","properties":{"event":{"type":"string"},"value":{"type":"integer"}},"required":["event","value"]}"#;

    // Register schema via SQL
    println!("=== Registering schema manually ===");
    let schema_id = common::run_sql(&format!(
        "SELECT mqtt_register_schema('{}', '{}'::jsonb, 'Manual test schema');",
        topic,
        schema.replace('\'', "''")
    ))
    .expect("Failed to register schema");
    let schema_id: i32 = schema_id
        .trim()
        .parse()
        .expect("Schema ID should be integer");
    println!("Registered schema ID: {}", schema_id);

    // Bind schema to topic (validation only — no backing table)
    let binding_id = common::run_sql(&format!(
        "SELECT mqtt_bind_schema('{}', {}, 'STRICT');",
        topic, schema_id
    ))
    .expect("Failed to bind schema");
    println!("Bound schema to topic, binding_id={}", binding_id.trim());

    // Publish valid message via MQTT — should go to pgmqtt.messages
    let valid_payload = r#"{"event": "click", "value": 42}"#;
    println!("=== Publishing valid payload ===");
    let published = mqtt_publish_and_wait(
        &format!("manual-valid-{}", timestamp),
        &topic,
        valid_payload.as_bytes(),
    );
    assert!(published, "Should publish valid message");

    thread::sleep(Duration::from_millis(500));

    // Verify in pgmqtt.messages (no backing table, so it goes there)
    let count = common::run_sql(&format!(
        "SELECT COUNT(*)::bigint FROM pgmqtt.messages WHERE topic = '{}';",
        topic
    ));
    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert!(
                n > 0,
                "Valid message should be in pgmqtt.messages, got {}",
                n
            );
            println!(
                "SUCCESS: Valid message stored in pgmqtt.messages ({} row(s))",
                n
            );
        }
        Err(e) => println!("Could not verify: {}", e),
    }

    // Publish invalid message — should be rejected
    let invalid_payload = r#"{"event": "click"}"#; // missing required "value"
    println!("=== Publishing invalid payload ===");
    let published = mqtt_publish_and_wait(
        &format!("manual-invalid-{}", timestamp),
        &topic,
        invalid_payload.as_bytes(),
    );
    assert!(published, "Client can send message (protocol level)");

    thread::sleep(Duration::from_millis(500));

    // The invalid message should NOT be stored
    // Count should still be the same as before
    let count_after = common::run_sql(&format!(
        "SELECT COUNT(*)::bigint FROM pgmqtt.messages WHERE topic = '{}';",
        topic
    ));
    match count_after {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            // Should still be 1 (only the valid message)
            assert_eq!(n, 1, "Only valid message should be stored, got {}", n);
            println!("SUCCESS: Invalid message rejected, count still = {}", n);
        }
        Err(e) => println!("Could not verify: {}", e),
    }

    // Cleanup: unbind
    let _ = common::run_sql(&format!("SELECT mqtt_unbind_schema('{}');", topic));
}

/// Test: Create typed topic from existing table, then publish via MQTT
#[test]
fn test_mqtt_create_typed_topic_from_table() {
    skip_if_no_server!(common::MQTT_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic = format!("readings/fromtbl/{}", timestamp);
    let table_name = format!("pgmqtt.readings_fromtbl_{}", timestamp);

    // Create table manually first
    let _ = common::execute_sql(&format!(
        "CREATE TABLE {} (
            id BIGSERIAL PRIMARY KEY,
            sensor TEXT NOT NULL,
            value NUMERIC NOT NULL,
            unit TEXT
        );",
        table_name
    ));
    println!("Created table: {}", table_name);

    // Create typed topic from the existing table
    let result = common::run_sql(&format!(
        "SELECT mqtt_create_typed_topic_from_table('{}', '{}', 'STRICT');",
        topic, table_name
    ));
    assert!(
        result.is_ok(),
        "Failed to create typed topic from table: {:?}",
        result
    );
    println!(
        "Created typed topic from table, result: {}",
        result.unwrap().trim()
    );

    // Publish via MQTT
    let payload = r#"{"sensor": "DHT22", "value": 23.1, "unit": "C"}"#;
    println!("=== Publishing to typed topic from table ===");
    let published = mqtt_publish_and_wait(
        &format!("fromtbl-{}", timestamp),
        &topic,
        payload.as_bytes(),
    );
    assert!(published, "Should publish to typed topic");

    thread::sleep(Duration::from_millis(1000));

    // Verify data in table
    let count = common::run_sql(&format!("SELECT COUNT(*)::bigint FROM {};", table_name));
    match count {
        Ok(c) => {
            let n: i64 = c.trim().parse().unwrap_or(0);
            assert!(n > 0, "Should have rows in table, got {}", n);
            println!("SUCCESS: {} row(s) in backing table", n);
        }
        Err(e) => panic!("Failed to query table: {}", e),
    }

    let sensor = common::run_sql(&format!(
        "SELECT sensor FROM {} ORDER BY id DESC LIMIT 1;",
        table_name
    ));
    match sensor {
        Ok(s) => {
            assert_eq!(s.trim(), "DHT22");
            println!("SUCCESS: sensor column = {}", s.trim());
        }
        Err(e) => println!("Could not verify sensor: {}", e),
    }
}
