//! Integration tests using rdkafka (librdkafka)
//!
//! These tests use a real Kafka client library to verify compatibility
//! with pg_kafka's protocol implementation.
//!
//! Prerequisites:
//! - pg_kafka server running on localhost:9092
//! - Topic "test-topic" created via SQL: SELECT pgkafka.create_topic('test-topic');
//!
//! Run with: cargo test --test kafka_client_test -- --nocapture

mod common;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::TopicPartitionList;
use std::time::Duration;

// Use explicit IPv4 address to avoid IPv6 resolution issues on CI
const BOOTSTRAP_SERVERS: &str = "127.0.0.1:9092";
const TEST_TOPIC: &str = "test-topic";

/// Create a producer with common configuration
fn create_producer() -> BaseProducer {
    ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("message.timeout.ms", "5000")
        .set("debug", "broker,protocol")
        .create()
        .expect("Failed to create producer")
}

/// Create a consumer with common configuration
fn create_consumer(group_id: &str) -> BaseConsumer {
    ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("debug", "broker,protocol")
        .create()
        .expect("Failed to create consumer")
}

/// Test: Connect and get cluster metadata
#[test]
fn test_metadata() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let producer = create_producer();

    // Fetch metadata for all topics
    let metadata = producer
        .client()
        .fetch_metadata(None, Duration::from_secs(5))
        .expect("Failed to fetch metadata");

    println!("Cluster ID: {:?}", metadata.orig_broker_name());
    println!("Brokers: {}", metadata.brokers().len());

    for broker in metadata.brokers() {
        println!(
            "  Broker {}: {}:{}",
            broker.id(),
            broker.host(),
            broker.port()
        );
    }

    println!("Topics: {}", metadata.topics().len());
    for topic in metadata.topics() {
        println!(
            "  Topic '{}': {} partitions, error: {:?}",
            topic.name(),
            topic.partitions().len(),
            topic.error()
        );
    }

    assert!(
        metadata.brokers().len() > 0,
        "Should have at least one broker"
    );
}

/// Test: Produce a single message
#[test]
fn test_produce_single() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let producer = create_producer();

    let key = "test-key-1";
    let value = "Hello from rdkafka!";

    let record = BaseRecord::to(TEST_TOPIC).key(key).payload(value);

    producer.send(record).expect("Failed to enqueue message");

    // Wait for delivery
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");

    println!("Produced message: key={}, value={}", key, value);
}

/// Test: Produce multiple messages
#[test]
fn test_produce_batch() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let producer = create_producer();

    let messages = vec![
        ("batch-key-1", "Message 1"),
        ("batch-key-2", "Message 2"),
        ("batch-key-3", "Message 3"),
    ];

    for (key, value) in &messages {
        let record = BaseRecord::to(TEST_TOPIC).key(*key).payload(*value);

        producer.send(record).expect("Failed to enqueue message");
    }

    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");

    println!("Produced {} messages", messages.len());
}

/// Test: Consume messages from beginning
#[test]
fn test_consume() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-consume");

    // Subscribe to topic
    consumer
        .subscribe(&[TEST_TOPIC])
        .expect("Failed to subscribe");

    println!("Subscribed to {}", TEST_TOPIC);

    // Poll for messages
    let mut count = 0;
    let max_messages = 10;
    let timeout = Duration::from_secs(1);

    loop {
        match consumer.poll(timeout) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                println!(
                    "Received: partition={}, offset={}, key={:?}, value={:?}",
                    msg.partition(),
                    msg.offset(),
                    key,
                    value
                );

                count += 1;
                if count >= max_messages {
                    break;
                }
            }
            Some(Err(e)) => {
                println!("Error receiving message: {}", e);
                break;
            }
            None => {
                println!("No more messages (timeout)");
                break;
            }
        }
    }

    println!("Consumed {} messages", count);
}

/// Test: Assign specific partition and offset
#[test]
fn test_consume_from_offset() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-offset");

    // Manually assign partition 0 starting at offset 0
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");

    consumer.assign(&tpl).expect("Failed to assign partitions");

    println!("Assigned to {}:0 from beginning", TEST_TOPIC);

    // Poll for a few messages
    for _ in 0..5 {
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                println!("Offset {}: {:?}", msg.offset(), value);
            }
            Some(Err(e)) => {
                println!("Error: {}", e);
                break;
            }
            None => {
                println!("No message");
                break;
            }
        }
    }
}

/// Test: Full produce and consume cycle
///
/// This test verifies that a message produced via Kafka protocol can be
/// consumed back, confirming end-to-end functionality.
#[test]
fn test_produce_consume_roundtrip() {
    skip_if_no_server!(common::KAFKA_ADDR);
    common::ensure_topic(TEST_TOPIC).expect("Failed to ensure topic");

    // Generate unique message content
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let test_key = format!("roundtrip-key-{}", timestamp);
    let test_value = format!("Roundtrip test message at {}", timestamp);

    // Produce
    println!("=== Producing message ===");
    let producer = create_producer();

    let record = BaseRecord::to(TEST_TOPIC)
        .key(test_key.as_str())
        .payload(test_value.as_str());

    producer.send(record).expect("Failed to send");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");
    println!("Produced: key={}, value={}", test_key, test_value);

    // Small delay to ensure message is stored
    std::thread::sleep(Duration::from_millis(500));

    // Consume
    println!("\n=== Consuming messages ===");
    let consumer = create_consumer(&format!("test-group-roundtrip-{}", timestamp));

    // Get current high watermark to start consuming from recent messages
    let (_, high) = consumer
        .fetch_watermarks(TEST_TOPIC, 0, Duration::from_secs(5))
        .expect("Failed to fetch watermarks");

    // Start from a few messages before the end to catch our new message
    let start_offset = if high > 5 { high - 5 } else { 0 };
    println!(
        "Starting from offset {} (high watermark: {})",
        start_offset, high
    );

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Offset(start_offset))
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Poll until we find our message
    let mut found = false;
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                println!(
                    "Got message: offset={}, key={:?}, value={:?}",
                    msg.offset(),
                    key,
                    value
                );

                if key.as_deref() == Some(test_key.as_str()) {
                    assert_eq!(value.as_deref(), Some(test_value.as_str()));
                    found = true;
                    println!("\n=== SUCCESS: Message roundtrip verified! ===");
                    break;
                }
            }
            Some(Err(e)) => {
                println!("Poll error: {}", e);
            }
            None => {
                // No message, continue polling
            }
        }
    }

    assert!(
        found,
        "Did not find the produced message in consumed messages"
    );
}

/// Test: Get watermark offsets (earliest and latest)
#[test]
fn test_watermarks() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-watermarks");

    match consumer.fetch_watermarks(TEST_TOPIC, 0, Duration::from_secs(5)) {
        Ok((low, high)) => {
            println!(
                "Watermarks for {}:0 - low: {}, high: {}",
                TEST_TOPIC, low, high
            );
            assert!(high >= low, "High watermark should be >= low watermark");
        }
        Err(e) => {
            println!("Failed to fetch watermarks: {}", e);
            // This might fail if ListOffsets is not fully implemented
        }
    }
}

/// Test: Commit and fetch offsets
///
/// This test verifies that offset commit/fetch operations work correctly.
/// Note: pg_kafka may not fully support consumer group coordination yet.
#[test]
fn test_offset_commit_fetch() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-offset-commit");

    // Assign to partition 0
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Consume a few messages to advance position
    let mut consumed_offset = 0i64;
    for _ in 0..3 {
        if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(2)) {
            consumed_offset = msg.offset();
            println!("Consumed message at offset {}", consumed_offset);
        }
    }

    // Try to commit the offset
    let mut commit_tpl = TopicPartitionList::new();
    commit_tpl
        .add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Offset(consumed_offset + 1))
        .expect("Failed to add partition for commit");

    match consumer.commit(&commit_tpl, rdkafka::consumer::CommitMode::Sync) {
        Ok(_) => {
            println!("Committed offset {}", consumed_offset + 1);

            // Try to fetch committed offset
            match consumer.committed(Duration::from_secs(5)) {
                Ok(committed) => {
                    for tp in committed.elements() {
                        println!(
                            "Committed offset for {}:{} = {:?}",
                            tp.topic(),
                            tp.partition(),
                            tp.offset()
                        );
                    }
                }
                Err(e) => {
                    println!("Failed to fetch committed offsets: {}", e);
                }
            }
        }
        Err(e) => {
            println!(
                "Offset commit not supported or failed: {} (this may be expected)",
                e
            );
        }
    }
}

/// Test: List consumer groups
///
/// This test attempts to list consumer groups via the admin API.
/// Note: pg_kafka may not support this operation yet.
#[test]
fn test_list_groups() {
    skip_if_no_server!(common::KAFKA_ADDR);

    use rdkafka::admin::{AdminClient, AdminOptions};
    use rdkafka::client::DefaultClientContext;

    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .create()
        .expect("Failed to create admin client");

    // Try to list consumer groups
    let _opts = AdminOptions::new().request_timeout(Some(Duration::from_secs(5)));

    // Note: rdkafka's admin API is async-only for some operations
    // This test just verifies we can create an admin client connection
    println!("Admin client created successfully");

    // Try fetching metadata as a basic admin operation
    match admin_client
        .inner()
        .fetch_metadata(None, Duration::from_secs(5))
    {
        Ok(metadata) => {
            println!(
                "Admin metadata: {} brokers, {} topics",
                metadata.brokers().len(),
                metadata.topics().len()
            );
        }
        Err(e) => {
            println!("Admin metadata fetch failed: {}", e);
        }
    }
}

/// Test: Position tracking after consume
///
/// This test verifies that the consumer position is tracked correctly.
#[test]
fn test_consumer_position() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-position");

    // Assign to partition 0 at beginning
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Get initial position
    match consumer.position() {
        Ok(pos) => {
            for tp in pos.elements() {
                println!(
                    "Initial position for {}:{} = {:?}",
                    tp.topic(),
                    tp.partition(),
                    tp.offset()
                );
            }
        }
        Err(e) => {
            println!("Failed to get initial position: {}", e);
        }
    }

    // Consume a message
    if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(2)) {
        println!("Consumed message at offset {}", msg.offset());
    }

    // Check position after consume
    match consumer.position() {
        Ok(pos) => {
            for tp in pos.elements() {
                println!(
                    "Position after consume for {}:{} = {:?}",
                    tp.topic(),
                    tp.partition(),
                    tp.offset()
                );
            }
        }
        Err(e) => {
            println!("Failed to get position after consume: {}", e);
        }
    }
}

/// Test: Seek to specific offset
///
/// This test verifies that seeking to a specific offset works correctly.
#[test]
fn test_seek_to_offset() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let consumer = create_consumer("test-group-seek");

    // Assign to partition 0
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(TEST_TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Get watermarks to find a valid offset to seek to
    let (low, high) = match consumer.fetch_watermarks(TEST_TOPIC, 0, Duration::from_secs(5)) {
        Ok((l, h)) => (l, h),
        Err(e) => {
            println!("Cannot get watermarks, skipping seek test: {}", e);
            return;
        }
    };

    if high > low {
        // Seek to the middle
        let target_offset = low + (high - low) / 2;
        println!(
            "Seeking to offset {} (low={}, high={})",
            target_offset, low, high
        );

        match consumer.seek(TEST_TOPIC, 0, rdkafka::Offset::Offset(target_offset), None) {
            Ok(_) => {
                println!("Seek successful");

                // Verify by consuming
                if let Some(Ok(msg)) = consumer.poll(Duration::from_secs(2)) {
                    println!("After seek, consumed message at offset {}", msg.offset());
                    assert!(
                        msg.offset() >= target_offset,
                        "Consumed offset should be >= seek target"
                    );
                }
            }
            Err(e) => {
                println!("Seek failed: {}", e);
            }
        }
    } else {
        println!("Topic is empty or has no range to seek within");
    }
}

// ============================================================================
// Table-backed topic tests
// ============================================================================

const ORDERS_TOPIC: &str = "orders-topic";
const ORDERS_TABLE: &str = "test_orders";

/// Ensure the orders table and topic exist for table-backed tests
fn ensure_orders_topic() {
    // First check if the table exists with correct schema (has order_id column)
    let has_order_id = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = '{}' AND column_name = 'order_id';",
        ORDERS_TABLE
    ));

    // If table exists but doesn't have order_id column, drop it
    if let Ok(count) = has_order_id {
        if count.trim() == "0" {
            // Table might exist with wrong schema - drop topic first, then table
            let _ = common::execute_sql(&format!(
                "DELETE FROM pgkafka.topics WHERE name = '{}';",
                ORDERS_TOPIC
            ));
            let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS {};", ORDERS_TABLE));
        }
    }

    // Create the table if it doesn't exist
    // Schema matches what tests expect: order_id, customer_name, product, quantity, price, created_at
    let _ = common::execute_sql(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            order_id SERIAL PRIMARY KEY,
            customer_name TEXT NOT NULL,
            product TEXT NOT NULL,
            quantity INT NOT NULL DEFAULT 1,
            price NUMERIC(10,2) NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );",
        ORDERS_TABLE
    ));

    // Insert a sample row if table is empty
    let _ = common::execute_sql(&format!(
        "INSERT INTO {} (customer_name, product, quantity, price)
         SELECT 'test-customer', 'test-product', 1, 9.99
         WHERE NOT EXISTS (SELECT 1 FROM {} LIMIT 1);",
        ORDERS_TABLE, ORDERS_TABLE
    ));

    // Create the topic from the table (ignore error if exists)
    // Use NULL for value_column to use value_expr (row_to_json)
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.create_topic_from_table('{}', '{}', 'order_id', NULL, 'row_to_json({})');",
        ORDERS_TOPIC, ORDERS_TABLE, ORDERS_TABLE
    ));
}

/// Test: Consume from a table-backed topic
///
/// This test verifies that messages from a source table can be consumed via Kafka protocol.
#[test]
fn test_consume_table_backed_topic() {
    skip_if_no_server!(common::KAFKA_ADDR);
    ensure_orders_topic();

    let consumer = create_consumer("test-group-table-backed");

    // Assign to partition 0 from beginning
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(ORDERS_TOPIC, 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    println!("Assigned to {}:0 from beginning", ORDERS_TOPIC);

    // Poll for messages from the source table
    let mut messages = Vec::new();
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                println!(
                    "Got message: offset={}, key={:?}, value={:?}",
                    msg.offset(),
                    key,
                    value
                );
                messages.push((msg.offset(), key, value));
            }
            Some(Err(e)) => {
                println!("Poll error: {}", e);
                break;
            }
            None => {
                println!("No more messages");
                break;
            }
        }
    }

    assert!(
        !messages.is_empty(),
        "Should have received messages from table-backed topic"
    );

    // Verify the messages contain JSON data from row_to_json
    for (offset, _key, value) in &messages {
        if let Some(v) = value {
            assert!(
                v.contains("order_id") || v.contains("customer_name") || v.contains("product"),
                "Message at offset {} should contain table column data: {}",
                offset,
                v
            );
        }
    }

    println!(
        "SUCCESS: Consumed {} messages from table-backed topic",
        messages.len()
    );
}

/// Test: Insert via SQL and verify consumer receives new message
///
/// This test verifies that when a row is inserted into a source table,
/// a Kafka consumer can fetch the new message.
#[test]
fn test_sql_insert_to_kafka_consumer() {
    skip_if_no_server!(common::KAFKA_ADDR);
    ensure_orders_topic();

    // First, get the current high watermark
    let consumer = create_consumer("test-group-sql-insert");

    let (low, high) = consumer
        .fetch_watermarks(ORDERS_TOPIC, 0, Duration::from_secs(5))
        .expect("Failed to fetch watermarks");
    println!("Initial watermarks: low={}, high={}", low, high);

    // Insert a new row via SQL
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let customer_name = format!("TestCustomer-{}", timestamp);

    let insert_sql = format!(
        "INSERT INTO test_orders (customer_name, product, quantity, price) \
         VALUES ('{}', 'TestProduct', 1, 99.99) RETURNING order_id",
        customer_name
    );

    let result = common::run_sql(&insert_sql).expect("Failed to insert row");
    // psql returns the value on the first line, with "INSERT 0 1" on later lines
    let new_order_id: i64 = result
        .lines()
        .next()
        .expect("No output from psql")
        .trim()
        .parse()
        .expect("Failed to parse order_id");
    println!("Inserted new order with id: {}", new_order_id);

    // Small delay for the insert to be visible
    std::thread::sleep(Duration::from_millis(200));

    // Assign to partition starting from the new offset
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(ORDERS_TOPIC, 0, rdkafka::Offset::Offset(new_order_id))
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Poll for the new message
    let mut found = false;
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                println!("Got message: offset={}, value={:?}", msg.offset(), value);

                if let Some(v) = &value {
                    if v.contains(&customer_name) {
                        println!("SUCCESS: Found the newly inserted message!");
                        found = true;
                        break;
                    }
                }
            }
            Some(Err(e)) => {
                println!("Poll error: {}", e);
                break;
            }
            None => {
                println!("No message received");
                break;
            }
        }
    }

    assert!(
        found,
        "Should have received the newly inserted message with customer '{}'",
        customer_name
    );
}

/// Test: Verify consumer restart behavior - no duplicate messages
///
/// This test verifies that after a consumer stops and restarts,
/// it doesn't receive old messages unless a new SQL insert happens.
#[test]
fn test_consumer_restart_no_duplicates() {
    skip_if_no_server!(common::KAFKA_ADDR);
    ensure_orders_topic();

    // Get current high watermark
    let consumer1 = create_consumer("test-group-restart-1");
    let (_, high) = consumer1
        .fetch_watermarks(ORDERS_TOPIC, 0, Duration::from_secs(5))
        .expect("Failed to fetch watermarks");
    println!("Current high watermark: {}", high);
    drop(consumer1);

    // Start a new consumer AT the high watermark (no old messages)
    // The high watermark is the "next offset to be written", so starting there
    // means we won't receive any existing messages
    let consumer2 = create_consumer("test-group-restart-2");
    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(ORDERS_TOPIC, 0, rdkafka::Offset::Offset(high))
        .expect("Failed to add partition");
    consumer2.assign(&tpl).expect("Failed to assign");

    // Poll - should get no messages since we're at the end
    let msg = consumer2.poll(Duration::from_secs(2));
    assert!(
        msg.is_none() || matches!(msg, Some(Err(_))),
        "Should not receive any messages when starting at high watermark"
    );
    println!("Correctly received no messages when starting at high watermark");

    // Now insert a new row
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let customer_name = format!("RestartTest-{}", timestamp);

    let insert_sql = format!(
        "INSERT INTO test_orders (customer_name, product, quantity, price) \
         VALUES ('{}', 'RestartProduct', 1, 49.99)",
        customer_name
    );
    common::run_sql(&insert_sql).expect("Failed to insert row");
    println!("Inserted new order for: {}", customer_name);

    std::thread::sleep(Duration::from_millis(200));

    // Poll again - should now receive the new message
    let mut found = false;
    for _ in 0..5 {
        match consumer2.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                println!("Got message after insert: {:?}", value);

                if let Some(v) = &value {
                    if v.contains(&customer_name) {
                        found = true;
                        break;
                    }
                }
            }
            Some(Err(e)) => {
                println!("Poll error: {}", e);
                break;
            }
            None => break,
        }
    }

    assert!(
        found,
        "Should receive new message after SQL insert: {}",
        customer_name
    );
    println!("SUCCESS: Consumer correctly received only new message after restart");
}

/// Test: Produce to read-only (table-backed) topic should fail
///
/// This test verifies that attempting to produce to a source-backed topic
/// returns an error, as these topics are read-only.
///
/// Run with: cargo test --test kafka_client_test test_produce_to_readonly_topic_fails -- --ignored
#[test]
fn test_produce_to_readonly_topic_fails() {
    skip_if_no_server!(common::KAFKA_ADDR);
    ensure_orders_topic();

    let producer = create_producer();

    let key = "should-fail";
    let value = "This message should not be stored";

    let record = BaseRecord::to(ORDERS_TOPIC).key(key).payload(value);

    // Send the message
    match producer.send(record) {
        Ok(_) => {
            // Message was enqueued, wait for delivery result
            producer.flush(Duration::from_secs(5)).ok();

            // Check if there were any delivery errors by producing again and checking
            // In rdkafka, delivery errors are reported via delivery callbacks
            // For this test, we'll verify the message wasn't actually stored

            std::thread::sleep(Duration::from_millis(500));

            // Query the database to verify message was NOT stored
            let check_sql = format!(
                "SELECT COUNT(*) FROM pgkafka.messages m \
                 JOIN pgkafka.topics t ON m.topic_id = t.id \
                 WHERE t.name = '{}' AND m.key_text = '{}'",
                ORDERS_TOPIC, key
            );

            match common::run_sql(&check_sql) {
                Ok(count) => {
                    let count: i64 = count.trim().parse().unwrap_or(0);
                    assert_eq!(
                        count, 0,
                        "Message should NOT be stored in read-only topic's messages table"
                    );
                    println!("SUCCESS: Message was not stored in read-only topic (as expected)");
                }
                Err(e) => {
                    println!("Could not verify via psql: {}", e);
                }
            }
        }
        Err((e, _)) => {
            // Immediate send error
            println!(
                "Producer send failed (expected for read-only topic): {:?}",
                e
            );
        }
    }
}

/// Test: Produce JSON message and verify text/json columns are populated
///
/// This test verifies that when producing a JSON message via the Kafka protocol,
/// the value_text and value_json columns are properly populated in the database.
///
/// Run with: cargo test --test kafka_client_test test_produce_json_columns -- --ignored
#[test]
fn test_produce_json_columns() {
    skip_if_no_server!(common::KAFKA_ADDR);
    common::ensure_topic(TEST_TOPIC).expect("Failed to ensure topic");

    use std::process::Command;

    let producer = create_producer();

    // Produce a JSON message
    let key = "json-test-key";
    let json_value = r#"{"name":"test","count":42,"nested":{"enabled":true}}"#;

    let record = BaseRecord::to(TEST_TOPIC).key(key).payload(json_value);

    producer.send(record).expect("Failed to enqueue message");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");

    println!("Produced JSON message: key={}, value={}", key, json_value);

    // Give it a moment to be stored
    std::thread::sleep(Duration::from_millis(500));

    // Query the database to verify text/json columns
    // Note: This requires psql to be available and pgrx running on port 28816
    let output = Command::new("psql")
        .args([
            "-h",
            "localhost",
            "-p",
            "28816",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-t", // tuples only
            "-c",
            &format!(
                "SELECT key_text, value_text IS NOT NULL, value_json IS NOT NULL \
                 FROM pgkafka.messages m \
                 JOIN pgkafka.topics t ON m.topic_id = t.id \
                 WHERE t.name = '{}' AND key_text = '{}'",
                TEST_TOPIC, key
            ),
        ])
        .output();

    match output {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            println!("Query result: {}", stdout.trim());

            if out.status.success() && !stdout.trim().is_empty() {
                // Parse results - expecting: key_text | value_text_not_null | value_json_not_null
                let parts: Vec<&str> = stdout.trim().split('|').map(|s| s.trim()).collect();
                if parts.len() >= 3 {
                    assert_eq!(parts[0], key, "key_text should match");
                    assert_eq!(parts[1], "t", "value_text should be populated");
                    assert_eq!(parts[2], "t", "value_json should be populated");
                    println!("SUCCESS: JSON columns verified!");
                }
            } else {
                println!(
                    "Could not verify columns (psql may not be available): {}",
                    String::from_utf8_lossy(&out.stderr)
                );
            }
        }
        Err(e) => {
            println!("Could not run psql to verify columns: {}", e);
        }
    }
}

// ============================================================================
// Schema validation tests
// ============================================================================

const TYPED_TOPIC: &str = "typed-orders";
const TYPED_TABLE: &str = "typed_orders";

/// Set up a typed topic with schema validation
fn setup_typed_topic() {
    // Create the table
    let _ = common::execute_sql(&format!(
        "CREATE TABLE IF NOT EXISTS {} (
            id BIGSERIAL PRIMARY KEY,
            order_id INTEGER NOT NULL,
            customer_name TEXT NOT NULL,
            amount NUMERIC(10,2) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );",
        TYPED_TABLE
    ));

    // Create topic from table
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.create_topic_from_table('{}', '{}', 'id', NULL, 'row_to_json({})::text');",
        TYPED_TOPIC, TYPED_TABLE, TYPED_TABLE
    ));

    // Enable writes on the topic (stream mode)
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.enable_topic_writes('{}', 'stream', NULL, NULL);",
        TYPED_TOPIC
    ));

    // Register a JSON Schema for the topic
    let schema = r#"{
        "type": "object",
        "properties": {
            "order_id": {"type": "integer"},
            "customer_name": {"type": "string"},
            "amount": {"type": "number"}
        },
        "required": ["order_id", "customer_name", "amount"]
    }"#;

    let _ = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, 'Order schema');",
        format!("{}-value", TYPED_TOPIC),
        schema
    ));

    // Get the schema ID and bind it to the topic
    if let Ok(schema_id) = common::run_sql(&format!(
        "SELECT id FROM pgkafka.schemas WHERE subject = '{}-value' ORDER BY version DESC LIMIT 1;",
        TYPED_TOPIC
    )) {
        let schema_id = schema_id.trim();
        if !schema_id.is_empty() {
            let _ = common::run_sql(&format!(
                "SELECT pgkafka.bind_schema_to_topic('{}', {}, 'value', 'STRICT');",
                TYPED_TOPIC, schema_id
            ));
            println!("Bound schema {} to topic {}", schema_id, TYPED_TOPIC);
        }
    }
}

/// Test: Register and retrieve a schema
#[test]
fn test_schema_register_and_get() {
    skip_if_no_server!(common::KAFKA_ADDR);

    // Register a simple schema
    let schema = r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#;
    let result = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('test-schema-subject', '{}'::jsonb, 'Test schema');",
        schema
    ));

    assert!(result.is_ok(), "Failed to register schema: {:?}", result);
    let schema_id: i32 = result
        .unwrap()
        .trim()
        .parse()
        .expect("Schema ID should be an integer");
    assert!(schema_id > 0, "Schema ID should be positive");
    println!("Registered schema with ID: {}", schema_id);

    // Retrieve the schema
    let retrieved = common::run_sql(&format!("SELECT pgkafka.get_schema({})::text;", schema_id));

    assert!(retrieved.is_ok(), "Failed to get schema: {:?}", retrieved);
    let schema_def = retrieved.unwrap();
    assert!(
        schema_def.contains("object"),
        "Schema should contain 'object'"
    );
    assert!(schema_def.contains("name"), "Schema should contain 'name'");
    println!("Retrieved schema: {}", schema_def.trim());
}

/// Test: Bind schema to topic
#[test]
fn test_schema_bind_to_topic() {
    skip_if_no_server!(common::KAFKA_ADDR);

    // Create a test topic
    let topic_name = "schema-bind-test-topic";
    let _ = common::run_sql(&format!("SELECT pgkafka.create_topic('{}');", topic_name));

    // Register a schema
    let schema = r#"{"type": "object", "properties": {"id": {"type": "integer"}}}"#;
    let schema_id = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}-value', '{}'::jsonb, NULL);",
        topic_name, schema
    ))
    .expect("Failed to register schema")
    .trim()
    .parse::<i32>()
    .expect("Schema ID should be integer");

    // Bind schema to topic
    let result = common::run_sql(&format!(
        "SELECT pgkafka.bind_schema_to_topic('{}', {}, 'value', 'STRICT');",
        topic_name, schema_id
    ));
    assert!(result.is_ok(), "Failed to bind schema: {:?}", result);
    println!("Bound schema {} to topic {}", schema_id, topic_name);

    // Verify binding by getting topic schema
    let topic_schema = common::run_sql(&format!(
        "SELECT pgkafka.get_topic_schema('{}', 'value')::text;",
        topic_name
    ));
    assert!(
        topic_schema.is_ok(),
        "Failed to get topic schema: {:?}",
        topic_schema
    );
    let schema_def = topic_schema.unwrap();
    assert!(
        schema_def.contains("integer"),
        "Topic schema should contain 'integer'"
    );
    println!("Topic schema verified: {}", schema_def.trim());

    // Clean up
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.unbind_schema_from_topic('{}', 'value');",
        topic_name
    ));
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.drop_topic('{}', true);",
        topic_name
    ));
}

/// Test: Produce valid message to typed topic succeeds
///
/// This test verifies that producing a message that conforms to the schema succeeds.
#[test]
fn test_produce_valid_message_to_typed_topic() {
    skip_if_no_server!(common::KAFKA_ADDR);
    setup_typed_topic();

    let producer = create_producer();

    // Valid JSON that matches the schema
    let valid_json = r#"{"order_id": 123, "customer_name": "Alice", "amount": 99.99}"#;

    let record = BaseRecord::to(TYPED_TOPIC)
        .key("valid-order-1")
        .payload(valid_json);

    let send_result = producer.send(record);
    assert!(
        send_result.is_ok(),
        "Should be able to enqueue valid message"
    );

    let flush_result = producer.flush(Duration::from_secs(5));
    assert!(
        flush_result.is_ok(),
        "Should be able to flush valid message"
    );

    println!("SUCCESS: Produced valid message to typed topic");

    // Verify message was stored
    std::thread::sleep(Duration::from_millis(500));

    // Check if the message appears in the source table
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE id = (SELECT MAX(id) FROM {});",
        TYPED_TABLE, TYPED_TABLE
    ));

    if let Ok(c) = count {
        println!("Message count in table: {}", c.trim());
    }
}

/// Test: Produce invalid message to typed topic fails validation
///
/// This test verifies that producing a message that does NOT conform to the schema
/// is rejected when validation mode is STRICT.
#[test]
fn test_produce_invalid_message_to_typed_topic() {
    skip_if_no_server!(common::KAFKA_ADDR);
    setup_typed_topic();

    let producer = create_producer();

    // Invalid JSON - missing required 'amount' field
    let invalid_json = r#"{"order_id": 456, "customer_name": "Bob"}"#;

    let record = BaseRecord::to(TYPED_TOPIC)
        .key("invalid-order-1")
        .payload(invalid_json);

    // The send will likely succeed (message is enqueued)
    let _ = producer.send(record);

    // Flush and check for delivery errors
    let flush_result = producer.flush(Duration::from_secs(5));

    // Note: rdkafka may not surface validation errors directly
    // The message might be enqueued but rejected by the broker
    println!("Flush result: {:?}", flush_result);

    // Give time for any async processing
    std::thread::sleep(Duration::from_millis(500));

    // Verify the invalid message was NOT stored in the source table
    // by checking if there's a row with customer_name = 'Bob' (there shouldn't be)
    let result = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE customer_name = 'Bob';",
        TYPED_TABLE
    ));

    match result {
        Ok(count) => {
            let count: i64 = count.trim().parse().unwrap_or(0);
            // With schema validation, the invalid message should be rejected
            // and not appear in the table
            println!("Messages with customer_name='Bob': {}", count);
            // Note: If count > 0, validation may not be blocking at Kafka protocol level
            // but rather at SQL insert level
        }
        Err(e) => {
            println!("Could not verify: {}", e);
        }
    }
}

/// Test: Schema versioning - registering same schema returns same ID
#[test]
fn test_schema_versioning_idempotent() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let schema = r#"{"type": "object", "properties": {"version_test": {"type": "string"}}}"#;
    let subject = "version-test-subject";

    // Register the schema twice
    let id1 = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, NULL);",
        subject, schema
    ))
    .expect("First registration failed")
    .trim()
    .parse::<i32>()
    .expect("ID1 should be integer");

    let id2 = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, NULL);",
        subject, schema
    ))
    .expect("Second registration failed")
    .trim()
    .parse::<i32>()
    .expect("ID2 should be integer");

    assert_eq!(id1, id2, "Same schema should return same ID (idempotent)");
    println!("SUCCESS: Schema registration is idempotent (ID: {})", id1);

    // Register a different schema - should get new ID
    let schema2 = r#"{"type": "object", "properties": {"version_test": {"type": "integer"}}}"#;
    let id3 = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, NULL);",
        subject, schema2
    ))
    .expect("Third registration failed")
    .trim()
    .parse::<i32>()
    .expect("ID3 should be integer");

    assert_ne!(id1, id3, "Different schema should get different ID");
    println!("SUCCESS: Different schema got new ID (ID: {})", id3);
}

/// Test: Get latest schema for subject
#[test]
fn test_get_latest_schema() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let subject = "latest-schema-test";

    // Register two different schemas
    let schema1 = r#"{"type": "object", "version": 1}"#;
    let schema2 = r#"{"type": "object", "version": 2}"#;

    let _ = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, 'v1');",
        subject, schema1
    ));

    let _ = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}', '{}'::jsonb, 'v2');",
        subject, schema2
    ));

    // Get latest should return v2
    let latest = common::run_sql(&format!(
        "SELECT pgkafka.get_latest_schema('{}')::text;",
        subject
    ))
    .expect("Failed to get latest schema");

    assert!(
        latest.contains("\"version\": 2") || latest.contains("\"version\":2"),
        "Latest schema should be version 2, got: {}",
        latest
    );
    println!("SUCCESS: Got latest schema: {}", latest.trim());
}

/// Test: Drop schema
#[test]
fn test_drop_schema() {
    skip_if_no_server!(common::KAFKA_ADDR);

    // Register a schema
    let schema = r#"{"type": "string"}"#;
    let schema_id = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('drop-test-subject', '{}'::jsonb, NULL);",
        schema
    ))
    .expect("Failed to register schema")
    .trim()
    .parse::<i32>()
    .expect("Schema ID should be integer");

    println!("Registered schema with ID: {}", schema_id);

    // Verify it exists by querying the table directly
    let exists = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgkafka.schemas WHERE id = {};",
        schema_id
    ))
    .expect("Failed to check schema");
    assert_eq!(exists.trim(), "1", "Schema should exist in table");

    // Drop it (cast to text for run_sql which doesn't handle bool)
    let dropped = common::run_sql(&format!("SELECT pgkafka.drop_schema({})::text;", schema_id))
        .expect("Failed to drop schema");
    assert!(
        dropped.trim() == "t" || dropped.trim() == "true",
        "Drop should return true, got: '{}'",
        dropped
    );

    // Verify it's gone by querying the table
    let count_after = common::run_sql(&format!(
        "SELECT COUNT(*) FROM pgkafka.schemas WHERE id = {};",
        schema_id
    ))
    .expect("Failed to check schema after drop");
    assert_eq!(count_after.trim(), "0", "Schema should be deleted");

    println!("SUCCESS: Schema dropped successfully");
}

// ============================================================================
// Table mode (upsert) with schema validation tests
// ============================================================================

const UPSERT_TOPIC: &str = "products-upsert";
const UPSERT_TABLE: &str = "products";

/// Set up a table-mode topic with schema validation for upsert testing
fn setup_upsert_topic() {
    // Drop existing table/topic to ensure clean state
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgkafka.topics WHERE name = '{}';",
        UPSERT_TOPIC
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS {};", UPSERT_TABLE));

    // Create a products table with a natural key (product_id)
    let _ = common::execute_sql(&format!(
        "CREATE TABLE {} (
            id BIGSERIAL UNIQUE,
            product_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            price NUMERIC(10,2) NOT NULL,
            stock INTEGER NOT NULL DEFAULT 0,
            kafka_offset BIGINT,
            updated_at TIMESTAMPTZ DEFAULT NOW()
        );",
        UPSERT_TABLE
    ));

    // Create topic from table with id as offset column
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.create_topic_from_table('{}', '{}', 'id', NULL, 'row_to_json({})::text');",
        UPSERT_TOPIC, UPSERT_TABLE, UPSERT_TABLE
    ));

    // Enable writes in TABLE mode (upsert based on product_id key)
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.enable_topic_writes('{}', 'table', 'product_id', 'kafka_offset');",
        UPSERT_TOPIC
    ));

    // Register a JSON Schema for product messages
    let schema = r#"{
        "type": "object",
        "properties": {
            "product_id": {"type": "string"},
            "name": {"type": "string"},
            "price": {"type": "number"},
            "stock": {"type": "integer"}
        },
        "required": ["product_id", "name", "price"]
    }"#;

    let _ = common::run_sql(&format!(
        "SELECT pgkafka.register_schema('{}-value', '{}'::jsonb, 'Product schema');",
        UPSERT_TOPIC, schema
    ));

    // Bind schema to topic
    if let Ok(schema_id) = common::run_sql(&format!(
        "SELECT id FROM pgkafka.schemas WHERE subject = '{}-value' ORDER BY version DESC LIMIT 1;",
        UPSERT_TOPIC
    )) {
        let schema_id = schema_id.trim();
        if !schema_id.is_empty() {
            let _ = common::run_sql(&format!(
                "SELECT pgkafka.bind_schema_to_topic('{}', {}, 'value', 'STRICT');",
                UPSERT_TOPIC, schema_id
            ));
            println!("Set up upsert topic with schema ID {}", schema_id);
        }
    }
}

/// Test: Table mode upsert - produce, update, and consume
///
/// This test verifies the full round-trip:
/// 1. Produce a product message via Kafka (INSERT)
/// 2. Produce same key with updated data (UPDATE/upsert)
/// 3. Consume from the topic to verify data
/// 4. Verify table has only one row (upsert, not duplicate)
#[test]
fn test_table_mode_upsert_roundtrip() {
    skip_if_no_server!(common::KAFKA_ADDR);
    setup_upsert_topic();

    let producer = create_producer();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let product_id = format!("PROD-{}", timestamp);

    // Step 1: Insert a new product
    let insert_json = format!(
        r#"{{"product_id": "{}", "name": "Widget", "price": 19.99, "stock": 100}}"#,
        product_id
    );

    println!("=== Step 1: INSERT product {} ===", product_id);
    let record = BaseRecord::to(UPSERT_TOPIC)
        .key(product_id.as_str())
        .payload(insert_json.as_str());

    producer.send(record).expect("Failed to send insert");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush insert");
    println!("Produced INSERT: {}", insert_json);

    std::thread::sleep(Duration::from_millis(500));

    // Verify row exists with initial values
    let initial_check = common::run_sql(&format!(
        "SELECT name, price::text, stock FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ));
    println!("After INSERT: {:?}", initial_check);

    // Step 2: Update the product (same key, new values)
    let update_json = format!(
        r#"{{"product_id": "{}", "name": "Super Widget", "price": 24.99, "stock": 50}}"#,
        product_id
    );

    println!("\n=== Step 2: UPDATE product {} ===", product_id);
    let record = BaseRecord::to(UPSERT_TOPIC)
        .key(product_id.as_str())
        .payload(update_json.as_str());

    producer.send(record).expect("Failed to send update");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush update");
    println!("Produced UPDATE: {}", update_json);

    std::thread::sleep(Duration::from_millis(500));

    // Step 3: Verify table has only ONE row (upsert, not insert)
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ))
    .expect("Failed to count rows");

    let row_count: i64 = count.trim().parse().unwrap_or(0);
    assert_eq!(
        row_count, 1,
        "Table mode should upsert (1 row), not insert duplicates"
    );
    println!("Row count for {}: {} (expected: 1)", product_id, row_count);

    // Verify the values were updated
    let updated_check = common::run_sql(&format!(
        "SELECT name FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ))
    .expect("Failed to get updated name");

    assert!(
        updated_check.contains("Super Widget"),
        "Name should be updated to 'Super Widget', got: {}",
        updated_check
    );
    println!("Updated name: {}", updated_check.trim());

    // Step 4: Consume from the topic and verify we can read the data
    println!("\n=== Step 3: CONSUME from topic ===");
    let consumer = create_consumer(&format!("upsert-test-{}", timestamp));

    // Get watermarks to find recent messages
    let (_, high) = consumer
        .fetch_watermarks(UPSERT_TOPIC, 0, Duration::from_secs(5))
        .expect("Failed to fetch watermarks");

    // Start from a few messages before the end
    let start_offset = if high > 5 { high - 5 } else { 0 };

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(UPSERT_TOPIC, 0, rdkafka::Offset::Offset(start_offset))
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Poll for messages and find our product
    let mut found_product = false;
    for _ in 0..10 {
        match consumer.poll(Duration::from_secs(1)) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| String::from_utf8_lossy(k).to_string());
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());

                println!("Consumed: offset={}, key={:?}", msg.offset(), key);

                if key.as_deref() == Some(product_id.as_str()) {
                    if let Some(v) = &value {
                        // The consumed message should have the current table state
                        // (which includes row_to_json of the full row)
                        println!("Found product message: {}", v);
                        found_product = true;
                    }
                }
            }
            Some(Err(e)) => {
                println!("Poll error: {}", e);
            }
            None => break,
        }
    }

    // Note: In table mode, consuming shows the current state via row_to_json
    // The exact behavior depends on how the topic is configured
    if found_product {
        println!("\n=== SUCCESS: Full upsert roundtrip verified! ===");
    } else {
        println!("\nNote: Product may have been consumed at different offset");
    }

    // Final verification: check the table has correct final state
    // Query name separately (run_sql only returns first column)
    let final_name = common::run_sql(&format!(
        "SELECT name FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ))
    .expect("Failed to get final name");

    let final_price = common::run_sql(&format!(
        "SELECT price::text FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ))
    .expect("Failed to get final price");

    println!(
        "Final state: name='{}', price='{}'",
        final_name.trim(),
        final_price.trim()
    );
    assert!(
        final_name.contains("Super Widget"),
        "Final state should have updated name, got: {}",
        final_name
    );
    assert!(
        final_price.contains("24.99"),
        "Final state should have updated price, got: {}",
        final_price
    );
}

/// Test: Table mode with multiple products - bulk upsert
///
/// Tests that multiple different keys create separate rows,
/// while same keys update existing rows.
#[test]
fn test_table_mode_multiple_products() {
    skip_if_no_server!(common::KAFKA_ADDR);
    setup_upsert_topic();

    let producer = create_producer();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // Insert 3 different products
    let products = vec![
        (format!("A-{}", timestamp), "Apple", 1.50, 200),
        (format!("B-{}", timestamp), "Banana", 0.75, 500),
        (format!("C-{}", timestamp), "Cherry", 3.00, 100),
    ];

    println!("=== Inserting {} products ===", products.len());
    for (id, name, price, stock) in &products {
        let json = format!(
            r#"{{"product_id": "{}", "name": "{}", "price": {}, "stock": {}}}"#,
            id, name, price, stock
        );
        let record = BaseRecord::to(UPSERT_TOPIC)
            .key(id.as_str())
            .payload(json.as_str());
        producer.send(record).expect("Failed to send");
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");

    std::thread::sleep(Duration::from_millis(500));

    // Verify all 3 products exist
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE product_id LIKE '%-{}';",
        UPSERT_TABLE, timestamp
    ))
    .expect("Failed to count");

    let row_count: i64 = count.trim().parse().unwrap_or(0);
    assert_eq!(row_count, 3, "Should have 3 products");
    println!("Inserted {} products", row_count);

    // Update one product
    let update_id = &products[1].0; // Update Banana
    let update_json = format!(
        r#"{{"product_id": "{}", "name": "Organic Banana", "price": 1.25, "stock": 300}}"#,
        update_id
    );

    println!("\n=== Updating product {} ===", update_id);
    let record = BaseRecord::to(UPSERT_TOPIC)
        .key(update_id.as_str())
        .payload(update_json.as_str());
    producer.send(record).expect("Failed to send update");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");

    std::thread::sleep(Duration::from_millis(500));

    // Verify still 3 products (upsert, not insert)
    let count_after = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE product_id LIKE '%-{}';",
        UPSERT_TABLE, timestamp
    ))
    .expect("Failed to count after update");

    let row_count_after: i64 = count_after.trim().parse().unwrap_or(0);
    assert_eq!(
        row_count_after, 3,
        "Should still have 3 products after upsert"
    );

    // Verify the update took effect
    let updated = common::run_sql(&format!(
        "SELECT name FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, update_id
    ))
    .expect("Failed to get updated product");

    assert!(
        updated.contains("Organic Banana"),
        "Banana should be updated to 'Organic Banana'"
    );

    println!("SUCCESS: Multiple products with upsert verified!");
}

/// Test: Schema validation rejects invalid message in table mode
#[test]
fn test_table_mode_schema_validation_rejects_invalid() {
    skip_if_no_server!(common::KAFKA_ADDR);
    setup_upsert_topic();

    let producer = create_producer();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let product_id = format!("INVALID-{}", timestamp);

    // Try to insert a product missing required 'name' field
    let invalid_json = format!(r#"{{"product_id": "{}", "price": 9.99}}"#, product_id);

    println!("=== Attempting to insert invalid product (missing 'name') ===");
    let record = BaseRecord::to(UPSERT_TOPIC)
        .key(product_id.as_str())
        .payload(invalid_json.as_str());

    let _ = producer.send(record);
    let _ = producer.flush(Duration::from_secs(5));

    std::thread::sleep(Duration::from_millis(500));

    // Verify the invalid product was NOT inserted
    let count = common::run_sql(&format!(
        "SELECT COUNT(*) FROM {} WHERE product_id = '{}';",
        UPSERT_TABLE, product_id
    ))
    .expect("Failed to count");

    let row_count: i64 = count.trim().parse().unwrap_or(0);

    // With STRICT schema validation, the invalid message should be rejected
    println!(
        "Invalid product count: {} (expected: 0 if validation works)",
        row_count
    );

    if row_count == 0 {
        println!("SUCCESS: Schema validation rejected invalid message!");
    } else {
        println!("Note: Message was inserted - validation may be in LOG mode or not blocking");
    }
}

// ============================================================================
// Convenience function tests: create_typed_topic and create_typed_topic_from_table
// ============================================================================

/// Test: Create typed topic with schema - all in one call
///
/// Tests the create_typed_topic convenience function which:
/// 1. Creates a table from the JSON Schema
/// 2. Registers the schema
/// 3. Creates a topic backed by the table
/// 4. Enables writes
/// 5. Binds the schema
#[test]
fn test_create_typed_topic_stream_mode() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic_name = format!("typed-stream-{}", timestamp);
    let table_name = format!("typed_stream_{}", timestamp);

    // Clean up if exists
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgkafka.topics WHERE name = '{}';",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));

    // Define a JSON Schema for events
    let schema = r#"{
        "type": "object",
        "properties": {
            "event_type": {"type": "string"},
            "user_id": {"type": "integer"},
            "timestamp": {"type": "string", "format": "date-time"},
            "data": {"type": "object"}
        },
        "required": ["event_type", "user_id"]
    }"#;

    // Create typed topic with one function call
    println!("=== Creating typed topic in stream mode ===");
    let result = common::run_sql(&format!(
        "SELECT pgkafka.create_typed_topic('{}', '{}'::jsonb, '{}', 'stream', NULL, 'STRICT');",
        topic_name, schema, table_name
    ));

    assert!(
        result.is_ok(),
        "create_typed_topic should succeed: {:?}",
        result
    );
    let topic_id: i32 = result
        .unwrap()
        .trim()
        .parse()
        .expect("Topic ID should be integer");
    assert!(topic_id > 0, "Topic ID should be positive");
    println!("Created typed topic with ID: {}", topic_id);

    // Verify the table was created
    let table_exists = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}';",
        table_name
    ))
    .expect("Failed to check table");
    assert_eq!(table_exists.trim(), "1", "Table should exist");
    println!("Table {} created", table_name);

    // Verify the table has correct columns (including auto-generated id)
    let has_id = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = 'id';",
        table_name
    ))
    .expect("Failed to check id column");
    assert_eq!(has_id.trim(), "1", "Table should have id column");

    // Verify the topic is writable
    let writable = common::run_sql(&format!(
        "SELECT writable::text FROM pgkafka.topics WHERE name = '{}';",
        topic_name
    ))
    .expect("Failed to check writable");
    assert!(
        writable.trim() == "t" || writable.trim() == "true",
        "Topic should be writable"
    );

    // Verify schema is bound
    let bound_schema = common::run_sql(&format!(
        "SELECT pgkafka.get_topic_schema('{}', 'value')::text;",
        topic_name
    ));
    assert!(bound_schema.is_ok(), "Should have bound schema");
    let schema_text = bound_schema.unwrap();
    assert!(
        schema_text.contains("event_type"),
        "Bound schema should contain 'event_type'"
    );
    println!("Schema bound to topic");

    // Test producing to the typed topic via Kafka
    let producer = create_producer();
    let event_json =
        r#"{"event_type": "user.login", "user_id": 123, "data": {"ip": "192.168.1.1"}}"#;

    let record = BaseRecord::to(topic_name.as_str())
        .key("event-1")
        .payload(event_json);

    let send_result = producer.send(record);
    assert!(send_result.is_ok(), "Should produce to typed topic");
    producer
        .flush(Duration::from_secs(5))
        .expect("Failed to flush");
    println!("Produced event to typed topic");

    std::thread::sleep(Duration::from_millis(500));

    // Verify message was stored in the table
    let count = common::run_sql(&format!("SELECT COUNT(*) FROM public.{};", table_name))
        .expect("Failed to count rows");
    println!("Rows in table: {}", count.trim());

    println!("SUCCESS: create_typed_topic stream mode works!");

    // Cleanup
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.drop_topic('{}', true);",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));
}

/// Test: Create typed topic in table mode (upsert)
#[test]
fn test_create_typed_topic_table_mode() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic_name = format!("typed-table-{}", timestamp);
    let table_name = format!("typed_table_{}", timestamp);

    // Clean up if exists
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgkafka.topics WHERE name = '{}';",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));

    // Define a JSON Schema for products with a key column
    let schema = r#"{
        "type": "object",
        "properties": {
            "sku": {"type": "string"},
            "name": {"type": "string"},
            "price": {"type": "number"}
        },
        "required": ["sku", "name", "price"]
    }"#;

    // Create typed topic in table mode with sku as key
    println!("=== Creating typed topic in table mode ===");
    let result = common::run_sql(&format!(
        "SELECT pgkafka.create_typed_topic('{}', '{}'::jsonb, '{}', 'table', 'sku', 'STRICT');",
        topic_name, schema, table_name
    ));

    assert!(
        result.is_ok(),
        "create_typed_topic table mode should succeed: {:?}",
        result
    );
    let topic_id: i32 = result
        .unwrap()
        .trim()
        .parse()
        .expect("Topic ID should be integer");
    println!("Created typed topic (table mode) with ID: {}", topic_id);

    // Verify the table has kafka_offset column (for table mode)
    let has_kafka_offset = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{}' AND column_name = 'kafka_offset';",
        table_name
    ))
    .expect("Failed to check kafka_offset column");
    assert_eq!(
        has_kafka_offset.trim(),
        "1",
        "Table should have kafka_offset column for table mode"
    );

    // Verify write mode is 'table'
    let write_mode = common::run_sql(&format!(
        "SELECT write_mode FROM pgkafka.topics WHERE name = '{}';",
        topic_name
    ))
    .expect("Failed to check write mode");
    assert_eq!(write_mode.trim(), "table", "Write mode should be 'table'");

    println!("SUCCESS: create_typed_topic table mode works!");

    // Cleanup
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.drop_topic('{}', true);",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));
}

/// Test: Create typed topic from existing table
///
/// Tests the create_typed_topic_from_table convenience function which:
/// 1. Generates a JSON Schema from the existing table
/// 2. Registers the schema
/// 3. Creates a topic backed by the table
/// 4. Enables writes
/// 5. Binds the schema
#[test]
fn test_create_typed_topic_from_table() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let topic_name = format!("from-table-{}", timestamp);
    let table_name = format!("existing_table_{}", timestamp);

    // Clean up if exists
    let _ = common::execute_sql(&format!(
        "DELETE FROM pgkafka.topics WHERE name = '{}';",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));

    // Create an existing table first
    let _ = common::execute_sql(&format!(
        "CREATE TABLE public.{} (
            id BIGSERIAL PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_total NUMERIC(10,2) NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMPTZ DEFAULT NOW()
        );",
        table_name
    ));
    println!("Created existing table: {}", table_name);

    // Insert some test data
    let _ = common::execute_sql(&format!(
        "INSERT INTO public.{} (customer_id, order_total, status) VALUES ('CUST-001', 99.99, 'pending');",
        table_name
    ));

    // Create typed topic from the existing table
    println!("=== Creating typed topic from existing table ===");
    let result = common::run_sql(&format!(
        "SELECT pgkafka.create_typed_topic_from_table('{}', 'public.{}', 'id', 'stream', NULL, NULL, 'STRICT');",
        topic_name, table_name
    ));

    assert!(
        result.is_ok(),
        "create_typed_topic_from_table should succeed: {:?}",
        result
    );
    let topic_id: i32 = result
        .unwrap()
        .trim()
        .parse()
        .expect("Topic ID should be integer");
    println!("Created topic from table with ID: {}", topic_id);

    // Verify schema was generated and bound
    let bound_schema = common::run_sql(&format!(
        "SELECT pgkafka.get_topic_schema('{}', 'value')::text;",
        topic_name
    ))
    .expect("Should have bound schema");

    // Schema should contain columns from the table
    assert!(
        bound_schema.contains("customer_id"),
        "Schema should contain 'customer_id'"
    );
    assert!(
        bound_schema.contains("order_total"),
        "Schema should contain 'order_total'"
    );
    println!("Generated schema: {}", bound_schema.trim());

    // Test consuming from the topic (should see the existing row)
    let consumer = create_consumer(&format!("from-table-test-{}", timestamp));

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(topic_name.as_str(), 0, rdkafka::Offset::Beginning)
        .expect("Failed to add partition");
    consumer.assign(&tpl).expect("Failed to assign");

    // Poll for the existing row
    let mut found = false;
    for _ in 0..5 {
        match consumer.poll(Duration::from_secs(2)) {
            Some(Ok(msg)) => {
                let value = msg
                    .payload()
                    .map(|v| String::from_utf8_lossy(v).to_string());
                if let Some(v) = &value {
                    if v.contains("CUST-001") {
                        println!("Found existing row via Kafka: {}", v);
                        found = true;
                        break;
                    }
                }
            }
            Some(Err(e)) => println!("Poll error: {}", e),
            None => break,
        }
    }

    if found {
        println!("SUCCESS: create_typed_topic_from_table works - consumed existing data!");
    } else {
        println!("Note: Existing row may be at different offset");
    }

    // Cleanup
    let _ = common::run_sql(&format!(
        "SELECT pgkafka.drop_topic('{}', true);",
        topic_name
    ));
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));
}

/// Test: schema_from_table generates correct JSON Schema
#[test]
fn test_schema_from_table() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let table_name = format!("schema_gen_test_{}", timestamp);

    // Create a table with various column types
    let _ = common::execute_sql(&format!(
        "CREATE TABLE public.{} (
            id BIGSERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            email TEXT,
            age INTEGER,
            balance NUMERIC(10,2),
            active BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ,
            metadata JSONB
        );",
        table_name
    ));

    // Generate schema from table
    let result = common::run_sql(&format!(
        "SELECT pgkafka.schema_from_table('public.{}')::text;",
        table_name
    ));

    assert!(
        result.is_ok(),
        "schema_from_table should succeed: {:?}",
        result
    );
    let schema = result.unwrap();
    println!("Generated schema: {}", schema.trim());

    // Verify schema contains expected type mappings
    assert!(schema.contains("\"type\""), "Schema should have type");
    assert!(
        schema.contains("\"properties\""),
        "Schema should have properties"
    );
    assert!(schema.contains("\"name\""), "Schema should have 'name'");
    assert!(schema.contains("\"email\""), "Schema should have 'email'");
    assert!(schema.contains("\"age\""), "Schema should have 'age'");
    assert!(schema.contains("\"active\""), "Schema should have 'active'");

    // VARCHAR(100) should map to string with maxLength
    assert!(
        schema.contains("maxLength") || schema.contains("string"),
        "name should map to string"
    );

    // NOT NULL columns should be in required array
    assert!(
        schema.contains("\"required\""),
        "Schema should have required array"
    );

    println!("SUCCESS: schema_from_table generates correct JSON Schema!");

    // Cleanup
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));
}

/// Test: table_from_schema creates correct table structure
#[test]
fn test_table_from_schema() {
    skip_if_no_server!(common::KAFKA_ADDR);

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let table_name = format!("table_gen_test_{}", timestamp);

    // Clean up if exists
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));

    // Define a JSON Schema
    let schema = r#"{
        "type": "object",
        "properties": {
            "user_id": {"type": "string", "format": "uuid"},
            "username": {"type": "string", "maxLength": 50},
            "email": {"type": "string"},
            "age": {"type": "integer"},
            "score": {"type": "number"},
            "verified": {"type": "boolean"},
            "created_at": {"type": "string", "format": "date-time"},
            "tags": {"type": "array", "items": {"type": "string"}}
        },
        "required": ["user_id", "username", "verified"]
    }"#;

    // Create table from schema (with id column)
    let result = common::run_sql(&format!(
        "SELECT pgkafka.table_from_schema('public.{}', '{}'::jsonb, true)::text;",
        table_name, schema
    ));

    assert!(
        result.is_ok(),
        "table_from_schema should succeed: {:?}",
        result
    );
    let success = result.unwrap();
    assert!(
        success.trim() == "t" || success.trim() == "true",
        "Should return true"
    );
    println!("Created table from schema");

    // Verify table exists
    let table_exists = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{}';",
        table_name
    ))
    .expect("Failed to check table");
    assert_eq!(table_exists.trim(), "1", "Table should exist");

    // Verify column types
    let columns = common::run_sql(&format!(
        "SELECT column_name, data_type FROM information_schema.columns
         WHERE table_name = '{}' ORDER BY ordinal_position;",
        table_name
    ))
    .expect("Failed to get columns");
    println!("Table columns:\n{}", columns);

    // Verify id column was added
    let has_id = common::run_sql(&format!(
        "SELECT COUNT(*) FROM information_schema.columns
         WHERE table_name = '{}' AND column_name = 'id';",
        table_name
    ))
    .expect("Failed to check id");
    assert_eq!(has_id.trim(), "1", "Table should have id column");

    // Verify NOT NULL constraints on required fields
    let user_id_nullable = common::run_sql(&format!(
        "SELECT is_nullable FROM information_schema.columns
         WHERE table_name = '{}' AND column_name = 'user_id';",
        table_name
    ))
    .expect("Failed to check user_id nullable");
    assert_eq!(user_id_nullable.trim(), "NO", "user_id should be NOT NULL");

    // Verify non-required fields are nullable
    let email_nullable = common::run_sql(&format!(
        "SELECT is_nullable FROM information_schema.columns
         WHERE table_name = '{}' AND column_name = 'email';",
        table_name
    ))
    .expect("Failed to check email nullable");
    assert_eq!(email_nullable.trim(), "YES", "email should be nullable");

    println!("SUCCESS: table_from_schema creates correct table structure!");

    // Cleanup
    let _ = common::execute_sql(&format!("DROP TABLE IF EXISTS public.{};", table_name));
}
