//! Protocol-level integration tests for pg_kafka
//!
//! These tests verify the Kafka protocol implementation by sending
//! raw protocol messages and checking responses.

mod common;

use bytes::{BufMut, BytesMut};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Build a Kafka request with header and body
fn build_request(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: &str,
    body: &[u8],
) -> BytesMut {
    let mut header = BytesMut::new();
    header.put_i16(api_key);
    header.put_i16(api_version);
    header.put_i32(correlation_id);

    // Client ID as nullable string
    header.put_i16(client_id.len() as i16);
    header.put_slice(client_id.as_bytes());

    let total_len = header.len() + body.len();
    let mut message = BytesMut::with_capacity(4 + total_len);
    message.put_i32(total_len as i32);
    message.put(header);
    message.put_slice(body);

    message
}

/// Send request and receive response
fn send_request(stream: &mut TcpStream, request: &[u8]) -> Vec<u8> {
    stream.write_all(request).expect("Failed to send request");
    stream.flush().expect("Failed to flush");

    // Read response length
    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .expect("Failed to read response length");
    let len = i32::from_be_bytes(len_buf) as usize;

    // Read response body
    let mut response = vec![0u8; len];
    stream
        .read_exact(&mut response)
        .expect("Failed to read response");

    response
}

/// Test ApiVersions request/response
#[test]
fn test_api_versions() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // ApiVersions request (API key 18, version 0)
    let request = build_request(18, 0, 1, "test-client", &[]);
    let response = send_request(&mut stream, &request);

    // Parse response
    // correlation_id (4 bytes) + error_code (2 bytes) + api_versions array
    assert!(response.len() >= 10, "Response too short");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 1, "Correlation ID mismatch");

    let error_code = i16::from_be_bytes([response[4], response[5]]);
    assert_eq!(error_code, 0, "Expected no error");

    let num_apis = i32::from_be_bytes([response[6], response[7], response[8], response[9]]);
    assert!(num_apis > 0, "Expected at least one API version");

    println!("ApiVersions: {} APIs supported", num_apis);
}

/// Test Metadata request/response
#[test]
fn test_metadata() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Metadata request for all topics (API key 3, version 0)
    // Body: topic_count = -1 (all topics)
    let mut body = BytesMut::new();
    body.put_i32(-1); // all topics

    let request = build_request(3, 0, 2, "test-client", &body);
    let response = send_request(&mut stream, &request);

    // Parse response
    assert!(response.len() >= 8, "Response too short");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 2, "Correlation ID mismatch");

    // Brokers array (4 bytes count + broker data)
    let broker_count = i32::from_be_bytes([response[4], response[5], response[6], response[7]]);
    assert_eq!(broker_count, 1, "Expected exactly one broker");

    println!("Metadata: {} broker(s)", broker_count);
}

/// Test Metadata request for specific topic (unknown)
#[test]
fn test_metadata_unknown_topic() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // Metadata request for unknown topic
    let mut body = BytesMut::new();
    body.put_i32(1); // 1 topic
    let topic_name = "unknown-topic-12345";
    body.put_i16(topic_name.len() as i16);
    body.put_slice(topic_name.as_bytes());

    let request = build_request(3, 0, 3, "test-client", &body);
    let response = send_request(&mut stream, &request);

    assert!(response.len() >= 8, "Response too short");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 3, "Correlation ID mismatch");

    println!(
        "Metadata for unknown topic returned {} bytes",
        response.len()
    );
}

/// Build a ProduceRequest with a single message
fn build_produce_request(topic: &str, key: Option<&[u8]>, value: &[u8]) -> BytesMut {
    let mut body = BytesMut::new();

    // acks
    body.put_i16(-1); // all replicas

    // timeout_ms
    body.put_i32(5000);

    // topic_data array
    body.put_i32(1); // 1 topic

    // topic name
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());

    // partition_data array
    body.put_i32(1); // 1 partition

    // partition index
    body.put_i32(0);

    // Build RecordBatch
    let record_batch = build_simple_record_batch(key, value);

    // records (length-prefixed)
    body.put_i32(record_batch.len() as i32);
    body.put(record_batch);

    body
}

/// Build a minimal RecordBatch with one record
fn build_simple_record_batch(key: Option<&[u8]>, value: &[u8]) -> BytesMut {
    // Build the record first
    let mut record = BytesMut::new();
    record.put_i8(0); // attributes

    // timestamp_delta as varint (0)
    record.put_u8(0);

    // offset_delta as varint (0)
    record.put_u8(0);

    // key as varlen bytes
    match key {
        None => record.put_u8(0x01), // varint -1 encoded
        Some(k) => {
            put_varint(&mut record, k.len() as i32);
            record.put_slice(k);
        }
    }

    // value as varlen bytes
    put_varint(&mut record, value.len() as i32);
    record.put_slice(value);

    // headers count (0)
    record.put_u8(0);

    // Wrap record with length prefix
    let mut record_with_len = BytesMut::new();
    put_varint(&mut record_with_len, record.len() as i32);
    record_with_len.extend_from_slice(&record);

    // Build batch body (after base_offset and batch_length)
    let mut batch_body = BytesMut::new();
    batch_body.put_i32(0); // partition_leader_epoch
    batch_body.put_i8(2); // magic = 2
    batch_body.put_u32(0); // CRC placeholder
    batch_body.put_i16(0); // attributes
    batch_body.put_i32(0); // last_offset_delta
    batch_body.put_i64(0); // first_timestamp
    batch_body.put_i64(0); // max_timestamp
    batch_body.put_i64(-1); // producer_id
    batch_body.put_i16(-1); // producer_epoch
    batch_body.put_i32(-1); // base_sequence
    batch_body.put_i32(1); // records count
    batch_body.extend_from_slice(&record_with_len);

    // Build complete batch
    let mut batch = BytesMut::new();
    batch.put_i64(0); // base_offset
    batch.put_i32(batch_body.len() as i32); // batch_length
    batch.extend_from_slice(&batch_body);

    batch
}

/// Write a varint (zigzag encoded)
fn put_varint(buf: &mut BytesMut, value: i32) {
    let mut v = ((value << 1) ^ (value >> 31)) as u32;
    while v >= 0x80 {
        buf.put_u8((v as u8) | 0x80);
        v >>= 7;
    }
    buf.put_u8(v as u8);
}

/// Test Produce request (requires topic to exist)
#[test]
fn test_produce() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let body = build_produce_request("test-topic", Some(b"key1"), b"Hello, pg_kafka!");
    let request = build_request(0, 0, 4, "test-client", &body);
    let response = send_request(&mut stream, &request);

    assert!(response.len() >= 8, "Response too short");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 4, "Correlation ID mismatch");

    println!("Produce response: {} bytes", response.len());
}

/// Build a FetchRequest
fn build_fetch_request(topic: &str, offset: i64, max_bytes: i32) -> BytesMut {
    let mut body = BytesMut::new();

    // replica_id (-1 for consumers)
    body.put_i32(-1);

    // max_wait_ms
    body.put_i32(1000);

    // min_bytes
    body.put_i32(1);

    // topics array
    body.put_i32(1); // 1 topic

    // topic name
    body.put_i16(topic.len() as i16);
    body.put_slice(topic.as_bytes());

    // partitions array
    body.put_i32(1); // 1 partition

    // partition index
    body.put_i32(0);

    // fetch_offset
    body.put_i64(offset);

    // partition_max_bytes
    body.put_i32(max_bytes);

    body
}

/// Test Fetch request (requires topic with messages)
#[test]
fn test_fetch() {
    skip_if_no_server!(common::KAFKA_ADDR);
    let mut stream = TcpStream::connect("127.0.0.1:9092").expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let body = build_fetch_request("test-topic", 0, 1024 * 1024);
    let request = build_request(1, 0, 5, "test-client", &body);
    let response = send_request(&mut stream, &request);

    assert!(response.len() >= 8, "Response too short");

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 5, "Correlation ID mismatch");

    println!("Fetch response: {} bytes", response.len());
}
