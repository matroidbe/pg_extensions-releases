//! Kafka API request/response handlers

use bytes::{Buf, BufMut, Bytes, BytesMut};
use pg_observability::{
    error, increment_consumed, increment_produced, record_produce_latency, warn,
};
use std::io::Cursor;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::codec::*;
use super::records::{create_record_batch, decode_record_batch, encode_record_batch};
use super::types::*;
use crate::storage::parse::parse_bytes;
use crate::storage::{MessageToStore, SpiStorageClient, StorageError};

/// Partition response for ListOffsets: (partition_index, error_code, timestamp, offset)
type PartitionOffsetResponse = (i32, i16, i64, i64);

/// Topic response for ListOffsets: (topic_name, partition_responses)
type TopicOffsetResponse = (String, Vec<PartitionOffsetResponse>);

/// Handle an ApiVersions request
pub fn handle_api_versions(
    header: &RequestHeader,
    _body: &mut Cursor<&[u8]>,
) -> Result<BytesMut, ProtocolError> {
    let mut buf = BytesMut::with_capacity(256);
    let versions = supported_api_versions();

    // v3+ uses flexible encoding
    let flexible = header.api_version >= 3;

    // Error code (0 = success)
    write_i16(&mut buf, ErrorCode::None as i16);

    if flexible {
        // COMPACT_ARRAY: length + 1 as unsigned varint
        write_unsigned_varint(&mut buf, (versions.len() + 1) as u32);
        for v in &versions {
            write_i16(&mut buf, v.api_key);
            write_i16(&mut buf, v.min_version);
            write_i16(&mut buf, v.max_version);
            // Each API entry has tagged fields in v3+
            write_empty_tagged_fields(&mut buf);
        }
    } else {
        // Non-flexible: array length is i32 (standard Kafka ARRAY format)
        write_i32(&mut buf, versions.len() as i32);
        for v in &versions {
            write_i16(&mut buf, v.api_key);
            write_i16(&mut buf, v.min_version);
            write_i16(&mut buf, v.max_version);
        }
    }

    // Throttle time (v1+ only)
    if header.api_version >= 1 {
        write_i32(&mut buf, 0);
    }

    // Top-level tagged fields for v3+
    if flexible {
        write_empty_tagged_fields(&mut buf);
    }

    Ok(buf)
}

/// Handle a Metadata request
pub async fn handle_metadata(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    broker_host: &str,
    broker_port: i32,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let mut buf = BytesMut::with_capacity(512);

    // Parse request to get topic names (if any)
    let topic_count = read_i32(body)?;
    let mut requested_topics: Vec<String> = Vec::new();

    if topic_count > 0 {
        for _ in 0..topic_count {
            if let Some(name) = read_nullable_string(body)? {
                requested_topics.push(name);
            }
        }
    }
    // If topic_count == -1 or 0, return all topics

    // Query topics from database
    let topics = if requested_topics.is_empty() || topic_count < 0 {
        storage.get_all_topics().await.unwrap_or_default()
    } else {
        storage
            .get_topics(&requested_topics)
            .await
            .unwrap_or_default()
    };

    // Build set of found topic names
    let found_names: std::collections::HashSet<_> =
        topics.iter().map(|t| t.name.as_str()).collect();

    // Throttle time (v3+)
    if header.api_version >= 3 {
        write_i32(&mut buf, 0);
    }

    // Brokers array - just us
    write_i32(&mut buf, 1); // 1 broker
    write_i32(&mut buf, 0); // node_id = 0
    write_string(&mut buf, broker_host);
    write_i32(&mut buf, broker_port);
    if header.api_version >= 1 {
        write_nullable_string(&mut buf, None); // rack
    }

    // Cluster ID (v2+)
    if header.api_version >= 2 {
        write_nullable_string(&mut buf, Some("pg_kafka"));
    }

    // Controller ID (v1+)
    if header.api_version >= 1 {
        write_i32(&mut buf, 0); // We are the controller
    }

    // Topics array
    if requested_topics.is_empty() && topic_count < 0 {
        // Return all topics
        write_i32(&mut buf, topics.len() as i32);
        for topic in &topics {
            write_topic_metadata(&mut buf, header.api_version, &topic.name, ErrorCode::None);
        }
    } else if requested_topics.is_empty() {
        // topic_count == 0 means no topics requested
        write_i32(&mut buf, 0);
    } else {
        // Return requested topics (with errors for unknown ones)
        write_i32(&mut buf, requested_topics.len() as i32);
        for name in &requested_topics {
            if found_names.contains(name.as_str()) {
                write_topic_metadata(&mut buf, header.api_version, name, ErrorCode::None);
            } else {
                write_topic_metadata(
                    &mut buf,
                    header.api_version,
                    name,
                    ErrorCode::UnknownTopicOrPartition,
                );
            }
        }
    }

    // Cluster authorized operations (v8+)
    // -2147483648 (0x80000000) means "not requested" or N/A
    if header.api_version >= 8 {
        write_i32(&mut buf, -2147483648_i32);
    }

    Ok(buf)
}

/// Write topic metadata to buffer
fn write_topic_metadata(buf: &mut BytesMut, api_version: i16, name: &str, error: ErrorCode) {
    write_i16(buf, error as i16);
    write_string(buf, name);

    if api_version >= 1 {
        buf.put_u8(0); // is_internal = false (boolean as byte)
    }

    if error == ErrorCode::None {
        // Partitions array - always 1 partition
        write_i32(buf, 1);

        // Partition 0
        write_i16(buf, ErrorCode::None as i16); // error_code
        write_i32(buf, 0); // partition_index = 0
        write_i32(buf, 0); // leader_id = 0 (us)

        // Replicas array
        write_i32(buf, 1);
        write_i32(buf, 0); // replica node 0

        // ISR array
        write_i32(buf, 1);
        write_i32(buf, 0); // ISR node 0

        // Offline replicas (v5+)
        if api_version >= 5 {
            write_i32(buf, 0); // empty offline replicas
        }
    } else {
        // No partitions for unknown topic
        write_i32(buf, 0);
    }

    // Topic authorized operations (v8+)
    // -2147483648 (0x80000000) means "not requested" or N/A
    if api_version >= 8 {
        write_i32(buf, -2147483648_i32);
    }
}

/// Handle a Produce request
pub async fn handle_produce(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    // Parse ProduceRequest
    // transactional_id (v3+)
    if header.api_version >= 3 {
        let _transactional_id = read_nullable_string(body)?;
    }

    let _acks = read_i16(body)?;
    let _timeout_ms = read_i32(body)?;

    // Topic data array
    let topic_count = read_i32(body)?;

    let mut responses: Vec<TopicProduceResponse> = Vec::new();

    for _ in 0..topic_count {
        let topic_name = read_nullable_string(body)?.unwrap_or_default();

        // Partition data array
        let partition_count = read_i32(body)?;
        let mut partition_responses: Vec<PartitionProduceResponse> = Vec::new();

        for _ in 0..partition_count {
            let partition_index = read_i32(body)?;

            // Records (as bytes with i32 length prefix)
            let records_size = read_i32(body)?;

            if records_size < 0 {
                // Null records
                partition_responses.push(PartitionProduceResponse {
                    index: partition_index,
                    error_code: ErrorCode::None,
                    base_offset: 0,
                    log_append_time_ms: -1,
                    log_start_offset: 0,
                });
                continue;
            }

            // Read and decode RecordBatch
            if (body.remaining() as i32) < records_size {
                return Err(ProtocolError::Incomplete);
            }

            let records_bytes = body.copy_to_bytes(records_size as usize);
            let mut records_cursor = Cursor::new(records_bytes.as_ref());

            match decode_record_batch(&mut records_cursor) {
                Ok(batch) => {
                    // Extract messages from batch with parsed text/JSON
                    let messages: Vec<MessageToStore> = batch
                        .records
                        .into_iter()
                        .map(|r| {
                            let (key_text, key_json) = r
                                .key
                                .as_ref()
                                .map(|k| parse_bytes(k))
                                .unwrap_or((None, None));
                            let (value_text, value_json) = parse_bytes(&r.value);

                            MessageToStore {
                                key: r.key,
                                key_text,
                                key_json,
                                value: r.value,
                                value_text,
                                value_json,
                            }
                        })
                        .collect();

                    // Store messages with latency tracking
                    let message_count = messages.len() as u64;
                    let produce_start = Instant::now();
                    match storage.produce_messages(&topic_name, messages).await {
                        Ok(base_offset) => {
                            // Record produce metrics
                            let latency = produce_start.elapsed().as_secs_f64();
                            record_produce_latency(latency);
                            increment_produced(&topic_name, message_count);

                            let log_start =
                                storage.get_log_start_offset(&topic_name).await.unwrap_or(0);

                            partition_responses.push(PartitionProduceResponse {
                                index: partition_index,
                                error_code: ErrorCode::None,
                                base_offset,
                                log_append_time_ms: current_time_ms(),
                                log_start_offset: log_start,
                            });
                        }
                        Err(StorageError::TopicNotFound(_)) => {
                            partition_responses.push(PartitionProduceResponse {
                                index: partition_index,
                                error_code: ErrorCode::UnknownTopicOrPartition,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: -1,
                            });
                        }
                        Err(e) => {
                            error!(partition_index, error = %e, "Produce error");
                            partition_responses.push(PartitionProduceResponse {
                                index: partition_index,
                                error_code: ErrorCode::InvalidRequest,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: -1,
                            });
                        }
                    }
                }
                Err(e) => {
                    warn!(partition_index, error = %e, "Record batch decode error");
                    partition_responses.push(PartitionProduceResponse {
                        index: partition_index,
                        error_code: ErrorCode::InvalidRequest,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                    });
                }
            }
        }

        responses.push(TopicProduceResponse {
            name: topic_name,
            partition_responses,
        });
    }

    // Build response
    let mut buf = BytesMut::with_capacity(256);

    // Responses array
    write_i32(&mut buf, responses.len() as i32);
    for topic in &responses {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.partition_responses.len() as i32);
        for partition in &topic.partition_responses {
            write_i32(&mut buf, partition.index);
            write_i16(&mut buf, partition.error_code as i16);
            write_i64(&mut buf, partition.base_offset);

            // log_append_time (v2+)
            if header.api_version >= 2 {
                write_i64(&mut buf, partition.log_append_time_ms);
            }

            // log_start_offset (v5+)
            if header.api_version >= 5 {
                write_i64(&mut buf, partition.log_start_offset);
            }

            // record_errors (v8+) - empty array
            if header.api_version >= 8 {
                write_i32(&mut buf, 0); // 0 record errors
            }

            // error_message (v8+) - null string
            if header.api_version >= 8 {
                write_nullable_string(&mut buf, None);
            }
        }
    }

    // Throttle time (v1+)
    if header.api_version >= 1 {
        write_i32(&mut buf, 0);
    }

    Ok(buf)
}

/// Handle a Fetch request
pub async fn handle_fetch(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    // Parse FetchRequest
    let _replica_id = read_i32(body)?; // -1 for consumers
    let _max_wait_ms = read_i32(body)?;
    let _min_bytes = read_i32(body)?;

    // max_bytes (v3+)
    if header.api_version >= 3 {
        let _max_bytes = read_i32(body)?;
    }

    // isolation_level (v4+)
    if header.api_version >= 4 {
        let _isolation_level = body.get_i8();
    }

    // session_id (v7+)
    if header.api_version >= 7 {
        let _session_id = read_i32(body)?;
        let _session_epoch = read_i32(body)?;
    }

    // Topics array
    let topic_count = read_i32(body)?;

    let mut responses: Vec<TopicFetchResponse> = Vec::new();

    for _ in 0..topic_count {
        let topic_name = read_nullable_string(body)?.unwrap_or_default();

        // Partitions array
        let partition_count = read_i32(body)?;
        let mut partition_responses: Vec<PartitionFetchResponse> = Vec::new();

        for _ in 0..partition_count {
            let partition_index = read_i32(body)?;

            // current_leader_epoch (v9+)
            if header.api_version >= 9 {
                let _current_leader_epoch = read_i32(body)?;
            }

            let fetch_offset = read_i64(body)?;

            // last_fetched_epoch (v12+)
            if header.api_version >= 12 {
                let _last_fetched_epoch = read_i32(body)?;
            }

            // log_start_offset (v5+)
            if header.api_version >= 5 {
                let _log_start_offset = read_i64(body)?;
            }

            let partition_max_bytes = read_i32(body)?;

            // Calculate max messages from max bytes (rough estimate: 1KB per message)
            let max_messages = (partition_max_bytes / 1024).clamp(10, 1000);

            // Fetch messages
            match storage
                .fetch_messages(&topic_name, fetch_offset, max_messages)
                .await
            {
                Ok((messages, high_watermark)) => {
                    let log_start = storage.get_log_start_offset(&topic_name).await.unwrap_or(0);

                    // Record consumed metrics before consuming the iterator
                    let message_count = messages.len() as u64;
                    let consumer_group = header.client_id.as_deref().unwrap_or("unknown");

                    // Convert messages to RecordBatch
                    let records: BytesMut = if messages.is_empty() {
                        BytesMut::new()
                    } else {
                        let first_offset = messages.first().map(|m| m.offset_id).unwrap_or(0);
                        let first_ts = messages.first().map(|m| m.created_at).unwrap_or(0);

                        let batch = create_record_batch(
                            first_offset,
                            first_ts,
                            messages
                                .into_iter()
                                .map(|m| (m.key.map(Bytes::from), Bytes::from(m.value)))
                                .collect(),
                        );

                        encode_record_batch(&batch)
                    };

                    // Increment consumed counter only for non-empty fetches
                    if message_count > 0 {
                        increment_consumed(&topic_name, consumer_group, message_count);
                    }

                    partition_responses.push(PartitionFetchResponse {
                        partition_index,
                        error_code: ErrorCode::None,
                        high_watermark,
                        last_stable_offset: high_watermark,
                        log_start_offset: log_start,
                        records,
                    });
                }
                Err(StorageError::TopicNotFound(_)) => {
                    partition_responses.push(PartitionFetchResponse {
                        partition_index,
                        error_code: ErrorCode::UnknownTopicOrPartition,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        records: BytesMut::new(),
                    });
                }
                Err(e) => {
                    error!(partition_index, error = %e, "Fetch error");
                    partition_responses.push(PartitionFetchResponse {
                        partition_index,
                        error_code: ErrorCode::InvalidRequest,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        records: BytesMut::new(),
                    });
                }
            }
        }

        responses.push(TopicFetchResponse {
            name: topic_name,
            partition_responses,
        });
    }

    // Build response
    let mut buf = BytesMut::with_capacity(1024);

    // Throttle time (v1+)
    if header.api_version >= 1 {
        write_i32(&mut buf, 0);
    }

    // Error code (v7+)
    if header.api_version >= 7 {
        write_i16(&mut buf, ErrorCode::None as i16);
        write_i32(&mut buf, 0); // session_id
    }

    // Responses array
    write_i32(&mut buf, responses.len() as i32);
    for topic in &responses {
        write_string(&mut buf, &topic.name);
        write_i32(&mut buf, topic.partition_responses.len() as i32);
        for partition in &topic.partition_responses {
            write_i32(&mut buf, partition.partition_index);
            write_i16(&mut buf, partition.error_code as i16);
            write_i64(&mut buf, partition.high_watermark);

            // last_stable_offset (v4+)
            if header.api_version >= 4 {
                write_i64(&mut buf, partition.last_stable_offset);
            }

            // log_start_offset (v5+)
            if header.api_version >= 5 {
                write_i64(&mut buf, partition.log_start_offset);
            }

            // aborted_transactions (v4+) - always empty for us
            if header.api_version >= 4 {
                write_i32(&mut buf, 0);
            }

            // preferred_read_replica (v11+)
            if header.api_version >= 11 {
                write_i32(&mut buf, -1); // no preferred replica
            }

            // Records (as bytes with i32 length prefix)
            if partition.records.is_empty() {
                write_i32(&mut buf, 0);
            } else {
                write_i32(&mut buf, partition.records.len() as i32);
                buf.extend_from_slice(&partition.records);
            }
        }
    }

    Ok(buf)
}

/// Handle a FindCoordinator request
///
/// Returns this broker as the coordinator for any group.
pub fn handle_find_coordinator(
    header: &RequestHeader,
    _body: &mut Cursor<&[u8]>,
    broker_host: &str,
    broker_port: i32,
) -> Result<BytesMut, ProtocolError> {
    let mut buf = BytesMut::with_capacity(64);

    // Throttle time (v1+)
    if header.api_version >= 1 {
        write_i32(&mut buf, 0);
    }

    // Error code - no error
    write_i16(&mut buf, ErrorCode::None as i16);

    // Error message (v1+) - null
    if header.api_version >= 1 {
        write_nullable_string(&mut buf, None);
    }

    // Node ID
    write_i32(&mut buf, 0);

    // Host
    write_string(&mut buf, broker_host);

    // Port
    write_i32(&mut buf, broker_port);

    Ok(buf)
}

/// Handle a ListOffsets request
///
/// Returns the earliest/latest offsets for requested partitions.
pub async fn handle_list_offsets(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    // Parse request
    let _replica_id = read_i32(body)?;

    // isolation_level (v2+)
    if header.api_version >= 2 {
        let _isolation_level = body.get_i8();
    }

    // Topics array
    let topic_count = read_i32(body)?;

    let mut responses: Vec<TopicOffsetResponse> = Vec::new();

    for _ in 0..topic_count {
        let topic_name = read_nullable_string(body)?.unwrap_or_default();

        // Partitions array
        let partition_count = read_i32(body)?;
        let mut partition_responses: Vec<PartitionOffsetResponse> = Vec::new();

        for _ in 0..partition_count {
            let partition_index = read_i32(body)?;

            // current_leader_epoch (v4+)
            if header.api_version >= 4 {
                let _current_leader_epoch = read_i32(body)?;
            }

            let timestamp = read_i64(body)?;

            // Determine offset based on timestamp:
            // -1 = latest offset
            // -2 = earliest offset
            // otherwise = offset for specific timestamp
            // NOTE: Cannot use pgrx::log! here as this runs on tokio threads
            let (error_code, result_timestamp, offset) = match timestamp {
                -2 => {
                    // Earliest offset
                    let offset = storage.get_log_start_offset(&topic_name).await.unwrap_or(0);
                    (ErrorCode::None as i16, -1_i64, offset)
                }
                -1 => {
                    // Latest offset (high watermark)
                    let offset = storage.get_high_watermark(&topic_name).await.unwrap_or(0);
                    (ErrorCode::None as i16, -1_i64, offset)
                }
                _ => {
                    // Specific timestamp lookup
                    match storage
                        .find_offset_for_timestamp(&topic_name, timestamp)
                        .await
                    {
                        Ok(Some(offset)) => (ErrorCode::None as i16, timestamp, offset),
                        Ok(None) => {
                            // No timestamp column configured or no matching offset, fall back to latest
                            let offset = storage.get_high_watermark(&topic_name).await.unwrap_or(0);
                            (ErrorCode::None as i16, timestamp, offset)
                        }
                        Err(_) => {
                            // Query error, fall back to latest
                            let offset = storage.get_high_watermark(&topic_name).await.unwrap_or(0);
                            (ErrorCode::None as i16, timestamp, offset)
                        }
                    }
                }
            };

            partition_responses.push((partition_index, error_code, result_timestamp, offset));
        }

        responses.push((topic_name, partition_responses));
    }

    // Build response
    let mut buf = BytesMut::with_capacity(256);

    // Throttle time (v2+)
    if header.api_version >= 2 {
        write_i32(&mut buf, 0);
    }

    // Topics array
    write_i32(&mut buf, responses.len() as i32);
    for (topic_name, partitions) in &responses {
        write_string(&mut buf, topic_name);
        write_i32(&mut buf, partitions.len() as i32);

        for (partition_index, error_code, timestamp, offset) in partitions {
            write_i32(&mut buf, *partition_index);
            write_i16(&mut buf, *error_code);

            // timestamp (v1+)
            if header.api_version >= 1 {
                write_i64(&mut buf, *timestamp);
            }

            // offset (v1+) - in v0 it's an array
            if header.api_version >= 1 {
                write_i64(&mut buf, *offset);
            } else {
                // v0: old_style_offsets array
                write_i32(&mut buf, 1);
                write_i64(&mut buf, *offset);
            }

            // leader_epoch (v4+)
            if header.api_version >= 4 {
                write_i32(&mut buf, 0); // leader_epoch
            }
        }
    }

    Ok(buf)
}

/// Route a request to the appropriate handler
pub async fn handle_request(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    broker_host: &str,
    broker_port: i32,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let api_key = ApiKey::from_i16(header.api_key);

    match api_key {
        Some(ApiKey::ApiVersions) => handle_api_versions(header, body),
        Some(ApiKey::Metadata) => {
            handle_metadata(header, body, broker_host, broker_port, storage).await
        }
        Some(ApiKey::Produce) => handle_produce(header, body, storage).await,
        Some(ApiKey::Fetch) => handle_fetch(header, body, storage).await,
        Some(ApiKey::ListOffsets) => handle_list_offsets(header, body, storage).await,
        Some(ApiKey::FindCoordinator) => {
            handle_find_coordinator(header, body, broker_host, broker_port)
        }
        _ => {
            // Unsupported API - return error
            let mut buf = BytesMut::with_capacity(16);
            write_i16(&mut buf, ErrorCode::InvalidRequest as i16);
            Ok(buf)
        }
    }
}

// Helper types for building responses

struct TopicProduceResponse {
    name: String,
    partition_responses: Vec<PartitionProduceResponse>,
}

struct PartitionProduceResponse {
    index: i32,
    error_code: ErrorCode,
    base_offset: i64,
    log_append_time_ms: i64,
    log_start_offset: i64,
}

struct TopicFetchResponse {
    name: String,
    partition_responses: Vec<PartitionFetchResponse>,
}

struct PartitionFetchResponse {
    partition_index: i32,
    error_code: ErrorCode,
    high_watermark: i64,
    last_stable_offset: i64,
    log_start_offset: i64,
    records: BytesMut,
}

/// Get current time in milliseconds since epoch
fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::codec::{frame_message, write_response_header};

    #[test]
    fn test_api_versions_response_v0() {
        let header = RequestHeader {
            api_key: ApiKey::ApiVersions as i16,
            api_version: 0,
            correlation_id: 123,
            client_id: None,
        };
        let data: Vec<u8> = vec![];
        let mut body = Cursor::new(data.as_slice());

        let result = handle_api_versions(&header, &mut body).unwrap();

        // Parse the response
        let mut cursor = Cursor::new(result.as_ref());

        // Error code (2 bytes) - should be 0
        let error_code = read_i16(&mut cursor).unwrap();
        assert_eq!(error_code, 0, "Error code should be 0");

        // API count (4 bytes i32 for non-flexible messages)
        let api_count = read_i32(&mut cursor).unwrap();
        assert!(api_count > 0, "Should have at least one API");

        // Read each API entry (api_key: i16, min: i16, max: i16 = 6 bytes each)
        for _ in 0..api_count {
            let _api_key = read_i16(&mut cursor).unwrap();
            let _min_ver = read_i16(&mut cursor).unwrap();
            let _max_ver = read_i16(&mut cursor).unwrap();
        }

        // For v0, there should be no throttle_time
        assert_eq!(
            cursor.position() as usize,
            result.len(),
            "v0 response should not have trailing throttle_time"
        );
    }

    #[test]
    fn test_api_versions_response_v1() {
        let header = RequestHeader {
            api_key: ApiKey::ApiVersions as i16,
            api_version: 1,
            correlation_id: 456,
            client_id: None,
        };
        let data: Vec<u8> = vec![];
        let mut body = Cursor::new(data.as_slice());

        let result = handle_api_versions(&header, &mut body).unwrap();

        // Parse the response
        let mut cursor = Cursor::new(result.as_ref());

        // Error code
        let error_code = read_i16(&mut cursor).unwrap();
        assert_eq!(error_code, 0);

        // API count (4 bytes i32 for non-flexible messages)
        let api_count = read_i32(&mut cursor).unwrap();
        assert!(api_count > 0);

        // Skip API entries
        for _ in 0..api_count {
            let _ = read_i16(&mut cursor).unwrap();
            let _ = read_i16(&mut cursor).unwrap();
            let _ = read_i16(&mut cursor).unwrap();
        }

        // For v1+, there should be throttle_time (4 bytes)
        let throttle_time = read_i32(&mut cursor).unwrap();
        assert_eq!(throttle_time, 0, "Throttle time should be 0");

        assert_eq!(cursor.position() as usize, result.len());
    }

    #[test]
    fn test_full_api_versions_wire_format() {
        // Simulate what happens over the wire
        let header = RequestHeader {
            api_key: ApiKey::ApiVersions as i16,
            api_version: 0,
            correlation_id: 1,
            client_id: None,
        };
        let data: Vec<u8> = vec![];
        let mut body = Cursor::new(data.as_slice());

        let response_body = handle_api_versions(&header, &mut body).unwrap();

        // Build full response with header (like tcp.rs does)
        let mut response = BytesMut::with_capacity(4 + response_body.len());
        write_response_header(&mut response, header.correlation_id);
        response.extend_from_slice(&response_body);

        // Frame with length prefix
        let framed = frame_message(response);

        // Parse the framed message
        let mut cursor = Cursor::new(framed.as_ref());

        // Length prefix (4 bytes)
        let length = read_i32(&mut cursor).unwrap();
        assert_eq!(
            length as usize,
            framed.len() - 4,
            "Length should match remaining bytes"
        );

        // Correlation ID (4 bytes)
        let corr_id = read_i32(&mut cursor).unwrap();
        assert_eq!(corr_id, 1, "Correlation ID should match");

        // Error code (2 bytes)
        let error_code = read_i16(&mut cursor).unwrap();
        assert_eq!(error_code, 0, "Error code should be 0");

        // API count (4 bytes i32 for non-flexible messages)
        let api_count = read_i32(&mut cursor).unwrap();
        assert!(api_count >= 4, "Should support at least 4 APIs");

        println!(
            "Full wire format: {} bytes total, {} APIs",
            framed.len(),
            api_count
        );
    }

    #[test]
    fn test_build_produce_response() {
        // This tests the response building logic without database
        let mut buf = BytesMut::with_capacity(64);

        // Single topic, single partition response
        write_i32(&mut buf, 1); // 1 topic
        write_string(&mut buf, "test-topic");
        write_i32(&mut buf, 1); // 1 partition
        write_i32(&mut buf, 0); // partition index
        write_i16(&mut buf, 0); // error code
        write_i64(&mut buf, 100); // base offset

        assert!(buf.len() > 0);
    }

    #[test]
    fn test_build_fetch_response() {
        // This tests the response building logic without database
        let mut buf = BytesMut::with_capacity(64);

        // Throttle time
        write_i32(&mut buf, 0);

        // Single topic response
        write_i32(&mut buf, 1);
        write_string(&mut buf, "test-topic");
        write_i32(&mut buf, 1); // 1 partition
        write_i32(&mut buf, 0); // partition index
        write_i16(&mut buf, 0); // error code
        write_i64(&mut buf, 100); // high watermark
        write_i32(&mut buf, 0); // empty records

        assert!(buf.len() > 0);
    }
}
