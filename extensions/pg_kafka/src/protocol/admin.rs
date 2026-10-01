//! Admin APIs: CreateTopics (19) and DeleteTopics (20).
//!
//! These map onto the same rows `pgkafka.create_topic()` / `drop_topic()`
//! manage, so standard tooling (kafka-topics, GUIs, kafkajs admin) can
//! manage topics without out-of-band SQL. Only non-flexible versions are
//! implemented.

use bytes::BytesMut;
use pg_observability::{info, warn};
use std::io::Cursor;

use super::codec::*;
use super::types::*;
use crate::storage::groups::CreateTopicOutcome;
use crate::storage::SpiStorageClient;

/// Kafka's topic-name limit.
const MAX_TOPIC_NAME_LEN: usize = 249;

/// Validate a topic name the way a Kafka broker does.
pub fn validate_topic_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        return Err("Topic name is illegal, it can't be empty");
    }
    if name == "." || name == ".." {
        return Err("Topic name cannot be \".\" or \"..\"");
    }
    if name.len() > MAX_TOPIC_NAME_LEN {
        return Err("Topic name is illegal, it can't be longer than 249 characters");
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(
            "Topic name is illegal, valid characters are ASCII alphanumerics, '.', '_' and '-'",
        );
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
// CreateTopics (19)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
    /// Explicit partition assignments (ignored: single broker)
    pub assignments: Vec<(i32, Vec<i32>)>,
    /// Topic configs (ignored)
    pub configs: Vec<(String, Option<String>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

pub fn parse_create_topics_request(
    version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<CreateTopicsRequest, ProtocolError> {
    let n = read_array_len(body)?;
    let mut topics = Vec::with_capacity(n);
    for _ in 0..n {
        let name = read_string(body)?;
        let num_partitions = read_i32(body)?;
        let replication_factor = read_i16(body)?;
        let na = read_array_len(body)?;
        let mut assignments = Vec::with_capacity(na);
        for _ in 0..na {
            let partition_index = read_i32(body)?;
            let nb = read_array_len(body)?;
            let mut brokers = Vec::with_capacity(nb);
            for _ in 0..nb {
                brokers.push(read_i32(body)?);
            }
            assignments.push((partition_index, brokers));
        }
        let nc = read_array_len(body)?;
        let mut configs = Vec::with_capacity(nc);
        for _ in 0..nc {
            let key = read_string(body)?;
            let value = read_nullable_string(body)?;
            configs.push((key, value));
        }
        topics.push(CreatableTopic {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        });
    }
    let timeout_ms = read_i32(body)?;
    let validate_only = if version >= 1 {
        read_bool(body)?
    } else {
        false
    };
    Ok(CreateTopicsRequest {
        topics,
        timeout_ms,
        validate_only,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTopicResult {
    pub name: String,
    pub error_code: ErrorCode,
    pub error_message: Option<String>,
}

pub fn encode_create_topics_response(version: i16, results: &[CreateTopicResult]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    if version >= 2 {
        write_i32(&mut buf, 0);
    }
    write_i32(&mut buf, results.len() as i32);
    for r in results {
        write_string(&mut buf, &r.name);
        write_i16(&mut buf, r.error_code as i16);
        if version >= 1 {
            write_nullable_string(&mut buf, r.error_message.as_deref());
        }
    }
    buf
}

/// Decide the outcome for one topic before touching storage.
///
/// pg_kafka topics have exactly one partition, so a request for more is
/// rejected honestly rather than silently creating a single partition the
/// client does not expect.
pub fn precheck_create(topic: &CreatableTopic) -> Result<(), (ErrorCode, String)> {
    if let Err(msg) = validate_topic_name(&topic.name) {
        return Err((ErrorCode::InvalidTopic, msg.to_string()));
    }
    if topic.num_partitions > 1 || topic.assignments.len() > 1 {
        return Err((
            ErrorCode::InvalidPartitions,
            "pg_kafka topics have exactly one partition".to_string(),
        ));
    }
    Ok(())
}

async fn create_topics(
    storage: &SpiStorageClient,
    req: &CreateTopicsRequest,
) -> Vec<CreateTopicResult> {
    let mut results = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let (error_code, error_message) = match precheck_create(topic) {
            Err((code, msg)) => (code, Some(msg)),
            Ok(()) if req.validate_only => (ErrorCode::None, None),
            Ok(()) => match storage.create_native_topic(&topic.name).await {
                Ok(CreateTopicOutcome::Created) => {
                    info!(topic = %topic.name, "CreateTopics: created");
                    (ErrorCode::None, None)
                }
                Ok(CreateTopicOutcome::AlreadyExists) => (
                    ErrorCode::TopicAlreadyExists,
                    Some(format!("Topic '{}' already exists.", topic.name)),
                ),
                Err(e) => {
                    warn!(topic = %topic.name, error = %e, "CreateTopics failed");
                    (ErrorCode::InvalidRequest, Some(e.to_string()))
                }
            },
        };
        results.push(CreateTopicResult {
            name: topic.name.clone(),
            error_code,
            error_message,
        });
    }
    results
}

pub async fn handle_create_topics(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_create_topics_request(header.api_version, body)?;
    let results = create_topics(storage, &req).await;
    Ok(encode_create_topics_response(header.api_version, &results))
}

// ═══════════════════════════════════════════════════════════════════════
// DeleteTopics (20)
// ═══════════════════════════════════════════════════════════════════════

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

pub fn parse_delete_topics_request(
    _version: i16,
    body: &mut Cursor<&[u8]>,
) -> Result<DeleteTopicsRequest, ProtocolError> {
    let topic_names = read_string_array(body)?;
    let timeout_ms = read_i32(body)?;
    Ok(DeleteTopicsRequest {
        topic_names,
        timeout_ms,
    })
}

pub fn encode_delete_topics_response(version: i16, results: &[(String, ErrorCode)]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    if version >= 1 {
        write_i32(&mut buf, 0);
    }
    write_i32(&mut buf, results.len() as i32);
    for (name, code) in results {
        write_string(&mut buf, name);
        write_i16(&mut buf, *code as i16);
    }
    buf
}

async fn delete_topics(
    storage: &SpiStorageClient,
    req: &DeleteTopicsRequest,
) -> Vec<(String, ErrorCode)> {
    let mut results = Vec::with_capacity(req.topic_names.len());
    for name in &req.topic_names {
        let code = match storage.delete_topic(name).await {
            Ok(true) => {
                info!(topic = %name, "DeleteTopics: deleted");
                ErrorCode::None
            }
            Ok(false) => ErrorCode::UnknownTopicOrPartition,
            Err(e) => {
                warn!(topic = %name, error = %e, "DeleteTopics failed");
                ErrorCode::InvalidRequest
            }
        };
        results.push((name.clone(), code));
    }
    results
}

pub async fn handle_delete_topics(
    header: &RequestHeader,
    body: &mut Cursor<&[u8]>,
    storage: &SpiStorageClient,
) -> Result<BytesMut, ProtocolError> {
    let req = parse_delete_topics_request(header.api_version, body)?;
    let results = delete_topics(storage, &req).await;
    Ok(encode_delete_topics_response(header.api_version, &results))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Buf;

    fn cursor(buf: &BytesMut) -> Cursor<&[u8]> {
        Cursor::new(buf.as_ref())
    }

    #[test]
    fn test_validate_topic_name() {
        assert!(validate_topic_name("orders").is_ok());
        assert!(validate_topic_name("my.topic_v2-x").is_ok());
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
        assert!(validate_topic_name("has space").is_err());
        assert!(validate_topic_name("üñí").is_err());
        assert!(validate_topic_name(&"a".repeat(250)).is_err());
        assert!(validate_topic_name(&"a".repeat(249)).is_ok());
    }

    #[test]
    fn test_precheck_create_rejects_multiple_partitions() {
        let t = CreatableTopic {
            name: "t".into(),
            num_partitions: 3,
            replication_factor: 1,
            assignments: vec![],
            configs: vec![],
        };
        assert_eq!(
            precheck_create(&t).unwrap_err().0,
            ErrorCode::InvalidPartitions
        );
        let ok = CreatableTopic {
            num_partitions: -1,
            ..t.clone()
        };
        assert!(precheck_create(&ok).is_ok());
        let one = CreatableTopic {
            num_partitions: 1,
            ..t
        };
        assert!(precheck_create(&one).is_ok());
    }

    fn create_body(version: i16) -> BytesMut {
        let mut b = BytesMut::new();
        write_i32(&mut b, 1);
        write_string(&mut b, "new-topic");
        write_i32(&mut b, 1);
        write_i16(&mut b, 1);
        write_i32(&mut b, 1); // assignments
        write_i32(&mut b, 0);
        write_i32(&mut b, 1);
        write_i32(&mut b, 0);
        write_i32(&mut b, 1); // configs
        write_string(&mut b, "retention.ms");
        write_nullable_string(&mut b, Some("1000"));
        write_i32(&mut b, 5000);
        if version >= 1 {
            write_bool(&mut b, true);
        }
        b
    }

    #[test]
    fn test_parse_create_topics_all_versions() {
        for version in 0..=4 {
            let body = create_body(version);
            let mut cur = cursor(&body);
            let req = parse_create_topics_request(version, &mut cur).unwrap();
            assert_eq!(req.topics.len(), 1);
            assert_eq!(req.topics[0].name, "new-topic");
            assert_eq!(req.topics[0].num_partitions, 1);
            assert_eq!(req.topics[0].assignments, vec![(0, vec![0])]);
            assert_eq!(
                req.topics[0].configs,
                vec![("retention.ms".to_string(), Some("1000".to_string()))]
            );
            assert_eq!(req.timeout_ms, 5000);
            assert_eq!(req.validate_only, version >= 1);
            assert_eq!(cur.remaining(), 0, "v{} left bytes unread", version);
        }
    }

    #[test]
    fn test_encode_create_topics_response_versions() {
        let results = vec![CreateTopicResult {
            name: "t".into(),
            error_code: ErrorCode::TopicAlreadyExists,
            error_message: Some("exists".into()),
        }];
        let v0 = encode_create_topics_response(0, &results);
        assert_eq!(v0.len(), 4 + 3 + 2);
        let v1 = encode_create_topics_response(1, &results);
        assert_eq!(v1.len(), v0.len() + 2 + 6);
        let v2 = encode_create_topics_response(2, &results);
        assert_eq!(v2.len(), v1.len() + 4);
        let mut cur = cursor(&v2);
        assert_eq!(read_i32(&mut cur).unwrap(), 0);
        assert_eq!(read_i32(&mut cur).unwrap(), 1);
        assert_eq!(read_string(&mut cur).unwrap(), "t");
        assert_eq!(read_i16(&mut cur).unwrap(), 36);
        assert_eq!(
            read_nullable_string(&mut cur).unwrap().as_deref(),
            Some("exists")
        );
    }

    #[test]
    fn test_delete_topics_roundtrip() {
        let mut b = BytesMut::new();
        write_i32(&mut b, 2);
        write_string(&mut b, "a");
        write_string(&mut b, "b");
        write_i32(&mut b, 1000);
        let mut cur = cursor(&b);
        let req = parse_delete_topics_request(3, &mut cur).unwrap();
        assert_eq!(req.topic_names, vec!["a", "b"]);
        assert_eq!(req.timeout_ms, 1000);
        assert_eq!(cur.remaining(), 0);

        let results = vec![
            ("a".to_string(), ErrorCode::None),
            ("b".to_string(), ErrorCode::UnknownTopicOrPartition),
        ];
        let v0 = encode_delete_topics_response(0, &results);
        assert_eq!(v0.len(), 4 + (3 + 2) * 2);
        let v1 = encode_delete_topics_response(1, &results);
        assert_eq!(v1.len(), v0.len() + 4);
        let mut cur = cursor(&v1);
        assert_eq!(read_i32(&mut cur).unwrap(), 0);
        assert_eq!(read_i32(&mut cur).unwrap(), 2);
        assert_eq!(read_string(&mut cur).unwrap(), "a");
        assert_eq!(read_i16(&mut cur).unwrap(), 0);
        assert_eq!(read_string(&mut cur).unwrap(), "b");
        assert_eq!(read_i16(&mut cur).unwrap(), 3);
    }
}
