//! SPI-based storage client for pg_kafka
//!
//! Uses the SPI bridge to execute queries via PostgreSQL's in-process SPI interface.
//! This eliminates network overhead compared to tokio-postgres.

use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;

use super::source_insert::{SourceInsertError, SourceInserter};
use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};
use super::validation::{SchemaValidator, ValidationResult};

/// Storage error type
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("SPI error: {0}")]
    Spi(#[from] SpiError),
    #[error("topic not found: {0}")]
    TopicNotFound(String),
    #[error("topic '{0}' is backed by source table and is read-only")]
    ReadOnlyTopic(String),
    #[error("validation failed: {0}")]
    ValidationFailed(String),
    #[error("source insert failed: {0}")]
    SourceInsertFailed(#[from] SourceInsertError),
}

/// Topic metadata from the database
#[derive(Debug, Clone)]
pub struct TopicInfo {
    pub id: i32,
    pub name: String,
    /// Source table for table-backed topics (e.g., 'public.orders')
    pub source_table: Option<String>,
    /// Column to use as offset for source tables
    pub offset_column: Option<String>,
    /// Column to use as value for source tables
    pub value_column: Option<String>,
    /// SQL expression to use as value for source tables
    pub value_expr: Option<String>,
    /// Column to use as key for source tables
    pub key_column: Option<String>,
    /// SQL expression to use as key for source tables
    pub key_expr: Option<String>,
    /// Column to use as timestamp for source tables
    pub timestamp_column: Option<String>,
    /// SQL expression to use as timestamp for source tables
    pub timestamp_expr: Option<String>,
    /// Whether this topic supports writes (for source-backed topics)
    pub writable: bool,
    /// Write mode: "stream" (append) or "table" (upsert)
    pub write_mode: Option<String>,
    /// For table mode: column to match Kafka message key
    pub write_key_column: Option<String>,
    /// For table mode: column to track Kafka offset
    pub kafka_offset_column: Option<String>,
}

impl TopicInfo {
    /// Returns true if this topic is backed by a source table
    pub fn is_source_backed(&self) -> bool {
        self.source_table.is_some()
    }

    /// Returns true if this topic supports writes
    pub fn is_writable(&self) -> bool {
        self.writable
    }

    /// Returns true if this topic uses stream (append-only) mode
    pub fn is_stream_mode(&self) -> bool {
        self.write_mode.as_deref() == Some("stream")
    }

    /// Returns true if this topic uses table (upsert) mode
    pub fn is_table_mode(&self) -> bool {
        self.write_mode.as_deref() == Some("table")
    }
}

/// A message record from the database
#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub offset_id: i64,
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub created_at: i64, // Unix timestamp in ms
}

/// A message to be stored with parsed text/JSON columns
#[derive(Debug, Clone)]
pub struct MessageToStore {
    pub key: Option<Bytes>,
    pub key_text: Option<String>,
    pub key_json: Option<Value>,
    pub value: Bytes,
    pub value_text: Option<String>,
    pub value_json: Option<Value>,
}

/// SPI-based storage client
pub struct SpiStorageClient {
    bridge: Arc<SpiBridge>,
}

impl SpiStorageClient {
    /// Create a new SPI storage client with the given bridge
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    /// SQL columns to select for TopicInfo
    const TOPIC_COLUMNS: &'static str =
        "id, name, source_table, offset_column, value_column, value_expr, key_column, key_expr, timestamp_column, timestamp_expr, writable, write_mode, write_key_column, kafka_offset_column";

    /// Build TopicInfo from SPI row values
    fn topic_from_row(row: &super::spi_bridge::SpiRow) -> TopicInfo {
        TopicInfo {
            id: row.get_i32(0).unwrap_or(0),
            name: row.get_string(1).unwrap_or_default(),
            source_table: row.get_string(2),
            offset_column: row.get_string(3),
            value_column: row.get_string(4),
            value_expr: row.get_string(5),
            key_column: row.get_string(6),
            key_expr: row.get_string(7),
            timestamp_column: row.get_string(8),
            timestamp_expr: row.get_string(9),
            writable: row.get_bool(10).unwrap_or(false),
            write_mode: row.get_string(11),
            write_key_column: row.get_string(12),
            kafka_offset_column: row.get_string(13),
        }
    }

    /// Column types for TopicInfo query
    fn topic_column_types() -> Vec<ColumnType> {
        vec![
            ColumnType::Int32, // id
            ColumnType::Text,  // name
            ColumnType::Text,  // source_table
            ColumnType::Text,  // offset_column
            ColumnType::Text,  // value_column
            ColumnType::Text,  // value_expr
            ColumnType::Text,  // key_column
            ColumnType::Text,  // key_expr
            ColumnType::Text,  // timestamp_column
            ColumnType::Text,  // timestamp_expr
            ColumnType::Bool,  // writable
            ColumnType::Text,  // write_mode
            ColumnType::Text,  // write_key_column
            ColumnType::Text,  // kafka_offset_column
        ]
    }

    /// Get all topics
    pub async fn get_all_topics(&self) -> Result<Vec<TopicInfo>, StorageError> {
        let query = format!(
            "SELECT {} FROM pgkafka.topics ORDER BY name",
            Self::TOPIC_COLUMNS
        );

        let result = self
            .bridge
            .query(&query, vec![], Self::topic_column_types())
            .await?;
        Ok(result.rows.iter().map(Self::topic_from_row).collect())
    }

    /// Get specific topics by name
    pub async fn get_topics(&self, names: &[String]) -> Result<Vec<TopicInfo>, StorageError> {
        if names.is_empty() {
            return self.get_all_topics().await;
        }

        let mut topics = Vec::new();
        for name in names {
            if let Some(topic) = self.get_topic(name).await? {
                topics.push(topic);
            }
        }
        Ok(topics)
    }

    /// Get topic by name
    pub async fn get_topic(&self, name: &str) -> Result<Option<TopicInfo>, StorageError> {
        let query = format!(
            "SELECT {} FROM pgkafka.topics WHERE name = $1",
            Self::TOPIC_COLUMNS
        );

        let result = self
            .bridge
            .query(
                &query,
                vec![SpiParam::Text(Some(name.to_string()))],
                Self::topic_column_types(),
            )
            .await?;

        Ok(result.first().map(Self::topic_from_row))
    }

    /// Produce messages to a topic using batch INSERT
    ///
    /// Returns the base offset (first offset assigned).
    /// Uses multi-row INSERT for better performance (single SPI call per batch).
    /// For very large batches (>5000 messages), chunks into multiple INSERTs.
    ///
    /// For source-backed topics with writes enabled:
    /// - Stream mode: INSERTs each message as a new row
    /// - Table mode: UPSERTs using the message key as the primary key
    pub async fn produce_messages(
        &self,
        topic_name: &str,
        messages: Vec<MessageToStore>,
    ) -> Result<i64, StorageError> {
        if messages.is_empty() {
            return Ok(0);
        }

        // Get topic first
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        // Handle source-backed topics
        if topic.is_source_backed() {
            // Check if writes are enabled
            if !topic.is_writable() {
                return Err(StorageError::ReadOnlyTopic(topic_name.to_string()));
            }

            // Writable source-backed topic - use source inserter
            return self.produce_to_source_table(&topic, messages).await;
        }

        // Native topic - use batch INSERT to pgkafka.messages
        // For small batches, use single batch INSERT
        if messages.len() <= MAX_MESSAGES_PER_BATCH {
            let (query, params) = build_batch_insert_query(topic.id, &messages);
            let result = self
                .bridge
                .query(&query, params, vec![ColumnType::Int64])
                .await?;

            // First returned offset_id is the base offset
            let base_offset = result
                .first()
                .map(|r| r.get_i64(0))
                .transpose()?
                .unwrap_or(0);
            return Ok(base_offset);
        }

        // For very large batches, chunk to stay within PostgreSQL's parameter limit
        let mut base_offset: Option<i64> = None;

        for chunk in messages.chunks(MAX_MESSAGES_PER_BATCH) {
            let (query, params) = build_batch_insert_query(topic.id, chunk);
            let result = self
                .bridge
                .query(&query, params, vec![ColumnType::Int64])
                .await?;

            // Track the first offset from the first chunk
            if base_offset.is_none() {
                base_offset = result.first().map(|r| r.get_i64(0)).transpose()?;
            }
        }

        Ok(base_offset.unwrap_or(0))
    }

    /// Produce messages to a source-backed writable topic
    async fn produce_to_source_table(
        &self,
        topic: &TopicInfo,
        messages: Vec<MessageToStore>,
    ) -> Result<i64, StorageError> {
        // Validate messages against schema (if a schema is bound to the topic)
        let validator = SchemaValidator::new(Arc::clone(&self.bridge));

        // Convert messages to JSON for validation and insertion
        let mut json_messages: Vec<(Option<String>, serde_json::Value)> =
            Vec::with_capacity(messages.len());

        for msg in &messages {
            // Parse value as JSON
            let value_json = if let Some(ref json) = msg.value_json {
                json.clone()
            } else if let Some(ref text) = msg.value_text {
                serde_json::from_str(text).map_err(|e| {
                    StorageError::ValidationFailed(format!(
                        "message value must be valid JSON: {}",
                        e
                    ))
                })?
            } else {
                return Err(StorageError::ValidationFailed(
                    "message value must be text or JSON for source-backed topics".to_string(),
                ));
            };

            // Get key as string
            let key_str = msg.key_text.clone();

            // Validate against schema if bound
            if let Some(ref text) = msg.value_text {
                match validator
                    .validate_for_topic(topic.name.as_str(), text, "value")
                    .await
                {
                    Ok(ValidationResult::Invalid(err)) => {
                        return Err(StorageError::ValidationFailed(err));
                    }
                    Ok(_) => {} // Valid or NoSchema - proceed
                    Err(e) => {
                        // Validation error (e.g., invalid schema) - log and proceed
                        pgrx::warning!("Schema validation error: {}", e);
                    }
                }
            }

            json_messages.push((key_str, value_json));
        }

        // Get base kafka offset for table mode
        let base_kafka_offset = self.get_high_watermark(&topic.name).await?;

        // Insert messages using source inserter
        let inserter = SourceInserter::new(Arc::clone(&self.bridge));
        let offsets = inserter
            .insert_batch(topic, &json_messages, base_kafka_offset)
            .await?;

        // Return the first offset
        Ok(offsets.first().copied().unwrap_or(0))
    }

    /// Build a SQL query for fetching messages from a source table
    fn build_source_table_fetch_query(topic: &TopicInfo) -> String {
        let source_table = topic
            .source_table
            .as_ref()
            .expect("source_table must be set for source-backed topics");
        let offset_column = topic
            .offset_column
            .as_ref()
            .expect("offset_column must be set for source-backed topics");

        // Build value expression
        let value_expr = if let Some(expr) = &topic.value_expr {
            format!("({})::text::bytea", expr)
        } else if let Some(col) = &topic.value_column {
            format!("{}::text::bytea", col)
        } else {
            "NULL::bytea".to_string()
        };

        // Build key expression
        let key_expr = if let Some(expr) = &topic.key_expr {
            format!("({})::text::bytea", expr)
        } else if let Some(col) = &topic.key_column {
            format!("{}::text::bytea", col)
        } else {
            "NULL::bytea".to_string()
        };

        // Build timestamp expression
        let ts_expr = if let Some(expr) = &topic.timestamp_expr {
            format!("({})", expr)
        } else if let Some(col) = &topic.timestamp_column {
            format!("EXTRACT(EPOCH FROM {})::bigint * 1000", col)
        } else {
            "EXTRACT(EPOCH FROM now())::bigint * 1000".to_string()
        };

        // Extract just the table name for use as an alias
        let table_alias = source_table
            .rsplit('.')
            .next()
            .unwrap_or(source_table.as_str());

        format!(
            "SELECT {offset_col}::bigint as offset_id, {key} as key, {value} as value, {ts} as ts \
             FROM {table} AS {alias} \
             WHERE {offset_col}::bigint >= $1 \
             ORDER BY {offset_col} \
             LIMIT $2",
            offset_col = offset_column,
            key = key_expr,
            value = value_expr,
            ts = ts_expr,
            table = source_table,
            alias = table_alias,
        )
    }

    /// Fetch messages from a topic starting at an offset
    pub async fn fetch_messages(
        &self,
        topic_name: &str,
        start_offset: i64,
        max_messages: i32,
    ) -> Result<(Vec<MessageRecord>, i64), StorageError> {
        // Get topic first
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        let column_types = vec![
            ColumnType::Int64, // offset_id
            ColumnType::Bytea, // key
            ColumnType::Bytea, // value
            ColumnType::Int64, // ts
        ];

        let (messages, high_watermark) = if topic.is_source_backed() {
            // Fetch from source table
            let query = Self::build_source_table_fetch_query(&topic);
            let params = vec![
                SpiParam::Int8(Some(start_offset)),
                SpiParam::Int8(Some(max_messages as i64)),
            ];

            let result = self.bridge.query(&query, params, column_types).await?;

            let messages: Vec<MessageRecord> = result
                .rows
                .iter()
                .map(|row| MessageRecord {
                    offset_id: row.get_i64(0).unwrap_or(0),
                    key: row.get_bytes(1),
                    value: row.get_bytes(2).unwrap_or_default(),
                    created_at: row.get_i64(3).unwrap_or(0),
                })
                .collect();

            // Get high watermark from source table
            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");
            let hw_query = format!(
                "SELECT COALESCE((MAX({}) + 1)::bigint, 0) FROM {}",
                offset_column, source_table
            );
            let hw_result = self
                .bridge
                .query(&hw_query, vec![], vec![ColumnType::Int64])
                .await?;
            let high_watermark = hw_result
                .first()
                .and_then(|r| r.get_i64(0).ok())
                .unwrap_or(0);

            (messages, high_watermark)
        } else {
            // Fetch from pgkafka.messages (native topic)
            let query =
                "SELECT offset_id, key, value, EXTRACT(EPOCH FROM created_at)::bigint * 1000 as ts
                         FROM pgkafka.messages
                         WHERE topic_id = $1 AND offset_id >= $2
                         ORDER BY offset_id
                         LIMIT $3";

            let params = vec![
                SpiParam::Int4(Some(topic.id)),
                SpiParam::Int8(Some(start_offset)),
                SpiParam::Int8(Some(max_messages as i64)),
            ];

            let result = self.bridge.query(query, params, column_types).await?;

            let messages: Vec<MessageRecord> = result
                .rows
                .iter()
                .map(|row| MessageRecord {
                    offset_id: row.get_i64(0).unwrap_or(0),
                    key: row.get_bytes(1),
                    value: row.get_bytes(2).unwrap_or_default(),
                    created_at: row.get_i64(3).unwrap_or(0),
                })
                .collect();

            // Get high watermark from pgkafka.messages
            let hw_query =
                "SELECT COALESCE(MAX(offset_id) + 1, 0) FROM pgkafka.messages WHERE topic_id = $1";
            let hw_result = self
                .bridge
                .query(
                    hw_query,
                    vec![SpiParam::Int4(Some(topic.id))],
                    vec![ColumnType::Int64],
                )
                .await?;
            let high_watermark = hw_result
                .first()
                .and_then(|r| r.get_i64(0).ok())
                .unwrap_or(0);

            (messages, high_watermark)
        };

        Ok((messages, high_watermark))
    }

    /// Get the high watermark for a topic (next offset to be produced)
    pub async fn get_high_watermark(&self, topic_name: &str) -> Result<i64, StorageError> {
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        let (query, params) = if topic.is_source_backed() {
            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");
            let q = format!(
                "SELECT COALESCE((MAX({}) + 1)::bigint, 0) FROM {}",
                offset_column, source_table
            );
            (q, vec![])
        } else {
            let q =
                "SELECT COALESCE(MAX(offset_id) + 1, 0) FROM pgkafka.messages WHERE topic_id = $1"
                    .to_string();
            (q, vec![SpiParam::Int4(Some(topic.id))])
        };

        let result = self
            .bridge
            .query(&query, params, vec![ColumnType::Int64])
            .await?;

        Ok(result.first().and_then(|r| r.get_i64(0).ok()).unwrap_or(0))
    }

    /// Get the log start offset for a topic
    pub async fn get_log_start_offset(&self, topic_name: &str) -> Result<i64, StorageError> {
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        let (query, params) = if topic.is_source_backed() {
            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");
            let q = format!(
                "SELECT COALESCE(MIN({})::bigint, 0) FROM {}",
                offset_column, source_table
            );
            (q, vec![])
        } else {
            let q = "SELECT COALESCE(MIN(offset_id), 0) FROM pgkafka.messages WHERE topic_id = $1"
                .to_string();
            (q, vec![SpiParam::Int4(Some(topic.id))])
        };

        let result = self
            .bridge
            .query(&query, params, vec![ColumnType::Int64])
            .await?;

        Ok(result.first().and_then(|r| r.get_i64(0).ok()).unwrap_or(0))
    }

    /// Find the first offset where timestamp >= the given timestamp (in milliseconds)
    ///
    /// Returns:
    /// - `Ok(Some(offset))` if an offset was found for the timestamp
    /// - `Ok(None)` if no timestamp column is configured or no matching offset exists
    /// - `Err(...)` on query failure
    ///
    /// For source-backed topics: uses the configured timestamp_column
    /// For native topics: uses the created_at column
    pub async fn find_offset_for_timestamp(
        &self,
        topic_name: &str,
        timestamp_ms: i64,
    ) -> Result<Option<i64>, StorageError> {
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        if topic.is_source_backed() {
            // For source-backed topics, check if timestamp_column is configured
            let timestamp_column = match &topic.timestamp_column {
                Some(col) => col,
                None => {
                    // No timestamp column configured, can't do timestamp-based lookup
                    return Ok(None);
                }
            };

            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");

            // Detect column type to build appropriate query
            // For bigint: compare directly (assumes epoch ms)
            // For timestamp types: convert using to_timestamp
            let query = format!(
                "SELECT MIN({offset_col})::bigint \
                 FROM {table} \
                 WHERE {ts_col} >= to_timestamp($1::double precision / 1000.0)",
                offset_col = offset_column,
                table = source_table,
                ts_col = timestamp_column,
            );

            let result = self
                .bridge
                .query(
                    &query,
                    vec![SpiParam::Int8(Some(timestamp_ms))],
                    vec![ColumnType::Int64],
                )
                .await?;

            // If result is NULL, no matching offset was found
            Ok(result.first().and_then(|r| r.get_i64(0).ok()))
        } else {
            // For native topics, use created_at column
            let query = "SELECT MIN(offset_id) \
                         FROM pgkafka.messages \
                         WHERE topic_id = $1 AND created_at >= to_timestamp($2::double precision / 1000.0)";

            let result = self
                .bridge
                .query(
                    query,
                    vec![
                        SpiParam::Int4(Some(topic.id)),
                        SpiParam::Int8(Some(timestamp_ms)),
                    ],
                    vec![ColumnType::Int64],
                )
                .await?;

            Ok(result.first().and_then(|r| r.get_i64(0).ok()))
        }
    }
}

/// Maximum messages per batch INSERT to stay within PostgreSQL's parameter limit
/// With 7 params per message, 5000 messages = 35000 params (well under 65535 limit)
const MAX_MESSAGES_PER_BATCH: usize = 5000;

/// Build a batch INSERT query with multi-row VALUES clause
///
/// Returns (query_string, params_vec) where:
/// - query_string has placeholders like ($1, $2, ...), ($8, $9, ...), etc.
/// - params_vec is flattened list of all parameters in order
fn build_batch_insert_query(topic_id: i32, messages: &[MessageToStore]) -> (String, Vec<SpiParam>) {
    let mut params = Vec::with_capacity(messages.len() * 7);
    let mut value_clauses = Vec::with_capacity(messages.len());

    for (i, msg) in messages.iter().enumerate() {
        let base = i * 7 + 1; // 1-indexed placeholders

        // Build VALUES clause for this row
        // JSONB handling: CASE WHEN $N::text IS NULL THEN NULL ELSE $N::text::jsonb END
        let clause = format!(
            "(${}, ${}, ${}, CASE WHEN ${}::text IS NULL THEN NULL ELSE ${}::text::jsonb END, ${}, ${}, CASE WHEN ${}::text IS NULL THEN NULL ELSE ${}::text::jsonb END)",
            base,     // topic_id
            base + 1, // key
            base + 2, // key_text
            base + 3, base + 3, // key_json (duplicated for CASE)
            base + 4, // value
            base + 5, // value_text
            base + 6, base + 6, // value_json (duplicated for CASE)
        );
        value_clauses.push(clause);

        // Add params for this message
        let key_json_str = msg.key_json.as_ref().map(|v| v.to_string());
        let value_json_str = msg.value_json.as_ref().map(|v| v.to_string());

        params.push(SpiParam::Int4(Some(topic_id)));
        params.push(SpiParam::Bytea(msg.key.as_ref().map(|b| b.to_vec())));
        params.push(SpiParam::Text(msg.key_text.clone()));
        params.push(SpiParam::Text(key_json_str));
        params.push(SpiParam::Bytea(Some(msg.value.to_vec())));
        params.push(SpiParam::Text(msg.value_text.clone()));
        params.push(SpiParam::Text(value_json_str));
    }

    let query = format!(
        "INSERT INTO pgkafka.messages (topic_id, key, key_text, key_json, value, value_text, value_json) VALUES {} RETURNING offset_id",
        value_clauses.join(", ")
    );

    (query, params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_message(key: Option<&[u8]>, value: &[u8]) -> MessageToStore {
        MessageToStore {
            key: key.map(|k| Bytes::copy_from_slice(k)),
            key_text: key.and_then(|k| std::str::from_utf8(k).ok().map(|s| s.to_string())),
            key_json: None,
            value: Bytes::copy_from_slice(value),
            value_text: std::str::from_utf8(value).ok().map(|s| s.to_string()),
            value_json: None,
        }
    }

    fn make_json_message(key: Option<&str>, value_json: serde_json::Value) -> MessageToStore {
        let value_str = value_json.to_string();
        MessageToStore {
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            key_text: key.map(|s| s.to_string()),
            key_json: None,
            value: Bytes::copy_from_slice(value_str.as_bytes()),
            value_text: Some(value_str.clone()),
            value_json: Some(value_json),
        }
    }

    #[test]
    fn test_build_batch_insert_single_message() {
        let messages = vec![make_message(Some(b"key1"), b"value1")];
        let (query, params) = build_batch_insert_query(42, &messages);

        // Should have 7 params for 1 message
        assert_eq!(params.len(), 7);

        // Query should have single VALUES tuple with placeholders 1-7
        assert!(query.contains("VALUES ($1, $2, $3,"));
        assert!(query.contains("RETURNING offset_id"));

        // Verify first param is topic_id
        let SpiParam::Int4(Some(id)) = &params[0] else {
            panic!("Expected Int4 for topic_id, got {:?}", params[0])
        };
        assert_eq!(*id, 42);
    }

    #[test]
    fn test_build_batch_insert_multiple_messages() {
        let messages = vec![
            make_message(Some(b"key1"), b"value1"),
            make_message(Some(b"key2"), b"value2"),
            make_message(Some(b"key3"), b"value3"),
        ];
        let (query, params) = build_batch_insert_query(1, &messages);

        // Should have 7 params * 3 messages = 21 params
        assert_eq!(params.len(), 21);

        // Query should have 3 VALUES tuples
        assert!(query.contains("($1, $2, $3,"));
        assert!(query.contains("($8, $9, $10,"));
        assert!(query.contains("($15, $16, $17,"));

        // Verify each message's topic_id is set correctly
        for i in 0..3 {
            let SpiParam::Int4(Some(id)) = &params[i * 7] else {
                panic!(
                    "Expected Int4 for topic_id at position {}, got {:?}",
                    i * 7,
                    params[i * 7]
                )
            };
            assert_eq!(*id, 1);
        }
    }

    #[test]
    fn test_build_batch_insert_null_key() {
        let messages = vec![make_message(None, b"value-no-key")];
        let (query, params) = build_batch_insert_query(1, &messages);

        assert_eq!(params.len(), 7);

        // Key param (index 1) should be None
        assert!(
            matches!(&params[1], SpiParam::Bytea(None)),
            "Expected Bytea(None) for null key, got {:?}",
            params[1]
        );

        // key_text param (index 2) should be None
        assert!(
            matches!(&params[2], SpiParam::Text(None)),
            "Expected Text(None) for null key_text, got {:?}",
            params[2]
        );

        // Query structure should still be correct
        assert!(query.contains("INSERT INTO pgkafka.messages"));
    }

    #[test]
    fn test_build_batch_insert_with_json() {
        let messages = vec![make_json_message(
            Some("user-1"),
            json!({"count": 42, "active": true}),
        )];
        let (query, params) = build_batch_insert_query(5, &messages);

        assert_eq!(params.len(), 7);

        // value_json param (index 6) should contain JSON string
        let SpiParam::Text(Some(json_str)) = &params[6] else {
            panic!(
                "Expected Text(Some(...)) for value_json, got {:?}",
                params[6]
            )
        };
        assert!(json_str.contains("count"));
        assert!(json_str.contains("42"));

        // Query should have JSONB casting
        assert!(query.contains("::text::jsonb"));
    }

    #[test]
    fn test_build_batch_insert_null_json() {
        let messages = vec![make_message(Some(b"key"), b"plain text value")];
        let (query, params) = build_batch_insert_query(1, &messages);

        // key_json param (index 3) should be None
        assert!(
            matches!(&params[3], SpiParam::Text(None)),
            "Expected Text(None) for null key_json, got {:?}",
            params[3]
        );

        // value_json param (index 6) should be None
        assert!(
            matches!(&params[6], SpiParam::Text(None)),
            "Expected Text(None) for null value_json, got {:?}",
            params[6]
        );

        // Query should still have CASE WHEN for NULL handling
        assert!(query.contains("CASE WHEN"));
    }

    #[test]
    fn test_build_batch_insert_mixed_messages() {
        let messages = vec![
            make_message(None, b"no-key"),
            make_json_message(Some("with-key"), json!({"id": 1})),
            make_message(Some(b"binary-key"), b"binary value"),
        ];
        let (_query, params) = build_batch_insert_query(10, &messages);

        // 3 messages * 7 params = 21
        assert_eq!(params.len(), 21);

        // First message: null key
        assert!(
            matches!(&params[1], SpiParam::Bytea(None)),
            "First message should have null key, got {:?}",
            params[1]
        );

        // Second message: has key and JSON value
        let SpiParam::Bytea(Some(k)) = &params[7 + 1] else {
            panic!("Second message should have key, got {:?}", params[7 + 1])
        };
        assert_eq!(k, b"with-key");

        let SpiParam::Text(Some(json_str)) = &params[7 + 6] else {
            panic!(
                "Second message should have JSON value, got {:?}",
                params[7 + 6]
            )
        };
        assert!(json_str.contains("id"));

        // Third message: has binary key, no JSON
        let SpiParam::Bytea(Some(k)) = &params[14 + 1] else {
            panic!(
                "Third message should have binary key, got {:?}",
                params[14 + 1]
            )
        };
        assert_eq!(k, b"binary-key");

        assert!(
            matches!(&params[14 + 6], SpiParam::Text(None)),
            "Third message should have null value_json, got {:?}",
            params[14 + 6]
        );
    }

    // Test TopicInfo helper
    fn make_topic_info(
        source_table: Option<&str>,
        offset_column: Option<&str>,
        value_column: Option<&str>,
        key_column: Option<&str>,
    ) -> TopicInfo {
        TopicInfo {
            id: 1,
            name: "test-topic".to_string(),
            source_table: source_table.map(String::from),
            offset_column: offset_column.map(String::from),
            value_column: value_column.map(String::from),
            value_expr: None,
            key_column: key_column.map(String::from),
            key_expr: None,
            timestamp_column: None,
            timestamp_expr: None,
            writable: false,
            write_mode: None,
            write_key_column: None,
            kafka_offset_column: None,
        }
    }

    #[test]
    fn test_topic_info_is_source_backed() {
        let native = make_topic_info(None, None, None, None);
        assert!(!native.is_source_backed());

        let source_backed =
            make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        assert!(source_backed.is_source_backed());
    }

    #[test]
    fn test_topic_info_is_writable() {
        let mut topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        assert!(!topic.is_writable());

        topic.writable = true;
        assert!(topic.is_writable());
    }

    #[test]
    fn test_topic_info_write_modes() {
        let mut topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);

        // No write mode set
        assert!(!topic.is_stream_mode());
        assert!(!topic.is_table_mode());

        // Stream mode
        topic.write_mode = Some("stream".to_string());
        assert!(topic.is_stream_mode());
        assert!(!topic.is_table_mode());

        // Table mode
        topic.write_mode = Some("table".to_string());
        assert!(!topic.is_stream_mode());
        assert!(topic.is_table_mode());
    }

    #[test]
    fn test_build_source_table_fetch_query_basic() {
        let topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        assert!(query.contains("FROM public.orders"));
        assert!(query.contains("id::bigint as offset_id"));
        assert!(query.contains("payload::text::bytea as value"));
        assert!(query.contains("WHERE id::bigint >= $1"));
        assert!(query.contains("ORDER BY id"));
        assert!(query.contains("LIMIT $2"));
    }

    #[test]
    fn test_build_source_table_fetch_query_with_key_column() {
        let topic = make_topic_info(
            Some("public.orders"),
            Some("id"),
            Some("payload"),
            Some("customer_id"),
        );
        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        assert!(query.contains("customer_id::text::bytea as key"));
    }

    #[test]
    fn test_build_source_table_fetch_query_with_expressions() {
        let mut topic = make_topic_info(Some("public.orders"), Some("id"), None, None);
        topic.value_expr = Some("row_to_json(orders)".to_string());
        topic.key_expr = Some("customer_id::text".to_string());

        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        assert!(query.contains("(row_to_json(orders))::text::bytea as value"));
        assert!(query.contains("(customer_id::text)::text::bytea as key"));
    }

    #[test]
    fn test_build_source_table_fetch_query_with_timestamp() {
        let mut topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        topic.timestamp_column = Some("created_at".to_string());

        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        assert!(query.contains("EXTRACT(EPOCH FROM created_at)::bigint * 1000"));
    }

    #[test]
    fn test_build_source_table_fetch_query_schema_qualified() {
        let topic = make_topic_info(Some("myschema.orders"), Some("id"), Some("payload"), None);
        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        // Should use just table name as alias
        assert!(query.contains("FROM myschema.orders AS orders"));
    }

    #[test]
    fn test_build_source_table_fetch_query_no_value_column_or_expr() {
        let topic = make_topic_info(Some("public.orders"), Some("id"), None, None);
        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        // Should use NULL::bytea for value
        assert!(query.contains("NULL::bytea as value"));
    }

    #[test]
    fn test_build_source_table_fetch_query_no_key() {
        let topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        // Should use NULL::bytea for key
        assert!(query.contains("NULL::bytea as key"));
    }

    #[test]
    fn test_build_source_table_fetch_query_with_timestamp_expr() {
        let mut topic = make_topic_info(Some("public.orders"), Some("id"), Some("payload"), None);
        topic.timestamp_expr = Some("updated_at_ms".to_string());

        let query = SpiStorageClient::build_source_table_fetch_query(&topic);

        // Should use the expression directly (wrapped in parens)
        assert!(query.contains("(updated_at_ms)"));
        // Should not contain EXTRACT since we have an expression
        assert!(!query.contains("EXTRACT(EPOCH FROM updated_at_ms)"));
    }

    #[test]
    fn test_storage_error_display() {
        let err = StorageError::TopicNotFound("my-topic".to_string());
        assert_eq!(format!("{}", err), "topic not found: my-topic");

        let err = StorageError::ReadOnlyTopic("source-topic".to_string());
        assert_eq!(
            format!("{}", err),
            "topic 'source-topic' is backed by source table and is read-only"
        );

        let err = StorageError::ValidationFailed("invalid field".to_string());
        assert_eq!(format!("{}", err), "validation failed: invalid field");
    }

    #[test]
    fn test_message_to_store_creation() {
        let msg = MessageToStore {
            key: Some(Bytes::from("key")),
            key_text: Some("key".to_string()),
            key_json: None,
            value: Bytes::from("value"),
            value_text: Some("value".to_string()),
            value_json: None,
        };

        assert!(msg.key.is_some());
        assert_eq!(msg.value_text, Some("value".to_string()));
    }

    #[test]
    fn test_message_to_store_with_json() {
        let msg = MessageToStore {
            key: None,
            key_text: None,
            key_json: None,
            value: Bytes::from(r#"{"id":1}"#),
            value_text: Some(r#"{"id":1}"#.to_string()),
            value_json: Some(json!({"id": 1})),
        };

        assert!(msg.key.is_none());
        assert!(msg.value_json.is_some());
        assert_eq!(msg.value_json.unwrap()["id"], 1);
    }

    #[test]
    fn test_message_record_structure() {
        let record = MessageRecord {
            offset_id: 42,
            key: Some(vec![1, 2, 3]),
            value: vec![4, 5, 6],
            created_at: 1705430400000, // Unix timestamp in ms
        };

        assert_eq!(record.offset_id, 42);
        assert_eq!(record.key, Some(vec![1, 2, 3]));
        assert_eq!(record.value, vec![4, 5, 6]);
        assert_eq!(record.created_at, 1705430400000);
    }
}
