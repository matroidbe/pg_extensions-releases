//! Database client for pg_kafka storage operations
//!
//! Uses tokio-postgres to connect to the local PostgreSQL instance.

use bytes::Bytes;
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};

/// Storage error type
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("topic not found: {0}")]
    TopicNotFound(String),
    #[error("no connection available")]
    NoConnection,
    #[error("topic '{0}' is backed by source table and is read-only")]
    ReadOnlyTopic(String),
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
}

impl TopicInfo {
    /// Returns true if this topic is backed by a source table
    pub fn is_source_backed(&self) -> bool {
        self.source_table.is_some()
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

/// Database client wrapper with connection pooling
pub struct StorageClient {
    /// The active database client (if connected)
    client: Arc<RwLock<Option<Client>>>,
    /// Connection string for reconnection
    connection_string: String,
}

impl StorageClient {
    /// Create a new storage client
    pub fn new() -> Self {
        // Default connection to local PostgreSQL via localhost
        // Uses the standard postgres port (5432) by default
        // Can be overridden with PGKAFKA_DATABASE_URL environment variable
        let connection_string = std::env::var("PGKAFKA_DATABASE_URL")
            .unwrap_or_else(|_| "host=localhost dbname=postgres".to_string());

        Self {
            client: Arc::new(RwLock::new(None)),
            connection_string,
        }
    }

    /// Connect to the database
    pub async fn connect(&self) -> Result<(), StorageError> {
        pgrx::log!(
            "pg_kafka storage: connecting to '{}'",
            self.connection_string
        );
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls).await?;

        // Spawn the connection handler
        tokio::spawn(async move {
            pgrx::log!("pg_kafka storage: connection driver started");
            if let Err(e) = connection.await {
                pgrx::warning!("pg_kafka storage: connection driver error: {}", e);
            }
            pgrx::log!("pg_kafka storage: connection driver ended");
        });

        // Set search_path to include public and pgkafka schemas
        client
            .execute("SET search_path TO public, pgkafka", &[])
            .await?;
        pgrx::log!("pg_kafka storage: connected and search_path set");

        let mut guard = self.client.write().await;
        *guard = Some(client);

        Ok(())
    }

    /// Ensure we have a connection, reconnecting if necessary
    async fn ensure_connected(&self) -> Result<(), StorageError> {
        let guard = self.client.read().await;
        if guard.is_none() {
            drop(guard);
            self.connect().await?;
        }
        Ok(())
    }

    /// SQL columns to select for TopicInfo
    const TOPIC_COLUMNS: &'static str =
        "id, name, source_table, offset_column, value_column, value_expr, key_column, key_expr, timestamp_column, timestamp_expr";

    /// Build TopicInfo from a database row
    fn topic_from_row(row: &tokio_postgres::Row) -> TopicInfo {
        TopicInfo {
            id: row.get(0),
            name: row.get(1),
            source_table: row.get(2),
            offset_column: row.get(3),
            value_column: row.get(4),
            value_expr: row.get(5),
            key_column: row.get(6),
            key_expr: row.get(7),
            timestamp_column: row.get(8),
            timestamp_expr: row.get(9),
        }
    }

    /// Get all topics
    pub async fn get_all_topics(&self) -> Result<Vec<TopicInfo>, StorageError> {
        self.ensure_connected().await?;

        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let query = format!(
            "SELECT {} FROM pgkafka.topics ORDER BY name",
            Self::TOPIC_COLUMNS
        );
        let rows = client.query(&query, &[]).await?;

        Ok(rows.iter().map(Self::topic_from_row).collect())
    }

    /// Get specific topics by name
    pub async fn get_topics(&self, names: &[String]) -> Result<Vec<TopicInfo>, StorageError> {
        if names.is_empty() {
            return self.get_all_topics().await;
        }

        self.ensure_connected().await?;

        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let query = format!(
            "SELECT {} FROM pgkafka.topics WHERE name = $1",
            Self::TOPIC_COLUMNS
        );

        let mut topics = Vec::new();
        for name in names {
            let rows = client.query(&query, &[name]).await?;

            if let Some(row) = rows.first() {
                topics.push(Self::topic_from_row(row));
            }
        }

        Ok(topics)
    }

    /// Get topic by name
    pub async fn get_topic(&self, name: &str) -> Result<Option<TopicInfo>, StorageError> {
        self.ensure_connected().await?;

        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let query = format!(
            "SELECT {} FROM pgkafka.topics WHERE name = $1",
            Self::TOPIC_COLUMNS
        );
        let rows = client.query(&query, &[&name]).await?;

        Ok(rows.first().map(Self::topic_from_row))
    }

    /// Produce messages to a topic
    ///
    /// Returns the base offset (first offset assigned)
    ///
    /// Note: Source-backed topics are read-only and will return an error.
    pub async fn produce_messages(
        &self,
        topic_name: &str,
        messages: Vec<MessageToStore>,
    ) -> Result<i64, StorageError> {
        if messages.is_empty() {
            return Ok(0);
        }

        // Get topic first (this acquires and releases its own lock)
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        // Source-backed topics are read-only
        if topic.is_source_backed() {
            return Err(StorageError::ReadOnlyTopic(topic_name.to_string()));
        }

        // Now acquire lock for the insert
        self.ensure_connected().await?;
        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        // Insert messages and get first offset
        let mut base_offset: Option<i64> = None;

        for msg in messages {
            let key_bytes: Option<&[u8]> = msg.key.as_deref();
            let value_bytes: &[u8] = &msg.value;

            // Convert JSON values to strings for JSONB columns
            let key_json_str = msg.key_json.as_ref().map(|v| v.to_string());
            let value_json_str = msg.value_json.as_ref().map(|v| v.to_string());

            // Use CASE expressions to handle the jsonb conversion properly
            // When the value is NULL, we pass NULL directly
            // When it's a string, we cast it to jsonb
            let row = client
                .query_one(
                    "INSERT INTO pgkafka.messages (topic_id, key, key_text, key_json, value, value_text, value_json)
                     VALUES ($1, $2, $3, CASE WHEN $4::text IS NULL THEN NULL ELSE $4::text::jsonb END, $5, $6, CASE WHEN $7::text IS NULL THEN NULL ELSE $7::text::jsonb END)
                     RETURNING offset_id",
                    &[
                        &topic.id,
                        &key_bytes,
                        &msg.key_text,
                        &key_json_str,
                        &value_bytes,
                        &msg.value_text,
                        &value_json_str,
                    ],
                )
                .await?;

            let offset: i64 = row.get(0);
            if base_offset.is_none() {
                base_offset = Some(offset);
            }
        }

        Ok(base_offset.unwrap_or(0))
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

        // Build value expression - prefer value_expr over value_column
        // Use ::text::bytea to handle JSON types which can't cast directly to bytea
        let value_expr = if let Some(expr) = &topic.value_expr {
            format!("({})::text::bytea", expr)
        } else if let Some(col) = &topic.value_column {
            format!("{}::text::bytea", col)
        } else {
            "NULL::bytea".to_string()
        };

        // Build key expression - prefer key_expr over key_column
        // Use ::text::bytea to handle various types
        let key_expr = if let Some(expr) = &topic.key_expr {
            format!("({})::text::bytea", expr)
        } else if let Some(col) = &topic.key_column {
            format!("{}::text::bytea", col)
        } else {
            "NULL::bytea".to_string()
        };

        // Build timestamp expression - prefer timestamp_expr over timestamp_column
        let ts_expr = if let Some(expr) = &topic.timestamp_expr {
            format!("({})", expr)
        } else if let Some(col) = &topic.timestamp_column {
            format!("EXTRACT(EPOCH FROM {})::bigint * 1000", col)
        } else {
            "EXTRACT(EPOCH FROM now())::bigint * 1000".to_string()
        };

        // Extract just the table name (without schema) for use as an alias
        // This allows expressions like row_to_json(table_name) to work
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
    ///
    /// For source-backed topics, this queries the source table directly.
    /// For native topics, this queries pgkafka.messages.
    pub async fn fetch_messages(
        &self,
        topic_name: &str,
        start_offset: i64,
        max_messages: i32,
    ) -> Result<(Vec<MessageRecord>, i64), StorageError> {
        // Get topic first (this acquires and releases its own lock)
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        // Now acquire lock for the query
        self.ensure_connected().await?;
        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let (messages, high_watermark) = if topic.is_source_backed() {
            // Fetch from source table
            let query = Self::build_source_table_fetch_query(&topic);
            let rows = client
                .query(&query, &[&start_offset, &(max_messages as i64)])
                .await?;

            let messages: Vec<MessageRecord> = rows
                .iter()
                .map(|row| MessageRecord {
                    offset_id: row.get(0),
                    key: row.get(1),
                    value: row.get(2),
                    created_at: row.get::<_, i64>(3),
                })
                .collect();

            // Get high watermark from source table - cast to bigint for i64 compatibility
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
            let hw_row = client.query_one(&hw_query, &[]).await?;
            let high_watermark: i64 = hw_row.get(0);

            (messages, high_watermark)
        } else {
            // Fetch from pgkafka.messages (native topic)
            let rows = client
                .query(
                    "SELECT offset_id, key, value, EXTRACT(EPOCH FROM created_at)::bigint * 1000 as ts
                     FROM pgkafka.messages
                     WHERE topic_id = $1 AND offset_id >= $2
                     ORDER BY offset_id
                     LIMIT $3",
                    &[&topic.id, &start_offset, &(max_messages as i64)],
                )
                .await?;

            let messages: Vec<MessageRecord> = rows
                .iter()
                .map(|row| MessageRecord {
                    offset_id: row.get(0),
                    key: row.get(1),
                    value: row.get(2),
                    created_at: row.get::<_, i64>(3),
                })
                .collect();

            // Get high watermark from pgkafka.messages
            let hw_row = client
                .query_one(
                    "SELECT COALESCE(MAX(offset_id) + 1, 0) FROM pgkafka.messages WHERE topic_id = $1",
                    &[&topic.id],
                )
                .await?;
            let high_watermark: i64 = hw_row.get(0);

            (messages, high_watermark)
        };

        Ok((messages, high_watermark))
    }

    /// Get the high watermark for a topic (next offset to be produced)
    ///
    /// For source-backed topics, this queries the source table's offset column.
    /// For native topics, this queries pgkafka.messages.
    #[allow(dead_code)]
    pub async fn get_high_watermark(&self, topic_name: &str) -> Result<i64, StorageError> {
        // Get topic first (this acquires and releases its own lock)
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        // Now get the watermark (acquire lock only for the query)
        self.ensure_connected().await?;
        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let row = if topic.is_source_backed() {
            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");
            // Cast to bigint to ensure i64 compatibility regardless of source column type
            let query = format!(
                "SELECT COALESCE((MAX({}) + 1)::bigint, 0) FROM {}",
                offset_column, source_table
            );
            client.query_one(&query, &[]).await?
        } else {
            client
                .query_one(
                    "SELECT COALESCE(MAX(offset_id) + 1, 0) FROM pgkafka.messages WHERE topic_id = $1",
                    &[&topic.id],
                )
                .await?
        };

        Ok(row.get(0))
    }

    /// Get the log start offset for a topic (earliest available offset)
    ///
    /// For source-backed topics, this queries the source table's offset column.
    /// For native topics, this queries pgkafka.messages.
    pub async fn get_log_start_offset(&self, topic_name: &str) -> Result<i64, StorageError> {
        // Get topic first (this acquires and releases its own lock)
        let topic = self
            .get_topic(topic_name)
            .await?
            .ok_or_else(|| StorageError::TopicNotFound(topic_name.to_string()))?;

        // Now get the offset (acquire lock only for the query)
        self.ensure_connected().await?;
        let guard = self.client.read().await;
        let client = guard.as_ref().ok_or(StorageError::NoConnection)?;

        let row = if topic.is_source_backed() {
            let offset_column = topic
                .offset_column
                .as_ref()
                .expect("offset_column must be set for source-backed topics");
            let source_table = topic
                .source_table
                .as_ref()
                .expect("source_table must be set for source-backed topics");
            // Cast to bigint to ensure i64 compatibility regardless of source column type
            let query = format!(
                "SELECT COALESCE(MIN({})::bigint, 0) FROM {}",
                offset_column, source_table
            );
            client.query_one(&query, &[]).await?
        } else {
            client
                .query_one(
                    "SELECT COALESCE(MIN(offset_id), 0) FROM pgkafka.messages WHERE topic_id = $1",
                    &[&topic.id],
                )
                .await?
        };

        Ok(row.get(0))
    }
}

impl Default for StorageClient {
    fn default() -> Self {
        Self::new()
    }
}
