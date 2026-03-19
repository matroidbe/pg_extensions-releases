//! Source table INSERT/UPSERT module for pg_kafka
//!
//! This module handles inserting Kafka messages into source-backed tables
//! in both stream (append-only) and table (upsert) modes.

use std::sync::Arc;

use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};
use super::spi_client::TopicInfo;

/// Source insert error
#[derive(Debug, thiserror::Error)]
pub enum SourceInsertError {
    #[error("SPI error: {0}")]
    Spi(#[from] SpiError),
    #[error("topic not writable: {0}")]
    NotWritable(String),
    #[error("missing required column: {0}")]
    MissingColumn(String),
    #[error("JSON parse error: {0}")]
    JsonParse(String),
    #[allow(dead_code)]
    #[error("column type mismatch for {0}: expected {1}")]
    TypeMismatch(String, String),
}

/// Column information from a table
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
}

/// Handles inserting messages into source tables
pub struct SourceInserter {
    bridge: Arc<SpiBridge>,
}

impl SourceInserter {
    /// Create a new source inserter
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    /// Get column information for a source table
    pub async fn get_table_columns(
        &self,
        source_table: &str,
    ) -> Result<Vec<ColumnInfo>, SourceInsertError> {
        // Parse schema.table
        let (schema, table) = parse_table_name(source_table);

        // Cast to text because information_schema uses sql_identifier and character_data types
        // which are not compatible with Rust's String type
        let query =
            "SELECT column_name::text, data_type::text, is_nullable::text, column_default::text
                     FROM information_schema.columns
                     WHERE table_schema = $1 AND table_name = $2
                     ORDER BY ordinal_position";

        let result = self
            .bridge
            .query(
                query,
                vec![
                    SpiParam::Text(Some(schema.to_string())),
                    SpiParam::Text(Some(table.to_string())),
                ],
                vec![
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                    ColumnType::Text,
                ],
            )
            .await?;

        let columns = result
            .rows
            .iter()
            .map(|row| ColumnInfo {
                name: row.get_string(0).unwrap_or_default(),
                data_type: row.get_string(1).unwrap_or_default(),
                is_nullable: row.get_string(2).map(|s| s == "YES").unwrap_or(true),
                column_default: row.get_string(3),
            })
            .collect();

        Ok(columns)
    }

    /// Insert a message in stream mode (append-only)
    ///
    /// Extracts JSON fields and INSERTs a new row.
    /// Returns the offset (value from offset_column).
    pub async fn insert_stream_message(
        &self,
        topic: &TopicInfo,
        json_value: &serde_json::Value,
        _key: Option<&str>,
    ) -> Result<i64, SourceInsertError> {
        let source_table = topic
            .source_table
            .as_ref()
            .ok_or_else(|| SourceInsertError::NotWritable("not source-backed".to_string()))?;

        let offset_column = topic.offset_column.as_ref().ok_or_else(|| {
            SourceInsertError::MissingColumn("offset_column not configured".to_string())
        })?;

        // Get table columns
        let columns = self.get_table_columns(source_table).await?;

        // Build INSERT query dynamically based on JSON fields
        let json_obj = json_value.as_object().ok_or_else(|| {
            SourceInsertError::JsonParse("message value must be a JSON object".to_string())
        })?;

        // Find columns that match JSON fields (excluding auto-increment offset column)
        let mut insert_columns = Vec::new();
        let mut insert_values = Vec::new();
        let mut params = Vec::new();
        let mut param_idx = 1;

        for col in &columns {
            // Skip columns that have auto-increment defaults (serial columns)
            if col
                .column_default
                .as_ref()
                .map(|d| d.contains("nextval"))
                .unwrap_or(false)
            {
                continue;
            }

            // Check if this column exists in the JSON
            if let Some(val) = json_obj.get(&col.name) {
                insert_columns.push(col.name.clone());
                insert_values.push(format!("${}", param_idx));
                params.push(json_value_to_spi_param(val, &col.data_type));
                param_idx += 1;
            } else if !col.is_nullable && col.column_default.is_none() {
                // Required column not in JSON and no default
                return Err(SourceInsertError::MissingColumn(col.name.clone()));
            }
        }

        if insert_columns.is_empty() {
            return Err(SourceInsertError::JsonParse(
                "no matching columns found in JSON".to_string(),
            ));
        }

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({}) RETURNING {}",
            source_table,
            insert_columns.join(", "),
            insert_values.join(", "),
            offset_column
        );

        let result = self
            .bridge
            .query(&query, params, vec![ColumnType::Int64])
            .await?;

        result
            .first()
            .and_then(|r| r.get_i64(0).ok())
            .ok_or(SourceInsertError::Spi(SpiError::NoRows))
    }

    /// Upsert a message in table mode
    ///
    /// Uses INSERT ... ON CONFLICT DO UPDATE to upsert based on key column.
    /// Returns the kafka_offset value.
    pub async fn upsert_table_message(
        &self,
        topic: &TopicInfo,
        json_value: &serde_json::Value,
        key: &str,
        kafka_offset: i64,
    ) -> Result<i64, SourceInsertError> {
        let source_table = topic
            .source_table
            .as_ref()
            .ok_or_else(|| SourceInsertError::NotWritable("not source-backed".to_string()))?;

        let key_column = topic.write_key_column.as_ref().ok_or_else(|| {
            SourceInsertError::MissingColumn("write_key_column not configured".to_string())
        })?;

        let kafka_offset_col = topic.kafka_offset_column.as_ref().ok_or_else(|| {
            SourceInsertError::MissingColumn("kafka_offset_column not configured".to_string())
        })?;

        // Get table columns
        let columns = self.get_table_columns(source_table).await?;

        let json_obj = json_value.as_object().ok_or_else(|| {
            SourceInsertError::JsonParse("message value must be a JSON object".to_string())
        })?;

        // Build column lists
        let mut insert_columns = Vec::new();
        let mut insert_values = Vec::new();
        let mut update_clauses = Vec::new();
        let mut params = Vec::new();
        let mut param_idx = 1;

        // Add key column first
        insert_columns.push(key_column.clone());
        insert_values.push(format!("${}", param_idx));
        params.push(SpiParam::Text(Some(key.to_string())));
        param_idx += 1;

        // Add kafka_offset column
        insert_columns.push(kafka_offset_col.clone());
        insert_values.push(format!("${}", param_idx));
        params.push(SpiParam::Int8(Some(kafka_offset)));
        param_idx += 1;

        // Add other columns from JSON
        for col in &columns {
            // Skip key column (already added) and kafka_offset column (already added)
            if &col.name == key_column || &col.name == kafka_offset_col {
                continue;
            }

            // Skip auto-increment columns
            if col
                .column_default
                .as_ref()
                .map(|d| d.contains("nextval"))
                .unwrap_or(false)
            {
                continue;
            }

            if let Some(val) = json_obj.get(&col.name) {
                insert_columns.push(col.name.clone());
                insert_values.push(format!("${}", param_idx));
                update_clauses.push(format!("{} = EXCLUDED.{}", col.name, col.name));
                params.push(json_value_to_spi_param(val, &col.data_type));
                param_idx += 1;
            }
        }

        // Always update kafka_offset on conflict
        update_clauses.push(format!(
            "{} = EXCLUDED.{}",
            kafka_offset_col, kafka_offset_col
        ));

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({}) \
             ON CONFLICT ({}) DO UPDATE SET {} \
             RETURNING {}",
            source_table,
            insert_columns.join(", "),
            insert_values.join(", "),
            key_column,
            update_clauses.join(", "),
            kafka_offset_col
        );

        let result = self
            .bridge
            .query(&query, params, vec![ColumnType::Int64])
            .await?;

        result
            .first()
            .and_then(|r| r.get_i64(0).ok())
            .ok_or(SourceInsertError::Spi(SpiError::NoRows))
    }

    /// Insert a batch of messages to a source table
    ///
    /// Handles both stream and table modes based on topic configuration.
    pub async fn insert_batch(
        &self,
        topic: &TopicInfo,
        messages: &[(Option<String>, serde_json::Value)], // (key, value)
        base_kafka_offset: i64,
    ) -> Result<Vec<i64>, SourceInsertError> {
        if !topic.is_writable() {
            return Err(SourceInsertError::NotWritable(topic.name.clone()));
        }

        let mut offsets = Vec::with_capacity(messages.len());

        if topic.is_stream_mode() {
            // Stream mode: INSERT each message
            for (key, value) in messages {
                let offset = self
                    .insert_stream_message(topic, value, key.as_deref())
                    .await?;
                offsets.push(offset);
            }
        } else if topic.is_table_mode() {
            // Table mode: UPSERT each message
            for (i, (key, value)) in messages.iter().enumerate() {
                let key_str = key.as_deref().ok_or_else(|| {
                    SourceInsertError::MissingColumn(
                        "message key required for table mode".to_string(),
                    )
                })?;
                let kafka_offset = base_kafka_offset + i as i64;
                let offset = self
                    .upsert_table_message(topic, value, key_str, kafka_offset)
                    .await?;
                offsets.push(offset);
            }
        } else {
            return Err(SourceInsertError::NotWritable(format!(
                "unknown write mode for topic '{}'",
                topic.name
            )));
        }

        Ok(offsets)
    }
}

/// Parse a table name into (schema, table)
fn parse_table_name(table_name: &str) -> (&str, &str) {
    if let Some(dot_pos) = table_name.find('.') {
        (&table_name[..dot_pos], &table_name[dot_pos + 1..])
    } else {
        ("public", table_name)
    }
}

/// Convert a JSON value to an SPI parameter
fn json_value_to_spi_param(value: &serde_json::Value, data_type: &str) -> SpiParam {
    match value {
        serde_json::Value::Null => SpiParam::Text(None),
        serde_json::Value::Bool(b) => {
            // Convert boolean to text representation for database
            SpiParam::Text(Some(b.to_string()))
        }
        serde_json::Value::Number(n) => {
            if data_type.contains("int") {
                if let Some(i) = n.as_i64() {
                    SpiParam::Int8(Some(i))
                } else {
                    SpiParam::Text(Some(n.to_string()))
                }
            } else {
                SpiParam::Text(Some(n.to_string()))
            }
        }
        serde_json::Value::String(s) => SpiParam::Text(Some(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            // For complex types, store as JSON text
            SpiParam::Text(Some(value.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("public.orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("orders");
        assert_eq!(schema, "public");
        assert_eq!(table, "orders");
    }

    #[test]
    fn test_json_value_to_spi_param_string() {
        let value = json!("hello");
        let param = json_value_to_spi_param(&value, "text");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected SpiParam::Text(Some(_)), got {:?}", param)
        };
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_json_value_to_spi_param_number_int() {
        let value = json!(42);
        let param = json_value_to_spi_param(&value, "bigint");
        let SpiParam::Int8(Some(n)) = param else {
            panic!("expected SpiParam::Int8(Some(_)), got {:?}", param)
        };
        assert_eq!(n, 42);
    }

    #[test]
    fn test_json_value_to_spi_param_null() {
        let value = json!(null);
        let param = json_value_to_spi_param(&value, "text");
        assert!(
            matches!(param, SpiParam::Text(None)),
            "expected SpiParam::Text(None), got {:?}",
            param
        );
    }

    #[test]
    fn test_json_value_to_spi_param_object() {
        let value = json!({"nested": "object"});
        let param = json_value_to_spi_param(&value, "jsonb");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected SpiParam::Text(Some(_)), got {:?}", param)
        };
        assert!(s.contains("nested"));
    }

    #[test]
    fn test_source_insert_error_display() {
        let err = SourceInsertError::NotWritable("test-topic".to_string());
        assert!(err.to_string().contains("not writable"));

        let err = SourceInsertError::MissingColumn("name".to_string());
        assert!(err.to_string().contains("missing required column"));
    }
}
