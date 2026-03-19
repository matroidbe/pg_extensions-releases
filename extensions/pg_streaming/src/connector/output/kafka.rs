//! Kafka output connectors — writes to pgkafka topics
//!
//! Two output types:
//! - KafkaOutput: writes to regular topics via pgkafka.produce()
//! - TypedTopicOutput: writes directly to source-backed table (stream or table mode)

use crate::connector::OutputConnector;
use crate::record::RecordBatch;
use pgrx::prelude::*;

// =============================================================================
// Regular Kafka output (via pgkafka.produce)
// =============================================================================

pub struct KafkaOutput {
    topic: String,
    key_field: Option<String>,
}

impl KafkaOutput {
    pub fn new(topic: &str, key_field: Option<&str>) -> Self {
        Self {
            topic: topic.to_string(),
            key_field: key_field.map(|s| s.to_string()),
        }
    }
}

impl OutputConnector for KafkaOutput {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        for record in records {
            let value_str = serde_json::to_string(record)
                .map_err(|e| format!("Failed to serialize record: {}", e))?;
            let value_bytes = value_str.as_bytes().to_vec();

            let key_bytes: Option<Vec<u8>> = self.key_field.as_ref().and_then(|kf| {
                record.get(kf).map(|v| match v {
                    serde_json::Value::String(s) => s.as_bytes().to_vec(),
                    other => other.to_string().into_bytes(),
                })
            });

            match &key_bytes {
                Some(kb) => {
                    Spi::get_one_with_args::<i64>(
                        "SELECT pgkafka.produce($1, $2, $3, NULL)",
                        &[
                            self.topic.as_str().into(),
                            value_bytes.as_slice().into(),
                            kb.as_slice().into(),
                        ],
                    )
                    .map_err(|e| format!("Failed to produce to '{}': {}", self.topic, e))?;
                }
                None => {
                    Spi::get_one_with_args::<i64>(
                        "SELECT pgkafka.produce($1, $2, NULL, NULL)",
                        &[self.topic.as_str().into(), value_bytes.as_slice().into()],
                    )
                    .map_err(|e| format!("Failed to produce to '{}': {}", self.topic, e))?;
                }
            }
        }

        Ok(())
    }
}

// =============================================================================
// Typed topic output (direct table INSERT/UPSERT)
// =============================================================================

/// Output connector for writable source-backed typed topics.
/// Writes records directly to the source table via INSERT (stream mode)
/// or INSERT...ON CONFLICT (table mode).
pub struct TypedTopicOutput {
    source_table: String,
    write_mode: String,
    write_key_column: Option<String>,
    /// Writable columns: (name, pg_type) — excludes auto-generated columns
    columns: Vec<(String, String)>,
}

impl TypedTopicOutput {
    pub fn new(
        source_table: &str,
        write_mode: &str,
        write_key_column: Option<&str>,
        columns: Vec<(String, String)>,
    ) -> Self {
        Self {
            source_table: source_table.to_string(),
            write_mode: write_mode.to_string(),
            write_key_column: write_key_column.map(|s| s.to_string()),
            columns,
        }
    }

    /// Build the INSERT SQL for stream mode (append-only)
    fn build_stream_insert(&self, record: &serde_json::Value) -> Option<(String, Vec<String>)> {
        let mut col_names = Vec::new();
        let mut values = Vec::new();

        for (name, _) in &self.columns {
            if let Some(val) = record.get(name) {
                if !val.is_null() {
                    col_names.push(name.clone());
                    values.push(json_value_to_sql_literal(val));
                }
            }
        }

        if col_names.is_empty() {
            return None;
        }

        // Use literal values in the query since SPI arg passing for dynamic
        // column sets is complex. Values are safely escaped via json_value_to_sql_literal.
        let query = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.source_table,
            col_names.join(", "),
            values.join(", ")
        );

        Some((query, col_names))
    }

    /// Build the UPSERT SQL for table mode
    fn build_table_upsert(&self, record: &serde_json::Value) -> Option<String> {
        let key_col = self.write_key_column.as_ref()?;

        let mut col_names = Vec::new();
        let mut values = Vec::new();

        for (name, _) in &self.columns {
            if let Some(val) = record.get(name) {
                if !val.is_null() {
                    col_names.push(name.clone());
                    values.push(json_value_to_sql_literal(val));
                }
            }
        }

        if col_names.is_empty() {
            return None;
        }

        let update_clauses: Vec<String> = col_names
            .iter()
            .filter(|n| n.as_str() != key_col.as_str())
            .map(|n| format!("{} = EXCLUDED.{}", n, n))
            .collect();

        let query = format!(
            "INSERT INTO {} ({}) VALUES ({}) \
             ON CONFLICT ({}) DO UPDATE SET {}",
            self.source_table,
            col_names.join(", "),
            values.join(", "),
            key_col,
            if update_clauses.is_empty() {
                format!("{} = EXCLUDED.{}", key_col, key_col)
            } else {
                update_clauses.join(", ")
            },
        );

        Some(query)
    }
}

impl OutputConnector for TypedTopicOutput {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        for record in records {
            let query = match self.write_mode.as_str() {
                "table" => self
                    .build_table_upsert(record)
                    .ok_or_else(|| "No writable fields in record".to_string())?,
                _ => {
                    // "stream" mode (default)
                    let (q, _) = self
                        .build_stream_insert(record)
                        .ok_or_else(|| "No writable fields in record".to_string())?;
                    q
                }
            };

            Spi::run(&query)
                .map_err(|e| format!("Failed to write to '{}': {}", self.source_table, e))?;
        }

        Ok(())
    }
}

/// Convert a JSON value to a safe SQL literal string
fn json_value_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            // Escape single quotes for SQL safety
            format!("'{}'", s.replace('\'', "''"))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let json_str = serde_json::to_string(val).unwrap_or_default();
            format!("'{}'::jsonb", json_str.replace('\'', "''"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_value_to_sql_literal() {
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(null)), "NULL");
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(true)), "true");
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(42)), "42");
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(9.99)), "9.99");
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!("hello")),
            "'hello'"
        );
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!("O'Brien")),
            "'O''Brien'"
        );
    }

    #[test]
    fn test_typed_output_stream_insert() {
        let output = TypedTopicOutput::new(
            "public.orders",
            "stream",
            None,
            vec![
                ("customer_id".to_string(), "text".to_string()),
                ("amount".to_string(), "numeric".to_string()),
                ("status".to_string(), "text".to_string()),
            ],
        );

        let record = serde_json::json!({
            "customer_id": "cust-1",
            "amount": 99.99,
            "status": "pending"
        });

        let (query, _cols) = output.build_stream_insert(&record).unwrap();
        assert!(query.starts_with("INSERT INTO public.orders"));
        assert!(query.contains("customer_id"));
        assert!(query.contains("amount"));
        assert!(query.contains("'cust-1'"));
        assert!(query.contains("99.99"));
    }

    #[test]
    fn test_typed_output_table_upsert() {
        let output = TypedTopicOutput::new(
            "public.customers",
            "table",
            Some("customer_id"),
            vec![
                ("customer_id".to_string(), "text".to_string()),
                ("name".to_string(), "text".to_string()),
                ("email".to_string(), "text".to_string()),
            ],
        );

        let record = serde_json::json!({
            "customer_id": "cust-1",
            "name": "Alice",
            "email": "alice@example.com"
        });

        let query = output.build_table_upsert(&record).unwrap();
        assert!(query.starts_with("INSERT INTO public.customers"));
        assert!(query.contains("ON CONFLICT (customer_id)"));
        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains("name = EXCLUDED.name"));
        assert!(query.contains("email = EXCLUDED.email"));
    }

    #[test]
    fn test_typed_output_empty_record_returns_none() {
        let output = TypedTopicOutput::new(
            "public.orders",
            "stream",
            None,
            vec![("amount".to_string(), "numeric".to_string())],
        );

        let record = serde_json::json!({"other_field": 42});
        assert!(output.build_stream_insert(&record).is_none());
    }
}
