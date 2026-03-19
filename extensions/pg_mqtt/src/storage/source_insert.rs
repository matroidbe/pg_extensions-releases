//! Source table INSERT module for pg_mqtt typed topics
//!
//! Handles inserting MQTT payloads into typed topic backing tables
//! by decomposing JSON into table columns.

use std::sync::Arc;

use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};

/// Source insert error
#[derive(Debug, thiserror::Error)]
pub enum SourceInsertError {
    #[error("SPI error: {0}")]
    Spi(#[from] SpiError),
    #[error("missing required column: {0}")]
    MissingColumn(String),
    #[error("JSON parse error: {0}")]
    JsonParse(String),
}

/// Column information from a table
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
}

/// Handles inserting messages into typed topic backing tables
pub struct MqttSourceInserter {
    bridge: Arc<SpiBridge>,
}

impl MqttSourceInserter {
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    /// Get column information for a source table
    pub async fn get_table_columns(
        &self,
        source_table: &str,
    ) -> Result<Vec<ColumnInfo>, SourceInsertError> {
        let (schema, table) = parse_table_name(source_table);

        let result = self
            .bridge
            .query(
                "SELECT column_name::text, data_type::text, is_nullable::text, column_default::text
                 FROM information_schema.columns
                 WHERE table_schema = $1 AND table_name = $2
                 ORDER BY ordinal_position",
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

    /// Insert a JSON message into a typed topic's backing table
    ///
    /// Decomposes the JSON object into columns and executes a dynamic INSERT.
    /// Returns the generated id.
    pub async fn insert_message(
        &self,
        source_table: &str,
        json_value: &serde_json::Value,
    ) -> Result<i64, SourceInsertError> {
        let columns = self.get_table_columns(source_table).await?;

        let json_obj = json_value.as_object().ok_or_else(|| {
            SourceInsertError::JsonParse("payload must be a JSON object".to_string())
        })?;

        let mut insert_columns = Vec::new();
        let mut insert_values = Vec::new();
        let mut params = Vec::new();
        let mut param_idx = 1;

        for col in &columns {
            // Skip auto-increment columns (serial/bigserial)
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
                params.push(json_value_to_spi_param(val, &col.data_type));
                param_idx += 1;
            } else if !col.is_nullable && col.column_default.is_none() {
                return Err(SourceInsertError::MissingColumn(col.name.clone()));
            }
        }

        if insert_columns.is_empty() {
            return Err(SourceInsertError::JsonParse(
                "no matching columns found in JSON".to_string(),
            ));
        }

        // Check if the table has an 'id' column to use RETURNING
        let has_id = columns.iter().any(|c| c.name == "id");

        let query = if has_id {
            format!(
                "INSERT INTO {} ({}) VALUES ({}) RETURNING id",
                source_table,
                insert_columns.join(", "),
                insert_values.join(", ")
            )
        } else {
            format!(
                "INSERT INTO {} ({}) VALUES ({})",
                source_table,
                insert_columns.join(", "),
                insert_values.join(", ")
            )
        };

        if has_id {
            let result = self
                .bridge
                .query(&query, params, vec![ColumnType::Int64])
                .await?;

            result
                .first()
                .and_then(|r| r.get_i64(0).ok())
                .ok_or(SourceInsertError::Spi(SpiError::NoRows))
        } else {
            self.bridge.query(&query, params, vec![]).await?;
            Ok(0) // No id to return for tables without 'id' column
        }
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
        serde_json::Value::Bool(b) => SpiParam::Text(Some(b.to_string())),
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
        let (schema, table) = parse_table_name("pgmqtt.sensors");
        assert_eq!(schema, "pgmqtt");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("sensors");
        assert_eq!(schema, "public");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_json_value_to_spi_param_string() {
        let value = json!("hello");
        let param = json_value_to_spi_param(&value, "text");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_json_value_to_spi_param_number_int() {
        let value = json!(42);
        let param = json_value_to_spi_param(&value, "bigint");
        let SpiParam::Int8(Some(n)) = param else {
            panic!("expected Int8");
        };
        assert_eq!(n, 42);
    }

    #[test]
    fn test_json_value_to_spi_param_number_numeric() {
        let value = json!(3.14);
        let param = json_value_to_spi_param(&value, "numeric");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert!(s.starts_with("3.14"));
    }

    #[test]
    fn test_json_value_to_spi_param_null() {
        let value = json!(null);
        let param = json_value_to_spi_param(&value, "text");
        assert!(matches!(param, SpiParam::Text(None)));
    }

    #[test]
    fn test_json_value_to_spi_param_bool() {
        let value = json!(true);
        let param = json_value_to_spi_param(&value, "boolean");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert_eq!(s, "true");
    }

    #[test]
    fn test_json_value_to_spi_param_object() {
        let value = json!({"nested": "object"});
        let param = json_value_to_spi_param(&value, "jsonb");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert!(s.contains("nested"));
    }

    #[test]
    fn test_source_insert_error_display() {
        let err = SourceInsertError::MissingColumn("name".to_string());
        assert!(err.to_string().contains("missing required column"));

        let err = SourceInsertError::JsonParse("bad json".to_string());
        assert!(err.to_string().contains("JSON parse error"));
    }
}
