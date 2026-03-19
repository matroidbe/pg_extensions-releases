//! Schema validation for the MQTT protocol path
//!
//! Provides async validation of MQTT payloads against JSON Schemas bound to topics.
//! Uses the SPI bridge for database queries since this runs on tokio worker threads.

use std::sync::Arc;

use crate::storage::{ColumnType, SpiBridge, SpiError, SpiParam};

/// Validation result
#[derive(Debug, Clone)]
pub enum ValidationResult {
    Valid,
    Invalid(String),
    NoSchema,
}

/// Validation mode
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationMode {
    Strict,
    Log,
}

/// Topic schema info including optional source table
#[derive(Debug, Clone)]
pub struct TopicSchemaInfo {
    pub schema_def: serde_json::Value,
    pub source_table: Option<String>,
    pub validation_mode: ValidationMode,
}

/// Validation error
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("SPI error: {0}")]
    Spi(#[from] SpiError),
    #[error("Schema compilation error: {0}")]
    SchemaCompilation(String),
    #[error("JSON parse error: {0}")]
    JsonParse(String),
}

/// Async schema validator for the MQTT protocol path
pub struct MqttSchemaValidator {
    bridge: Arc<SpiBridge>,
}

impl MqttSchemaValidator {
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    /// Get topic schema info (schema definition, source table, validation mode)
    pub async fn get_topic_info(
        &self,
        topic: &str,
    ) -> Result<Option<TopicSchemaInfo>, ValidationError> {
        let result = self
            .bridge
            .query(
                r#"
                SELECT s.schema_def::text, ts.source_table, ts.validation_mode
                FROM pgmqtt.topic_schemas ts
                JOIN pgmqtt.schemas s ON ts.schema_id = s.id
                WHERE ts.topic = $1
                "#,
                vec![SpiParam::Text(Some(topic.to_string()))],
                vec![ColumnType::Text, ColumnType::Text, ColumnType::Text],
            )
            .await?;

        let row = match result.first() {
            Some(row) => row,
            None => return Ok(None),
        };

        let schema_text = match row.get_string(0) {
            Some(s) => s,
            None => return Ok(None),
        };

        let schema_def: serde_json::Value = serde_json::from_str(&schema_text)
            .map_err(|e| ValidationError::JsonParse(format!("Invalid schema JSON: {}", e)))?;

        let source_table = row.get_string(1);

        let validation_mode = match row.get_string(2).as_deref() {
            Some("LOG") => ValidationMode::Log,
            _ => ValidationMode::Strict,
        };

        Ok(Some(TopicSchemaInfo {
            schema_def,
            source_table,
            validation_mode,
        }))
    }

    /// Validate a payload against the schema bound to a topic
    pub async fn validate_payload(
        &self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(ValidationResult, ValidationMode, Option<TopicSchemaInfo>), ValidationError> {
        let info = match self.get_topic_info(topic).await? {
            Some(info) => info,
            None => return Ok((ValidationResult::NoSchema, ValidationMode::Strict, None)),
        };

        let mode = info.validation_mode.clone();

        // Parse payload as UTF-8
        let payload_str = match std::str::from_utf8(payload) {
            Ok(s) => s,
            Err(e) => {
                return Ok((
                    ValidationResult::Invalid(format!("Payload is not valid UTF-8: {}", e)),
                    mode,
                    Some(info),
                ));
            }
        };

        // Parse as JSON
        let payload_json: serde_json::Value = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(e) => {
                return Ok((
                    ValidationResult::Invalid(format!("Payload is not valid JSON: {}", e)),
                    mode,
                    Some(info),
                ));
            }
        };

        // Compile schema and validate
        match jsonschema::validator_for(&info.schema_def) {
            Ok(validator) => {
                if validator.is_valid(&payload_json) {
                    Ok((ValidationResult::Valid, mode, Some(info)))
                } else {
                    let errors: Vec<String> = validator
                        .iter_errors(&payload_json)
                        .map(|e| format!("{}", e))
                        .collect();
                    Ok((
                        ValidationResult::Invalid(format!(
                            "Schema validation failed: {}",
                            errors.join("; ")
                        )),
                        mode,
                        Some(info),
                    ))
                }
            }
            Err(e) => Err(ValidationError::SchemaCompilation(format!("{}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_result_debug() {
        let valid = ValidationResult::Valid;
        let invalid = ValidationResult::Invalid("test error".to_string());
        let no_schema = ValidationResult::NoSchema;
        assert!(format!("{:?}", valid).contains("Valid"));
        assert!(format!("{:?}", invalid).contains("Invalid"));
        assert!(format!("{:?}", no_schema).contains("NoSchema"));
    }

    #[test]
    fn test_validation_mode_equality() {
        assert_eq!(ValidationMode::Strict, ValidationMode::Strict);
        assert_eq!(ValidationMode::Log, ValidationMode::Log);
        assert_ne!(ValidationMode::Strict, ValidationMode::Log);
    }

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::JsonParse("bad json".to_string());
        assert!(err.to_string().contains("JSON parse error"));

        let err = ValidationError::SchemaCompilation("bad schema".to_string());
        assert!(err.to_string().contains("Schema compilation error"));
    }

    #[test]
    fn test_topic_schema_info_clone() {
        let info = TopicSchemaInfo {
            schema_def: serde_json::json!({"type": "object"}),
            source_table: Some("pgmqtt.test".to_string()),
            validation_mode: ValidationMode::Strict,
        };
        let cloned = info.clone();
        assert_eq!(cloned.source_table, Some("pgmqtt.test".to_string()));
        assert_eq!(cloned.validation_mode, ValidationMode::Strict);
    }
}
