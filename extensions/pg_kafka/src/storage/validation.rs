//! Schema validation for pg_kafka
//!
//! This module validates Kafka messages against JSON schemas bound to topics.
//! Uses the pgkafka.schemas and pgkafka.topic_schemas tables for schema storage,
//! and the jsonschema crate for validation.

use std::sync::Arc;

use jsonschema::Validator;

use super::spi_bridge::{ColumnType, SpiBridge, SpiError, SpiParam};

/// Validation result
#[derive(Debug, Clone)]
pub enum ValidationResult {
    /// Validation passed
    Valid,
    /// Validation failed with error message
    Invalid(String),
    /// No schema bound to topic (passes by default)
    NoSchema,
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

/// Schema validator using pgkafka schema tables
pub struct SchemaValidator {
    bridge: Arc<SpiBridge>,
}

impl SchemaValidator {
    /// Create a new schema validator
    pub fn new(bridge: Arc<SpiBridge>) -> Self {
        Self { bridge }
    }

    /// Get the schema definition bound to a topic
    ///
    /// Returns None if no schema is bound
    pub async fn get_topic_schema(
        &self,
        topic_name: &str,
        schema_type: &str,
    ) -> Result<Option<serde_json::Value>, ValidationError> {
        let query = r#"
            SELECT s.schema_def::text
            FROM pgkafka.topic_schemas ts
            JOIN pgkafka.schemas s ON ts.schema_id = s.id
            WHERE ts.topic_name = $1 AND ts.schema_type = $2
        "#;

        let result = self
            .bridge
            .query(
                query,
                vec![
                    SpiParam::Text(Some(topic_name.to_string())),
                    SpiParam::Text(Some(schema_type.to_string())),
                ],
                vec![ColumnType::Text],
            )
            .await?;

        if let Some(row) = result.first() {
            if let Some(schema_json) = row.get_string(0) {
                let schema: serde_json::Value = serde_json::from_str(&schema_json)
                    .map_err(|e| ValidationError::JsonParse(e.to_string()))?;
                return Ok(Some(schema));
            }
        }

        Ok(None)
    }

    /// Get the validation mode for a topic
    ///
    /// Returns "STRICT" (default) or "LOG"
    #[allow(dead_code)]
    pub async fn get_validation_mode(
        &self,
        topic_name: &str,
        schema_type: &str,
    ) -> Result<String, ValidationError> {
        let query = r#"
            SELECT COALESCE(ts.validation_mode, 'STRICT')
            FROM pgkafka.topic_schemas ts
            WHERE ts.topic_name = $1 AND ts.schema_type = $2
        "#;

        let result = self
            .bridge
            .query(
                query,
                vec![
                    SpiParam::Text(Some(topic_name.to_string())),
                    SpiParam::Text(Some(schema_type.to_string())),
                ],
                vec![ColumnType::Text],
            )
            .await?;

        Ok(result
            .first()
            .and_then(|r| r.get_string(0))
            .unwrap_or_else(|| "STRICT".to_string()))
    }

    /// Validate a JSON value against a topic's bound schema
    pub async fn validate_for_topic(
        &self,
        topic_name: &str,
        json_value: &str,
        schema_type: &str,
    ) -> Result<ValidationResult, ValidationError> {
        // Get the schema bound to this topic
        let schema_def = match self.get_topic_schema(topic_name, schema_type).await? {
            Some(s) => s,
            None => {
                // No schema bound, validation passes
                return Ok(ValidationResult::NoSchema);
            }
        };

        // Parse the JSON value to validate
        let data: serde_json::Value = serde_json::from_str(json_value)
            .map_err(|e| ValidationError::JsonParse(e.to_string()))?;

        // Compile the schema and validate
        let validator = Validator::new(&schema_def)
            .map_err(|e| ValidationError::SchemaCompilation(e.to_string()))?;

        if validator.is_valid(&data) {
            Ok(ValidationResult::Valid)
        } else {
            // Collect error details
            let errors: Vec<String> = validator
                .iter_errors(&data)
                .map(|e| format!("{}", e))
                .collect();

            let error_msg = if errors.is_empty() {
                format!(
                    "Message does not conform to schema for topic '{}'",
                    topic_name
                )
            } else {
                format!(
                    "Message does not conform to schema for topic '{}': {}",
                    topic_name,
                    errors.join("; ")
                )
            };

            Ok(ValidationResult::Invalid(error_msg))
        }
    }

    /// Validate a batch of JSON messages against a topic's schema
    ///
    /// Returns a list of validation results for each message.
    /// For efficiency, fetches the schema once and validates all messages.
    #[allow(dead_code)]
    pub async fn validate_batch(
        &self,
        topic_name: &str,
        json_values: &[&str],
        schema_type: &str,
    ) -> Result<Vec<ValidationResult>, ValidationError> {
        // Get the schema bound to this topic
        let schema_def = match self.get_topic_schema(topic_name, schema_type).await? {
            Some(s) => s,
            None => {
                // No schema bound, all pass
                return Ok(vec![ValidationResult::NoSchema; json_values.len()]);
            }
        };

        // Compile the schema once
        let validator = Validator::new(&schema_def)
            .map_err(|e| ValidationError::SchemaCompilation(e.to_string()))?;

        // Validate each message
        let mut results = Vec::with_capacity(json_values.len());
        for json_value in json_values {
            let result = match serde_json::from_str::<serde_json::Value>(json_value) {
                Ok(data) => {
                    if validator.is_valid(&data) {
                        ValidationResult::Valid
                    } else {
                        ValidationResult::Invalid(format!(
                            "Message does not conform to schema for topic '{}'",
                            topic_name
                        ))
                    }
                }
                Err(e) => ValidationResult::Invalid(format!("Invalid JSON: {}", e)),
            };
            results.push(result);
        }

        Ok(results)
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
    fn test_validation_error_display() {
        let spi_err = ValidationError::Spi(SpiError::QueryFailed("test".to_string()));
        let schema_err = ValidationError::SchemaCompilation("invalid".to_string());
        let json_err = ValidationError::JsonParse("bad json".to_string());

        assert!(spi_err.to_string().contains("SPI error"));
        assert!(schema_err.to_string().contains("Schema compilation"));
        assert!(json_err.to_string().contains("JSON parse"));
    }
}
