//! Schema validation for the MQTT protocol path
//!
//! Checks MQTT payloads against the JSON Schemas bound to their topics. The
//! batch writer (`server/batch.rs`) reads the bindings of a batch's topics in
//! one query and calls these pure functions for each message.

/// Validation result
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    Valid,
    Invalid(String),
    NoSchema,
}

/// Validation mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValidationMode {
    Strict,
    Log,
}

impl ValidationMode {
    /// From `pgmqtt.topic_schemas.validation_mode`; anything but LOG is STRICT
    pub fn from_column(value: Option<&str>) -> Self {
        match value {
            Some("LOG") => ValidationMode::Log,
            _ => ValidationMode::Strict,
        }
    }
}

/// A topic's binding: its schema, compiled, and where its messages go
#[derive(Clone)]
pub struct TopicBinding {
    pub validator: std::sync::Arc<jsonschema::Validator>,
    pub source_table: Option<String>,
    pub mode: ValidationMode,
}

/// Compile a stored schema definition
pub fn compile_schema(schema_text: &str) -> Result<jsonschema::Validator, String> {
    let schema: serde_json::Value =
        serde_json::from_str(schema_text).map_err(|e| format!("invalid schema JSON: {e}"))?;
    jsonschema::validator_for(&schema).map_err(|e| format!("invalid schema: {e}"))
}

/// Check a payload against a topic's schema. Returns the parsed JSON when it
/// is valid JSON, valid or not against the schema.
pub fn check_payload(
    validator: &jsonschema::Validator,
    payload: &[u8],
) -> (ValidationResult, Option<serde_json::Value>) {
    let payload_str = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(e) => {
            return (
                ValidationResult::Invalid(format!("Payload is not valid UTF-8: {}", e)),
                None,
            )
        }
    };
    let payload_json: serde_json::Value = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(e) => {
            return (
                ValidationResult::Invalid(format!("Payload is not valid JSON: {}", e)),
                None,
            )
        }
    };
    if validator.is_valid(&payload_json) {
        return (ValidationResult::Valid, Some(payload_json));
    }
    let errors: Vec<String> = validator
        .iter_errors(&payload_json)
        .map(|e| format!("{}", e))
        .collect();
    (
        ValidationResult::Invalid(format!("Schema validation failed: {}", errors.join("; "))),
        Some(payload_json),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn validator() -> jsonschema::Validator {
        compile_schema(r#"{"type":"object","properties":{"v":{"type":"number"}},"required":["v"]}"#)
            .unwrap()
    }

    #[test]
    fn test_validation_mode_from_column() {
        assert_eq!(
            ValidationMode::from_column(Some("LOG")),
            ValidationMode::Log
        );
        assert_eq!(
            ValidationMode::from_column(Some("STRICT")),
            ValidationMode::Strict
        );
        assert_eq!(ValidationMode::from_column(None), ValidationMode::Strict);
    }

    #[test]
    fn test_check_payload() {
        let v = validator();
        let (result, json) = check_payload(&v, br#"{"v": 1.5}"#);
        assert_eq!(result, ValidationResult::Valid);
        assert_eq!(json.unwrap()["v"], 1.5);

        let (result, json) = check_payload(&v, br#"{"v": "x"}"#);
        assert!(matches!(result, ValidationResult::Invalid(_)));
        assert!(json.is_some(), "valid JSON is returned for LOG mode");

        let (result, json) = check_payload(&v, b"not json");
        assert!(matches!(result, ValidationResult::Invalid(m) if m.contains("not valid JSON")));
        assert!(json.is_none());

        let (result, _) = check_payload(&v, &[0xff, 0xfe]);
        assert!(matches!(result, ValidationResult::Invalid(m) if m.contains("UTF-8")));
    }

    #[test]
    fn test_compile_schema_rejects_bad_json() {
        assert!(compile_schema("{").is_err());
    }
}
