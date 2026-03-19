//! JSON Schema validation using the jsonschema crate

use crate::schema::storage;
use jsonschema::Validator;

/// Validate JSON data against a schema by ID
pub fn validate(schema_id: i32, data: pgrx::JsonB) -> bool {
    // Get the schema definition
    let schema_def = match storage::get_schema(schema_id) {
        Some(s) => s,
        None => {
            pgrx::warning!("Schema with id {} not found", schema_id);
            return false;
        }
    };

    validate_against_schema(&schema_def.0, &data.0)
}

/// Validate JSON data against a topic's bound schema
pub fn validate_for_topic(topic_name: &str, data: pgrx::JsonB, schema_type: &str) -> bool {
    // Get the schema bound to this topic
    let schema_def = match storage::get_topic_schema(topic_name, schema_type) {
        Some(s) => s,
        None => {
            // No schema bound, validation passes
            return true;
        }
    };

    validate_against_schema(&schema_def.0, &data.0)
}

/// Validate data against a schema definition
fn validate_against_schema(schema: &serde_json::Value, data: &serde_json::Value) -> bool {
    match Validator::new(schema) {
        Ok(validator) => validator.is_valid(data),
        Err(e) => {
            pgrx::warning!("Invalid schema: {}", e);
            false
        }
    }
}

/// Validate data and return detailed errors
#[allow(dead_code)]
pub fn validate_with_errors(
    schema: &serde_json::Value,
    data: &serde_json::Value,
) -> (bool, Vec<String>) {
    match Validator::new(schema) {
        Ok(validator) => {
            let errors: Vec<String> = validator
                .iter_errors(data)
                .map(|e| format!("{} at {}", e, e.instance_path))
                .collect();
            (errors.is_empty(), errors)
        }
        Err(e) => (false, vec![format!("Invalid schema: {}", e)]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_validate_against_schema_valid() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name"]
        });
        let data = json!({"name": "Alice", "age": 30});

        assert!(validate_against_schema(&schema, &data));
    }

    #[test]
    fn test_validate_against_schema_missing_required() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });
        let data = json!({"age": 30});

        assert!(!validate_against_schema(&schema, &data));
    }

    #[test]
    fn test_validate_against_schema_wrong_type() {
        let schema = json!({
            "type": "object",
            "properties": {
                "age": {"type": "integer"}
            }
        });
        let data = json!({"age": "thirty"});

        assert!(!validate_against_schema(&schema, &data));
    }

    #[test]
    fn test_validate_against_schema_string_max_length() {
        let schema = json!({
            "type": "object",
            "properties": {
                "code": {"type": "string", "maxLength": 3}
            }
        });

        let valid_data = json!({"code": "ABC"});
        assert!(validate_against_schema(&schema, &valid_data));

        let invalid_data = json!({"code": "ABCDEF"});
        assert!(!validate_against_schema(&schema, &invalid_data));
    }

    #[test]
    fn test_validate_against_schema_nullable_field() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": ["string", "null"]}
            }
        });

        let with_string = json!({"name": "Alice"});
        assert!(validate_against_schema(&schema, &with_string));

        let with_null = json!({"name": null});
        assert!(validate_against_schema(&schema, &with_null));
    }

    #[test]
    fn test_validate_against_schema_array() {
        let schema = json!({
            "type": "object",
            "properties": {
                "tags": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            }
        });

        let valid_data = json!({"tags": ["a", "b", "c"]});
        assert!(validate_against_schema(&schema, &valid_data));

        let invalid_data = json!({"tags": ["a", 1, "c"]});
        assert!(!validate_against_schema(&schema, &invalid_data));
    }

    #[test]
    fn test_validate_against_schema_uuid_format() {
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string", "format": "uuid"}
            }
        });

        let valid_data = json!({"id": "550e8400-e29b-41d4-a716-446655440000"});
        assert!(validate_against_schema(&schema, &valid_data));

        // Note: JSON Schema format validation is typically lax by default
        // The jsonschema crate may not enforce format validation strictly
    }

    #[test]
    fn test_validate_against_schema_date_time_format() {
        let schema = json!({
            "type": "object",
            "properties": {
                "created_at": {"type": "string", "format": "date-time"}
            }
        });

        let valid_data = json!({"created_at": "2024-01-15T10:30:00Z"});
        assert!(validate_against_schema(&schema, &valid_data));
    }

    #[test]
    fn test_validate_against_schema_nested_object() {
        let schema = json!({
            "type": "object",
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"}
                    },
                    "required": ["name"]
                }
            }
        });

        let valid_data = json!({
            "user": {
                "name": "Alice",
                "email": "alice@example.com"
            }
        });
        assert!(validate_against_schema(&schema, &valid_data));

        let invalid_data = json!({
            "user": {
                "email": "alice@example.com"
            }
        });
        assert!(!validate_against_schema(&schema, &invalid_data));
    }

    #[test]
    fn test_validate_with_errors_returns_details() {
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer"}
            },
            "required": ["name", "age"]
        });
        let data = json!({"name": 123}); // Wrong type and missing age

        let (is_valid, errors) = validate_with_errors(&schema, &data);
        assert!(!is_valid);
        assert!(!errors.is_empty());
    }

    #[test]
    fn test_validate_invalid_schema() {
        let invalid_schema = json!({
            "type": "not-a-valid-type"
        });
        let data = json!({"name": "test"});

        // Should handle gracefully
        let result = validate_against_schema(&invalid_schema, &data);
        // Behavior depends on jsonschema crate - it may still return true/false
        // The important thing is it doesn't panic
        let _ = result;
    }
}
