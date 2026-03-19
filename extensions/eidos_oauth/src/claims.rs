use serde_json::Value;

/// Flatten nested JSON claims into dot-notation key-value pairs.
///
/// Examples:
/// - `{"sub": "alice"}` → `[("sub", "alice")]`
/// - `{"org": {"id": "123"}}` → `[("org.id", "123")]`
/// - `{"roles": ["admin", "user"]}` → `[("roles", "[\"admin\",\"user\"]")]`
pub fn flatten_claims(claims: &serde_json::Map<String, Value>) -> Vec<(String, String)> {
    let mut result = Vec::new();
    flatten_recursive(claims, "", &mut result);
    result
}

fn flatten_recursive(
    obj: &serde_json::Map<String, Value>,
    prefix: &str,
    result: &mut Vec<(String, String)>,
) {
    for (key, value) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };

        match value {
            Value::Object(nested) => {
                flatten_recursive(nested, &full_key, result);
            }
            Value::String(s) => {
                result.push((full_key, s.clone()));
            }
            Value::Number(n) => {
                result.push((full_key, n.to_string()));
            }
            Value::Bool(b) => {
                result.push((full_key, b.to_string()));
            }
            Value::Null => {
                // Skip null values — no GUC to set
            }
            Value::Array(_) => {
                // Serialize arrays as JSON strings
                result.push((full_key, value.to_string()));
            }
        }
    }
}

/// Sanitize a claim key for use as a GUC variable name.
/// Replaces dots with underscores and lowercases.
pub fn sanitize_key(key: &str) -> String {
    key.replace('.', "_").to_lowercase()
}

/// Inject flattened claims as `app.user_<key>` GUC session variables.
///
/// Uses `pg_sys::SetConfigOption` with `PGC_USERSET` context and `PGC_S_SESSION` source.
///
/// # Safety
/// Must be called from within a PG backend process.
pub unsafe fn inject_claims_as_gucs(claims: &serde_json::Map<String, Value>) {
    use pgrx::pg_sys;
    use std::ffi::CString;

    let flat = flatten_claims(claims);

    for (key, value) in &flat {
        let guc_key = format!("app.user_{}", sanitize_key(key));

        let c_name = match CString::new(guc_key.as_bytes()) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let c_value = match CString::new(value.as_bytes()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        pg_sys::SetConfigOption(
            c_name.as_ptr(),
            c_value.as_ptr(),
            pg_sys::GucContext::PGC_USERSET,
            pg_sys::GucSource::PGC_S_SESSION,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_flatten_simple_claims() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"sub": "alice", "email": "alice@example.com"})).unwrap();

        let flat = flatten_claims(&claims);

        assert!(flat.contains(&("sub".to_string(), "alice".to_string())));
        assert!(flat.contains(&("email".to_string(), "alice@example.com".to_string())));
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn test_flatten_nested_claims() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"org": {"id": "123", "name": "Acme"}})).unwrap();

        let flat = flatten_claims(&claims);

        assert!(flat.contains(&("org.id".to_string(), "123".to_string())));
        assert!(flat.contains(&("org.name".to_string(), "Acme".to_string())));
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn test_flatten_array_claims() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"roles": ["admin", "user"]})).unwrap();

        let flat = flatten_claims(&claims);

        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0].0, "roles");
        assert_eq!(flat[0].1, r#"["admin","user"]"#);
    }

    #[test]
    fn test_flatten_bool_and_number() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"active": true, "age": 30})).unwrap();

        let flat = flatten_claims(&claims);

        assert!(flat.contains(&("active".to_string(), "true".to_string())));
        assert!(flat.contains(&("age".to_string(), "30".to_string())));
    }

    #[test]
    fn test_flatten_null_skipped() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"sub": "alice", "middle_name": null})).unwrap();

        let flat = flatten_claims(&claims);

        assert_eq!(flat.len(), 1);
        assert!(flat.contains(&("sub".to_string(), "alice".to_string())));
    }

    #[test]
    fn test_flatten_deeply_nested() {
        let claims: serde_json::Map<String, Value> =
            serde_json::from_value(json!({"a": {"b": {"c": "deep"}}})).unwrap();

        let flat = flatten_claims(&claims);

        assert_eq!(flat.len(), 1);
        assert_eq!(flat[0], ("a.b.c".to_string(), "deep".to_string()));
    }

    #[test]
    fn test_sanitize_key() {
        assert_eq!(sanitize_key("org.id"), "org_id");
        assert_eq!(sanitize_key("Role"), "role");
        assert_eq!(sanitize_key("a.b.c"), "a_b_c");
    }
}
