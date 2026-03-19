//! Storage configuration - builds storage options from GUCs.

use std::collections::HashMap;

/// Build storage options HashMap from GUCs for delta-rs.
pub fn build_storage_options(uri: &str) -> HashMap<String, String> {
    let mut options = HashMap::new();

    let uri_lower = uri.to_lowercase();

    if uri_lower.starts_with("s3://") {
        build_s3_options(&mut options);
    } else if uri_lower.starts_with("az://") || uri_lower.starts_with("azure://") {
        build_azure_options(&mut options);
    } else if uri_lower.starts_with("gs://") {
        build_gcs_options(&mut options);
    }
    // file:// URIs don't need storage options

    options
}

fn build_s3_options(options: &mut HashMap<String, String>) {
    // Region
    if let Some(region) = crate::PG_DELTA_AWS_REGION.get() {
        if let Ok(s) = region.to_str() {
            options.insert("AWS_REGION".to_string(), s.to_string());
        }
    }

    // Check for instance profile first
    if crate::PG_DELTA_AWS_USE_INSTANCE_PROFILE.get() {
        // delta-rs will use the instance metadata service
        // No explicit credentials needed
    } else {
        // Static credentials
        if let Some(access_key) = crate::PG_DELTA_AWS_ACCESS_KEY_ID.get() {
            if let Ok(s) = access_key.to_str() {
                options.insert("AWS_ACCESS_KEY_ID".to_string(), s.to_string());
            }
        }

        if let Some(secret_key) = crate::PG_DELTA_AWS_SECRET_ACCESS_KEY.get() {
            if let Ok(s) = secret_key.to_str() {
                options.insert("AWS_SECRET_ACCESS_KEY".to_string(), s.to_string());
            }
        }

        if let Some(session_token) = crate::PG_DELTA_AWS_SESSION_TOKEN.get() {
            if let Ok(s) = session_token.to_str() {
                options.insert("AWS_SESSION_TOKEN".to_string(), s.to_string());
            }
        }
    }

    // Custom endpoint (for S3-compatible storage)
    if let Some(endpoint) = crate::PG_DELTA_AWS_ENDPOINT.get() {
        if let Ok(s) = endpoint.to_str() {
            options.insert("AWS_ENDPOINT_URL".to_string(), s.to_string());
        }
    }

    // Allow HTTP (for local development)
    if crate::PG_DELTA_AWS_ALLOW_HTTP.get() {
        options.insert("AWS_ALLOW_HTTP".to_string(), "true".to_string());
    }
}

fn build_azure_options(options: &mut HashMap<String, String>) {
    if let Some(account) = crate::PG_DELTA_AZURE_STORAGE_ACCOUNT.get() {
        if let Ok(s) = account.to_str() {
            options.insert("AZURE_STORAGE_ACCOUNT_NAME".to_string(), s.to_string());
        }
    }

    if crate::PG_DELTA_AZURE_USE_MANAGED_IDENTITY.get() {
        options.insert("AZURE_USE_AZURE_CLI".to_string(), "true".to_string());
    } else {
        if let Some(key) = crate::PG_DELTA_AZURE_STORAGE_KEY.get() {
            if let Ok(s) = key.to_str() {
                options.insert("AZURE_STORAGE_ACCOUNT_KEY".to_string(), s.to_string());
            }
        }

        if let Some(sas) = crate::PG_DELTA_AZURE_SAS_TOKEN.get() {
            if let Ok(s) = sas.to_str() {
                options.insert("AZURE_STORAGE_SAS_TOKEN".to_string(), s.to_string());
            }
        }
    }
}

fn build_gcs_options(options: &mut HashMap<String, String>) {
    if crate::PG_DELTA_GCS_USE_DEFAULT_CREDENTIALS.get() {
        // Uses GOOGLE_APPLICATION_CREDENTIALS or metadata service
        // No explicit options needed
    } else {
        if let Some(path) = crate::PG_DELTA_GCS_SERVICE_ACCOUNT_PATH.get() {
            if let Ok(s) = path.to_str() {
                options.insert("GOOGLE_SERVICE_ACCOUNT".to_string(), s.to_string());
            }
        }

        if let Some(key) = crate::PG_DELTA_GCS_SERVICE_ACCOUNT_KEY.get() {
            if let Ok(s) = key.to_str() {
                options.insert("GOOGLE_SERVICE_ACCOUNT_KEY".to_string(), s.to_string());
            }
        }
    }
}

/// Merge per-stream storage options with global config.
pub fn merge_storage_options(
    uri: &str,
    overrides: Option<&serde_json::Value>,
) -> HashMap<String, String> {
    let mut options = build_storage_options(uri);

    if let Some(overrides) = overrides {
        if let Some(obj) = overrides.as_object() {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    options.insert(key.clone(), s.to_string());
                } else if let Some(b) = value.as_bool() {
                    options.insert(key.clone(), b.to_string());
                } else if let Some(n) = value.as_i64() {
                    options.insert(key.clone(), n.to_string());
                }
            }
        }
    }

    options
}

/// Merge overrides into an existing options map (without reading GUCs).
/// This is useful for testing and for cases where base options are provided externally.
#[allow(dead_code)]
pub fn merge_overrides(
    base: HashMap<String, String>,
    overrides: Option<&serde_json::Value>,
) -> HashMap<String, String> {
    let mut options = base;

    if let Some(overrides) = overrides {
        if let Some(obj) = overrides.as_object() {
            for (key, value) in obj {
                if let Some(s) = value.as_str() {
                    options.insert(key.clone(), s.to_string());
                } else if let Some(b) = value.as_bool() {
                    options.insert(key.clone(), b.to_string());
                } else if let Some(n) = value.as_i64() {
                    options.insert(key.clone(), n.to_string());
                }
            }
        }
    }

    options
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Tests that call build_storage_options() with cloud URIs (s3://, az://, gs://)
    // cannot run as unit tests because they access pgrx GUCs which require PostgreSQL.
    // Those are tested via #[pg_test] integration tests instead.

    #[test]
    fn test_build_storage_options_file() {
        // file:// URIs don't access GUCs, so this is safe as a unit test
        let options = build_storage_options("file:///tmp/delta");
        assert!(options.is_empty());
    }

    #[test]
    fn test_merge_overrides() {
        let base = HashMap::new();
        let overrides = serde_json::json!({
            "AWS_REGION": "eu-west-1",
            "custom_option": "value",
            "bool_option": true,
            "int_option": 42
        });

        let options = merge_overrides(base, Some(&overrides));
        assert_eq!(options.get("AWS_REGION"), Some(&"eu-west-1".to_string()));
        assert_eq!(options.get("custom_option"), Some(&"value".to_string()));
        assert_eq!(options.get("bool_option"), Some(&"true".to_string()));
        assert_eq!(options.get("int_option"), Some(&"42".to_string()));
    }

    #[test]
    fn test_merge_overrides_with_base() {
        let mut base = HashMap::new();
        base.insert("AWS_REGION".to_string(), "us-east-1".to_string());
        base.insert("existing_key".to_string(), "existing_value".to_string());

        let overrides = serde_json::json!({
            "AWS_REGION": "eu-west-1"  // Override existing
        });

        let options = merge_overrides(base, Some(&overrides));
        assert_eq!(options.get("AWS_REGION"), Some(&"eu-west-1".to_string()));
        assert_eq!(
            options.get("existing_key"),
            Some(&"existing_value".to_string())
        );
    }

    #[test]
    fn test_merge_overrides_none() {
        let mut base = HashMap::new();
        base.insert("key".to_string(), "value".to_string());

        let options = merge_overrides(base, None);
        assert_eq!(options.get("key"), Some(&"value".to_string()));
    }
}
