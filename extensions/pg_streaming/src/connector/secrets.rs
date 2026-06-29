//! Secret storage and `${secret:NAME}` / `${env:NAME}` interpolation.
//!
//! Secrets are stored in the `pgstreams.secrets` table. Values are loaded via
//! SPI just before a connector is instantiated and never logged.
//!
//! Interpolation walks a JSONB value and replaces any string of the form
//! `${secret:NAME}` (or `${env:NAME}` as a dev convenience) with the
//! corresponding secret/env value.

// `resolve_with` is a test-injection seam used by external integration tests.
#![allow(dead_code)]

use pgrx::prelude::*;
use serde_json::Value;

/// Walk `value` and substitute every `${secret:NAME}` placeholder with the
/// secret's value loaded from `pgstreams.secrets`. Also expands `${env:NAME}`.
///
/// Returns `Err` if any referenced secret/env var is missing.
///
/// This function calls SPI to fetch secrets — it must run inside a Postgres
/// SPI-enabled context (engine compile path).
pub fn resolve(value: &Value) -> Result<Value, String> {
    let mut resolver = Resolver::new();
    resolver.walk(value)
}

/// Walk `value` and substitute placeholders using `loader` (test-injectable).
/// Pure function — does not touch SPI.
pub fn resolve_with<F>(value: &Value, mut loader: F) -> Result<Value, String>
where
    F: FnMut(SecretKind, &str) -> Result<Option<String>, String>,
{
    let mut resolver = Resolver::new();
    resolver.walk_with(value, &mut loader)
}

/// Kind of placeholder being resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretKind {
    Secret,
    Env,
}

struct Resolver {}

impl Resolver {
    fn new() -> Self {
        Self {}
    }

    fn walk(&mut self, value: &Value) -> Result<Value, String> {
        self.walk_with(value, &mut spi_loader)
    }

    fn walk_with<F>(&mut self, value: &Value, loader: &mut F) -> Result<Value, String>
    where
        F: FnMut(SecretKind, &str) -> Result<Option<String>, String>,
    {
        match value {
            Value::String(s) => Ok(Value::String(self.interpolate_string(s, loader)?)),
            Value::Array(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for v in arr {
                    out.push(self.walk_with(v, loader)?);
                }
                Ok(Value::Array(out))
            }
            Value::Object(map) => {
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, v) in map {
                    out.insert(k.clone(), self.walk_with(v, loader)?);
                }
                Ok(Value::Object(out))
            }
            other => Ok(other.clone()),
        }
    }

    /// Replace every `${secret:NAME}` and `${env:NAME}` in `s`. If a string
    /// contains only a single placeholder and nothing else, the result preserves
    /// the raw string from the loader; otherwise the result is the concatenation.
    fn interpolate_string<F>(&mut self, s: &str, loader: &mut F) -> Result<String, String>
    where
        F: FnMut(SecretKind, &str) -> Result<Option<String>, String>,
    {
        let mut out = String::with_capacity(s.len());
        let mut rest = s;
        loop {
            match rest.find("${") {
                None => {
                    out.push_str(rest);
                    return Ok(out);
                }
                Some(start) => {
                    out.push_str(&rest[..start]);
                    let after_brace = &rest[start + 2..];
                    let end = after_brace
                        .find('}')
                        .ok_or_else(|| format!("Unterminated ${{ in: {}", s))?;
                    let body = &after_brace[..end];
                    let (kind, name) = parse_placeholder(body)
                        .ok_or_else(|| format!("Invalid placeholder body: {}", body))?;
                    let resolved = loader(kind, name)?.ok_or_else(|| match kind {
                        SecretKind::Secret => format!("Unknown secret: {}", name),
                        SecretKind::Env => format!("Unset environment variable: {}", name),
                    })?;
                    out.push_str(&resolved);
                    rest = &after_brace[end + 1..];
                }
            }
        }
    }
}

fn parse_placeholder(body: &str) -> Option<(SecretKind, &str)> {
    if let Some(name) = body.strip_prefix("secret:") {
        Some((SecretKind::Secret, name.trim()))
    } else if let Some(name) = body.strip_prefix("env:") {
        Some((SecretKind::Env, name.trim()))
    } else {
        None
    }
}

/// SPI-backed loader. Looks up secrets in `pgstreams.secrets` and env vars
/// via `std::env`.
fn spi_loader(kind: SecretKind, name: &str) -> Result<Option<String>, String> {
    match kind {
        SecretKind::Secret => Spi::get_one_with_args::<String>(
            "SELECT value FROM pgstreams.secrets WHERE name = $1",
            &[name.into()],
        )
        .map_err(|e| format!("Failed to load secret {}: {}", name, e)),
        SecretKind::Env => Ok(std::env::var(name).ok()),
    }
}

/// SQL-callable: create/replace a secret.
pub fn set_secret_impl(name: &str, value: &str, description: Option<&str>) {
    let result = Spi::run_with_args(
        "INSERT INTO pgstreams.secrets (name, value, description) \
         VALUES ($1, $2, $3) \
         ON CONFLICT (name) DO UPDATE \
         SET value = EXCLUDED.value, \
             description = COALESCE(EXCLUDED.description, pgstreams.secrets.description), \
             updated_at = now()",
        &[name.into(), value.into(), description.into()],
    );
    if let Err(e) = result {
        pgrx::error!("Failed to set secret '{}': {}", name, e);
    }
}

/// SQL-callable: drop a secret. Returns true if it existed.
pub fn drop_secret_impl(name: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "WITH deleted AS (DELETE FROM pgstreams.secrets WHERE name = $1 RETURNING 1) \
         SELECT EXISTS(SELECT 1 FROM deleted)",
        &[name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

/// SQL-callable: list secret names (NEVER values).
pub fn list_secrets_impl() -> Vec<(String, Option<String>, pgrx::datum::TimestampWithTimeZone)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT name, description, created_at \
             FROM pgstreams.secrets ORDER BY name",
            None,
            &[],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let description: Option<String> = row.get(2)?;
            let created_at: pgrx::datum::TimestampWithTimeZone = row
                .get(3)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());
            rows.push((name, description, created_at));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Build a loader that returns canned values from a map; unknown keys
    /// return Ok(None).
    fn map_loader<'a>(
        secrets: &'a [(&'a str, &'a str)],
        envs: &'a [(&'a str, &'a str)],
    ) -> impl FnMut(SecretKind, &str) -> Result<Option<String>, String> + 'a {
        move |kind, name| match kind {
            SecretKind::Secret => Ok(secrets
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).to_string())),
            SecretKind::Env => Ok(envs
                .iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| (*v).to_string())),
        }
    }

    #[test]
    fn resolve_simple_secret() {
        let v = json!({"token": "${secret:api_key}"});
        let resolved = resolve_with(&v, map_loader(&[("api_key", "abc123")], &[])).unwrap();
        assert_eq!(resolved["token"], "abc123");
    }

    #[test]
    fn resolve_concatenated_string() {
        let v = json!({"url": "https://${secret:host}/path"});
        let resolved = resolve_with(&v, map_loader(&[("host", "example.com")], &[])).unwrap();
        assert_eq!(resolved["url"], "https://example.com/path");
    }

    #[test]
    fn resolve_multiple_placeholders() {
        let v = json!({"creds": "${secret:user}:${secret:pass}"});
        let resolved = resolve_with(
            &v,
            map_loader(&[("user", "alice"), ("pass", "s3cret")], &[]),
        )
        .unwrap();
        assert_eq!(resolved["creds"], "alice:s3cret");
    }

    #[test]
    fn resolve_env_var() {
        let v = json!({"home": "${env:HOME_TEST}"});
        let resolved = resolve_with(&v, map_loader(&[], &[("HOME_TEST", "/tmp")])).unwrap();
        assert_eq!(resolved["home"], "/tmp");
    }

    #[test]
    fn resolve_nested_object() {
        let v = json!({
            "input": {
                "auth": {
                    "type": "bearer",
                    "token": "${secret:api_token}"
                }
            }
        });
        let resolved = resolve_with(&v, map_loader(&[("api_token", "tok-42")], &[])).unwrap();
        assert_eq!(resolved["input"]["auth"]["token"], "tok-42");
        assert_eq!(resolved["input"]["auth"]["type"], "bearer");
    }

    #[test]
    fn resolve_array_of_strings() {
        let v = json!(["${secret:a}", "literal", "${secret:b}"]);
        let resolved = resolve_with(&v, map_loader(&[("a", "alpha"), ("b", "beta")], &[])).unwrap();
        assert_eq!(resolved[0], "alpha");
        assert_eq!(resolved[1], "literal");
        assert_eq!(resolved[2], "beta");
    }

    #[test]
    fn resolve_passes_through_non_strings() {
        let v = json!({"n": 42, "b": true, "x": null, "s": "${secret:a}"});
        let resolved = resolve_with(&v, map_loader(&[("a", "v")], &[])).unwrap();
        assert_eq!(resolved["n"], 42);
        assert_eq!(resolved["b"], true);
        assert_eq!(resolved["x"], serde_json::Value::Null);
        assert_eq!(resolved["s"], "v");
    }

    #[test]
    fn resolve_missing_secret_errors() {
        let v = json!({"x": "${secret:nope}"});
        let err = resolve_with(&v, map_loader(&[], &[])).unwrap_err();
        assert!(err.contains("Unknown secret"));
        assert!(err.contains("nope"));
    }

    #[test]
    fn resolve_missing_env_errors() {
        let v = json!({"x": "${env:NOPE}"});
        let err = resolve_with(&v, map_loader(&[], &[])).unwrap_err();
        assert!(err.contains("Unset environment variable"));
    }

    #[test]
    fn resolve_unterminated_placeholder_errors() {
        let v = json!({"x": "${secret:abc"});
        let err = resolve_with(&v, map_loader(&[], &[])).unwrap_err();
        assert!(err.contains("Unterminated"));
    }

    #[test]
    fn resolve_invalid_kind_errors() {
        let v = json!({"x": "${unknown:abc}"});
        let err = resolve_with(&v, map_loader(&[], &[])).unwrap_err();
        assert!(err.contains("Invalid placeholder"));
    }

    #[test]
    fn resolve_no_placeholders_is_identity() {
        let v = json!({"a": "no secrets", "b": 1});
        let resolved = resolve_with(&v, map_loader(&[], &[])).unwrap();
        assert_eq!(resolved, v);
    }

    #[test]
    fn parse_placeholder_recognizes_kinds() {
        assert_eq!(
            parse_placeholder("secret:foo"),
            Some((SecretKind::Secret, "foo"))
        );
        assert_eq!(parse_placeholder("env:BAR"), Some((SecretKind::Env, "BAR")));
        assert_eq!(
            parse_placeholder("secret: trimmed "),
            Some((SecretKind::Secret, "trimmed"))
        );
        assert_eq!(parse_placeholder("nope"), None);
    }
}
