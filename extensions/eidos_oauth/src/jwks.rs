use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::DecodingKey;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

struct JwksCache {
    keys: HashMap<String, jsonwebtoken::jwk::Jwk>,
    fetched_at: Instant,
}

static CACHE: RwLock<Option<JwksCache>> = RwLock::new(None);

/// Fetch JWKS from a URL using synchronous HTTP.
pub fn fetch_jwks(url: &str, timeout_ms: u64) -> Result<JwkSet, String> {
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_millis(timeout_ms)))
        .build()
        .new_agent();

    let mut response = agent
        .get(url)
        .call()
        .map_err(|e| format!("JWKS fetch failed: {e}"))?;

    let jwks: JwkSet = response
        .body_mut()
        .read_json()
        .map_err(|e| format!("JWKS parse failed: {e}"))?;

    Ok(jwks)
}

/// Update the JWKS cache with a fresh key set.
pub fn update_cache(jwks: JwkSet) {
    let mut keys_map = HashMap::new();
    for key in jwks.keys {
        if let Some(ref kid) = key.common.key_id {
            keys_map.insert(kid.clone(), key);
        }
    }

    let mut guard = CACHE.write().expect("JWKS cache lock poisoned");
    *guard = Some(JwksCache {
        keys: keys_map,
        fetched_at: Instant::now(),
    });
}

/// Parse a JWKS JSON string and update the cache.
pub fn update_cache_from_json(json: &str) -> Result<(), String> {
    let jwks: JwkSet = serde_json::from_str(json).map_err(|e| format!("JWKS parse failed: {e}"))?;
    update_cache(jwks);
    Ok(())
}

/// Resolve a decoding key by `kid`. Returns from cache if fresh, otherwise fetches.
pub fn resolve_key(kid: &str) -> Result<DecodingKey, String> {
    // Try cache first
    {
        let guard = CACHE.read().map_err(|_| "JWKS cache lock poisoned")?;
        if let Some(ref cache) = *guard {
            if let Some(jwk) = cache.keys.get(kid) {
                return DecodingKey::from_jwk(jwk)
                    .map_err(|e| format!("Failed to create decoding key: {e}"));
            }
        }
    }

    Err(format!("No key with kid={kid} in JWKS cache"))
}

/// Resolve a key, fetching from URL if not cached or if kid is unknown.
pub fn resolve_key_with_fetch(
    kid: &str,
    jwks_url: &str,
    cache_secs: u64,
    timeout_ms: u64,
) -> Result<DecodingKey, String> {
    // Try cache first
    {
        let guard = CACHE.read().map_err(|_| "JWKS cache lock poisoned")?;
        if let Some(ref cache) = *guard {
            if cache.fetched_at.elapsed().as_secs() < cache_secs {
                if let Some(jwk) = cache.keys.get(kid) {
                    return DecodingKey::from_jwk(jwk)
                        .map_err(|e| format!("Failed to create decoding key: {e}"));
                }
            }
        }
    }

    // Cache miss or stale — fetch
    let jwks = fetch_jwks(jwks_url, timeout_ms)?;
    update_cache(jwks);

    // Try again after refresh
    resolve_key(kid)
}

/// Get the number of cached keys.
pub fn key_count() -> usize {
    CACHE
        .read()
        .ok()
        .and_then(|g| g.as_ref().map(|c| c.keys.len()))
        .unwrap_or(0)
}

/// Get cache status for diagnostics.
pub fn cache_status() -> (usize, Option<u64>, Option<String>) {
    let guard = CACHE.read().ok();
    match guard.as_ref().and_then(|g| g.as_ref()) {
        Some(cache) => (
            cache.keys.len(),
            Some(cache.fetched_at.elapsed().as_secs()),
            None, // URL is stored in GUC, not in cache
        ),
        None => (0, None, None),
    }
}

/// Force refresh the JWKS cache from the configured URL.
pub fn force_refresh(url: &str, timeout_ms: u64) -> Result<(), String> {
    let jwks = fetch_jwks(url, timeout_ms)?;
    update_cache(jwks);
    Ok(())
}

/// Clear the cache (for testing).
#[cfg(test)]
pub fn clear_cache() {
    let mut guard = CACHE.write().expect("JWKS cache lock poisoned");
    *guard = None;
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_JWKS_RSA: &str = r#"{
        "keys": [
            {
                "kty": "RSA",
                "kid": "test-key-1",
                "use": "sig",
                "alg": "RS256",
                "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                "e": "AQAB"
            }
        ]
    }"#;

    const TEST_JWKS_EC: &str = r#"{
        "keys": [
            {
                "kty": "EC",
                "kid": "ec-key-1",
                "use": "sig",
                "alg": "ES256",
                "crv": "P-256",
                "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
                "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0"
            }
        ]
    }"#;

    #[test]
    fn test_parse_jwks_rsa() {
        clear_cache();
        update_cache_from_json(TEST_JWKS_RSA).unwrap();

        assert_eq!(key_count(), 1);

        let key = resolve_key("test-key-1");
        assert!(key.is_ok());
    }

    #[test]
    fn test_parse_jwks_ec() {
        clear_cache();
        update_cache_from_json(TEST_JWKS_EC).unwrap();

        assert_eq!(key_count(), 1);

        let key = resolve_key("ec-key-1");
        assert!(key.is_ok());
    }

    #[test]
    fn test_cache_miss_returns_error() {
        clear_cache();
        update_cache_from_json(TEST_JWKS_RSA).unwrap();

        let result = resolve_key("nonexistent-kid");
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(err.contains("No key with kid=nonexistent-kid"));
    }

    #[test]
    fn test_empty_cache_returns_error() {
        clear_cache();

        let result = resolve_key("any-kid");
        assert!(result.is_err());
    }

    #[test]
    fn test_cache_status_empty() {
        clear_cache();
        let (count, age, _) = cache_status();
        assert_eq!(count, 0);
        assert!(age.is_none());
    }

    #[test]
    fn test_cache_status_populated() {
        clear_cache();
        update_cache_from_json(TEST_JWKS_RSA).unwrap();

        let (count, age, _) = cache_status();
        assert_eq!(count, 1);
        assert!(age.is_some());
        assert!(age.unwrap() < 2); // should be near-instant
    }
}
