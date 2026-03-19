use pgrx::prelude::*;
use std::ffi::CString;

pub mod claims;
pub mod ffi;
pub mod jwks;
pub mod jwt;
pub mod validator;

pgrx::pg_module_magic!();

// ── GUC Settings ────────────────────────────────────────────────────

pub(crate) static JWKS_URL: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub(crate) static ISSUER: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub(crate) static AUDIENCE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub(crate) static ROLE_CLAIM: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub(crate) static ROLE_PREFIX: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub(crate) static JWKS_CACHE_SECS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(300);

pub(crate) static JWKS_TIMEOUT_MS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(5000);

// ── Initialization ──────────────────────────────────────────────────

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    pgrx::GucRegistry::define_string_guc(
        c"eidos_oauth.jwks_url",
        c"URL to fetch JWKS for token validation",
        c"The JWKS endpoint URL (e.g., https://auth.example.com/.well-known/jwks.json)",
        &JWKS_URL,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        c"eidos_oauth.issuer",
        c"Expected JWT issuer (iss claim)",
        c"If set, tokens with a different issuer will be rejected",
        &ISSUER,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        c"eidos_oauth.audience",
        c"Expected JWT audience (aud claim)",
        c"If set, tokens not containing this audience will be rejected",
        &AUDIENCE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        c"eidos_oauth.role_claim",
        c"JWT claim to use as the authenticated identity",
        c"The claim whose value becomes the authn_id. Defaults to 'sub'.",
        &ROLE_CLAIM,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_string_guc(
        c"eidos_oauth.role_prefix",
        c"Prefix added to the identity extracted from JWT",
        c"E.g., 'oauth_' maps sub=admin to authn_id oauth_admin. Defaults to empty.",
        &ROLE_PREFIX,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_int_guc(
        c"eidos_oauth.jwks_cache_seconds",
        c"How long to cache JWKS keys (seconds)",
        c"JWKS keys are cached in memory. Set to 0 to disable caching.",
        &JWKS_CACHE_SECS,
        0,
        86400,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
    pgrx::GucRegistry::define_int_guc(
        c"eidos_oauth.jwks_timeout_ms",
        c"HTTP timeout for JWKS fetch in milliseconds",
        c"Maximum time to wait for JWKS endpoint response",
        &JWKS_TIMEOUT_MS,
        100,
        30000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
}

// ── GUC Helpers ─────────────────────────────────────────────────────

/// Read a string GUC value. Returns None if not set or not valid UTF-8.
pub(crate) fn guc_str(guc: &pgrx::GucSetting<Option<CString>>) -> Option<String> {
    let val = guc.get();
    val.as_ref()
        .and_then(|c| c.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
}

// ── SQL Helper Functions ────────────────────────────────────────────

/// Validate a JWT token and return its claims as JSONB.
/// Useful for testing token validation without the OAuth auth flow.
#[pg_extern]
fn oauth_validate_token(token: &str) -> pgrx::JsonB {
    let issuer = guc_str(&ISSUER);
    let audience = guc_str(&AUDIENCE);

    match jwt::decode_and_verify(token, issuer.as_deref(), audience.as_deref()) {
        Ok(decoded) => pgrx::JsonB(serde_json::Value::Object(decoded.claims)),
        Err(e) => pgrx::error!("Token validation failed: {}", e),
    }
}

/// Get a specific claim from a JWT token.
#[pg_extern]
fn oauth_get_claim(token: &str, claim: &str) -> Option<String> {
    let issuer = guc_str(&ISSUER);
    let audience = guc_str(&AUDIENCE);

    match jwt::decode_and_verify(token, issuer.as_deref(), audience.as_deref()) {
        Ok(decoded) => decoded.claims.get(claim).map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
        Err(e) => pgrx::error!("Token validation failed: {}", e),
    }
}

/// Inject JWT claims into session GUCs manually.
/// Useful on PG < 18 where the validator callback isn't available,
/// or for testing GUC injection.
#[pg_extern]
fn oauth_inject_claims(token: &str) -> bool {
    let issuer = guc_str(&ISSUER);
    let audience = guc_str(&AUDIENCE);

    match jwt::decode_and_verify(token, issuer.as_deref(), audience.as_deref()) {
        Ok(decoded) => {
            unsafe { claims::inject_claims_as_gucs(&decoded.claims) };
            true
        }
        Err(e) => pgrx::error!("Token validation failed: {}", e),
    }
}

/// Get the current JWKS cache status.
#[pg_extern]
fn oauth_jwks_status() -> pgrx::JsonB {
    let (key_count, age_secs, _) = jwks::cache_status();
    pgrx::JsonB(serde_json::json!({
        "cached_keys": key_count,
        "cache_age_seconds": age_secs,
    }))
}

/// Force refresh the JWKS cache from the configured URL.
#[pg_extern]
fn oauth_refresh_jwks() -> bool {
    let url = guc_str(&JWKS_URL);
    let url = match url {
        Some(u) => u,
        None => pgrx::error!("eidos_oauth.jwks_url is not configured"),
    };
    let timeout_ms = JWKS_TIMEOUT_MS.get() as u64;

    match jwks::force_refresh(&url, timeout_ms) {
        Ok(()) => true,
        Err(e) => pgrx::error!("JWKS refresh failed: {}", e),
    }
}

/// Return extension documentation.
#[pg_extern]
fn oauth_docs() -> &'static str {
    include_str!("../README.md")
}

// ── Test Setup ──────────────────────────────────────────────────────

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // noop — required by #[pg_test] macro
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_oauth_jwks_status_returns_json() {
        let result = Spi::get_one::<pgrx::JsonB>("SELECT eidos_oauth.oauth_jwks_status()");
        assert!(result.is_ok());
        let json = result.unwrap().unwrap();
        let obj = json.0.as_object().unwrap();
        assert!(obj.contains_key("cached_keys"));
        assert!(obj.contains_key("cache_age_seconds"));
    }
}
