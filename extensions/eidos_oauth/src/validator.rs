use crate::ffi::{ValidatorModuleResult, ValidatorModuleState};
use std::ffi::{c_char, CStr, CString};

/// Startup callback — called once when PG loads the validator module.
/// Pre-warms the JWKS cache if a URL is configured.
///
/// # Safety
/// Called by PostgreSQL's OAuth validator infrastructure. `state` must be a valid pointer.
pub unsafe extern "C-unwind" fn startup_cb(state: *mut ValidatorModuleState) {
    if let Some(url) = crate::guc_str(&crate::JWKS_URL) {
        let timeout_ms = crate::JWKS_TIMEOUT_MS.get() as u64;
        match crate::jwks::fetch_jwks(&url, timeout_ms) {
            Ok(jwks) => {
                crate::jwks::update_cache(jwks);
                pgrx::log!(
                    "eidos_oauth: pre-warmed JWKS cache with {} keys",
                    crate::jwks::key_count()
                );
            }
            Err(e) => {
                pgrx::warning!("eidos_oauth: failed to pre-warm JWKS cache: {}", e);
            }
        }
    }

    (*state).private_data = std::ptr::null_mut();
}

/// Shutdown callback — called when PG unloads the validator module.
///
/// # Safety
/// Called by PostgreSQL's OAuth validator infrastructure. `_state` must be a valid pointer.
pub unsafe extern "C-unwind" fn shutdown_cb(_state: *mut ValidatorModuleState) {
    pgrx::log!("eidos_oauth: validator shutdown");
}

/// Validate callback — called for each OAuth connection attempt.
///
/// Returns `true` if validation was performed (check `result.authorized`),
/// or `false` if the module encountered an internal error.
///
/// # Safety
/// Called by PostgreSQL's OAuth validator infrastructure. All pointers must be valid.
/// `result` must point to a writable `ValidatorModuleResult`.
pub unsafe extern "C-unwind" fn validate_cb(
    _state: *const ValidatorModuleState,
    token: *const c_char,
    role: *const c_char,
    result: *mut ValidatorModuleResult,
) -> bool {
    // Convert C strings to Rust
    let token_str = match CStr::from_ptr(token).to_str() {
        Ok(s) => s,
        Err(_) => {
            pgrx::log!("eidos_oauth: token is not valid UTF-8");
            (*result).authorized = false;
            (*result).authn_id = std::ptr::null_mut();
            return true;
        }
    };

    let _role_str = match CStr::from_ptr(role).to_str() {
        Ok(s) => s,
        Err(_) => {
            pgrx::log!("eidos_oauth: role is not valid UTF-8");
            (*result).authorized = false;
            (*result).authn_id = std::ptr::null_mut();
            return true;
        }
    };

    // Read GUC config
    let issuer = crate::guc_str(&crate::ISSUER);
    let audience = crate::guc_str(&crate::AUDIENCE);
    let jwks_url = crate::guc_str(&crate::JWKS_URL);

    // Ensure JWKS is available — fetch if cache is empty
    if let Some(ref url) = jwks_url {
        if crate::jwks::key_count() == 0 {
            let timeout_ms = crate::JWKS_TIMEOUT_MS.get() as u64;
            match crate::jwks::fetch_jwks(url, timeout_ms) {
                Ok(jwks) => crate::jwks::update_cache(jwks),
                Err(e) => {
                    pgrx::log!("eidos_oauth: JWKS fetch failed during validation: {}", e);
                    (*result).authorized = false;
                    (*result).authn_id = std::ptr::null_mut();
                    return true;
                }
            }
        }
    }

    // Decode and verify the JWT
    let decoded =
        match crate::jwt::decode_and_verify(token_str, issuer.as_deref(), audience.as_deref()) {
            Ok(d) => d,
            Err(e) => {
                // If unknown kid, try refreshing JWKS and retrying
                if e.contains("No key with kid=") {
                    if let Some(ref url) = jwks_url {
                        let timeout_ms = crate::JWKS_TIMEOUT_MS.get() as u64;
                        if let Ok(jwks) = crate::jwks::fetch_jwks(url, timeout_ms) {
                            crate::jwks::update_cache(jwks);
                            match crate::jwt::decode_and_verify(
                                token_str,
                                issuer.as_deref(),
                                audience.as_deref(),
                            ) {
                                Ok(d) => d,
                                Err(e2) => {
                                    pgrx::log!(
                                        "eidos_oauth: validation failed after JWKS refresh: {}",
                                        e2
                                    );
                                    (*result).authorized = false;
                                    (*result).authn_id = std::ptr::null_mut();
                                    return true;
                                }
                            }
                        } else {
                            pgrx::log!("eidos_oauth: token validation failed: {}", e);
                            (*result).authorized = false;
                            (*result).authn_id = std::ptr::null_mut();
                            return true;
                        }
                    } else {
                        pgrx::log!("eidos_oauth: token validation failed: {}", e);
                        (*result).authorized = false;
                        (*result).authn_id = std::ptr::null_mut();
                        return true;
                    }
                } else {
                    pgrx::log!("eidos_oauth: token validation failed: {}", e);
                    (*result).authorized = false;
                    (*result).authn_id = std::ptr::null_mut();
                    return true;
                }
            }
        };

    // Extract identity from configured claim (default: "sub")
    let role_claim_setting = crate::guc_str(&crate::ROLE_CLAIM);
    let role_claim = role_claim_setting.as_deref().unwrap_or("sub");

    let identity = match decoded.claims.get(role_claim) {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(v) => v.to_string().trim_matches('"').to_string(),
        None => {
            pgrx::log!(
                "eidos_oauth: token missing claim '{}', rejecting",
                role_claim
            );
            (*result).authorized = false;
            (*result).authn_id = std::ptr::null_mut();
            return true;
        }
    };

    // Apply role prefix
    let prefix_setting = crate::guc_str(&crate::ROLE_PREFIX);
    let prefix = prefix_setting.as_deref().unwrap_or("");

    let authn_id = format!("{prefix}{identity}");

    // Inject all claims as session GUCs
    crate::claims::inject_claims_as_gucs(&decoded.claims);

    // Always authorize — let PG's pg_ident.conf handle role mapping
    let authn_id_cstr =
        CString::new(authn_id.as_bytes()).unwrap_or_else(|_| CString::new("unknown").unwrap());
    (*result).authn_id = pgrx::pg_sys::pstrdup(authn_id_cstr.as_ptr());
    (*result).authorized = true;

    true
}
