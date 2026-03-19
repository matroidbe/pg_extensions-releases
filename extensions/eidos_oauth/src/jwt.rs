use serde_json::Value;

/// Decoded and verified JWT token
pub struct DecodedToken {
    pub claims: serde_json::Map<String, Value>,
}

/// Decode and verify a JWT bearer token.
///
/// Steps:
/// 1. Parse header to get `kid` and algorithm
/// 2. Resolve public key from JWKS cache
/// 3. Build validation with issuer/audience
/// 4. Decode + verify signature
pub fn decode_and_verify(
    token: &str,
    issuer: Option<&str>,
    audience: Option<&str>,
) -> Result<DecodedToken, String> {
    use jsonwebtoken::{decode, decode_header, Validation};

    // 1. Parse header (no signature check yet)
    let header = decode_header(token).map_err(|e| format!("Invalid JWT header: {e}"))?;

    let kid = header
        .kid
        .as_deref()
        .ok_or("JWT header missing 'kid' field")?;

    // 2. Get decoding key from JWKS cache
    let key = crate::jwks::resolve_key(kid)?;

    // 3. Build validation
    let mut validation = Validation::new(header.alg);
    validation.validate_exp = true;
    // Disable audience validation by default — enable only if configured
    validation.validate_aud = false;

    if let Some(iss) = issuer {
        if !iss.is_empty() {
            validation.set_issuer(&[iss]);
        }
    }

    if let Some(aud) = audience {
        if !aud.is_empty() {
            validation.validate_aud = true;
            validation.set_audience(&[aud]);
        }
    }

    // 4. Decode and verify
    let token_data = decode::<serde_json::Map<String, Value>>(token, &key, &validation)
        .map_err(|e| format!("JWT validation failed: {e}"))?;

    Ok(DecodedToken {
        claims: token_data.claims,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use serde_json::json;

    const TEST_KID: &str = "test-key-1";
    const TEST_PRIVATE_KEY: &[u8] = include_bytes!("test_fixtures/test_rsa_private.pem");
    const TEST_PUBLIC_KEY: &[u8] = include_bytes!("test_fixtures/test_rsa_public.pem");

    /// Helper: create a signed JWT with the test RSA key
    fn make_token(claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());

        let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    /// Helper: load the test public key into the JWKS cache
    fn setup_cache() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
        use simple_asn1::{from_der, ASN1Block};

        crate::jwks::clear_cache();

        // Parse RSA public key PEM to extract n and e components
        let pem_str = std::str::from_utf8(TEST_PUBLIC_KEY).unwrap();

        // Strip PEM header/footer and decode base64
        let b64: String = pem_str
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(&b64)
            .unwrap();

        // Parse ASN.1 DER to extract n and e
        let asn1 = from_der(&der).unwrap();

        // RSA public key structure: SEQUENCE { SEQUENCE { OID, NULL }, BIT STRING { SEQUENCE { n, e } } }
        let (n_bytes, e_bytes) = match &asn1[0] {
            ASN1Block::Sequence(_, items) => {
                if let ASN1Block::BitString(_, _, bits) = &items[1] {
                    let inner = from_der(bits).unwrap();
                    match &inner[0] {
                        ASN1Block::Sequence(_, inner_items) => {
                            let n = match &inner_items[0] {
                                ASN1Block::Integer(_, n) => n.to_bytes_be().1,
                                _ => panic!("Expected integer for n"),
                            };
                            let e = match &inner_items[1] {
                                ASN1Block::Integer(_, e) => e.to_bytes_be().1,
                                _ => panic!("Expected integer for e"),
                            };
                            (n, e)
                        }
                        _ => panic!("Expected sequence"),
                    }
                } else {
                    panic!("Expected bit string")
                }
            }
            _ => panic!("Expected sequence"),
        };

        // Build JWKS JSON with Base64URL-encoded n and e
        let n_b64 = URL_SAFE_NO_PAD.encode(&n_bytes);
        let e_b64 = URL_SAFE_NO_PAD.encode(&e_bytes);

        let jwks_json = serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "kid": TEST_KID,
                "use": "sig",
                "alg": "RS256",
                "n": n_b64,
                "e": e_b64,
            }]
        });

        crate::jwks::update_cache_from_json(&jwks_json.to_string()).unwrap();
    }

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    #[test]
    fn test_decode_valid_rs256_token() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "email": "alice@example.com",
            "iss": "https://auth.example.com",
            "aud": "my-app",
            "exp": now_secs() + 3600,
            "iat": now_secs(),
        });

        let token = make_token(claims);

        let result = decode_and_verify(&token, None, None);
        assert!(result.is_ok(), "Expected Ok, got: {:?}", result.err());

        let decoded = result.unwrap();
        assert_eq!(
            decoded.claims.get("sub").unwrap().as_str().unwrap(),
            "alice"
        );
        assert_eq!(
            decoded.claims.get("email").unwrap().as_str().unwrap(),
            "alice@example.com"
        );
    }

    #[test]
    fn test_decode_expired_token_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "exp": now_secs() - 3600, // expired 1 hour ago
            "iat": now_secs() - 7200,
        });

        let token = make_token(claims);

        let result = decode_and_verify(&token, None, None);
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("ExpiredSignature"));
    }

    #[test]
    fn test_decode_wrong_issuer_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "iss": "https://evil.com",
            "exp": now_secs() + 3600,
        });

        let token = make_token(claims);

        let result = decode_and_verify(&token, Some("https://auth.example.com"), None);
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("InvalidIssuer"));
    }

    #[test]
    fn test_decode_correct_issuer_succeeds() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "iss": "https://auth.example.com",
            "exp": now_secs() + 3600,
        });

        let token = make_token(claims);

        let result = decode_and_verify(&token, Some("https://auth.example.com"), None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_decode_wrong_audience_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "aud": "wrong-app",
            "exp": now_secs() + 3600,
        });

        let token = make_token(claims);

        let result = decode_and_verify(&token, None, Some("my-app"));
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("InvalidAudience"));
    }

    #[test]
    fn test_decode_missing_kid_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "exp": now_secs() + 3600,
        });

        // Create token WITHOUT kid in header
        let header = Header::new(Algorithm::RS256);
        let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();
        let token = encode(&header, &claims, &key).unwrap();

        let result = decode_and_verify(&token, None, None);
        assert!(result.is_err());
        assert!(result.err().unwrap().contains("missing 'kid'"));
    }

    #[test]
    fn test_decode_unknown_kid_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "exp": now_secs() + 3600,
        });

        // Create token with unknown kid
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("unknown-kid".to_string());
        let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY).unwrap();
        let token = encode(&header, &claims, &key).unwrap();

        let result = decode_and_verify(&token, None, None);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .contains("No key with kid=unknown-kid"));
    }

    #[test]
    fn test_decode_tampered_token_fails() {
        setup_cache();

        let claims = json!({
            "sub": "alice",
            "exp": now_secs() + 3600,
        });

        let mut token = make_token(claims);

        // Tamper with the payload (flip a character)
        let parts: Vec<&str> = token.split('.').collect();
        let mut payload = parts[1].to_string();
        payload.push('x');
        token = format!("{}.{}.{}", parts[0], payload, parts[2]);

        let result = decode_and_verify(&token, None, None);
        assert!(result.is_err());
    }
}
