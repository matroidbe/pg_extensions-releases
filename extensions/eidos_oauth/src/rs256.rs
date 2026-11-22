//! RS256 key generation, signing and signature verification.
//!
//! The extension holds no key state: the caller owns the private key (for
//! Eidos, an owner-only table) and passes it in on every call. See
//! design/eidos_oauth/README.md → "Token signing".

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use serde_json::Value;

/// Smallest and largest modulus `generate` accepts.
pub const MIN_BITS: usize = 2048;
pub const MAX_BITS: usize = 4096;

/// A freshly generated key pair: the PKCS#8 PEM private key and its public JWK.
pub struct Generated {
    pub private_pem: String,
    pub jwk: Value,
}

pub fn generate(bits: usize) -> Result<Generated, String> {
    use rsa::pkcs8::{EncodePrivateKey, LineEnding};
    if !(MIN_BITS..=MAX_BITS).contains(&bits) {
        return Err(format!(
            "RSA key size must be between {MIN_BITS} and {MAX_BITS} bits, got {bits}"
        ));
    }
    let sk = rsa::RsaPrivateKey::new(&mut rand::rngs::OsRng, bits)
        .map_err(|e| format!("RSA key generation failed: {e}"))?;
    let private_pem = sk
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|e| format!("PKCS#8 encoding failed: {e}"))?
        .to_string();
    Ok(Generated {
        private_pem,
        jwk: jwk_of(&sk.to_public_key()),
    })
}

/// The public JWK of `pk`, with `kid` set to its RFC 7638 thumbprint.
pub fn jwk_of(pk: &rsa::RsaPublicKey) -> Value {
    use rsa::traits::PublicKeyParts;
    let n = B64.encode(pk.n().to_bytes_be());
    let e = B64.encode(pk.e().to_bytes_be());
    let kid = thumbprint(&n, &e);
    serde_json::json!({
        "kty": "RSA",
        "n": n,
        "e": e,
        "alg": "RS256",
        "use": "sig",
        "kid": kid,
    })
}

/// RFC 7638 JWK thumbprint (SHA-256, base64url) of an RSA public key given as
/// its base64url `n` and `e`: the hash of the required members in
/// lexicographic order, without whitespace.
pub fn thumbprint(n_b64: &str, e_b64: &str) -> String {
    use sha2::{Digest, Sha256};
    // n and e are base64url, so they never need JSON escaping.
    let canonical = format!(r#"{{"e":"{e_b64}","kty":"RSA","n":"{n_b64}"}}"#);
    B64.encode(Sha256::digest(canonical.as_bytes()))
}

/// Compact JWS of `claims`, signed RS256 (RSASSA-PKCS1-v1_5, SHA-256) with the
/// PKCS#8 PEM `private_pem`. The header is exactly
/// `{"alg":"RS256","typ":"JWT","kid":<kid>}`.
pub fn sign(private_pem: &str, kid: &str, claims: &Value) -> Result<String, String> {
    use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey};
    // The private-key operation runs on ring, which is constant-time; the rsa
    // crate's is not (RUSTSEC-2023-0071), and token minting is something a
    // remote client can trigger and time at will. rsa only parses the PEM.
    let der = rsa::RsaPrivateKey::from_pkcs8_pem(private_pem)
        .and_then(|sk| sk.to_pkcs8_der())
        .map_err(|e| format!("invalid RSA private key (expected PKCS#8 PEM): {e}"))?;
    let key = ring::signature::RsaKeyPair::from_pkcs8(der.as_bytes())
        .map_err(|e| format!("invalid RSA private key (expected PKCS#8 PEM): {e}"))?;
    let kid_json = serde_json::to_string(kid).map_err(|e| e.to_string())?;
    let header = format!(r#"{{"alg":"RS256","typ":"JWT","kid":{kid_json}}}"#);
    let payload = serde_json::to_string(claims).map_err(|e| e.to_string())?;
    let signing_input = format!("{}.{}", B64.encode(header), B64.encode(payload));
    let mut sig = vec![0u8; key.public().modulus_len()];
    key.sign(
        &ring::signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        signing_input.as_bytes(),
        &mut sig,
    )
    .map_err(|_| "RS256 signing failed".to_string())?;
    Ok(format!("{signing_input}.{}", B64.encode(sig)))
}

/// True only if `token` is a three-part compact JWS whose header `alg` is
/// exactly `RS256` and whose signature verifies against the RSA public key in
/// `jwk`. Checks the signature only — no claim (exp/iss/aud) validation.
/// Never fails: anything malformed is simply `false`.
pub fn verify(jwk: &Value, token: &str) -> bool {
    try_verify(jwk, token).unwrap_or(false)
}

fn try_verify(jwk: &Value, token: &str) -> Option<bool> {
    use rsa::signature::Verifier;
    let mut parts = token.split('.');
    let (h, p, s) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return Some(false);
    }
    let header: Value = serde_json::from_slice(&B64.decode(h).ok()?).ok()?;
    if header.get("alg")?.as_str()? != "RS256" {
        return Some(false);
    }
    B64.decode(p).ok()?;
    let sig = rsa::pkcs1v15::Signature::try_from(B64.decode(s).ok()?.as_slice()).ok()?;
    let key = public_key(jwk)?;
    let signing_input = &token[..h.len() + 1 + p.len()];
    Some(
        rsa::pkcs1v15::VerifyingKey::<sha2::Sha256>::new(key)
            .verify(signing_input.as_bytes(), &sig)
            .is_ok(),
    )
}

fn public_key(jwk: &Value) -> Option<rsa::RsaPublicKey> {
    if jwk.get("kty")?.as_str()? != "RSA" {
        return None;
    }
    let n = B64.decode(jwk.get("n")?.as_str()?).ok()?;
    let e = B64.decode(jwk.get("e")?.as_str()?).ok()?;
    rsa::RsaPublicKey::new(
        rsa::BigUint::from_bytes_be(&n),
        rsa::BigUint::from_bytes_be(&e),
    )
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::OnceLock;

    const FIXTURE_PRIVATE: &str = include_str!("test_fixtures/test_rsa_private.pem");
    const FIXTURE_PUBLIC: &[u8] = include_bytes!("test_fixtures/test_rsa_public.pem");

    /// Key generation is the slow part; share two keys across the tests.
    fn key_a() -> &'static Generated {
        static K: OnceLock<Generated> = OnceLock::new();
        K.get_or_init(|| generate(2048).unwrap())
    }
    fn key_b() -> &'static Generated {
        static K: OnceLock<Generated> = OnceLock::new();
        K.get_or_init(|| generate(2048).unwrap())
    }

    fn claims() -> Value {
        json!({"sub": "alice", "iss": "https://eidos.example", "exp": 4102444800u64})
    }

    fn part(token: &str, i: usize) -> String {
        token.split('.').nth(i).unwrap().to_string()
    }

    /// A token with `header` and `claims()`, carrying `sig`'s signature.
    fn forge(header: &Value, sig_from: &str) -> String {
        let h = B64.encode(header.to_string());
        let p = B64.encode(claims().to_string());
        format!("{h}.{p}.{}", part(sig_from, 2))
    }

    #[test]
    fn generated_jwk_has_the_public_fields() {
        let jwk = &key_a().jwk;
        assert_eq!(jwk["kty"], "RSA");
        assert_eq!(jwk["alg"], "RS256");
        assert_eq!(jwk["use"], "sig");
        assert_eq!(jwk["e"], "AQAB");
        // 2048-bit modulus = 256 bytes
        let n = B64.decode(jwk["n"].as_str().unwrap()).unwrap();
        assert_eq!(n.len(), 256);
        assert!(key_a()
            .private_pem
            .starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(
            jwk.get("d").is_none(),
            "the JWK must not carry private parts"
        );
    }

    #[test]
    fn rfc7638_example_thumbprint() {
        // RFC 7638 §3.1
        let n = "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw";
        assert_eq!(
            thumbprint(n, "AQAB"),
            "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"
        );
    }

    #[test]
    fn kid_is_the_rfc7638_thumbprint() {
        use sha2::{Digest, Sha256};
        let jwk = &key_a().jwk;
        let n = jwk["n"].as_str().unwrap();
        let e = jwk["e"].as_str().unwrap();
        // RFC 7638: required members only, lexicographic order, no whitespace.
        let canonical = format!(r#"{{"e":"{e}","kty":"RSA","n":"{n}"}}"#);
        let expected = B64.encode(Sha256::digest(canonical.as_bytes()));
        assert_eq!(jwk["kid"], expected);
    }

    #[test]
    fn rejects_out_of_range_bits() {
        assert!(generate(1024).is_err());
        assert!(generate(2047).is_err());
        assert!(generate(8192).is_err());
    }

    #[test]
    fn sign_then_verify_round_trips() {
        let k = key_a();
        let kid = k.jwk["kid"].as_str().unwrap();
        let token = sign(&k.private_pem, kid, &claims()).unwrap();
        assert_eq!(token.split('.').count(), 3);
        assert!(verify(&k.jwk, &token));
    }

    #[test]
    fn header_is_exact_and_payload_is_the_claims() {
        let k = key_a();
        let token = sign(&k.private_pem, "kid-1", &claims()).unwrap();
        let header = String::from_utf8(B64.decode(part(&token, 0)).unwrap()).unwrap();
        assert_eq!(header, r#"{"alg":"RS256","typ":"JWT","kid":"kid-1"}"#);
        let payload: Value = serde_json::from_slice(&B64.decode(part(&token, 1)).unwrap()).unwrap();
        assert_eq!(payload, claims());
    }

    #[test]
    fn kid_is_json_escaped_in_the_header() {
        let k = key_a();
        let token = sign(&k.private_pem, "a\"b", &claims()).unwrap();
        let header: Value = serde_json::from_slice(&B64.decode(part(&token, 0)).unwrap()).unwrap();
        assert_eq!(header["kid"], "a\"b");
    }

    #[test]
    fn sign_rejects_an_unparseable_key() {
        assert!(sign("not a key", "k", &claims()).is_err());
    }

    #[test]
    fn tampered_payload_fails() {
        let k = key_a();
        let token = sign(&k.private_pem, "k", &claims()).unwrap();
        let other = B64.encode(json!({"sub": "mallory"}).to_string());
        let tampered = format!("{}.{other}.{}", part(&token, 0), part(&token, 2));
        assert!(!verify(&k.jwk, &tampered));
    }

    #[test]
    fn another_keys_signature_fails() {
        let token = sign(&key_b().private_pem, "k", &claims()).unwrap();
        assert!(!verify(&key_a().jwk, &token));
        assert!(verify(&key_b().jwk, &token));
    }

    #[test]
    fn wrong_alg_fails_even_with_a_valid_signature() {
        let k = key_a();
        let token = sign(&k.private_pem, "k", &claims()).unwrap();
        // Same signing input, header alg switched: the signature no longer
        // matches either, but the alg check must reject it on its own.
        for alg in ["HS256", "none", "rs256", "RS512"] {
            let forged = forge(&json!({"alg": alg, "typ": "JWT"}), &token);
            assert!(!verify(&k.jwk, &forged), "alg {alg}");
        }
        // alg none with an empty signature
        let h = B64.encode(r#"{"alg":"none"}"#);
        let none = format!("{h}.{}.", part(&token, 1));
        assert!(!verify(&k.jwk, &none));
        // missing alg
        assert!(!verify(&k.jwk, &forge(&json!({"typ": "JWT"}), &token)));
    }

    #[test]
    fn garbage_tokens_fail() {
        let jwk = &key_a().jwk;
        for t in ["", "garbage", "a.b", "a.b.c", "a.b.c.d", "...", "!!.!!.!!"] {
            assert!(!verify(jwk, t), "{t:?}");
        }
        let token = sign(&key_a().private_pem, "k", &claims()).unwrap();
        assert!(!verify(jwk, &format!("{token}.extra")));
        assert!(!verify(jwk, &format!("{token}=")));
    }

    #[test]
    fn malformed_jwks_fail() {
        let k = key_a();
        let token = sign(&k.private_pem, "k", &claims()).unwrap();
        let n = k.jwk["n"].clone();
        for jwk in [
            json!({}),
            json!("string"),
            json!({"kty": "RSA", "n": n}),
            json!({"kty": "RSA", "e": "AQAB"}),
            json!({"kty": "RSA", "n": "!!", "e": "AQAB"}),
            json!({"kty": "RSA", "n": 5, "e": "AQAB"}),
            json!({"kty": "EC", "n": n, "e": "AQAB"}),
        ] {
            assert!(!verify(&jwk, &token), "{jwk}");
        }
        // RFC 7517 requires kty.
        assert!(!verify(&json!({"n": n, "e": "AQAB"}), &token));
    }

    #[test]
    fn jsonwebtoken_verifies_our_tokens() {
        use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
        let k = key_a();
        let token = sign(&k.private_pem, "k", &claims()).unwrap();
        let key = DecodingKey::from_rsa_components(
            k.jwk["n"].as_str().unwrap(),
            k.jwk["e"].as_str().unwrap(),
        )
        .unwrap();
        let mut v = Validation::new(Algorithm::RS256);
        v.validate_exp = false;
        v.required_spec_claims.clear();
        let data = decode::<Value>(&token, &key, &v).unwrap();
        assert_eq!(data.claims, claims());
        assert_eq!(data.header.kid.as_deref(), Some("k"));
    }

    #[test]
    fn externally_generated_pkcs8_key_interoperates() {
        use jsonwebtoken::{
            decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
        };
        // Our signature, their verification against the fixture public key.
        let token = sign(FIXTURE_PRIVATE, "fx", &claims()).unwrap();
        let mut v = Validation::new(Algorithm::RS256);
        v.validate_exp = false;
        v.required_spec_claims.clear();
        let key = DecodingKey::from_rsa_pem(FIXTURE_PUBLIC).unwrap();
        decode::<Value>(&token, &key, &v).unwrap();

        // Their signature, our verification against the fixture's JWK.
        let theirs = encode(
            &Header::new(Algorithm::RS256),
            &claims(),
            &EncodingKey::from_rsa_pem(FIXTURE_PRIVATE.as_bytes()).unwrap(),
        )
        .unwrap();
        let jwk = public_jwk_of(FIXTURE_PRIVATE);
        assert!(verify(&jwk, &theirs));
        assert!(verify(&jwk, &token));
    }

    fn public_jwk_of(pem: &str) -> Value {
        use rsa::pkcs8::DecodePrivateKey;
        let sk = rsa::RsaPrivateKey::from_pkcs8_pem(pem).unwrap();
        jwk_of(&sk.to_public_key())
    }
}
