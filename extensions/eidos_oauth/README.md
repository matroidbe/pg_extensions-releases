# eidos_oauth

OAuth 2.0 JWT validator module for PostgreSQL 18.

Validates JWT bearer tokens at connection time using the PG 18 OAuth validator API.
Extracts JWT claims and injects them as session GUC variables (`app.user_*`)
for use in Row-Level Security policies.

## Configuration

```sql
-- postgresql.conf
shared_preload_libraries = 'eidos_oauth'
oauth_validator_libraries = 'eidos_oauth'

eidos_oauth.jwks_url = 'https://auth.example.com/.well-known/jwks.json'
eidos_oauth.issuer = 'https://auth.example.com'
eidos_oauth.audience = 'my-app'
eidos_oauth.role_claim = 'sub'
eidos_oauth.role_prefix = ''
eidos_oauth.jwks_cache_seconds = 300
eidos_oauth.jwks_timeout_ms = 5000
```

## SQL Functions

- `oauth_validate_token(token TEXT) -> JSONB` — Validate a JWT and return claims
- `oauth_get_claim(token TEXT, claim TEXT) -> TEXT` — Get a specific claim
- `oauth_inject_claims(token TEXT) -> BOOL` — Inject claims as session GUCs
- `oauth_jwks_status() -> JSONB` — View JWKS cache status
- `oauth_refresh_jwks() -> BOOL` — Force refresh JWKS cache

## Token signing (RS256)

For a database that mints its own access tokens and publishes a JWKS so other
resource servers can verify them. The extension holds no key state: the caller
stores the private key (an owner-only table, say) and passes it on every call.
All three are `STRICT`.

- `jwt_rsa_generate(bits INT DEFAULT 2048) -> JSONB` (volatile) — a new RSA key
  pair as `{"private_pem": "<PKCS#8 PEM>", "jwk": {"kty": "RSA", "n": …, "e": …,
  "alg": "RS256", "use": "sig", "kid": …}}`. `kid` is the key's RFC 7638 SHA-256
  thumbprint (base64url). `bits` must be 2048–4096. The `jwk` holds only the
  public parts and can go straight into a JWKS `keys` array.
- `jwt_sign_rs256(private_pem TEXT, kid TEXT, claims JSONB) -> TEXT` (immutable,
  parallel safe) — a compact JWS with header exactly
  `{"alg":"RS256","typ":"JWT","kid":<kid>}` and the claims as payload,
  RSASSA-PKCS1-v1_5 with SHA-256. Raises on a key that is not a PKCS#8 PEM RSA
  key. The private-key operation is constant-time (ring).
- `jwt_verify_rs256(jwk JSONB, token TEXT) -> BOOL` (immutable, parallel safe) —
  true only when the token is a three-part compact JWS, its header `alg` is
  exactly `RS256`, and its signature verifies against the RSA key in `jwk`
  (`kty` `RSA`, `n`, `e`). Checks the signature only: `exp`, `iss`, `aud` and
  the rest are the caller's. Never raises: a malformed token or JWK, another
  `alg` (`none`, `HS256`, …) or a bad signature is `false`.

```sql
-- once: generate and keep the key
INSERT INTO app.signing_key (private_pem, jwk)
SELECT k->>'private_pem', k->'jwk' FROM eidos_oauth.jwt_rsa_generate() AS k;

-- mint
SELECT eidos_oauth.jwt_sign_rs256(private_pem, jwk->>'kid',
         jsonb_build_object('sub', 'alice', 'exp', extract(epoch FROM now() + interval '1 hour')::int))
FROM app.signing_key;

-- publish: {"keys": [...]}
SELECT jsonb_build_object('keys', jsonb_agg(jwk)) FROM app.signing_key;
```
