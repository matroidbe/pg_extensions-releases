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
