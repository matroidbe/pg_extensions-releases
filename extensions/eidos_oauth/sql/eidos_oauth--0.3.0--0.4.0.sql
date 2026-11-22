-- eidos_oauth 0.3.0 -> 0.4.0
-- Adds RS256 key generation, signing and signature verification
-- (matroidbe/eidos#322). The extension holds no key state: the private key is
-- an argument.

CREATE FUNCTION "jwt_rsa_generate"(
	"bits" INT DEFAULT 2048 /* i32 */
) RETURNS jsonb /* pgrx::datum::json::JsonB */
STRICT VOLATILE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'jwt_rsa_generate_wrapper';

CREATE FUNCTION "jwt_sign_rs256"(
	"private_pem" TEXT, /* &str */
	"kid" TEXT, /* &str */
	"claims" jsonb /* pgrx::datum::json::JsonB */
) RETURNS TEXT /* alloc::string::String */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'jwt_sign_rs256_wrapper';

CREATE FUNCTION "jwt_verify_rs256"(
	"jwk" jsonb, /* pgrx::datum::json::JsonB */
	"token" TEXT /* &str */
) RETURNS bool /* bool */
IMMUTABLE STRICT PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'jwt_verify_rs256_wrapper';
