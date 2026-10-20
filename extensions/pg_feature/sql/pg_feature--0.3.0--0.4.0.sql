-- pg_feature 0.3.0 -> 0.4.0
-- Adds list_primitives(). The rest of 0.4.0 is in Rust: reworked feature
-- generation and SQL (issue #168), new primitives.

CREATE FUNCTION "list_primitives"() RETURNS TABLE (
	"name" TEXT,  /* alloc::string::String */
	"primitive_type" TEXT,  /* alloc::string::String */
	"input" TEXT,  /* alloc::string::String */
	"input_types" TEXT,  /* core::option::Option<alloc::string::String> */
	"needs_time_index" bool,  /* bool */
	"description" TEXT  /* alloc::string::String */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'list_primitives_wrapper';
