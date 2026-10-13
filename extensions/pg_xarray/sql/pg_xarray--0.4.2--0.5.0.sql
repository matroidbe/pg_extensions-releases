-- pg_xarray 0.4.2 -> 0.5.0
--
-- 0.4.3: SELAFIN files with more than one variable failed to register (one
--        name record per variable, not one record for all). Rust only.
-- 0.5.0: SELAFIN honours the IPARAM origin, and register_file takes a
--        `transform` (2D affine, ST_Affine order) that maps a model's local
--        frame into `srid`. Registering again moves the mesh.

DROP FUNCTION "register_file"(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT, TEXT, bool);

CREATE  FUNCTION "register_file"(
	"dataset" TEXT, /* &str */
	"variable" TEXT, /* &str */
	"uri" TEXT, /* &str */
	"format" TEXT DEFAULT 'zarr', /* &str */
	"lat_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"lon_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"time_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"z_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"srid" INT DEFAULT NULL, /* core::option::Option<i32> */
	"x_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"y_axis" TEXT DEFAULT NULL, /* core::option::Option<&str> */
	"auto_create" bool DEFAULT true, /* bool */
	"transform" double precision[] DEFAULT NULL /* core::option::Option<alloc::vec::Vec<f64>> */
) RETURNS bigint /* i64 */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'register_file_wrapper';
