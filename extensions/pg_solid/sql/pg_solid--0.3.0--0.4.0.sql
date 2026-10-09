-- pg_solid 0.3.0 -> 0.4.0
-- ifc_true_north(): an IFC model's IfcGeometricRepresentationContext.TrueNorth
-- and the local axes' rotation, in ifc_map_conversion's convention. IFC2x3
-- files carry no IfcMapConversion, so this is their only orientation.
-- solid_georeference (4326 / 4978) now honours it when a file has no
-- IfcMapConversion (Rust-only).

CREATE  FUNCTION "ifc_true_north"(
	"filepath" TEXT /* &str */
) RETURNS TABLE (
	"north_x" double precision,  /* f64 */
	"north_y" double precision,  /* f64 */
	"rotation_deg" double precision  /* f64 */
)
IMMUTABLE STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'ifc_true_north_wrapper';
