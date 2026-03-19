use pgrx::prelude::*;

use crate::types::bbox3d::BBox3D;
use crate::types::solid::Solid;

/// bbox3d && bbox3d overlap test.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn bbox3d_overlaps(a: BBox3D, b: BBox3D) -> bool {
    a.overlaps(&b)
}

/// solid && solid overlap test (uses pre-computed AABB from header, zero OCCT cost).
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_overlaps(a: Solid, b: Solid) -> bool {
    a.header.bbox3d().overlaps(&b.header.bbox3d())
}

/// AABB min-distance between two solids. Used as the <-> operator function.
#[pg_extern(immutable, parallel_safe, strict)]
pub fn solid_bbox_distance(a: Solid, b: Solid) -> f64 {
    a.header.bbox3d().min_distance(&b.header.bbox3d())
}

pgrx::extension_sql!(
    r#"
CREATE OPERATOR @extschema@.&& (
    LEFTARG = @extschema@.bbox3d,
    RIGHTARG = @extschema@.bbox3d,
    FUNCTION = @extschema@.bbox3d_overlaps,
    COMMUTATOR = &&,
    RESTRICT = areasel,
    JOIN = areajoinsel
);

CREATE OPERATOR @extschema@.&& (
    LEFTARG = @extschema@.solid,
    RIGHTARG = @extschema@.solid,
    FUNCTION = @extschema@.solid_overlaps,
    COMMUTATOR = &&,
    RESTRICT = areasel,
    JOIN = areajoinsel
);

CREATE OPERATOR @extschema@.<-> (
    LEFTARG = @extschema@.solid,
    RIGHTARG = @extschema@.solid,
    FUNCTION = @extschema@.solid_bbox_distance,
    COMMUTATOR = '<->'
);
"#,
    name = "overlap_operators",
    requires = [
        BBox3D,
        Solid,
        bbox3d_overlaps,
        solid_overlaps,
        solid_bbox_distance
    ]
);
