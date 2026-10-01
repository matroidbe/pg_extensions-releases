//! Eidos catalog contract (`eidos_catalog_*`) for pg_solid.
//!
//! Implements the v1 contract defined in
//! <https://github.com/matroidbe/eidos/blob/main/docs/development/eidos-catalog-contract.md>
//! so Eidos can discover pg_solid's surface **from the live database** — the single
//! source of truth. Both `extends pg_solid` (full catalog → Eidos constructs) and
//! the `pg::pgsolid::*` typed rust-block helpers (a curated function subset)
//! are projections of this one catalog.
//!
//! pg_solid is a **compute** extension: it has no domain entities (a `solid`
//! construct's tables are authored in Eidos, not owned here), so
//! `eidos_catalog_entities()` returns zero rows.
//!
//! Every curated function below is `query` — construction, measurement,
//! predicates, booleans and in-memory export all return a value. The
//! `solid_export_*` family, which writes files to disk, is deliberately NOT
//! curated: it must never be reachable from a decider dry-run.
//!
//! Curation principle: the function SIGNATURES are introspected live from
//! `pg_proc` (so they can never drift from the real functions — the whole point
//! of the catalog-as-source design), while the CURATION (side-effect class,
//! display metadata) is authored in the `defs` VALUES table below.
//!
//! `side_effects` is load-bearing beyond documentation: Eidos' decider
//! (`return effects [...]`) dry-run guard rejects any callee that is not
//! provably read-only, and reads this per function. Classify honestly —
//! `query` means "cannot write", not "usually doesn't".

pgrx::extension_sql!(
    r#"
-- ============================================================================
-- pg_solid eidos_catalog contract — v1
--   Spec: https://github.com/matroidbe/eidos/blob/main/docs/development/eidos-catalog-contract.md
-- ============================================================================

-- Type mapping helper. Mirrors eidos-core's discover.rs::pg_type_to_eidos
-- exactly. Eidos cross-checks each arg/return pg_type → eidos_type at
-- discovery-time; a divergence here fails plugin import.
CREATE OR REPLACE FUNCTION _eidos_pg_to_eidos_type(udt_name TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE SET search_path = pgsolid, public AS $body$
    SELECT CASE lower(udt_name)
        WHEN 'int2'        THEN 'integer'
        WHEN 'int4'        THEN 'integer'
        WHEN 'int8'        THEN 'integer'
        WHEN 'smallint'    THEN 'integer'
        WHEN 'integer'     THEN 'integer'
        WHEN 'bigint'      THEN 'integer'
        WHEN 'float4'      THEN 'decimal'
        WHEN 'float8'      THEN 'decimal'
        WHEN 'numeric'     THEN 'decimal'
        WHEN 'decimal'     THEN 'decimal'
        WHEN 'real'        THEN 'decimal'
        WHEN 'bool'        THEN 'boolean'
        WHEN 'boolean'     THEN 'boolean'
        WHEN 'text'        THEN 'text'
        WHEN 'varchar'     THEN 'text'
        WHEN 'bpchar'      THEN 'text'
        WHEN 'character'   THEN 'text'
        WHEN 'date'        THEN 'date'
        WHEN 'timestamp'   THEN 'timestamp'
        WHEN 'timestamptz' THEN 'timestamp'
        WHEN 'uuid'        THEN 'uuid'
        WHEN 'jsonb'       THEN 'jsonb'
        WHEN 'json'        THEN 'jsonb'
        WHEN 'money'       THEN 'money'
        WHEN 'bytea'       THEN 'bytea'
        ELSE 'text /* ' || udt_name || ' */'
    END;
$body$;

-- Helper: build the args JSONB for a function from pg_proc so the live
-- signature is the source of truth. Returns NULL if the function doesn't exist
-- (so the catalog fails fast on schema drift), '[]' for a zero-arg function.
CREATE OR REPLACE FUNCTION _eidos_args_for(p_schema TEXT, p_func TEXT, descs JSONB DEFAULT '{}'::jsonb)
RETURNS JSONB LANGUAGE sql STABLE SET search_path = pgsolid, public AS $body$
    WITH proc AS (
        SELECT p.oid, p.proargnames, p.proargtypes, p.pronargs, p.pronargdefaults
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = p_schema AND p.proname = p_func
        -- Deterministic on overloaded names: lowest oid = earliest-created.
        ORDER BY p.oid
        LIMIT 1
    ),
    args AS (
        SELECT
            COALESCE(proc.proargnames[i+1], 'arg' || (i+1)::text) AS name,
            format_type(proc.proargtypes[i], NULL)              AS pg_type_full,
            (SELECT typname FROM pg_type WHERE oid = proc.proargtypes[i]) AS udt,
            (i+1) > (proc.pronargs - proc.pronargdefaults)      AS has_default,
            i AS pos
        FROM proc, generate_series(0, proc.pronargs - 1) i
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'name',        a.name,
        'pg_type',     a.pg_type_full,
        'eidos_type',  _eidos_pg_to_eidos_type(a.udt),
        'required',    NOT a.has_default,
        'description', descs->a.name
    ) ORDER BY a.pos), CASE WHEN EXISTS (SELECT 1 FROM proc) THEN '[]'::jsonb END)
    FROM args a;
$body$;

-- Helper: build the returns descriptor from pg_proc.
CREATE OR REPLACE FUNCTION _eidos_returns_for(p_schema TEXT, p_func TEXT, p_shape TEXT DEFAULT 'scalar')
RETURNS JSONB LANGUAGE sql STABLE SET search_path = pgsolid, public AS $body$
    WITH proc AS (
        SELECT format_type(p.prorettype, NULL) AS pg_type_full,
               (SELECT typname FROM pg_type WHERE oid = p.prorettype) AS udt
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = p_schema AND p.proname = p_func
        ORDER BY p.oid
        LIMIT 1
    )
    SELECT jsonb_build_object(
        'pg_type',    pg_type_full,
        'eidos_type', _eidos_pg_to_eidos_type(udt),
        'shape',      p_shape
    ) FROM proc;
$body$;

-- ============================================================================
-- eidos_catalog_meta — required
-- ============================================================================

CREATE OR REPLACE FUNCTION eidos_catalog_meta() RETURNS jsonb
LANGUAGE sql STABLE SET search_path = pgsolid, public AS $body$
    SELECT jsonb_build_object(
        'extension',      'pg_solid',
        -- Read the installed version live so meta can never go stale against
        -- the actual extension.
        'version',        COALESCE((SELECT extversion FROM pg_extension WHERE extname = 'pg_solid'), '0.0.0'),
        'eidos_protocol', '1',
        'display_name',   'Solid Geometry',
        'description',    '3D B-Rep solid modelling on OpenCASCADE: construction, booleans, measurement, export',
        'icon',           'box',
        'primary_color',  '#f59e0b',
        'domains',        ARRAY['geometry']
    );
$body$;

-- ============================================================================
-- eidos_catalog_entities — required
-- pg_solid is a compute extension: no domain entities. Return zero rows.
-- ============================================================================

CREATE OR REPLACE FUNCTION eidos_catalog_entities() RETURNS TABLE (
    name              text,
    schema_name       text,
    table_name        text,
    primary_key       text[],
    columns           jsonb,
    domain            text,
    display_name      text,
    icon              text,
    read_only         boolean,
    fsm_machine       text,
    default_sort      text,
    search_columns    text[]
) LANGUAGE sql STABLE SET search_path = pgsolid, public AS $body$
    SELECT NULL::text, NULL::text, NULL::text, NULL::text[], NULL::jsonb,
           NULL::text, NULL::text, NULL::text, NULL::boolean, NULL::text,
           NULL::text, NULL::text[]
    WHERE false;
$body$;

-- ============================================================================
-- eidos_catalog_functions — required
--
-- The curated public surface. SIGNATURES come from pg_proc via
-- _eidos_args_for / _eidos_returns_for (no drift possible); the side-effect
-- class + display metadata are authored here.
-- ============================================================================

CREATE OR REPLACE FUNCTION eidos_catalog_functions() RETURNS TABLE (
    name              text,
    schema_name       text,
    args              jsonb,
    returns           jsonb,
    side_effects      text,
    fsm_event         jsonb,
    suggested_roles   text[],
    domain            text,
    display_name      text,
    icon              text,
    danger            boolean
) LANGUAGE sql STABLE SET search_path = pgsolid, public AS $body$
    WITH defs(name, side_effects, domain, display_name, icon, danger, return_shape) AS (
        VALUES
            ('solid_box'             , 'query'  , 'geometry', 'Box'             , 'box'             , false, 'scalar'),
            ('solid_cylinder'        , 'query'  , 'geometry', 'Cylinder'        , 'cylinder'        , false, 'scalar'),
            ('solid_sphere'          , 'query'  , 'geometry', 'Sphere'          , 'circle'          , false, 'scalar'),
            ('solid_cone'            , 'query'  , 'geometry', 'Cone'            , 'cone'            , false, 'scalar'),
            ('solid_volume'          , 'query'  , 'geometry', 'Volume'          , 'box'             , false, 'scalar'),
            ('solid_surface_area'    , 'query'  , 'geometry', 'Surface area'    , 'square'          , false, 'scalar'),
            ('solid_bbox'            , 'query'  , 'geometry', 'Bounding box'    , 'frame'           , false, 'scalar'),
            ('solid_centroid'        , 'query'  , 'geometry', 'Centroid'        , 'crosshair'       , false, 'scalar'),
            ('solid_dimensions'      , 'query'  , 'geometry', 'Dimensions'      , 'ruler'           , false, 'scalar'),
            ('solid_face_count'      , 'query'  , 'geometry', 'Face count'      , 'hash'            , false, 'scalar'),
            ('solid_is_valid'        , 'query'  , 'geometry', 'Is valid'        , 'check-circle'    , false, 'scalar'),
            ('solid_num_solids'      , 'query'  , 'geometry', 'Body count'      , 'layers'          , false, 'scalar'),
            ('solid_distance'        , 'query'  , 'geometry', 'Distance'        , 'move-horizontal' , false, 'scalar'),
            ('solid_intersects'      , 'query'  , 'geometry', 'Intersects'      , 'git-merge'       , false, 'scalar'),
            ('solid_contains'        , 'query'  , 'geometry', 'Contains'        , 'package'         , false, 'scalar'),
            ('solid_touches'         , 'query'  , 'geometry', 'Touches'         , 'link'            , false, 'scalar'),
            ('solid_within_clearance', 'query'  , 'geometry', 'Within clearance', 'shield'          , false, 'scalar'),
            ('solid_shared_face_area', 'query'  , 'geometry', 'Shared face area', 'square'          , false, 'scalar'),
            ('solid_union'           , 'query'  , 'geometry', 'Union'           , 'git-merge'       , false, 'scalar'),
            ('solid_difference'      , 'query'  , 'geometry', 'Difference'      , 'minus-circle'    , false, 'scalar'),
            ('solid_intersection'    , 'query'  , 'geometry', 'Intersection'    , 'git-pull-request', false, 'scalar'),
            ('solid_heal'            , 'query'  , 'geometry', 'Heal'            , 'wrench'          , false, 'scalar'),
            ('solid_translate'       , 'query'  , 'geometry', 'Translate'       , 'move'            , false, 'scalar'),
            ('solid_rotate'          , 'query'  , 'geometry', 'Rotate'          , 'rotate-3d'       , false, 'scalar'),
            ('solid_to_stl'          , 'query'  , 'geometry', 'Export STL'      , 'file-down'       , false, 'scalar'),
            ('solid_to_step'         , 'query'  , 'geometry', 'Export STEP'     , 'file-down'       , false, 'scalar'),
            ('solid_to_glb'          , 'query'  , 'geometry', 'Export GLB'      , 'file-down'       , false, 'scalar')
    )
    SELECT
        d.name,
        'pgsolid'::text                                           AS schema_name,
        _eidos_args_for('pgsolid', d.name)                        AS args,
        _eidos_returns_for('pgsolid', d.name, d.return_shape)     AS returns,
        d.side_effects,
        NULL::jsonb                                                AS fsm_event,
        ARRAY['admin']::text[]                                     AS suggested_roles,
        d.domain,
        d.display_name,
        d.icon,
        d.danger
    FROM defs d;
$body$;

GRANT EXECUTE ON FUNCTION _eidos_pg_to_eidos_type(text)        TO PUBLIC;
GRANT EXECUTE ON FUNCTION _eidos_args_for(text, text, jsonb)   TO PUBLIC;
GRANT EXECUTE ON FUNCTION _eidos_returns_for(text, text, text) TO PUBLIC;
GRANT EXECUTE ON FUNCTION eidos_catalog_meta()                 TO PUBLIC;
GRANT EXECUTE ON FUNCTION eidos_catalog_entities()             TO PUBLIC;
GRANT EXECUTE ON FUNCTION eidos_catalog_functions()            TO PUBLIC;
"#,
    name = "eidos_catalog",
);
