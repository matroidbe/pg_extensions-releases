//! Eidos catalog contract (`eidos_catalog_*`) for pg_ml.
//!
//! Implements the v1 contract defined in
//! <https://github.com/matroidbe/eidos/blob/main/docs/development/eidos-catalog-contract.md>
//! so Eidos can discover pg_ml's surface **from the live database** — the single
//! source of truth. Both `extends pg_ml` (full catalog → Eidos constructs) and
//! the `pg::pgml::*` typed rust-block helpers (a curated function subset)
//! are projections of this one catalog.
//!
//! pg_ml owns its own experiment/model store rather than Eidos-visible domain
//! entities, so `eidos_catalog_entities()` returns zero rows.
//!
//! **The read/write split here is load-bearing.** Inference and embedding are
//! `query`; anything that configures an experiment, trains, deploys or drops a
//! project is `mutates`. Eidos' decider dry-run guard reads exactly this.
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
-- pg_ml eidos_catalog contract — v1
--   Spec: https://github.com/matroidbe/eidos/blob/main/docs/development/eidos-catalog-contract.md
-- ============================================================================

-- Type mapping helper. Mirrors eidos-core's discover.rs::pg_type_to_eidos
-- exactly. Eidos cross-checks each arg/return pg_type → eidos_type at
-- discovery-time; a divergence here fails plugin import.
CREATE OR REPLACE FUNCTION _eidos_pg_to_eidos_type(udt_name TEXT)
RETURNS TEXT LANGUAGE sql IMMUTABLE SET search_path = pgml, public AS $body$
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
RETURNS JSONB LANGUAGE sql STABLE SET search_path = pgml, public AS $body$
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
RETURNS JSONB LANGUAGE sql STABLE SET search_path = pgml, public AS $body$
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
LANGUAGE sql STABLE SET search_path = pgml, public AS $body$
    SELECT jsonb_build_object(
        'extension',      'pg_ml',
        -- Read the installed version live so meta can never go stale against
        -- the actual extension.
        'version',        COALESCE((SELECT extversion FROM pg_extension WHERE extname = 'pg_ml'), '0.0.0'),
        'eidos_protocol', '1',
        'display_name',   'PyCaret ML',
        'description',    'PyCaret AutoML in PostgreSQL: experiment setup, training, inference and embeddings',
        'icon',           'cpu',
        'primary_color',  '#ec4899',
        'domains',        ARRAY['ml']
    );
$body$;

-- ============================================================================
-- eidos_catalog_entities — required
-- pg_ml owns its own experiment store, not Eidos entities. Return zero rows.
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
) LANGUAGE sql STABLE SET search_path = pgml, public AS $body$
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
) LANGUAGE sql STABLE SET search_path = pgml, public AS $body$
    WITH defs(name, side_effects, domain, display_name, icon, danger, return_shape) AS (
        VALUES
            ('predict'        , 'query'  , 'ml', 'Predict'              , 'sparkles'     , false, 'scalar'),
            ('predict_row'    , 'query'  , 'ml', 'Predict (by name)'    , 'sparkles'     , false, 'scalar'),
            ('predict_proba'  , 'query'  , 'ml', 'Predict probabilities', 'percent'      , false, 'scalar'),
            ('predict_proba_row', 'query', 'ml', 'Probabilities (by name)', 'percent'    , false, 'scalar'),
            ('predict_dist'   , 'query'  , 'ml', 'Predict distribution' , 'bar-chart-3'  , false, 'scalar'),
            ('embed'          , 'query'  , 'ml', 'Embed text'           , 'vector-square', false, 'scalar'),
            ('training_status', 'query'  , 'ml', 'Training status'      , 'activity'     , false, 'jsonb'),
            ('setup'          , 'mutates', 'ml', 'Setup experiment'     , 'settings'     , false, 'scalar'),
            ('create_model'   , 'mutates', 'ml', 'Create model'         , 'plus-circle'  , false, 'scalar'),
            ('compare_models' , 'mutates', 'ml', 'Compare models'       , 'git-compare'  , false, 'scalar'),
            ('start_training' , 'mutates', 'ml', 'Train (async)'        , 'play-circle'  , false, 'scalar'),
            ('cancel_training', 'mutates', 'ml', 'Cancel training'      , 'x-circle'     , true , 'scalar'),
            ('deploy'         , 'mutates', 'ml', 'Deploy model'         , 'rocket'       , false, 'scalar'),
            ('drop_project'   , 'mutates', 'ml', 'Drop project'         , 'trash-2'      , true , 'scalar')
    )
    SELECT
        d.name,
        'pgml'::text                                           AS schema_name,
        _eidos_args_for('pgml', d.name)                        AS args,
        _eidos_returns_for('pgml', d.name, d.return_shape)     AS returns,
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
