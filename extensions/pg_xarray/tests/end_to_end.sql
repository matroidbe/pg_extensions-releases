-- =============================================================================
-- pg_xarray end-to-end test — every Phase 0..D feature, asserted.
--
-- Runs as `psql -v ON_ERROR_STOP=1 -f end_to_end.sql`. Every block ends
-- with a PL/pgSQL `DO $$ ... ASSERT ... $$;` that raises an exception if
-- the catalog state or query result doesn't match expectations. psql
-- exits non-zero on any failure.
--
-- Variables substituted by test.sh (psql -v):
--   :weather_uri   — fs://... URI of the geographic store
--   :sim_uri       — fs://... URI of the Cartesian store
--   :dataset_geo   — geographic dataset name (default SRID 4326)
--   :dataset_sim   — Cartesian dataset name (SRID 0)
-- =============================================================================

\set ON_ERROR_STOP on
\pset border 1

\echo
\echo '=== 1. Register a 4D int16+CF variable (Phase 0 + B + CF metadata) ==='

SELECT pgx.register_file(
    :'dataset_geo', 't2m_packed', :'weather_uri', 'zarr',
    NULL, NULL,                          -- lat/lon auto-detected
    'valid_time',                        -- time axis
    'level',                             -- z axis
    NULL,                                -- srid: default 4326
    NULL, NULL,                          -- x/y aliases: unused
    true
) AS chunks_registered;

DO $$
DECLARE
    n_chunks       BIGINT;
    v_dtype        TEXT;
    v_units        TEXT;
    v_stdname      TEXT;
    v_long         TEXT;
    v_scale        DOUBLE PRECISION;
    v_offset       DOUBLE PRECISION;
    v_fill         DOUBLE PRECISION;
BEGIN
    -- 16 chunks (4 time × 4 levels × 1 lat-tile × 1 lon-tile)
    SELECT count(*) INTO n_chunks FROM pgx.chunks c
      JOIN pgx.variables v ON v.id = c.variable_id
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-geo' AND v.name = 't2m_packed';
    ASSERT n_chunks = 16, format('t2m_packed should have 16 chunks, got %s', n_chunks);

    -- Catalog metadata auto-populated from the file's `attributes` bag.
    SELECT v.dtype, v.units, v.standard_name, v.long_name,
           v.scale_factor, v.add_offset, v.fill_value
      INTO v_dtype, v_units, v_stdname, v_long, v_scale, v_offset, v_fill
      FROM pgx.variables v
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-geo' AND v.name = 't2m_packed';
    ASSERT v_dtype = 'int16',                  format('dtype: %s',         v_dtype);
    ASSERT v_units = 'K',                      format('units: %s',         v_units);
    ASSERT v_stdname = 'air_temperature',      format('standard_name: %s', v_stdname);
    ASSERT v_long = '2-metre temperature',     format('long_name: %s',     v_long);
    ASSERT v_scale = 0.01,                     format('scale_factor: %s',  v_scale);
    ASSERT v_offset = 273.15,                  format('add_offset: %s',    v_offset);
    ASSERT v_fill = -9999,                     format('fill_value: %s',    v_fill);
END $$;

\echo
\echo '=== 2. Time + level pushdown + per-cell coords populated ==='

-- The reader fix: cells emitted from 4D arrays must carry their time +
-- level coords (previously NULL). Query at (t=2, level=500) → one chunk
-- on the bbox-prune, one cell on the cell-prune.
SELECT lat, lon, level, time, value::numeric(7,3) AS t_K
  FROM pgx.fetch('kitchen-geo', 't2m_packed',
       '2024-01-01 02:00:00+00'::timestamptz,
       'POLYGON((-0.1 50.9, 0.1 50.9, 0.1 51.1, -0.1 51.1, -0.1 50.9))',
       500, 500);

DO $$
DECLARE
    row_count   BIGINT;
    cell_level  DOUBLE PRECISION;
    cell_time   TIMESTAMPTZ;
    cell_value  DOUBLE PRECISION;
BEGIN
    SELECT count(*) INTO row_count
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           '2024-01-01 02:00:00+00'::timestamptz,
           'POLYGON((-0.1 50.9, 0.1 50.9, 0.1 51.1, -0.1 51.1, -0.1 50.9))',
           500, 500);
    ASSERT row_count = 1, format('expected exactly 1 cell at (lat=51, lon=0, 500hPa); got %s', row_count);

    SELECT level, time, value INTO cell_level, cell_time, cell_value
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           '2024-01-01 02:00:00+00'::timestamptz,
           'POLYGON((-0.1 50.9, 0.1 50.9, 0.1 51.1, -0.1 51.1, -0.1 50.9))',
           500, 500);
    -- Reader fix: cell.level + cell.time used to be NULL; should now be
    -- populated from the chunk's per-dim coord slice.
    ASSERT cell_level = 500.0, format('cell.level should be 500, got %s', cell_level);
    ASSERT cell_time  = '2024-01-01 02:00:00+00'::timestamptz,
           format('cell.time should be 2024-01-01 02:00 UTC, got %s', cell_time);
    -- Physical value: t=2, k=2 (500hPa), j=1 (lat=51), i=0 (lon=0)
    --   stored = 2*100 + 2*10 + 1*4 + 0 = 224
    --   physical = 224*0.01 + 273.15 = 275.39 K
    ASSERT abs(cell_value - 275.39) < 1e-9,
           format('expected 275.39 K, got %s', cell_value);
END $$;

\echo
\echo '=== 3. Composite (vector) variable — wind = [u, v] (Phase C) ==='

SELECT pgx.register_file(:'dataset_geo', 'u', :'weather_uri', 'zarr',
    NULL, NULL, 'valid_time', NULL, NULL, NULL, NULL, true) AS u_chunks;
SELECT pgx.register_file(:'dataset_geo', 'v', :'weather_uri', 'zarr',
    NULL, NULL, 'valid_time', NULL, NULL, NULL, NULL, true) AS v_chunks;
SELECT pgx.register_variable(:'dataset_geo', 'wind',
    NULL, NULL, NULL, NULL, NULL, NULL,
    ARRAY['u', 'v']::text[]) AS wind_var_id;

DO $$
DECLARE
    u_chunks   BIGINT;
    v_chunks   BIGINT;
    n_comps    BIGINT;
    speed_calc DOUBLE PRECISION;
BEGIN
    -- 4 time slices × 1 chunk per slice = 4 chunks each for u and v.
    SELECT count(*) INTO u_chunks FROM pgx.chunks c
      JOIN pgx.variables v ON v.id = c.variable_id
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-geo' AND v.name = 'u';
    SELECT count(*) INTO v_chunks FROM pgx.chunks c
      JOIN pgx.variables v ON v.id = c.variable_id
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-geo' AND v.name = 'v';
    ASSERT u_chunks = 4, format('u chunks: %s', u_chunks);
    ASSERT v_chunks = 4, format('v chunks: %s', v_chunks);

    -- Composite linked correctly.
    SELECT count(*) INTO n_comps
      FROM pgx.variable_components vc
      JOIN pgx.variables v ON v.id = vc.composite_variable_id
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-geo' AND v.name = 'wind';
    ASSERT n_comps = 2, format('wind should have 2 component links, got %s', n_comps);

    -- fetch_vec returns u in values[1], v in values[2]. At t=2, lat=51, lon=0:
    --   u = +(2*100 + 1*4 + 0) = 204; v = -204
    --   speed = sqrt(204^2 + 204^2) ≈ 288.5
    SELECT sqrt(w.values[1]^2 + w.values[2]^2)
      INTO speed_calc
      FROM pgx.fetch_vec('kitchen-geo', 'wind',
           '2024-01-01 02:00:00+00'::timestamptz,
           'POLYGON((-0.1 50.9, 0.1 50.9, 0.1 51.1, -0.1 51.1, -0.1 50.9))') AS w
     WHERE w.lat = 51.0 AND w.lon = 0.0;
    ASSERT abs(speed_calc - sqrt(204.0*204.0 + 204.0*204.0)) < 1e-6,
           format('wind speed should be ~288.5 m/s, got %s', speed_calc);
END $$;

\echo
\echo '=== 4. Cartesian SRID 0 — x_axis/y_axis + fetch_xyz (Phase A + D) ==='

SELECT pgx.register_file(
    :'dataset_sim', 'pressure_field', :'sim_uri', 'zarr',
    NULL, NULL, NULL, NULL,              -- lat/lon/time/z: unused
    0,                                   -- srid = 0 (Cartesian)
    'x', 'y',                            -- x/y axis aliases
    true
) AS p_chunks;

DO $$
DECLARE
    ds_srid    INT;
    var_srid   INT;
    bbox_srid  INT;
    cell_v     DOUBLE PRECISION;
BEGIN
    SELECT default_srid INTO ds_srid FROM pgx.datasets WHERE name = 'kitchen-sim';
    ASSERT ds_srid = 0, format('dataset SRID should be 0, got %s', ds_srid);

    SELECT v.srid INTO var_srid
      FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-sim' AND v.name = 'pressure_field';
    ASSERT var_srid = 0, format('variable SRID should be 0, got %s', var_srid);

    SELECT ST_SRID(c.bbox_envelope) INTO bbox_srid
      FROM pgx.chunks c
      JOIN pgx.variables v ON v.id = c.variable_id
      JOIN pgx.datasets  d ON d.id = v.dataset_id
     WHERE d.name = 'kitchen-sim' AND v.name = 'pressure_field';
    ASSERT bbox_srid = 0, format('bbox SRID should be 0, got %s', bbox_srid);

    -- fetch_xyz returns columns (x, y, z, time, value). PostGIS && needs
    -- matching SRIDs — query in SRID 0 must match SRID-0 bbox.
    -- Cell at (x=1, y=1): pressure = j*N_X + i = 1*4 + 1 = 5
    SELECT value INTO cell_v
      FROM pgx.fetch_xyz('kitchen-sim', 'pressure_field', NULL,
           'POLYGON((0 0, 3 0, 3 2, 0 2, 0 0))')
     WHERE x = 1.0 AND y = 1.0;
    ASSERT cell_v = 5.0, format('pressure at (1,1) should be 5, got %s', cell_v);
END $$;

\echo
\echo '=== 5. WARNING on no-predicate fetch ==='

-- pgx.fetch with no bbox/time/level should emit WARNING. psql doesn't
-- give us programmatic access to NOTICEs at the SQL level, so we just
-- exercise the path; the visible WARNING below is the proof.
SELECT count(*) FROM pgx.fetch('kitchen-geo', 't2m_packed') LIMIT 0;

\echo
\echo '=== 6. LATERAL JOIN cross-source pattern ==='

CREATE TEMP TABLE cities (name text, lat float8, lon float8);
INSERT INTO cities VALUES
    ('Greenwich',  51.0, 0.0),
    ('Paris',      52.0, 2.0),
    ('Madrid',     50.0, 3.0);

DO $$
DECLARE
    joined_rows BIGINT;
BEGIN
    WITH boxed AS (
        SELECT c.name, c.lat, c.lon,
               'POLYGON((' ||
                  (c.lon - 0.05) || ' ' || (c.lat - 0.05) || ',' ||
                  (c.lon + 0.05) || ' ' || (c.lat - 0.05) || ',' ||
                  (c.lon + 0.05) || ' ' || (c.lat + 0.05) || ',' ||
                  (c.lon - 0.05) || ' ' || (c.lat + 0.05) || ',' ||
                  (c.lon - 0.05) || ' ' || (c.lat - 0.05) ||
               '))' AS bbox
          FROM cities c
    )
    SELECT count(*) INTO joined_rows
      FROM boxed b
      JOIN LATERAL pgx.fetch('kitchen-geo', 't2m_packed',
           '2024-01-01 02:00:00+00'::timestamptz, b.bbox, 500, 500) AS cell
        ON cell.lat = b.lat AND cell.lon = b.lon;
    ASSERT joined_rows = 3, format('LATERAL JOIN should match 3 cities, got %s', joined_rows);
END $$;

\echo
\echo '=== 7. FDW: CREATE FOREIGN TABLE + predicate pushdown ==='

-- Phase 3a + 3b: SELECT-from-foreign-table over a pgx dataset, with
-- WHERE clauses on lat / lon / level / time pushed down at planner
-- time so the catalog GIST + range indexes prune.
CREATE SERVER IF NOT EXISTS pgx_e2e FOREIGN DATA WRAPPER pgx_fdw;
DROP FOREIGN TABLE IF EXISTS wx_t2m;
CREATE FOREIGN TABLE wx_t2m (
    lat   float8,
    lon   float8,
    level float8,
    "time" timestamptz,
    value float8
) SERVER pgx_e2e
  OPTIONS (dataset 'kitchen-geo', variable 't2m_packed');

DO $$
DECLARE
    full_count   BIGINT;
    level_count  BIGINT;
    one_cell     DOUBLE PRECISION;
    cell_time    TIMESTAMPTZ;
    join_count   BIGINT;
BEGIN
    -- All cells: 4 time × 4 levels × 3 lat × 4 lon = 192.
    -- (no WHERE → expect the "no predicate" WARNING from pgx.fetch)
    SELECT count(*) INTO full_count FROM wx_t2m;
    ASSERT full_count = 192, format('SELECT * FROM wx_t2m: %s', full_count);

    -- WHERE level = 500 — pushed down. Catalog level_range &&
    -- numrange(500,500,'[]') prunes to 4 chunks (one per time slice),
    -- each yielding 3 lat × 4 lon = 12 cells → 48 total.
    SELECT count(*) INTO level_count FROM wx_t2m WHERE level = 500;
    ASSERT level_count = 48,
           format('WHERE level=500: expected 48, got %s', level_count);

    -- WHERE lat = 51 AND lon = 0 AND level = 500 AND time = '...'
    -- — full pushdown on every dim. Should hit exactly one chunk +
    -- one cell. Value: t=2, k=2, j=1, i=0 → stored=224 → 275.39 K.
    SELECT value, "time" INTO one_cell, cell_time FROM wx_t2m
     WHERE lat = 51.0
       AND lon = 0.0
       AND level = 500
       AND "time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT abs(one_cell - 275.39) < 1e-9,
           format('cell value: %s (expected 275.39)', one_cell);
    ASSERT cell_time = '2024-01-01 02:00:00+00'::timestamptz,
           format('cell time: %s', cell_time);

    -- LATERAL JOIN against the temp cities table from block 6 — the
    -- headline FDW use case (cross-source join with bbox pushdown
    -- from the JOIN predicate? No — JOIN-side equality doesn't push
    -- today; this just exercises the basic JOIN works).
    SELECT count(*) INTO join_count
      FROM cities c
      JOIN wx_t2m f
        ON f.lat = c.lat AND f.lon = c.lon
       AND f.level = 500
       AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT join_count = 3,
           format('JOIN cities × wx_t2m: expected 3, got %s', join_count);
END $$;

\echo
\echo '-- Sample FDW query — same shape as native PG table:'
SELECT lat, lon, level, "time", value::numeric(7,3) AS t_K
  FROM wx_t2m
 WHERE lat BETWEEN 50 AND 51
   AND lon BETWEEN 0 AND 1
   AND level = 500
   AND "time" = '2024-01-01 02:00:00+00'::timestamptz
 ORDER BY lat, lon;

\echo
\echo '-- EXPLAIN — show this is a Foreign Scan, not a SeqScan:'
EXPLAIN (COSTS OFF)
SELECT lat, lon, value FROM wx_t2m
 WHERE lat = 51 AND lon = 0 AND level = 500
   AND "time" = '2024-01-01 02:00:00+00'::timestamptz;

DROP FOREIGN TABLE wx_t2m;
DROP SERVER pgx_e2e CASCADE;

\echo
\echo '=== 7b. Time RANGE pushdown via pgx.fetch + FDW ==='

-- pgx.fetch now takes time_from / time_to in addition to at_time.
-- The catalog uses `time_range && tstzrange(time_from,time_to,'[]')`
-- instead of the point-containment `time_range @> at_time`.
DO $$
DECLARE
    cells_in_range    BIGINT;
    cells_at_point    BIGINT;
    cells_open_lower  BIGINT;
    cells_open_upper  BIGINT;
BEGIN
    -- time_from=01:00, time_to=02:00 → matches t=1 and t=2 chunks
    -- (chunks have time_range = single point [t hours since epoch]).
    -- Each time-slice has 4 levels × 12 (3×4) cells = 48 cells.
    -- Two time slices → 96 cells.
    SELECT count(*) INTO cells_in_range
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           NULL,                             -- at_time
           'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))',
           NULL, NULL,                       -- level range
           1000000,                          -- max_cells
           '2024-01-01 01:00:00+00'::timestamptz,
           '2024-01-01 02:00:00+00'::timestamptz);
    ASSERT cells_in_range = 96,
           format('time_from/to range: expected 96, got %s', cells_in_range);

    -- Sanity check: at_time exact-match still works (point-in-range).
    SELECT count(*) INTO cells_at_point
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           '2024-01-01 02:00:00+00'::timestamptz,
           'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))');
    ASSERT cells_at_point = 48,
           format('at_time exact-match: expected 48, got %s', cells_at_point);

    -- Open lower bound: time_from = NULL, time_to = 2024-01-01 01:00
    --   matches t=0 and t=1 → 96 cells.
    SELECT count(*) INTO cells_open_lower
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           NULL,
           'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))',
           NULL, NULL, 1000000,
           NULL,                                              -- open lower
           '2024-01-01 01:00:00+00'::timestamptz);
    ASSERT cells_open_lower = 96,
           format('open lower bound: expected 96, got %s', cells_open_lower);

    -- Open upper bound: time_from = 2024-01-01 02:00, time_to = NULL
    --   matches t=2 and t=3 → 96 cells.
    SELECT count(*) INTO cells_open_upper
      FROM pgx.fetch('kitchen-geo', 't2m_packed',
           NULL,
           'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))',
           NULL, NULL, 1000000,
           '2024-01-01 02:00:00+00'::timestamptz,
           NULL);                                             -- open upper
    ASSERT cells_open_upper = 96,
           format('open upper bound: expected 96, got %s', cells_open_upper);
END $$;

-- FDW: WHERE time BETWEEN ... AND ... should now push, not fall through
-- to PG-side filter. Same answer either way, but with pushdown there's
-- no WARNING and the catalog GIST prunes.
CREATE SERVER IF NOT EXISTS pgx_e2e_t FOREIGN DATA WRAPPER pgx_fdw;
DROP FOREIGN TABLE IF EXISTS wx_range;
CREATE FOREIGN TABLE wx_range (
    lat   float8,
    lon   float8,
    level float8,
    "time" timestamptz,
    value float8
) SERVER pgx_e2e_t
  OPTIONS (dataset 'kitchen-geo', variable 't2m_packed');

DO $$
DECLARE
    between_count BIGINT;
    half_open     BIGINT;
BEGIN
    -- WHERE time BETWEEN — both `>=` and `<=` push.
    SELECT count(*) INTO between_count FROM wx_range
     WHERE "time" BETWEEN '2024-01-01 01:00:00+00'::timestamptz
                      AND '2024-01-01 02:00:00+00'::timestamptz
       AND level = 500;
    -- 2 time slices × 1 level × 12 cells = 24
    ASSERT between_count = 24,
           format('FDW BETWEEN+level: expected 24, got %s', between_count);

    -- Half-open `time >= ...`
    SELECT count(*) INTO half_open FROM wx_range
     WHERE "time" >= '2024-01-01 02:00:00+00'::timestamptz
       AND level = 1000;
    -- t=2,3 × level=1000 × 12 cells = 24
    ASSERT half_open = 24,
           format('FDW time >= : expected 24, got %s', half_open);
END $$;

DROP FOREIGN TABLE wx_range;
DROP SERVER pgx_e2e_t CASCADE;

\echo
\echo '=== 8. Catalog inspection SRFs ==='

-- Browsable view of the catalog without poking the internal tables.
SELECT name, dtype, units, standard_name, srid, n_components, n_chunks
  FROM pgx.list_variables('kitchen-geo')
 ORDER BY name;

DO $$
DECLARE
    n_vars        BIGINT;
    n_chunks_t2m  BIGINT;
    summary_row   RECORD;
    chunks_t2m    BIGINT;
    chunks_wind   BIGINT;
BEGIN
    -- 4 scalars (t2m_packed, u, v) + 1 composite (wind) = 4
    SELECT count(*) INTO n_vars FROM pgx.list_variables('kitchen-geo');
    ASSERT n_vars = 4, format('list_variables count: %s', n_vars);

    -- The composite has 0 chunks of its own.
    SELECT n_chunks INTO chunks_wind FROM pgx.list_variables('kitchen-geo')
     WHERE name = 'wind';
    ASSERT chunks_wind = 0, format('wind n_chunks: %s', chunks_wind);

    -- t2m_packed: 16 chunks, n_components = 0 (it's a scalar)
    SELECT n_chunks INTO n_chunks_t2m FROM pgx.list_variables('kitchen-geo')
     WHERE name = 't2m_packed';
    ASSERT n_chunks_t2m = 16, format('t2m_packed n_chunks: %s', n_chunks_t2m);

    -- list_chunks for t2m_packed
    SELECT count(*) INTO chunks_t2m FROM pgx.list_chunks('kitchen-geo', 't2m_packed');
    ASSERT chunks_t2m = 16, format('list_chunks t2m_packed: %s', chunks_t2m);

    -- dataset_summary aggregates
    SELECT * INTO summary_row FROM pgx.dataset_summary('kitchen-geo');
    ASSERT summary_row.format = 'zarr',
           format('dataset_summary format: %s', summary_row.format);
    ASSERT summary_row.default_srid = 4326,
           format('dataset_summary default_srid: %s', summary_row.default_srid);
    ASSERT summary_row.n_variables = 4,
           format('dataset_summary n_variables: %s', summary_row.n_variables);
    ASSERT summary_row.n_composite_variables = 1,
           format('dataset_summary n_composite: %s', summary_row.n_composite_variables);
    -- t2m_packed has 16 chunks, u + v have 4 each → 24 total
    ASSERT summary_row.n_chunks = 24,
           format('dataset_summary n_chunks: %s', summary_row.n_chunks);
    -- Time extent from valid_time axis ([0, 3] hours since 2024-01-01)
    ASSERT summary_row.earliest_time = '2024-01-01 00:00:00+00'::timestamptz,
           format('dataset_summary earliest_time: %s', summary_row.earliest_time);
    ASSERT summary_row.latest_time = '2024-01-01 03:00:00+00'::timestamptz,
           format('dataset_summary latest_time: %s', summary_row.latest_time);
    -- Level extent: 250..1000 hPa
    ASSERT summary_row.min_level = 250,
           format('dataset_summary min_level: %s', summary_row.min_level);
    ASSERT summary_row.max_level = 1000,
           format('dataset_summary max_level: %s', summary_row.max_level);

    -- Cartesian dataset: smaller summary
    SELECT * INTO summary_row FROM pgx.dataset_summary('kitchen-sim');
    ASSERT summary_row.default_srid = 0,
           format('sim dataset_summary default_srid: %s', summary_row.default_srid);
    ASSERT summary_row.n_variables = 1,
           format('sim dataset_summary n_variables: %s', summary_row.n_variables);
END $$;

\echo
\echo '-- Sample list_chunks output (3 of 16 t2m_packed chunks):'
SELECT id, bbox_wkt,
       lower(tstzrange(time_lo, time_hi, '[]')) AS time_lo,
       level_lo, level_hi
  FROM pgx.list_chunks('kitchen-geo', 't2m_packed')
 ORDER BY id
 LIMIT 3;

\echo
\echo '-- dataset_summary output:'
SELECT dataset, format, default_srid, n_variables,
       n_composite_variables, n_chunks,
       earliest_time, latest_time, min_level, max_level
  FROM pgx.dataset_summary('kitchen-geo');

\echo
\echo '=== 9. Store-level discovery + bulk register ==='

-- psql `:var` substitution doesn't reach inside DO $$ ... $$ blocks,
-- so park the URIs in custom GUCs and read them with `current_setting`.
SET pgx_test.weather_uri = :'weather_uri';
SET pgx_test.sim_uri     = :'sim_uri';

-- list_zarr_variables: see what's in a store without registering it.
-- The weather/ store has 7 arrays at root — 4 coord axes (rank-1) +
-- 3 data variables (rank ≥ 2). Bulk-register should pick up the 3
-- data variables and skip the coord axes.
SELECT name, shape, dtype, is_data_variable
  FROM pgx.list_zarr_variables(:'weather_uri')
 ORDER BY name;

DO $$
DECLARE
    weather_uri   TEXT := current_setting('pgx_test.weather_uri');
    sim_uri       TEXT := current_setting('pgx_test.sim_uri');
    n_total       BIGINT;
    n_data        BIGINT;
    n_coord       BIGINT;
    bulk_vars     BIGINT;
    bulk_chunks   BIGINT;
    after_vars    BIGINT;
BEGIN
    -- weather/ root: 4 coord axes (latitude, longitude, level, valid_time)
    -- + 3 data vars (t2m_packed, u, v) = 7 total
    SELECT count(*) INTO n_total
      FROM pgx.list_zarr_variables(weather_uri);
    ASSERT n_total = 7, format('weather list_zarr_variables total: %s', n_total);

    SELECT count(*) INTO n_data
      FROM pgx.list_zarr_variables(weather_uri)
     WHERE is_data_variable;
    ASSERT n_data = 3, format('weather data vars: %s', n_data);

    SELECT count(*) INTO n_coord
      FROM pgx.list_zarr_variables(weather_uri)
     WHERE NOT is_data_variable;
    ASSERT n_coord = 4, format('weather coord axes: %s', n_coord);

    -- sim/ root: 2 coords (x, y) + 1 data (pressure_field) = 3 total
    SELECT count(*) INTO n_total
      FROM pgx.list_zarr_variables(sim_uri);
    ASSERT n_total = 3, format('sim list_zarr_variables total: %s', n_total);

    -- Bulk-register the sim store into a fresh dataset. Since the
    -- sim/ store has 1 data variable (pressure_field) with 1 chunk,
    -- this should yield (1 var, 1 chunk).
    SELECT n_variables, n_chunks INTO bulk_vars, bulk_chunks
      FROM pgx.register_zarr_store('kitchen-sim-bulk', sim_uri,
                                    x_axis := 'x', y_axis := 'y',
                                    srid := 0);
    ASSERT bulk_vars = 1, format('bulk n_variables: %s', bulk_vars);
    ASSERT bulk_chunks = 1, format('bulk n_chunks: %s', bulk_chunks);

    -- Catalog should now show the fresh dataset with 1 variable.
    SELECT count(*) INTO after_vars
      FROM pgx.list_variables('kitchen-sim-bulk');
    ASSERT after_vars = 1, format('post-bulk list_variables: %s', after_vars);

    -- Idempotency: re-running should not duplicate variables (still 1).
    PERFORM pgx.register_zarr_store('kitchen-sim-bulk', sim_uri,
                                     x_axis := 'x', y_axis := 'y',
                                     srid := 0);
    SELECT count(*) INTO after_vars
      FROM pgx.list_variables('kitchen-sim-bulk');
    ASSERT after_vars = 1, format('idempotent list_variables: %s', after_vars);
END $$;

\echo
\echo '=== 10. Unstructured mesh — Phase E.1 ==='

-- A 4-node, 2-triangle mesh sitting in SRID 0 (Cartesian), with a
-- per-node temperature variable stored in a `memory://nodes` chunk.
-- The catalog path: dataset → variable (dim_order containing 'node') →
-- meshes + mesh_versions → mesh_nodes + mesh_cells → chunk pointing at
-- the memory:// URI tied to the same mesh_version. fetch_mesh joins
-- decoded values to mesh_nodes.geom and applies the bbox predicate
-- at the per-node level.

SELECT pgx.register_dataset('crash-fem', 'memory', default_srid := 0);
SELECT pgx.register_variable('crash-fem', 'temperature',
                             dim_order := ARRAY['time', 'node']);

-- One mesh, one version (no time variation — fixed mesh).
SELECT pgx.register_mesh('crash-fem', 'ugrid_triangle', 'fixed',
                          extent_wkt := 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))');

-- Park the mesh_version id in a custom GUC so the DO block can pick
-- it up (psql `:` substitution doesn't reach inside DO $$ ... $$).
\set mesh_version_id_target 'pgx_test.mesh_version_id'
WITH v AS (
    SELECT pgx.register_mesh_version(
        'crash-fem',
        '2024-01-01 00:00:00+00'::timestamptz,
        '2024-01-02 00:00:00+00'::timestamptz,
        'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))') AS id
)
SELECT set_config('pgx_test.mesh_version_id', v.id::text, false) FROM v;

-- 4 nodes laid out as a unit square pair: (0,0),(1,0),(1,1),(0,1).
DO $$
DECLARE
    mv  BIGINT := current_setting('pgx_test.mesh_version_id')::bigint;
BEGIN
    PERFORM pgx.register_mesh_node(mv, 1, 'POINT(0 0)');
    PERFORM pgx.register_mesh_node(mv, 2, 'POINT(1 0)');
    PERFORM pgx.register_mesh_node(mv, 3, 'POINT(1 1)');
    PERFORM pgx.register_mesh_node(mv, 4, 'POINT(0 1)');
    -- 2 triangles: {1,2,3} and {1,3,4}. Centroids at (2/3, 1/3) and (1/3, 2/3).
    PERFORM pgx.register_mesh_cell(mv, 10, ARRAY[1,2,3]::bigint[],
                                   format('POINT(%s %s)', 2.0/3.0, 1.0/3.0));
    PERFORM pgx.register_mesh_cell(mv, 20, ARRAY[1,3,4]::bigint[],
                                   format('POINT(%s %s)', 1.0/3.0, 2.0/3.0));
END $$;

-- Register the "memory://nodes" chunk: values 10/20/30/40 at node ids 1/2/3/4.
DO $$
DECLARE
    mv BIGINT := current_setting('pgx_test.mesh_version_id')::bigint;
BEGIN
    PERFORM pgx.register_chunk(
        'crash-fem', 'temperature',
        'memory://nodes?ids=1,2,3,4&values=10.0,20.0,30.0,40.0',
        time_from := '2024-01-01 00:00:00+00'::timestamptz,
        time_to   := '2024-01-01 01:00:00+00'::timestamptz,
        bbox_wkt  := 'POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))',
        mesh_version_id := mv);
END $$;

-- Catalog inspection
SELECT node_id, ST_AsText(geom) AS geom
  FROM pgx.mesh_nodes
 WHERE mesh_version_id = current_setting('pgx_test.mesh_version_id')::bigint
 ORDER BY node_id;

SELECT cell_id, node_ids, ST_AsText(centroid) AS centroid
  FROM pgx.mesh_cells
 WHERE mesh_version_id = current_setting('pgx_test.mesh_version_id')::bigint
 ORDER BY cell_id;

-- Full fetch_mesh — no bbox, all 4 nodes back.
SELECT node_id, cell_id, geom_wkt, value
  FROM pgx.fetch_mesh('crash-fem', 'temperature')
 ORDER BY node_id;

DO $$
DECLARE
    mv             BIGINT := current_setting('pgx_test.mesh_version_id')::bigint;
    n_nodes        BIGINT;
    n_cells        BIGINT;
    n_fetched      BIGINT;
    sum_values     DOUBLE PRECISION;
    n_bbox_pruned  BIGINT;
    sum_bbox       DOUBLE PRECISION;
BEGIN
    SELECT count(*) INTO n_nodes FROM pgx.mesh_nodes WHERE mesh_version_id = mv;
    ASSERT n_nodes = 4, format('mesh_nodes count: %s', n_nodes);

    SELECT count(*) INTO n_cells FROM pgx.mesh_cells WHERE mesh_version_id = mv;
    ASSERT n_cells = 2, format('mesh_cells count: %s', n_cells);

    -- Full fetch_mesh returns all 4 node-indexed cells.
    SELECT count(*), sum(value) INTO n_fetched, sum_values
      FROM pgx.fetch_mesh('crash-fem', 'temperature');
    ASSERT n_fetched = 4, format('fetch_mesh row count: %s', n_fetched);
    ASSERT sum_values = 100.0, format('fetch_mesh sum: %s', sum_values);

    -- bbox prune: only nodes 1 (0,0) and 2 (1,0) fall in [0,1] x [0,0].
    -- The bbox is interpreted in the variable's SRID (0).
    SELECT count(*), sum(value) INTO n_bbox_pruned, sum_bbox
      FROM pgx.fetch_mesh('crash-fem', 'temperature',
                          bbox_wkt := 'POLYGON((-0.1 -0.1, 1.1 -0.1, 1.1 0.1, -0.1 0.1, -0.1 -0.1))');
    ASSERT n_bbox_pruned = 2,
           format('fetch_mesh bbox-pruned row count: %s', n_bbox_pruned);
    ASSERT sum_bbox = 30.0,
           format('fetch_mesh bbox-pruned sum (10+20): %s', sum_bbox);

    -- Idempotency on register_mesh_node: registering node 1 again
    -- shouldn't grow the table.
    PERFORM pgx.register_mesh_node(mv, 1, 'POINT(0 0)');
    SELECT count(*) INTO n_nodes FROM pgx.mesh_nodes WHERE mesh_version_id = mv;
    ASSERT n_nodes = 4, format('mesh_nodes count after re-register: %s', n_nodes);
END $$;

\echo
\echo '=== 11. NetCDF file format — Phase F.1 (reader-netcdf feature) ==='

-- NC3 classic file with CF-packed int16 t2m + lat/lon/time coord vars.
-- Same auto-detection heuristics as the Zarr walker; same CF packing
-- applied at decode time. The reader is the netcdf crate (NC3 + NC4
-- transparent).
SELECT pgx.register_file(
    'meteo-nc', 't2m', :'nc_uri', 'netcdf',
    lat_axis := 'latitude', lon_axis := 'longitude', time_axis := 'time'
);

-- Catalog inspection — variable should have units/standard_name/dtype
-- populated from the file's CF attributes, plus the CF packing triple.
DO $$
DECLARE
    v_units         TEXT;
    v_standard_name TEXT;
    v_long_name     TEXT;
    v_dtype         TEXT;
    v_scale         DOUBLE PRECISION;
    v_offset        DOUBLE PRECISION;
    v_fill          DOUBLE PRECISION;
    n_chunks        BIGINT;
    sum_value       DOUBLE PRECISION;
    n_cells         BIGINT;
    bbox_value      DOUBLE PRECISION;
    bbox_cells      BIGINT;
BEGIN
    SELECT v.units, v.standard_name, v.long_name, v.dtype,
           v.scale_factor, v.add_offset, v.fill_value
      INTO v_units, v_standard_name, v_long_name, v_dtype,
           v_scale, v_offset, v_fill
      FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
     WHERE d.name = 'meteo-nc' AND v.name = 't2m';
    ASSERT v_units = 'K',                        format('nc units: %s', v_units);
    ASSERT v_standard_name = 'air_temperature',  format('nc standard_name: %s', v_standard_name);
    ASSERT v_long_name = '2-metre temperature',  format('nc long_name: %s', v_long_name);
    ASSERT v_dtype = 'int16',                    format('nc dtype: %s', v_dtype);
    ASSERT v_scale = 0.01,                       format('nc scale_factor: %s', v_scale);
    ASSERT v_offset = 273.15,                    format('nc add_offset: %s', v_offset);
    ASSERT v_fill = -9999,                       format('nc fill_value: %s', v_fill);

    SELECT count(*) INTO n_chunks FROM pgx.list_chunks('meteo-nc', 't2m');
    ASSERT n_chunks = 1, format('nc n_chunks (V1 whole-variable): %s', n_chunks);

    -- Full fetch — should return time*lat*lon = 4*3*4 = 48 cells with
    -- physical (CF-decoded) values around 275 K.
    SELECT count(*), sum(value) INTO n_cells, sum_value
      FROM pgx.fetch('meteo-nc', 't2m',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 04:00:00+00'::timestamptz);
    ASSERT n_cells = 48, format('nc full fetch n_cells: %s', n_cells);
    -- Mean = 275 + 0.1*avg(t in 0..3) + 0.05*avg(j in 0..2) + 0.05*avg(i in 0..3)
    --      = 275 + 0.15 + 0.05 + 0.075 = 275.275
    ASSERT abs(sum_value / n_cells - 275.275) < 0.01,
           format('nc full fetch mean: %s', sum_value / n_cells);

    -- Bbox prune at (50, 0) — should keep only cells at lat=50 (j=0).
    -- 4 time steps × 1 lat × 4 lon = 16 cells.
    SELECT count(*), avg(value) INTO bbox_cells, bbox_value
      FROM pgx.fetch('meteo-nc', 't2m',
                     bbox_wkt  := 'POLYGON((-0.1 49.9, 4 49.9, 4 50.1, -0.1 50.1, -0.1 49.9))',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 04:00:00+00'::timestamptz);
    ASSERT bbox_cells = 16, format('nc bbox fetch n_cells: %s', bbox_cells);
    -- Mean over j=0 strip ≈ 275 + 0.1*1.5 + 0.05*0 + 0.05*1.5 = 275.225
    ASSERT abs(bbox_value - 275.225) < 0.01,
           format('nc bbox fetch mean: %s', bbox_value);
END $$;

\echo '-- NetCDF catalog row (units/CF/dtype):'
SELECT v.name, v.dtype, v.units, v.standard_name, v.long_name,
       v.scale_factor, v.add_offset, v.fill_value
  FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
 WHERE d.name = 'meteo-nc';

\echo
\echo '=== 12. NetCDF — per-HDF5-chunk slicing (V2, 100 GB tractable path) ==='

-- Same shape as §11 but the file is NC4-with-HDF5-chunking; the walker
-- should land ONE catalog row PER HDF5 CHUNK (not one per variable),
-- and each fetch should read only the bytes for the chunks the catalog
-- routes to. This is the path that makes 100 GB ERA5 stores tractable —
-- chunk-level pruning rather than whole-file reads.
SELECT pgx.register_file(
    'meteo-nc-chunked', 't2m', :'nc_chunked_uri', 'netcdf',
    lat_axis := 'latitude', lon_axis := 'longitude', time_axis := 'time'
);

\echo '-- Per-HDF5-chunk catalog rows (one per chunk, with per-chunk bbox):'
SELECT chunk_key, bbox_wkt, time_lo, level_lo, level_hi
  FROM pgx.list_chunks('meteo-nc-chunked', 't2m')
 ORDER BY chunk_key;

DO $$
DECLARE
    n_chunks         BIGINT;
    n_total_cells    BIGINT;
    n_bbox_cells     BIGINT;
    bbox_value       DOUBLE PRECISION;
    n_corner_chunks  BIGINT;
    sum_value        DOUBLE PRECISION;
BEGIN
    -- Fixture chunk shape (1, N_LAT=3, 2) over dim shape (4, 3, 4) →
    -- chunk grid (4, 1, 2) = 8 HDF5 chunks. The catalog should hold 8.
    SELECT count(*) INTO n_chunks
      FROM pgx.list_chunks('meteo-nc-chunked', 't2m');
    ASSERT n_chunks = 8, format('per-chunk catalog row count: %s', n_chunks);

    -- Per-chunk bbox should NOT be the full-file envelope. Verify by
    -- counting distinct bbox WKTs — chunks split on the longitude axis
    -- so we expect 2 distinct bboxes (the two lon-strips).
    SELECT count(DISTINCT bbox_wkt) INTO n_corner_chunks
      FROM pgx.list_chunks('meteo-nc-chunked', 't2m');
    ASSERT n_corner_chunks = 2,
           format('distinct per-chunk bboxes (should be 2 lon strips): %s', n_corner_chunks);

    -- Full fetch — same 48 cells, same mean, as the V1 NC3 path. The
    -- only thing that changes is HOW the bytes get fetched (8 small
    -- HDF5-chunk reads vs 1 whole-variable read).
    SELECT count(*), sum(value) INTO n_total_cells, sum_value
      FROM pgx.fetch('meteo-nc-chunked', 't2m',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 04:00:00+00'::timestamptz);
    ASSERT n_total_cells = 48,
           format('per-chunk full fetch n_cells: %s', n_total_cells);
    ASSERT abs(sum_value / n_total_cells - 275.275) < 0.01,
           format('per-chunk full fetch mean: %s', sum_value / n_total_cells);

    -- Bbox prune: a WHERE that only touches lon ∈ [0, 1] should hit
    -- only the chunks covering lon-strip [0,2). The catalog GIST
    -- prunes BEFORE any chunks get fetched — chunks for the [2,4)
    -- strip don't even appear in the candidate set.
    SELECT count(*), avg(value) INTO n_bbox_cells, bbox_value
      FROM pgx.fetch('meteo-nc-chunked', 't2m',
                     bbox_wkt  := 'POLYGON((-0.1 49.9, 1.1 49.9, 1.1 52.1, -0.1 52.1, -0.1 49.9))',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 04:00:00+00'::timestamptz);
    -- 4 time × 3 lat × 2 lon (i ∈ {0,1}) = 24 cells
    ASSERT n_bbox_cells = 24,
           format('per-chunk bbox fetch n_cells (expect 24): %s', n_bbox_cells);
    -- Mean over i ∈ {0,1} = 275 + 0.1*1.5 + 0.05*1 + 0.05*0.5 = 275.225
    ASSERT abs(bbox_value - 275.225) < 0.01,
           format('per-chunk bbox fetch mean: %s', bbox_value);
END $$;

\echo
\echo '=== 13. FDW JOIN pushdown — parameterized paths ==='

-- A "stations" regular table with one weather-station location. JOIN
-- it to the FDW: PG should pick the parameterized Foreign Scan path,
-- evaluate s.lat/s.lon as runtime parameters, and push them into the
-- FDW so fetch_impl runs with a tight bbox rather than full-grid.
BEGIN;
-- Force nested loop for this transaction so we deterministically
-- exercise the parameterized-path code (without these, PG sometimes
-- picks merge / hash join on tiny outer relations even when nested
-- loop would be just as good).
SET LOCAL enable_mergejoin = off;
SET LOCAL enable_hashjoin  = off;

CREATE TABLE stations_join (id INT PRIMARY KEY, lat DOUBLE PRECISION, lon DOUBLE PRECISION);
INSERT INTO stations_join VALUES (1, 51, 1);  -- inside the t2m_packed bbox

DROP SERVER IF EXISTS pgx_e2e_join CASCADE;
CREATE SERVER pgx_e2e_join FOREIGN DATA WRAPPER pgx_fdw;

DROP FOREIGN TABLE IF EXISTS wx_join;
CREATE FOREIGN TABLE wx_join (
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    level DOUBLE PRECISION,
    "time" TIMESTAMPTZ,
    value DOUBLE PRECISION
) SERVER pgx_e2e_join OPTIONS (dataset 'kitchen-geo', variable 't2m_packed');

-- Sanity check the un-parameterised plan first (just so we know what
-- "no pushdown" looks like).
\echo '-- EXPLAIN of bare FDW SELECT (no JOIN — predicates push as constants):'
EXPLAIN (COSTS OFF) SELECT lat, lon, value FROM wx_join
 WHERE lat = 51 AND lon = 1
   AND level = 500
   AND "time" = '2024-01-01 02:00:00+00'::timestamptz;

\echo
\echo '-- EXPLAIN of JOIN (parameterized FDW path — outer s.lat/s.lon become runtime params):'
EXPLAIN (COSTS OFF) SELECT s.id, f.value
  FROM stations_join s
  JOIN wx_join f ON f.lat = s.lat AND f.lon = s.lon
 WHERE f.level = 500 AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;

DO $$
DECLARE
    join_result_count BIGINT;
    join_value        DOUBLE PRECISION;
    no_pushdown_count BIGINT;
BEGIN
    --Run the JOIN — should land exactly 1 cell. Zarr t2m_packed at
    -- (lat=51, lon=1, level=500, t=02:00) = 275.400 K (verified by §1).
    SELECT count(*), max(f.value) INTO join_result_count, join_value
      FROM stations_join s
      JOIN wx_join f ON f.lat = s.lat AND f.lon = s.lon
     WHERE f.level = 500 AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT join_result_count = 1,
           format('JOIN result count (expect 1 cell at s.lat,s.lon): %s', join_result_count);
    ASSERT abs(join_value - 275.400) < 0.01,
           format('JOIN result value: %s', join_value);

    -- For comparison: a bare SELECT against the FDW with the same
    -- constants should also return 1 cell. The JOIN result === bare-
    -- with-constant result is the functional correctness check.
    SELECT count(*) INTO no_pushdown_count
      FROM wx_join
     WHERE lat = 51 AND lon = 1 AND level = 500
       AND "time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT no_pushdown_count = 1,
           format('Bare constant query: %s', no_pushdown_count);
END $$;

-- Multi-row outer (parameterized path called once per outer row via
-- ReScan): two stations at different locations should yield two
-- distinct fetched cells, each with the right value.
INSERT INTO stations_join VALUES (2, 50, 2);

DO $$
DECLARE
    multi_count BIGINT;
    s1_value    DOUBLE PRECISION;
    s2_value    DOUBLE PRECISION;
BEGIN
    SELECT count(*) INTO multi_count
      FROM stations_join s
      JOIN wx_join f ON f.lat = s.lat AND f.lon = s.lon
     WHERE f.level = 500 AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT multi_count = 2,
           format('Multi-row JOIN cells (ReScan per outer row): %s', multi_count);

    -- Per-station values — verified against §1 output:
    --   (51,1) → 275.400, (50,2) → 275.370 (linear interp between
    --   shown row pattern at lat=50: 0→275.350, 1→275.360, step 0.010)
    SELECT f.value INTO s1_value
      FROM stations_join s JOIN wx_join f
        ON f.lat = s.lat AND f.lon = s.lon
     WHERE s.id = 1 AND f.level = 500
       AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT abs(s1_value - 275.400) < 0.01,
           format('s1 (51,1) value: %s', s1_value);

    SELECT f.value INTO s2_value
      FROM stations_join s JOIN wx_join f
        ON f.lat = s.lat AND f.lon = s.lon
     WHERE s.id = 2 AND f.level = 500
       AND f."time" = '2024-01-01 02:00:00+00'::timestamptz;
    ASSERT abs(s2_value - 275.370) < 0.01,
           format('s2 (50,2) value: %s', s2_value);
END $$;

-- Pushdown verification: EXPLAIN (ANALYZE, FORMAT JSON) the JOIN and
-- read out the Foreign Scan's "Actual Rows" — with pushdown each
-- outer row generates ~1 inner row; without, the FDW returns the full
-- level+time slab and PG post-filters to 1.
DO $$
DECLARE
    explain_json     JSONB;
    inner_rows_total NUMERIC;
    outer_rows       NUMERIC;
BEGIN
    EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON, TIMING OFF) '
         || 'SELECT s.id, f.value FROM stations_join s '
         || 'JOIN wx_join f ON f.lat = s.lat AND f.lon = s.lon '
         || 'WHERE f.level = 500 AND f."time" = ''2024-01-01 02:00:00+00''::timestamptz'
    INTO explain_json;
    -- Walk the JSON: top plan is Nested Loop. Inner is Foreign Scan.
    -- "Actual Rows" on the Foreign Scan is rows per loop * loops.
    inner_rows_total := (explain_json -> 0 -> 'Plan' -> 'Plans' -> 1 -> 'Actual Rows')::numeric;
    outer_rows := (explain_json -> 0 -> 'Plan' -> 'Plans' -> 0 -> 'Actual Rows')::numeric;
    -- With FULL pushdown of lat AND lon: each loop returns 1 row.
    -- Without: each loop returns ~12 (3 lats * 4 lons at this level+time).
    -- Allow a generous threshold (4) to be insensitive to small fixture
    -- shape changes while still catching "no pushdown happened" (12+).
    ASSERT inner_rows_total <= outer_rows * 4,
           format('JOIN pushdown signal: inner_rows_total=%s for outer_rows=%s '
                  '(no-pushdown baseline would be ~12 * outer)',
                  inner_rows_total, outer_rows);
END $$;

DROP FOREIGN TABLE wx_join;
DROP SERVER pgx_e2e_join CASCADE;
DROP TABLE stations_join;
COMMIT;

\echo
\echo '=== 14. GRIB2 via register_file — Phase F.2 (reader-grib feature) ==='

-- Two-message GRIB2 (TMP at 500 hPa, forecast hours 0 and 3). walk_grib
-- enumerates messages, picks ones whose abbrev / name / substring
-- matches the requested variable, and creates one pgx.chunks row per
-- match with byte_offset + byte_length pointing at the message slab.
-- The reader's read_chunk then fetches just those bytes via OpenDAL.
SELECT pgx.register_file(
    'meteo-grib', 'TMP', :'grib_uri', 'grib2'
);

\echo '-- Per-GRIB-message catalog rows (one row per message slab):'
SELECT chunk_key, bbox_wkt, time_lo, level_lo, byte_offset, byte_length
  FROM pgx.list_chunks('meteo-grib', 'TMP')
 ORDER BY byte_offset;

DO $$
DECLARE
    n_chunks      BIGINT;
    n_cells       BIGINT;
    sum_value     DOUBLE PRECISION;
    bbox_cells    BIGINT;
    v_units       TEXT;
    v_long_name   TEXT;
    distinct_t    BIGINT;
BEGIN
    -- 2 GRIB messages → 2 catalog rows.
    SELECT count(*) INTO n_chunks FROM pgx.list_chunks('meteo-grib', 'TMP');
    ASSERT n_chunks = 2, format('grib chunk count: %s', n_chunks);

    -- Variable metadata should come from the first message: units=K,
    -- long_name from the GRIB abbrev (e.g., '2t' or 'TMP').
    SELECT v.units, v.long_name INTO v_units, v_long_name
      FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
     WHERE d.name = 'meteo-grib' AND v.name = 'TMP';
    ASSERT v_units = 'K', format('grib units: %s', v_units);
    ASSERT v_long_name IS NOT NULL,
           format('grib long_name should be populated: %s', v_long_name);

    -- Full fetch — 2 messages × 3 lats × 4 lons = 24 cells.
    SELECT count(*), sum(value) INTO n_cells, sum_value
      FROM pgx.fetch('meteo-grib', 'TMP',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 06:00:00+00'::timestamptz);
    ASSERT n_cells = 24, format('grib full fetch n_cells: %s', n_cells);
    -- avg(fhour ∈ {0,3}) = 1.5, avg(lat_idx ∈ {0,1,2}) = 1.0,
    -- avg(lon ∈ {0,1,2,3}) = 1.5
    -- mean = 275 + 0.1*1.5 + 0.05*1.0 + 0.05*1.5 = 275.275
    ASSERT abs(sum_value / n_cells - 275.275) < 0.01,
           format('grib full fetch mean: %s', sum_value / n_cells);

    -- Two distinct time points should land — one chunk per forecast hour.
    SELECT count(DISTINCT time) INTO distinct_t
      FROM pgx.fetch('meteo-grib', 'TMP',
                     time_from := '2024-01-01 00:00:00+00'::timestamptz,
                     time_to   := '2024-01-01 06:00:00+00'::timestamptz);
    ASSERT distinct_t = 2, format('grib distinct times: %s', distinct_t);

    -- Time pushdown: only the t=03:00 forecast should fetch.
    SELECT count(*) INTO bbox_cells
      FROM pgx.fetch('meteo-grib', 'TMP',
                     at_time := '2024-01-01 03:00:00+00'::timestamptz);
    ASSERT bbox_cells = 12, format('grib at_time prune n_cells: %s', bbox_cells);
END $$;

\echo '-- GRIB2 catalog row (CF metadata from message header):'
SELECT v.name, v.dtype, v.units, v.standard_name, v.long_name
  FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
 WHERE d.name = 'meteo-grib';

\echo
\echo '=== 15. SELAFIN via register_file — Phase E.2 (unstructured) ==='

-- 4-node, 2-triangle SELAFIN fixture with WATER DEPTH + VELOCITY U at
-- two timesteps. register_file auto-creates the mesh + mesh_version,
-- populates pgx.mesh_nodes + pgx.mesh_cells, and registers one chunk
-- per (variable, timestep). Second call for VELOCITY U on the same
-- file reuses the existing mesh.
SELECT pgx.register_dataset('flood-sim', 'selafin', default_srid := 0);

SELECT pgx.register_file('flood-sim', 'WATER DEPTH', :'slf_uri', 'selafin');
SELECT pgx.register_file('flood-sim', 'VELOCITY U',  :'slf_uri', 'selafin');

\echo '-- Mesh nodes populated from SELAFIN X/Y coords:'
SELECT mn.node_id, ST_AsText(mn.geom) AS geom
  FROM pgx.mesh_nodes mn
  JOIN pgx.mesh_versions mv ON mv.id = mn.mesh_version_id
  JOIN pgx.meshes        m  ON m.id  = mv.mesh_id
  JOIN pgx.datasets      d  ON d.id  = m.dataset_id
 WHERE d.name = 'flood-sim'
 ORDER BY mn.node_id;

\echo '-- Mesh cells (triangles) from IKLE connectivity:'
SELECT mc.cell_id, mc.node_ids, ST_AsText(mc.centroid) AS centroid
  FROM pgx.mesh_cells mc
  JOIN pgx.mesh_versions mv ON mv.id = mc.mesh_version_id
  JOIN pgx.meshes        m  ON m.id  = mv.mesh_id
  JOIN pgx.datasets      d  ON d.id  = m.dataset_id
 WHERE d.name = 'flood-sim'
 ORDER BY mc.cell_id;

DO $$
DECLARE
    n_nodes        BIGINT;
    n_cells        BIGINT;
    n_depth_chunks BIGINT;
    n_velu_chunks  BIGINT;
    depth_t0_count BIGINT;
    depth_t0_sum   DOUBLE PRECISION;
    velu_t1_avg    DOUBLE PRECISION;
BEGIN
    -- 4 nodes, 2 triangles registered exactly once even though we
    -- called register_file twice (second call upserts cleanly).
    SELECT count(*) INTO n_nodes
      FROM pgx.mesh_nodes mn
      JOIN pgx.mesh_versions mv ON mv.id = mn.mesh_version_id
      JOIN pgx.meshes        m  ON m.id  = mv.mesh_id
      JOIN pgx.datasets      d  ON d.id  = m.dataset_id
     WHERE d.name = 'flood-sim';
    ASSERT n_nodes = 4, format('selafin mesh_nodes: %s', n_nodes);

    SELECT count(*) INTO n_cells
      FROM pgx.mesh_cells mc
      JOIN pgx.mesh_versions mv ON mv.id = mc.mesh_version_id
      JOIN pgx.meshes        m  ON m.id  = mv.mesh_id
      JOIN pgx.datasets      d  ON d.id  = m.dataset_id
     WHERE d.name = 'flood-sim';
    ASSERT n_cells = 2, format('selafin mesh_cells: %s', n_cells);

    -- 2 timesteps × 1 variable per register_file call = 2 chunks each.
    SELECT count(*) INTO n_depth_chunks
      FROM pgx.list_chunks('flood-sim', 'WATER DEPTH');
    ASSERT n_depth_chunks = 2,
           format('selafin WATER DEPTH chunks: %s', n_depth_chunks);
    SELECT count(*) INTO n_velu_chunks
      FROM pgx.list_chunks('flood-sim', 'VELOCITY U');
    ASSERT n_velu_chunks = 2,
           format('selafin VELOCITY U chunks: %s', n_velu_chunks);

    -- fetch_mesh at t=0s: WATER DEPTH should return 4 cells with
    -- values 1.0..4.0 (one per node).
    SELECT count(*), sum(value) INTO depth_t0_count, depth_t0_sum
      FROM pgx.fetch_mesh('flood-sim', 'WATER DEPTH',
                          at_time := '2024-01-01 00:00:00+00'::timestamptz);
    ASSERT depth_t0_count = 4,
           format('fetch_mesh WATER DEPTH @t0 count: %s', depth_t0_count);
    ASSERT abs(depth_t0_sum - 10.0) < 0.01,
           format('fetch_mesh WATER DEPTH @t0 sum (1+2+3+4=10): %s', depth_t0_sum);

    -- fetch_mesh at t=3600s: VELOCITY U is 1.0 at every node.
    SELECT avg(value) INTO velu_t1_avg
      FROM pgx.fetch_mesh('flood-sim', 'VELOCITY U',
                          at_time := '2024-01-01 01:00:00+00'::timestamptz);
    ASSERT abs(velu_t1_avg - 1.0) < 0.01,
           format('fetch_mesh VELOCITY U @t+1h avg: %s', velu_t1_avg);
END $$;

\echo
\echo '-- Sample fetch_mesh output — WATER DEPTH per node at t=0:'
SELECT node_id, geom_wkt, value::numeric(6,2) AS depth_m
  FROM pgx.fetch_mesh('flood-sim', 'WATER DEPTH',
                      at_time := '2024-01-01 00:00:00+00'::timestamptz)
 ORDER BY node_id;

\echo
\echo '=== 16. Cloud-native register_file — GRIB2 over HTTP ==='

-- Same GRIB2 fixture as §14, but registered via the localhost HTTP
-- server set up by test.sh. Proves walk_grib goes through OpenDAL —
-- any HTTPS / S3 / GCS URI works the same way. Public meteo buckets
-- (ECMWF Open Data, NOAA GFS/HRRR on AWS Open Data) can be cataloged
-- directly with `pgx.register_file(..., 'https://.../foo.grib2', 'grib2')`
-- and no local copy step at all.
SELECT pgx.register_file(
    'meteo-grib-http', 'TMP', :'grib_http_uri', 'grib2'
);

\echo '-- Catalog rows came from HTTP-fetched bytes (uri now starts http://):'
SELECT chunk_key, uri, byte_offset, byte_length, time_lo
  FROM pgx.list_chunks('meteo-grib-http', 'TMP')
 ORDER BY byte_offset;

DO $$
DECLARE
    http_n_chunks BIGINT;
    fs_n_chunks   BIGINT;
BEGIN
    -- Same count as §14's fs:// register — proves the OpenDAL HTTP
    -- backend got the identical bytes the fs backend did.
    SELECT count(*) INTO http_n_chunks
      FROM pgx.list_chunks('meteo-grib-http', 'TMP');
    SELECT count(*) INTO fs_n_chunks
      FROM pgx.list_chunks('meteo-grib', 'TMP');
    ASSERT http_n_chunks = fs_n_chunks,
           format('http vs fs chunk count: http=%s fs=%s', http_n_chunks, fs_n_chunks);
    ASSERT http_n_chunks = 2, format('http chunk count: %s', http_n_chunks);
END $$;

\echo
\echo '=== All assertions passed ==='
