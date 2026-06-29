-- =============================================================================
-- kitchen_sink_demo.sql
--
-- Visible end-to-end demo of every pg_xarray feature shipped so far.
-- Run by test.sh after the kitchen-sink Zarr fixture is built. Reads
-- like a tutorial: every block is a copy/pasteable shape a real user
-- could run against their own Zarr store.
--
-- Variables substituted by test.sh (psql -v):
--   :weather_uri   — fs://... URI of the geographic store
--   :sim_uri       — fs://... URI of the Cartesian store
--   :dataset_geo   — geographic dataset name (default SRID 4326)
--   :dataset_sim   — Cartesian dataset name (SRID 0)
-- =============================================================================

\set ON_ERROR_STOP on
\timing off
\pset border 1
\pset format aligned

-- ---------------------------------------------------------------------
-- 1. Register a 4-D int16 + CF-packed variable (t2m_packed) — exercises
--    Phase 0 (dtype + CF), Phase B (z_axis), the time-axis decoder.
--    Self-populating: register_file walks the Zarr `attributes` and
--    fills units / standard_name / long_name / scale_factor /
--    add_offset / fill_value automatically.
-- ---------------------------------------------------------------------
\echo
\echo '====================================================================='
\echo '  PHASE 0 + B + CF metadata: int16 + scale/offset + time + Z'
\echo '====================================================================='
\echo

SELECT pgx.register_file(
    :'dataset_geo', 't2m_packed', :'weather_uri', 'zarr',
    NULL, NULL,                         -- lat/lon auto-detected
    'valid_time',                       -- CF time axis
    'level',                            -- Z axis (pressure levels)
    NULL,                               -- srid: default 4326
    NULL, NULL,                         -- x/y aliases: unused
    true
)::text AS chunks_registered;

\echo
\echo '-- Catalog metadata auto-populated from the file:'
SELECT v.dtype, v.units, v.standard_name, v.long_name,
       v.scale_factor, v.add_offset, v.fill_value
  FROM pgx.variables v
  JOIN pgx.datasets d ON d.id = v.dataset_id
 WHERE d.name = :'dataset_geo' AND v.name = 't2m_packed';

\echo
\echo '-- Sample physical values (stored int16 * 0.01 + 273.15, fill→NaN)'
\echo '-- at one (t, level, lat, lon) cell; expect ~284.4 K:'
SELECT lat, lon, level, time, value
  FROM pgx.fetch(:'dataset_geo', 't2m_packed',
       '2024-01-01 02:00:00+00'::timestamptz,        -- at_time → time_range @>
       'POLYGON((-0.1 50.9, 0.1 50.9, 0.1 51.1, -0.1 51.1, -0.1 50.9))',
       500, 500)                                      -- level_from/to → numrange &&
 ORDER BY lat, lon
 LIMIT 5;

-- ---------------------------------------------------------------------
-- 2. Register two scalar wind components, declare a composite
--    `wind = [u, v]`, query it via pgx.fetch_vec (Phase C).
-- ---------------------------------------------------------------------
\echo
\echo '====================================================================='
\echo '  PHASE C: composite (vector) variable — wind = [u, v]'
\echo '====================================================================='
\echo

SELECT pgx.register_file(:'dataset_geo', 'u', :'weather_uri', 'zarr',
    NULL, NULL, 'valid_time', NULL, NULL, NULL, NULL, true)::text AS u_chunks;
SELECT pgx.register_file(:'dataset_geo', 'v', :'weather_uri', 'zarr',
    NULL, NULL, 'valid_time', NULL, NULL, NULL, NULL, true)::text AS v_chunks;

-- Declare the composite. Components must already be registered as
-- scalar variables (above); pgx.variable_components links them.
SELECT pgx.register_variable(:'dataset_geo', 'wind',
    NULL, NULL, NULL, NULL, NULL, NULL,
    ARRAY['u', 'v']::text[])::text AS wind_var_id;

\echo
\echo '-- A single cell, vector form: values[1] = u, values[2] = v'
SELECT lat, lon, time, values[1] AS u_ms, values[2] AS v_ms,
       sqrt(values[1]^2 + values[2]^2)::numeric(8,3) AS speed_ms
  FROM pgx.fetch_vec(:'dataset_geo', 'wind',
       '2024-01-01 02:00:00+00'::timestamptz,
       'POLYGON((-0.1 50.9, 1.1 50.9, 1.1 51.1, -0.1 51.1, -0.1 50.9))')
 ORDER BY lat, lon;

-- ---------------------------------------------------------------------
-- 3. Cartesian (SRID 0) data — engineering / simulation space.
--    Use x_axis / y_axis aliases instead of lat/lon for clarity, and
--    query via pgx.fetch_xyz returning (x, y, z, time, value).
-- ---------------------------------------------------------------------
\echo
\echo '====================================================================='
\echo '  PHASE A + D: Cartesian SRID 0 — x_axis / y_axis + fetch_xyz'
\echo '====================================================================='
\echo

SELECT pgx.register_file(
    :'dataset_sim', 'pressure_field', :'sim_uri', 'zarr',
    NULL, NULL,                         -- (lat/lon aliases unused)
    NULL,                               -- no time
    NULL,                               -- no Z
    0,                                  -- SRID 0 — Cartesian
    'x', 'y',                           -- x_axis / y_axis aliases
    true
)::text AS pressure_chunks;

\echo
\echo '-- Catalog state for the Cartesian variable:'
SELECT v.dtype, v.units, v.standard_name, v.srid,
       d.default_srid AS dataset_default_srid,
       ST_SRID(c.bbox_envelope) AS bbox_srid
  FROM pgx.variables v
  JOIN pgx.datasets  d ON d.id = v.dataset_id
  JOIN pgx.chunks    c ON c.variable_id = v.id
 WHERE d.name = :'dataset_sim' AND v.name = 'pressure_field';

\echo
\echo '-- fetch_xyz returns (x, y, z, time, value) — semantic columns'
\echo '-- for Cartesian space:'
SELECT x, y, value AS pressure_pa
  FROM pgx.fetch_xyz(:'dataset_sim', 'pressure_field', NULL,
       'POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))')      -- SRID 0 bbox
 ORDER BY y, x;

-- ---------------------------------------------------------------------
-- 4. The headline use case: cross-source LATERAL JOIN of a plain
--    Postgres table against the catalog, with full pushdown.
-- ---------------------------------------------------------------------
\echo
\echo '====================================================================='
\echo '  LATERAL JOIN — cities table joined against the Zarr-backed grid'
\echo '====================================================================='
\echo

CREATE TEMP TABLE cities (name text, lat float8, lon float8);
INSERT INTO cities VALUES
    ('Greenwich',  51.0, 0.0),
    ('Paris',      52.0, 2.0),
    ('Madrid',     50.0, 3.0);

\echo
\echo '-- Per-city temperature at 500 hPa, 2024-01-01 02:00 UTC:'
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
SELECT b.name, b.lat, b.lon, cell.level, cell.value::numeric(7,3) AS t_K
  FROM boxed b
  JOIN LATERAL pgx.fetch(:'dataset_geo', 't2m_packed',
       '2024-01-01 02:00:00+00'::timestamptz, b.bbox, 500, 500) AS cell
    ON cell.lat = b.lat AND cell.lon = b.lon
 ORDER BY b.name;

\echo
\echo '-- Per-city wind vector at 2024-01-01 02:00 UTC (composite via fetch_vec):'
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
SELECT b.name, b.lat, b.lon,
       w.values[1]::numeric(6,1) AS u_ms,
       w.values[2]::numeric(6,1) AS v_ms,
       sqrt(w.values[1]^2 + w.values[2]^2)::numeric(6,2) AS speed_ms
  FROM boxed b
  JOIN LATERAL pgx.fetch_vec(:'dataset_geo', 'wind',
       '2024-01-01 02:00:00+00'::timestamptz, b.bbox) AS w
    ON w.lat = b.lat AND w.lon = b.lon
 ORDER BY b.name;

-- ---------------------------------------------------------------------
-- 5. Inspect the catalog state — every column the file populated.
-- ---------------------------------------------------------------------
\echo
\echo '====================================================================='
\echo '  Catalog state after registration'
\echo '====================================================================='
\echo

\echo '-- Datasets:'
SELECT name, format, default_srid FROM pgx.datasets
 WHERE name IN (:'dataset_geo', :'dataset_sim')
 ORDER BY name;

\echo
\echo '-- Variables (scalar + composite) on both datasets:'
SELECT d.name AS dataset, v.name AS variable, v.dtype, v.units,
       v.standard_name,
       (SELECT count(*) FROM pgx.variable_components vc
         WHERE vc.composite_variable_id = v.id) AS n_components,
       (SELECT count(*) FROM pgx.chunks c WHERE c.variable_id = v.id) AS n_chunks
  FROM pgx.variables v
  JOIN pgx.datasets  d ON d.id = v.dataset_id
 WHERE d.name IN (:'dataset_geo', :'dataset_sim')
 ORDER BY d.name, v.name;

\echo
\echo '-- Component links for `wind` (the composite):'
SELECT v.name AS composite, vc.position, vc.component_name
  FROM pgx.variable_components vc
  JOIN pgx.variables v ON v.id = vc.composite_variable_id
  JOIN pgx.datasets  d ON d.id = v.dataset_id
 WHERE d.name = :'dataset_geo' AND v.name = 'wind'
 ORDER BY vc.position;

\echo
\echo 'Kitchen-sink demo complete.'
