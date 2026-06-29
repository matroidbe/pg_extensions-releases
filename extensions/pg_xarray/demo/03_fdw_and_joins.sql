-- ============================================================================
-- pg_xarray Demo · Chapter 3 — FDW and JOIN pushdown
-- ============================================================================
--
-- `CREATE FOREIGN TABLE … SERVER pgx_fdw OPTIONS (dataset 'x', variable 'y')`
-- gives you a regular-table façade over a (dataset, variable). WHERE
-- clauses on `lat` / `lon` / `level` / `time` push into the catalog as
-- bbox + range predicates. JOIN clauses against a regular PG table
-- push too — the planner picks a Nested Loop with the FDW as the
-- parameterized inner side, and runtime values from the outer row are
-- bound into the per-loop fetch.
--
-- Pre-requisite: Chapter 1 was run (catalog has demo-weather-zarr).
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo
\echo '==> Chapter 3: FDW + JOIN pushdown'
\echo

-- One server per (FDW, schema). Idempotent.
DROP SERVER IF EXISTS demo_pgx CASCADE;
CREATE SERVER demo_pgx FOREIGN DATA WRAPPER pgx_fdw;

DROP FOREIGN TABLE IF EXISTS demo_wx_t2m;
CREATE FOREIGN TABLE demo_wx_t2m (
    lat   DOUBLE PRECISION,
    lon   DOUBLE PRECISION,
    level DOUBLE PRECISION,
    "time" TIMESTAMPTZ,
    value DOUBLE PRECISION
) SERVER demo_pgx
  OPTIONS (dataset 'demo-weather-zarr', variable 't2m_packed');

-- -----------------------------------------------------------------------------
-- 3.1 — Basic WHERE pushdown
-- -----------------------------------------------------------------------------
\echo
\echo '-- 3.1 WHERE pushdown — bbox + time + level are folded into the catalog query'
\echo

EXPLAIN (COSTS OFF, VERBOSE)
SELECT lat, lon, value
FROM   demo_wx_t2m
WHERE  lat BETWEEN 50 AND 51
  AND  lon BETWEEN 0 AND 2
  AND  level = 500
  AND  "time" = '2024-01-01 02:00:00+00'::timestamptz;

SELECT lat, lon, value::numeric(7,3) AS t_K
FROM   demo_wx_t2m
WHERE  lat BETWEEN 50 AND 51
  AND  lon BETWEEN 0 AND 2
  AND  level = 500
  AND  "time" = '2024-01-01 02:00:00+00'::timestamptz
ORDER  BY lat, lon;

-- -----------------------------------------------------------------------------
-- 3.2 — JOIN pushdown with a stations table (the typical real workload)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 3.2 Per-station fetch via JOIN — runtime parameters bound from outer row'
\echo

-- Set up a tiny weather-station inventory. In real life this is your
-- live sensor catalog.
DROP TABLE IF EXISTS demo_stations;
CREATE TABLE demo_stations (
    station_id INT PRIMARY KEY,
    name       TEXT,
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION
);
INSERT INTO demo_stations VALUES
    (1, 'station-50-0', 50, 0),
    (2, 'station-51-1', 51, 1),
    (3, 'station-52-3', 52, 3);

BEGIN;
-- Force the planner to use the parameterized Nested Loop so this
-- chapter actually demonstrates pushdown (with these costs PG will
-- almost always pick it anyway on a 3-row stations table).
SET LOCAL enable_mergejoin = off;
SET LOCAL enable_hashjoin  = off;

\echo '-- EXPLAIN (the FDW becomes the parameterized inner of a Nested Loop):'
EXPLAIN (COSTS OFF)
SELECT s.name, f.value::numeric(7,3) AS t_K
FROM   demo_stations s
JOIN   demo_wx_t2m   f ON f.lat = s.lat AND f.lon = s.lon
WHERE  f.level = 500
  AND  f."time" = '2024-01-01 02:00:00+00'::timestamptz;

\echo
\echo '-- One row per station with the temperature at that station:'
SELECT s.name, f.value::numeric(7,3) AS t_K
FROM   demo_stations s
JOIN   demo_wx_t2m   f ON f.lat = s.lat AND f.lon = s.lon
WHERE  f.level = 500
  AND  f."time" = '2024-01-01 02:00:00+00'::timestamptz
ORDER  BY s.station_id;
COMMIT;

\echo
\echo '== Try this =='
\echo '-- "Local mean around each station" — JOIN with a bbox per station:'
\echo
\echo '  SELECT s.name, avg(f.value)::numeric(7,3) AS t_avg_K, count(*) AS n'
\echo '  FROM   demo_stations s,'
\echo '         LATERAL pgx.fetch('
\echo "           'demo-weather-zarr', 't2m_packed',"
\echo '           bbox_wkt := ST_AsText(ST_Buffer('
\echo '             ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326), 0.5)),'
\echo "           level_from := 500, level_to := 500,"
\echo "           at_time    := '2024-01-01 02:00:00+00'::timestamptz) f"
\echo '  GROUP  BY s.name'
\echo '  ORDER  BY s.name;'
