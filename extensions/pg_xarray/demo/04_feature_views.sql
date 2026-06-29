-- ============================================================================
-- pg_xarray Demo · Chapter 4 — Materialized views as a feature store
-- ============================================================================
--
-- This is where the architecture pays off. Once a forecast file is
-- cataloged, you express "per-station, per-time-step features for ML"
-- as a normal SQL view. `REFRESH MATERIALIZED VIEW CONCURRENTLY` after
-- the daily ingest, and your training corpus is always fresh.
--
-- Pre-requisite: Chapters 1 + 3 were run (catalog has demo-weather-zarr +
-- demo_stations).
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo
\echo '==> Chapter 4: per-station feature views'
\echo

-- -----------------------------------------------------------------------------
-- 4.1 — Local-mean / range / count around each station
-- -----------------------------------------------------------------------------
\echo
\echo '-- 4.1 Local statistics — "what was the temperature pattern around each station?"'
\echo

DROP MATERIALIZED VIEW IF EXISTS demo_features_local;
CREATE MATERIALIZED VIEW demo_features_local AS
SELECT
    s.station_id,
    s.name,
    f.time                                                  AS forecast_ts,
    avg(f.value)                                            AS t2m_mean_K,
    (max(f.value) - min(f.value))                           AS t2m_range_K,
    stddev_samp(f.value)                                    AS t2m_stddev,
    count(*)                                                AS n_cells
FROM demo_stations s,
LATERAL pgx.fetch(
    'demo-weather-zarr', 't2m_packed',
    bbox_wkt   := ST_AsText(ST_Buffer(
                    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326),
                    1.0)),  -- ~110 km buffer
    level_from := 500, level_to := 500
) f
GROUP BY s.station_id, s.name, f.time
ORDER BY s.station_id, f.time;

SELECT * FROM demo_features_local;

-- -----------------------------------------------------------------------------
-- 4.2 — Vertical profile features (uses level pushdown)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 4.2 Vertical features — temperature at multiple pressure levels'
\echo

DROP MATERIALIZED VIEW IF EXISTS demo_features_vertical;
CREATE MATERIALIZED VIEW demo_features_vertical AS
SELECT
    s.station_id,
    f.time                                                       AS forecast_ts,
    avg(f.value) FILTER (WHERE f.level = 1000)                   AS t_1000hPa,
    avg(f.value) FILTER (WHERE f.level =  850)                   AS t_850hPa,
    avg(f.value) FILTER (WHERE f.level =  500)                   AS t_500hPa,
    avg(f.value) FILTER (WHERE f.level = 1000)
        - avg(f.value) FILTER (WHERE f.level = 500)              AS lapse_K
FROM demo_stations s,
LATERAL pgx.fetch(
    'demo-weather-zarr', 't2m_packed',
    bbox_wkt   := ST_AsText(ST_Buffer(
                    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326),
                    1.0)),
    level_from := 500, level_to := 1000
) f
GROUP BY s.station_id, f.time
ORDER BY s.station_id, f.time;

SELECT station_id, forecast_ts,
       t_1000hPa::numeric(7,2),
       t_850hPa::numeric(7,2),
       t_500hPa::numeric(7,2),
       lapse_K::numeric(7,2)
FROM   demo_features_vertical;

-- -----------------------------------------------------------------------------
-- 4.3 — A "training-set" view: would join your observations table here
-- -----------------------------------------------------------------------------
\echo
\echo '-- 4.3 Training set shape — observations LEFT JOIN forecast features'
\echo

-- Synthetic observations table — in real life this is your live sensor feed.
DROP TABLE IF EXISTS demo_observations;
CREATE TABLE demo_observations (
    station_id INT,
    obs_ts     TIMESTAMPTZ,
    observed_K DOUBLE PRECISION
);
INSERT INTO demo_observations VALUES
    (1, '2024-01-01 01:00:00+00', 274.95),
    (1, '2024-01-01 02:00:00+00', 275.10),
    (2, '2024-01-01 02:00:00+00', 275.42),
    (3, '2024-01-01 02:00:00+00', 275.51);

\echo '-- The shape an ML model would consume:'
SELECT
    o.station_id,
    o.obs_ts,
    o.observed_K,
    l.t2m_mean_K::numeric(7,2)  AS forecast_local_mean,
    l.t2m_range_K::numeric(7,2) AS forecast_local_range,
    v.lapse_K::numeric(7,2)     AS forecast_lapse,
    (o.observed_K - l.t2m_mean_K)::numeric(7,2) AS forecast_error
FROM demo_observations o
LEFT JOIN demo_features_local    l ON (o.station_id, o.obs_ts) = (l.station_id, l.forecast_ts)
LEFT JOIN demo_features_vertical v ON (o.station_id, o.obs_ts) = (v.station_id, v.forecast_ts)
ORDER BY o.station_id, o.obs_ts;

\echo
\echo '== Try this =='
\echo '-- After your daily ingest:'
\echo
\echo '  REFRESH MATERIALIZED VIEW CONCURRENTLY demo_features_local;'
\echo '  REFRESH MATERIALIZED VIEW CONCURRENTLY demo_features_vertical;'
\echo
\echo '-- Or hook pg_ml in for in-DB training:'
\echo "--     SELECT pg_ml.train('temperature_model',"
\echo "--                        table_name := 'training_set',"
\echo "--                        target     := 'observed_K');"
