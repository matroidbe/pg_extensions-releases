-- ============================================================================
-- pg_xarray Demo · Chapter 1 — Catalog a local file
-- ============================================================================
--
-- One `pgx.register_file` call per (dataset, variable, file). The walker
-- reads the file's header / chunk index once, populates `pgx.chunks` with
-- per-chunk bbox + time + byte offsets, and writes CF metadata
-- (units, standard_name, scale_factor, ...) onto `pgx.variables`.
--
-- This works the same for Zarr, NetCDF (NC3 + NC4), and GRIB2.
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo
\echo '==> Chapter 1: catalog a local file'
\echo

-- -----------------------------------------------------------------------------
-- 1.1 — Zarr v3 (one chunk file per slab, one catalog row per chunk)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 1.1 Zarr v3 — store-level discovery + bulk register'
\echo

SELECT pgx.register_dataset('demo-weather-zarr', 'zarr');

-- See what's in the store WITHOUT registering anything yet.
SELECT name, shape, dtype, is_data_variable
FROM   pgx.list_zarr_variables('fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/weather')
ORDER  BY name;

-- One call registers every data variable (skips coord axes like
-- latitude / longitude / level / valid_time).
SELECT n_variables, n_chunks
FROM   pgx.register_zarr_store('demo-weather-zarr',
                               'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/weather',
                               lat_axis  := 'latitude',
                               lon_axis  := 'longitude',
                               time_axis := 'valid_time');

\echo
\echo '-- Catalog now sees the variables + their CF metadata:'
SELECT v.name, v.dtype, v.units, v.standard_name, v.scale_factor, v.add_offset
FROM   pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id
WHERE  d.name = 'demo-weather-zarr'
ORDER  BY v.name;

-- -----------------------------------------------------------------------------
-- 1.2 — NetCDF-4 with HDF5 chunking (the path that makes 100 GB ERA5 tractable)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 1.2 NetCDF-4 — one catalog row per HDF5 chunk, with per-chunk bbox'
\echo

SELECT pgx.register_file(
    'demo-weather-nc', 't2m',
    'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/weather_chunked.nc', 'netcdf',
    lat_axis := 'latitude', lon_axis := 'longitude', time_axis := 'time'
);

\echo '-- Per-HDF5-chunk catalog rows — bbox split on longitude:'
SELECT chunk_key, bbox_wkt, time_lo
FROM   pgx.list_chunks('demo-weather-nc', 't2m')
ORDER  BY chunk_key
LIMIT  4;

-- -----------------------------------------------------------------------------
-- 1.3 — GRIB2 (one catalog row per message slab)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 1.3 GRIB2 — one catalog row per message slab'
\echo

SELECT pgx.register_file(
    'demo-weather-grib', 'TMP',
    'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/weather.grib2', 'grib2'
);

\echo '-- Per-message catalog rows with byte_offset + byte_length:'
SELECT chunk_key, bbox_wkt, time_lo, byte_offset, byte_length
FROM   pgx.list_chunks('demo-weather-grib', 'TMP')
ORDER  BY byte_offset;

-- -----------------------------------------------------------------------------
-- 1.4 — Query everything the same way: pgx.fetch
-- -----------------------------------------------------------------------------
\echo
\echo '-- 1.4 pgx.fetch — same SRF for every format'
\echo

\echo '-- 4 cells around (lat=51, lon=1) at level=500 hPa, t=02:00 from the Zarr store:'
SELECT lat, lon, level, time, value::numeric(7,3) AS t2m_K
FROM   pgx.fetch('demo-weather-zarr', 't2m_packed',
                 bbox_wkt   := 'POLYGON((0.5 50.5, 1.5 50.5, 1.5 51.5, 0.5 51.5, 0.5 50.5))',
                 level_from := 500, level_to := 500,
                 at_time    := '2024-01-01 02:00:00+00'::timestamptz);

\echo
\echo '-- A single cell from the GRIB2 file:'
SELECT lat, lon, value::numeric(7,3) AS t_K, time
FROM   pgx.fetch('demo-weather-grib', 'TMP',
                 at_time := '2024-01-01 03:00:00+00'::timestamptz)
ORDER  BY lat, lon
LIMIT  4;

\echo
\echo '== Try this =='
\echo '  -- dataset summary (variable / chunk counts + extents):'
\echo "  SELECT * FROM pgx.dataset_summary('demo-weather-zarr');"
\echo
\echo '  -- inspect one chunks bbox + byte range:'
\echo "  SELECT * FROM pgx.list_chunks('demo-weather-grib', 'TMP');"
