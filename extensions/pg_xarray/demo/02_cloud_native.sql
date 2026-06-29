-- ============================================================================
-- pg_xarray Demo · Chapter 2 — Cloud-native: point at a public bucket
-- ============================================================================
--
-- Same workflow as Chapter 1, except the URI is `http://`, `https://`,
-- `s3://`, or `gs://`. The file never lands on local disk:
--
--   pgx.register_file → walker reads the header bytes via OpenDAL range
--                       GETs and writes the catalog
--   pgx.fetch         → predicate-pruned chunks → range GETs for just
--                       the bytes the query needs
--
-- This chapter has TWO runnable demos:
--   * 2.A — works offline: use the localhost HTTP server that test.sh
--           spins up (or run `python3 -m http.server` over the bundled
--           demo/fixtures directory).
--   * 2.B — live against NOAA GFS on AWS Open Data (public HTTPS, no
--           credentials). A ~42 MB initial download at register_file
--           time, then pgx.fetch range-GETs only the bytes the query
--           touches.
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo
\echo '==> Chapter 2: cloud-native register_file'
\echo

-- -----------------------------------------------------------------------------
-- 2.A — register a GRIB2 file via HTTP (run python3 -m http.server first!)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 2.A Register via HTTP — start a fixture server in another terminal:'
\echo '--'
\echo '--     cd /home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures && python3 -m http.server 29980'
\echo '--'
\echo '-- then this works — no local file step needed:'
\echo

SELECT pgx.register_file(
    'demo-grib-http', 'TMP',
    'http://127.0.0.1:29980/weather.grib2', 'grib2'
);

\echo '-- The URI in the catalog now points at the HTTP server:'
SELECT chunk_key, uri, byte_offset, byte_length
FROM   pgx.list_chunks('demo-grib-http', 'TMP')
ORDER  BY byte_offset;

\echo '-- pgx.fetch range-GETs only the matching message bytes:'
SELECT lat, lon, time, value::numeric(7,3) AS t_K
FROM   pgx.fetch('demo-grib-http', 'TMP',
                 at_time := '2024-01-01 00:00:00+00'::timestamptz)
LIMIT  4;

-- -----------------------------------------------------------------------------
-- 2.B — NOAA GFS on AWS Open Data: a real public forecast over HTTPS
-- -----------------------------------------------------------------------------
\echo
\echo '-- 2.B NOAA GFS (1°) — public HTTPS to noaa-gfs-bdp-pds, no AWS creds.'
\echo '--     ~42 MB/file. register_file does one full GET to walk the message'
\echo '--     index; pgx.fetch then range-GETs only the matching bytes.'
\echo

SELECT pgx.register_dataset('noaa-gfs-tmp', 'grib2');

-- Yesterday's 00z 1-degree GFS forecast at hour 0 (initial conditions).
-- NOAA keeps ~10 days of runs online. `current_date - 1` is safe — today's
-- run may not be uploaded yet depending on the hour you run this. Variable
-- 'TMP' matches every Temperature message in the file (every level,
-- every timestep) — gribberish recognises this as NCEP's standard abbrev
-- for GRIB2 parameter (0,0,0).
SELECT pgx.register_file(
    'noaa-gfs-tmp', 'TMP',
    'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.'
        || to_char(current_date - 1, 'YYYYMMDD')
        || '/00/atmos/gfs.t00z.pgrb2.1p00.f000',
    'grib2'
);

\echo '-- Catalog rows are tiny: one per matched message, with byte_offset/length:'
SELECT chunk_key, time_lo, byte_offset, byte_length
FROM   pgx.list_chunks('noaa-gfs-tmp', 'TMP')
ORDER  BY byte_offset
LIMIT  5;

\echo '-- A bbox around London at 1000 hPa — pgx.fetch range-GETs just one message:'
-- Three predicates push into the catalog: bbox (lat/lon), at_time, and the
-- level filter. Without `level_from`/`level_to`, fetch would return TMP at
-- every level the file carries (surface, 2 m, every pressure surface),
-- and the value column can spill past numeric(6,2) on missing-value
-- sentinels — so we use plain `numeric` and round for display.
SELECT lat, lon, level, time, round(value::numeric, 2) AS t_K
FROM   pgx.fetch('noaa-gfs-tmp', 'TMP',
                 bbox_wkt   := 'POLYGON((-0.5 51.0, 0.5 51.0, 0.5 52.0, -0.5 52.0, -0.5 51.0))',
                 level_from := 1000, level_to := 1000,
                 at_time    := ((current_date - 1) || ' 00:00:00+00')::timestamptz)
ORDER  BY lat, lon
LIMIT  4;

-- -----------------------------------------------------------------------------
-- More public datasets (commented — uncomment after wiring credentials)
-- -----------------------------------------------------------------------------
\echo
\echo '== Try this =='
\echo
\echo '-- (A) ECMWF Open Data — public HTTPS, no credentials. ~140 MB/file at 0.25°.'
\echo "--     URL pattern; ECMWF retains the last ~5 days. gribberish maps every"
\echo "--     ECMWF parameter via its discipline/category/number, so the variable"
\echo "--     name follows NCEP convention ('PRES' for pressure, 'TMP' for temp)."
\echo
\echo "--     SELECT pgx.register_file("
\echo "--       'ecmwf-open-pres', 'PRES',"
\echo "--       'https://data.ecmwf.int/forecasts/' || to_char(current_date, 'YYYYMMDD')"
\echo "--         || '/00z/ifs/0p25/oper/' || to_char(current_date, 'YYYYMMDD')"
\echo "--         || '000000-0h-oper-fc.grib2',"
\echo "--       'grib2'"
\echo "--     );"
\echo '--'
\echo '-- (B) NOAA GFS at 0.25° — same workflow as 2.B but ~500 MB/file (higher res).'
\echo "--     SELECT pgx.register_file("
\echo "--       'noaa-gfs-hires', 'TMP',"
\echo "--       'https://noaa-gfs-bdp-pds.s3.amazonaws.com/gfs.' || to_char(current_date - 1, 'YYYYMMDD')"
\echo "--         || '/00/atmos/gfs.t00z.pgrb2.0p25.f000',"
\echo "--       'grib2'"
\echo "--     );"
\echo '--'
\echo '-- (C) Pangeo ARCO-ERA5 — a public Zarr v3 store on Google Cloud.'
\echo "--     gs://gcp-public-data-arco-era5/... — register_zarr_store enumerates"
\echo '--     hundreds of variables in one call. Use generously — small files,'
\echo '--     large catalog, near-zero ingress cost.'
\echo
\echo "--     SELECT n_variables, n_chunks"
\echo "--     FROM   pgx.register_zarr_store("
\echo "--       'arco-era5',"
\echo "--       'gs://gcp-public-data-arco-era5/raw/.../single-level.zarr');"
\echo
\echo '-- Once registered, pgx.fetch / pgx.fetch_xyz / pgx.fetch_vec /'
\echo '-- pgx.fetch_mesh and the FDW all work the same as for local files.'
