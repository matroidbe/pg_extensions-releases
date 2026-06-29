-- ============================================================================
-- pg_xarray Demo · Chapter 0 — Setup
-- ============================================================================
--
-- Loads the two extensions you need (PostGIS + pg_xarray). The demo fixture
-- files are committed alongside this notebook under demo/fixtures/, so there
-- is no fixture-build step. The chapters below hardcode the absolute path:
--
--   /home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures
--
-- If you cloned the repo somewhere else, search-and-replace that path
-- throughout the chapter cells before running them. The path has to be
-- absolute because `fs://` URIs are resolved by the Postgres backend, not
-- the client.
--
-- ============================================================================

\set ON_ERROR_STOP on
\set QUIET on
\pset linestyle unicode
\pset border 1
\set QUIET off

\echo
\echo '==> pg_xarray demo book — chapter 0: setup'
\echo

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_xarray;

\echo
\echo '== Catalog tables that pg_xarray manages =='
SELECT table_name
FROM   information_schema.tables
WHERE  table_schema = 'pgx'
ORDER  BY table_name;

\echo
\echo '== Required SRF / FDW handlers =='
SELECT routine_name
FROM   information_schema.routines
WHERE  routine_schema = 'pgx'
  AND  routine_name IN ('fetch', 'fetch_xyz', 'fetch_vec', 'fetch_xyz_vec',
                        'fetch_mesh', 'register_file', 'register_zarr_store',
                        'list_zarr_variables', 'fdw_handler')
ORDER  BY routine_name;

\echo
\echo '== Bundled fixtures (committed under demo/fixtures) =='
\echo '   weather/          (Zarr v3 store, 3 data variables)'
\echo '   weather.nc        (NC3 — contiguous)'
\echo '   weather_chunked.nc (NC4 — HDF5 chunks; the 100 GB ERA5 path)'
\echo '   weather.grib2     (2-message GRIB2)'
\echo '   flood.slf         (4-node, 2-triangle SELAFIN)'
\echo
