# pg_streaming_xarray integration tests

Real end-to-end tests against a live PostgreSQL with PostGIS +
`pg_streaming` (built with `--features xarray`) + `pg_xarray`.

These exist because `#[pg_test]` (single SPI transaction that rolls
back) can't observe background-worker state, and `pg_streaming`'s
pipelines run inside background workers.

## Architecture note

The `xarray_index` sink and `xarray_header` processor are NOT a
separate pgrx extension — they're folded into `pg_streaming` behind
the `xarray` Cargo feature. Two pgrx cdylibs can't statically link
each other (each `pgrx::pg_module_magic!()` defines `Pg_magic_func`
and `_PG_init`, which collide at link time). Folding solves that
without giving up the custom-plugin DSL surface — pipelines still
reference these as `{ "custom": { "name": "xarray_index", ... } }`
or `{ "custom": { "name": "xarray_header", ... } }`, and they're
registered at `pg_streaming`'s `_PG_init` under `#[cfg(feature =
"xarray")]`.

`pg_xarray` is still a separate extension (catalog tables + the
`pgx.fetch` SRF + readers); `pg_streaming` talks to it via SPI from
the sink, not via Rust linkage. The two extensions stay independent
at the binary level.

## Running

```bash
./test.sh                      # all default tests (synthetic Zarr only)
./test.sh end_to_end_zarr      # one test by name
```

`test.sh` installs `pg_streaming --features xarray` and `pg_xarray`,
configures `postgresql.conf`, creates the test database + the two
extensions, runs `cargo test --tests`, and stops PG on exit.

## Test list

| Test | Default? | What it proves |
|---|---|---|
| `end_to_end_zarr::direct_catalog_path` | yes | Catalog CRUD + Zarr reader + `pgx.fetch` filter & cap. Writes a synthetic Zarr v3 store, registers one chunk, asserts known values. |
| `end_to_end_zarr::pipeline_path` | yes | Full pg_streaming pipeline (`opendal source → xarray_header → xarray_index → pgx.chunks → pgx.fetch`). Polls until the bg worker populates the catalog. |
| `end_to_end_grib::real_grib2_roundtrip` | opt-in | Real GRIB2 bytes through the GRIB reader. See env vars below. |

## Running the GRIB end-to-end test

```bash
RUN_GRIB_E2E=1 \
GRIB_SAMPLE_PATH=/abs/path/to/single_message.grib2 \
./test.sh end_to_end_grib
```

Optional overrides:

```bash
GRIB_EXPECT_VAR=t2m              # variable name to register
GRIB_EXPECT_MIN_CELLS=10000      # lower bound on cell count
```

When `RUN_GRIB_E2E=1` the script also rebuilds `pg_xarray` with
`--features reader-grib` so the `gribberish` crate is linked in.

### Where to get a sample

Any single-message GRIB2 file works. Suggestions:

- **NOAA GFS public data on AWS** (~5 MB per file, no auth):
  ```bash
  aws s3 cp --no-sign-request \
    s3://noaa-gfs-bdp-pds/gfs.YYYYMMDD/HH/atmos/gfs.tHHz.pgrb2.0p25.f000 \
    sample.grib2
  ```
- **gribberish crate fixtures** (~50–500 KB, public domain):
  see <https://github.com/mpiannucci/gribberish/tree/main/tests/data>
- **DWD opendata** (HTTP):
  see <https://opendata.dwd.de/weather/nwp/icon-eu/grib/>

## Prerequisites

- `cargo-pgrx init` has been run
- PostGIS is installed for the pgrx PostgreSQL. `test.sh` auto-runs
  `setup_postgis.sh` if PostGIS is missing (set `AUTO_SETUP_POSTGIS=0`
  to opt out and require a manual install).

### `setup_postgis.sh`

This helper installs PostGIS into the pgrx-managed PG. It tries three
strategies in order:

1. **Already installed?** Idempotent fast-exit if the
   `postgis.control` + `postgis-3.so` files are already in the pgrx
   `--sharedir` / `--pkglibdir`.
2. **Brew copy.** If `brew install postgis` has a bottle for the
   matching PG major version (`postgresql@$PG_VERSION`), copy the
   `.so` + `.sql` + `.control` files into the pgrx install. Fast.
3. **Build from source.** Falls back to building PostGIS
   (`POSTGIS_VERSION=3.5.2` by default) against the pgrx PG. Requires
   `brew install geos proj` (or system equivalents) plus
   `libxml2-dev`. Takes ~5 min on a modern laptop.

```bash
PG_VERSION=16 ./setup_postgis.sh        # one-shot
POSTGIS_VERSION=3.4.4 ./setup_postgis.sh # pin an older PostGIS
```

The script is safe to re-run; it short-circuits if PostGIS is already
in place.

## Why not `#[pg_test]`?

1. `#[pg_test]` runs in a single SPI transaction that rolls back.
   Background workers run in separate backend processes with their own
   transactions — they can't see the test's uncommitted writes.
2. The test can't see rows the worker has committed because the test
   transaction will roll back at the end.
3. The full multi-extension flow needs `shared_preload_libraries` set
   for `pg_streaming`, which the pgrx test harness doesn't configure.
