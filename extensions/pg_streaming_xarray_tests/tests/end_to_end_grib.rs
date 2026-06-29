//! End-to-end test: real GRIB2 sample → pg_xarray catalog → pgx.fetch().
//!
//! Gated behind both env vars (so default `cargo test` / `test.sh`
//! stays deterministic):
//!
//!   RUN_GRIB_E2E=1
//!   GRIB_SAMPLE_PATH=/abs/path/to/some.grib2
//!
//! Suitable inputs include any small NOAA-style GRIB2 message:
//!
//!   * NOAA GFS sample: download from `s3://noaa-gfs-bdp-pds/...`
//!   * `gribberish` crate's `tests/data/` fixtures
//!   * Any single-message file from your own pipeline output
//!
//! Optional asserts (override defaults via env):
//!
//!   GRIB_EXPECT_VAR=t2m            # variable name in your file's catalog
//!   GRIB_EXPECT_MIN_CELLS=1000     # minimum cells the fetch should return
//!
//! The test:
//!   1. Reads the file size to set byte_length on the chunk row.
//!   2. Registers a `grib2` dataset, variable, mesh, and one chunk
//!      pointing at the local file via `fs://` URI.
//!   3. Runs `pgx.fetch(...)` and asserts:
//!        - At least `GRIB_EXPECT_MIN_CELLS` cells returned
//!        - All lats are in [-90, 90], all lons in [-180, 180]
//!        - At least one value is finite (not NaN/Inf)

mod common;

use crate::common::{cleanup_dataset, execute, query_all_f64, query_one_i64};

const DATASET: &str = "e2e-grib-sample";

#[test]
fn real_grib2_roundtrip() {
    skip_if_not_running!();

    if std::env::var("RUN_GRIB_E2E").ok().as_deref() != Some("1") {
        eprintln!("SKIPPED: real_grib2_roundtrip needs RUN_GRIB_E2E=1 + GRIB_SAMPLE_PATH=<file>");
        return;
    }

    let path = match std::env::var("GRIB_SAMPLE_PATH") {
        Ok(p) => p,
        Err(_) => {
            eprintln!("SKIPPED: real_grib2_roundtrip needs GRIB_SAMPLE_PATH=<absolute path>");
            return;
        }
    };

    // Sanity-check the file exists and read its size.
    let meta = std::fs::metadata(&path).unwrap_or_else(|e| {
        panic!("GRIB_SAMPLE_PATH '{path}' not accessible: {e}");
    });
    let byte_length = meta.len() as i64;
    assert!(
        byte_length > 100,
        "GRIB sample looks suspiciously small: {byte_length} bytes"
    );

    let var = std::env::var("GRIB_EXPECT_VAR").unwrap_or_else(|_| "t2m".to_string());
    let min_cells: i64 = std::env::var("GRIB_EXPECT_MIN_CELLS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);

    // Clean slate.
    cleanup_dataset(DATASET);

    execute(&format!(
        "SELECT pgx.register_dataset('{DATASET}', 'grib2')"
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_variable('{DATASET}', '{var}')"
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_mesh('{DATASET}', 'regular_grid', 'fixed')"
    ))
    .unwrap();

    // Register one chunk covering the full file. Most NOAA-style
    // samples are one message per file; if your sample has multiple
    // messages, only the first one will be decoded — point
    // GRIB_SAMPLE_PATH at a single-message file.
    execute(&format!(
        "SELECT pgx.register_chunk(\
            '{DATASET}', '{var}', 'fs://{path}', \
            NULL, NULL, NULL, \
            0, {byte_length}, NULL, \
            NULL, NULL\
         )",
        path = path.replace('\'', "''"),
    ))
    .unwrap();

    // Pull all cells. We don't filter — we want to assert on the full
    // grid shape characteristics.
    let total = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch('{DATASET}', '{var}')"
    ))
    .unwrap()
    .unwrap();
    assert!(
        total >= min_cells,
        "expected at least {min_cells} cells from GRIB sample, got {total}. \
         Override expectation with GRIB_EXPECT_MIN_CELLS=<n>."
    );

    // Lat / lon plausibility — any real GRIB grid lives in [-90,90] x [-180,180]
    // (the reader normalizes 0..360 → -180..180).
    let extremes = query_all_f64(&format!(
        "SELECT min(lat), max(lat), min(lon), max(lon) \
         FROM pgx.fetch('{DATASET}', '{var}', NULL, NULL, NULL, NULL, 100000)"
    ))
    .unwrap();
    let row = &extremes[0];
    assert!(
        row[0] >= -90.0 && row[0] <= 90.0,
        "min lat out of range: {}",
        row[0]
    );
    assert!(
        row[1] >= -90.0 && row[1] <= 90.0,
        "max lat out of range: {}",
        row[1]
    );
    assert!(
        row[2] >= -180.0 && row[2] <= 180.0,
        "min lon out of range: {}",
        row[2]
    );
    assert!(
        row[3] >= -180.0 && row[3] <= 180.0,
        "max lon out of range: {}",
        row[3]
    );
    assert!(row[1] >= row[0], "max lat < min lat");
    assert!(row[3] >= row[2], "max lon < min lon");

    // At least one finite value (defends against an all-NaN bitmap-masked
    // chunk slipping through silently).
    let finite_count = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch('{DATASET}', '{var}', NULL, NULL, NULL, NULL, 100000) \
         WHERE value = value AND value <> 'infinity'::float8 AND value <> '-infinity'::float8"
    ))
    .unwrap()
    .unwrap();
    assert!(
        finite_count > 0,
        "no finite values found in {var} — bitmap masking or codec issue?"
    );

    eprintln!(
        "✓ GRIB E2E: {total} cells from '{path}', lat ∈ [{:.2}, {:.2}], lon ∈ [{:.2}, {:.2}], {finite_count} finite",
        row[0], row[1], row[2], row[3]
    );

    cleanup_dataset(DATASET);
}
