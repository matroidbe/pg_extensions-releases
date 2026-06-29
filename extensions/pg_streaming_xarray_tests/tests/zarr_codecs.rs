//! Codec coverage: verify that the shared `pgx_zarr_walker` (used by
//! both `pgx.register_file` AND `pgx.fetch` via the Zarr reader)
//! correctly walks codec chains that include `gzip` and `zstd`.
//!
//! Each test:
//!   1. Writes a tiny 3×4 Zarr v3 store with the variable's chunk
//!      compressed with the codec under test.
//!   2. Calls `pgx.register_file` — exercises the walker on the COORD
//!      arrays (those stay uncompressed in our fixture; the codec
//!      chain matters only for the data variable). Just asserts the
//!      registration completes + bbox populated.
//!   3. Calls `pgx.fetch` with a tight bbox — this runs the data
//!      chunk through `decode_chunk_values`, which is where the
//!      codec actually has to decompress.
//!   4. Asserts the cell value is what we'd get from raw bytes (no
//!      data loss through round-trip compression).

mod common;

use crate::common::{
    cleanup_dataset, query_one_f64, query_one_i64, write_compressed_zarr_2d, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;
const LAT_START: f32 = 50.0;
const LON_START: f32 = 0.0;

fn run_codec_roundtrip(codec: &str, dataset: &str, store_label: &str) {
    let store = TempStore::new(store_label);
    write_compressed_zarr_2d(
        &store.root,
        "t2m",
        N_LAT,
        N_LON,
        LAT_START,
        LON_START,
        codec,
    );

    cleanup_dataset(dataset);

    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )::bigint",
        ds = dataset,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(count, 1, "single-chunk store registers 1 row");

    // Cell at (lat=51, lon=2) → j=1, i=2 → value = 1*4 + 2 = 6. Tight
    // per-cell bbox so we exercise the chunk-decode path with the
    // smallest amount of work that still requires decompression.
    let bbox = "POLYGON((1.9 50.9, 2.1 50.9, 2.1 51.1, 1.9 51.1, 1.9 50.9))";
    let v = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         ) WHERE lat = 51.0 AND lon = 2.0",
        ds = dataset,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert!(
        (v - 6.0).abs() < 1e-6,
        "{codec}-compressed chunk decoded wrong: got {v}, expected 6"
    );

    // Sanity check: sum across the full grid must match the
    // arithmetic sum 0..11 = 66.
    let sum = query_one_f64(&format!(
        "SELECT sum(value)::float8 FROM pgx.fetch(\
            '{ds}', 't2m', NULL, \
            'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))'\
         )",
        ds = dataset
    ))
    .unwrap()
    .unwrap();
    assert!(
        (sum - 66.0).abs() < 1e-6,
        "{codec}: full-grid sum mismatch {sum}"
    );

    cleanup_dataset(dataset);
}

#[test]
fn gzip_chunk_round_trips() {
    skip_if_not_running!();
    run_codec_roundtrip("gzip", "e2e-codec-gzip", "e2e_codec_gzip");
}

#[test]
fn zstd_chunk_round_trips() {
    skip_if_not_running!();
    run_codec_roundtrip("zstd", "e2e-codec-zstd", "e2e_codec_zstd");
}
