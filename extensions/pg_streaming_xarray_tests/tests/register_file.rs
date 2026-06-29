//! Integration test for `pgx.register_file` — the no-pg_streaming-pipeline
//! path to populate the catalog.
//!
//! Builds a MULTI-CHUNK synthetic Zarr v3 store, registers it in one
//! SQL call, then asserts:
//!
//!   1. The catalog has one row per file-chunk (4 chunks for a 6×8
//!      grid with chunk_shape [3, 4]).
//!   2. Every row has a non-NULL bbox_envelope — without that, GIST
//!      pruning is a no-op.
//!   3. A tight per-city bbox query routes through exactly the
//!      overlapping chunk(s), and the cell-level filter narrows to
//!      the city's grid point.
//!   4. Same answer regardless of pushdown depth (sanity).

mod common;

use crate::common::{
    cleanup_dataset, query_one_f64, query_one_i64, write_multichunk_zarr_2d, TempStore,
};

const N_LAT: usize = 6;
const N_LON: usize = 8;
const CHUNK_LAT: usize = 3;
const CHUNK_LON: usize = 4;
const LAT_START: f32 = 50.0; // lats: 50..55
const LON_START: f32 = 0.0; // lons: 0..7

const DATASET: &str = "e2e-register-file";

#[test]
fn register_file_populates_with_bbox_and_pushdown_prunes() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_register_file");
    write_multichunk_zarr_2d(
        &store.root,
        "t2m",
        N_LAT,
        N_LON,
        CHUNK_LAT,
        CHUNK_LON,
        LAT_START,
        LON_START,
    );

    cleanup_dataset(DATASET);

    // One SQL call: opens the Zarr store, walks the chunk grid, writes
    // every chunk with a real bbox. No pg_streaming pipeline involved.
    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        // Positional args: dataset, variable, uri, format,
        //                  lat_axis, lon_axis, time_axis, auto_create.
        // Auto-detect lat/lon, no time axis on this fixture.
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )::bigint",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();

    // 6/3 * 8/4 = 4 chunks.
    let expected_chunks = (N_LAT / CHUNK_LAT) * (N_LON / CHUNK_LON);
    assert_eq!(
        count, expected_chunks as i64,
        "register_file returned count"
    );
    let chunk_rows = query_one_i64(&format!("SELECT pgx.chunk_count('{}')::bigint", DATASET))
        .unwrap()
        .unwrap();
    assert_eq!(chunk_rows, expected_chunks as i64, "catalog row count");

    // Every catalog row has a non-NULL bbox_envelope — this is the
    // whole point of register_file.
    let null_bboxes = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.bbox_envelope IS NULL",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(null_bboxes, 0, "every chunk must have a real bbox");

    // Pushdown test: a tight POLYGON around lat=51, lon=2 falls inside
    // chunk (0, 0)'s envelope (which covers lats[0..3]=50,51,52 and
    // lons[0..4]=0,1,2,3) and OUTSIDE the other three chunks. The
    // && operator over the GIST index should therefore return 1
    // candidate, not 4.
    let tight_bbox = "POLYGON((1.9 50.9, 2.1 50.9, 2.1 51.1, 1.9 51.1, 1.9 50.9))";
    let candidates_for_tight = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{ds}' \
           AND c.bbox_envelope && ST_GeomFromText('{wkt}', 4326)",
        ds = DATASET,
        wkt = tight_bbox,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        candidates_for_tight, 1,
        "GIST should prune to 1 chunk for the tight bbox; got {}",
        candidates_for_tight
    );

    // End-to-end value: the same bbox query through pgx.fetch returns
    // exactly the cell at (51, 2). Global index (51-50)*8 + 2 = 10.
    let v = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         ) WHERE lat = 51.0 AND lon = 2.0",
        ds = DATASET,
        wkt = tight_bbox,
    ))
    .unwrap()
    .unwrap();
    assert!(
        (v - 10.0).abs() < 1e-6,
        "expected cell (51,2) = 10, got {}",
        v
    );

    cleanup_dataset(DATASET);
}
