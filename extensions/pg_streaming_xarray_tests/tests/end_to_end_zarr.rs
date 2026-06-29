//! End-to-end test: synthetic Zarr v3 store → pg_streaming pipeline →
//! pg_xarray catalog → pgx.fetch() query.
//!
//! Exercises the FULL stack with NO mocks:
//!   * Real OpenDAL fs reads
//!   * Real ZarrReader byte → cell decode
//!   * Real PG (pgrx-managed) with PostGIS + pg_streaming +
//!     pg_xarray + pg_streaming_xarray
//!   * Real bg-worker pipeline (executor) — committed state visible to
//!     the test via autocommit tokio-postgres connection
//!
//! Two paths verified:
//!   1. Direct catalog path (no pipeline): manual register_chunk + fetch
//!   2. Pipeline path: opendal source → xarray_header → xarray_index sink,
//!      then fetch sees what the worker populated
//!
//! Both must produce identical, deterministic results.

mod common;

use crate::common::{
    cleanup_dataset, cleanup_pipeline, execute, query_all_f64, query_one_f64, query_one_i64,
    wait_for_count, write_synthetic_zarr_2d, TempStore,
};
use std::time::Duration;

const N_LAT: usize = 3;
const N_LON: usize = 4;
const LAT_START: f32 = 50.0; // lats: 50, 51, 52
const LON_START: f32 = 2.0; // lons: 2, 3, 4, 5

const DATASET_DIRECT: &str = "e2e-zarr-direct";
const DATASET_PIPELINE: &str = "e2e-zarr-pipe";
const PIPELINE_NAME: &str = "e2e_zarr_indexer";

/// Path 1 — direct catalog, no pg_streaming pipeline.
///
/// Write a synthetic Zarr store, register one chunk manually, then run
/// pgx.fetch and assert the cells match.
#[test]
fn direct_catalog_path() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_zarr_direct");
    write_synthetic_zarr_2d(&store.root, "t2m", N_LAT, N_LON, LAT_START, LON_START);

    // Clean slate before each run.
    cleanup_dataset(DATASET_DIRECT);

    execute(&format!(
        "SELECT pgx.register_dataset('{ds}', 'zarr')",
        ds = DATASET_DIRECT
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_variable('{ds}', 't2m')",
        ds = DATASET_DIRECT
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_mesh('{ds}', 'regular_grid', 'fixed')",
        ds = DATASET_DIRECT
    ))
    .unwrap();

    let store_uri = format!("fs://{}", store.root.display());
    execute(&format!(
        "SELECT pgx.register_chunk(\
            '{ds}', 't2m', '{uri}', \
            NULL, NULL, NULL, \
            NULL, NULL, 't2m/c/0/0', \
            NULL, NULL\
         )",
        ds = DATASET_DIRECT,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap();

    // Total cell count = N_LAT * N_LON = 12.
    let total = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch('{}', 't2m')",
        DATASET_DIRECT
    ))
    .unwrap()
    .unwrap();
    assert_eq!(total, (N_LAT * N_LON) as i64);

    // Sum of value column = sum 0..11 = 66.
    let sum = query_one_f64(&format!(
        "SELECT sum(value)::float8 FROM pgx.fetch('{}', 't2m')",
        DATASET_DIRECT
    ))
    .unwrap()
    .unwrap();
    let expected_sum: f64 = (0..N_LAT * N_LON).map(|k| k as f64).sum();
    assert!(
        (sum - expected_sum).abs() < 1e-6,
        "value sum mismatch: got {} expected {}",
        sum,
        expected_sum
    );

    // Cell at (lat=51, lon=3) is index (j=1, i=1) → value = j*4 + i = 5.
    let center = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch('{}', 't2m') \
         WHERE lat = 51.0 AND lon = 3.0",
        DATASET_DIRECT
    ))
    .unwrap()
    .unwrap();
    assert!(
        (center - 5.0).abs() < 1e-6,
        "center cell mismatch: {center}"
    );

    // bbox filter: POLYGON((2.5 50.5, 4.5 50.5, 4.5 51.5, 2.5 51.5, 2.5 50.5))
    // keeps lats {51} × lons {3, 4} = 2 cells.
    let filtered = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch(\
            '{}', 't2m', NULL, \
            'POLYGON((2.5 50.5, 4.5 50.5, 4.5 51.5, 2.5 51.5, 2.5 50.5))'\
         )",
        DATASET_DIRECT
    ))
    .unwrap()
    .unwrap();
    assert_eq!(filtered, 2, "bbox-filtered count");

    // max_cells defensive cap.
    let capped = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch(\
            '{}', 't2m', NULL, NULL, NULL, NULL, 5\
         )",
        DATASET_DIRECT
    ))
    .unwrap()
    .unwrap();
    assert_eq!(capped, 5, "max_cells cap");

    // Sanity: lat/lon are within the expected window.
    let extremes = query_all_f64(&format!(
        "SELECT min(lat), max(lat), min(lon), max(lon) \
         FROM pgx.fetch('{}', 't2m')",
        DATASET_DIRECT
    ))
    .unwrap();
    let row = &extremes[0];
    assert!((row[0] - LAT_START as f64).abs() < 1e-6, "min lat");
    assert!(
        (row[1] - (LAT_START as f64 + (N_LAT - 1) as f64)).abs() < 1e-6,
        "max lat"
    );
    assert!((row[2] - LON_START as f64).abs() < 1e-6, "min lon");
    assert!(
        (row[3] - (LON_START as f64 + (N_LON - 1) as f64)).abs() < 1e-6,
        "max lon"
    );

    cleanup_dataset(DATASET_DIRECT);
}

/// Path 2 — full pipeline. Background worker watches a directory,
/// reads zarr.json, fans out chunks via xarray_header, sinks them via
/// xarray_index. Test polls until the worker has populated the catalog,
/// then queries.
#[test]
fn pipeline_path() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_zarr_pipe");
    write_synthetic_zarr_2d(&store.root, "t2m", N_LAT, N_LON, LAT_START, LON_START);

    cleanup_pipeline(PIPELINE_NAME);
    cleanup_dataset(DATASET_PIPELINE);

    // The pipeline input is an OpenDAL source watching the
    // store directory and yielding records whose `source_topic` is the
    // file path of zarr.json. A `mapping` stage then sets `uri` to the
    // store root. xarray_header reads <root>/t2m/zarr.json, computes
    // the chunk grid (one chunk for this dataset), and emits one
    // record. xarray_index upserts that record into pgx.chunks.
    let pipeline_def = format!(
        r#"{{
            "input": {{"opendal": {{
                "service": "fs",
                "config":  {{"root": "{root}"}},
                "path":    "t2m/zarr.json",
                "parse_as":"bytes",
                "mode":    "watch",
                "watch":   {{"poll": "500ms"}}
            }}}},
            "pipeline": {{"processors": [
                {{"mapping": {{"uri": "'fs://{root_escaped}'"}}}},
                {{"custom":  {{"name": "xarray_header",
                              "config": {{"format": "zarr",
                                          "variables": ["t2m"]}}}}}}
            ]}},
            "output": {{"custom": {{
                "name": "xarray_index",
                "config": {{
                    "dataset":     "{ds}",
                    "format":      "zarr",
                    "mesh_kind":   "regular_grid",
                    "mesh_motion": "fixed",
                    "auto_create": true
                }}
            }}}}
        }}"#,
        root = store.root.display(),
        root_escaped = store.root.display(),
        ds = DATASET_PIPELINE,
    );

    let create_sql = format!(
        "SELECT pgstreams.create_pipeline('{name}', $body${json}$body$::jsonb)",
        name = PIPELINE_NAME,
        json = pipeline_def,
    );
    execute(&create_sql).unwrap();

    execute(&format!("SELECT pgstreams.start('{}')", PIPELINE_NAME)).unwrap();

    // Wait for the worker to populate pgx.chunks for this dataset.
    let count_sql = format!("SELECT pgx.chunk_count('{}')::bigint", DATASET_PIPELINE);
    wait_for_count(
        "pgx.chunk_count to reach 1",
        &count_sql,
        |c| c >= 1,
        Duration::from_secs(30),
    )
    .expect("pipeline never populated the catalog within 30s");

    // Worker should have written exactly 1 chunk (single chunk file).
    let chunks = query_one_i64(&count_sql).unwrap().unwrap();
    assert_eq!(chunks, 1, "expected exactly 1 chunk row");

    // The pipeline path MUST now populate bbox_envelope (same code path
    // as pgx.register_file via the shared pgx_zarr_walker rlib). A NULL
    // here would mean the GIST index can't prune for this dataset.
    let null_bboxes = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.bbox_envelope IS NULL",
        DATASET_PIPELINE
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        null_bboxes, 0,
        "pipeline path must produce non-NULL bbox_envelope for every chunk"
    );

    // Now query the catalog the same way as path 1.
    let total = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch('{}', 't2m')",
        DATASET_PIPELINE
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        total,
        (N_LAT * N_LON) as i64,
        "pipeline path returned wrong cell count"
    );

    let sum = query_one_f64(&format!(
        "SELECT sum(value)::float8 FROM pgx.fetch('{}', 't2m')",
        DATASET_PIPELINE
    ))
    .unwrap()
    .unwrap();
    let expected_sum: f64 = (0..N_LAT * N_LON).map(|k| k as f64).sum();
    assert!((sum - expected_sum).abs() < 1e-6);

    // Idempotency: poll the chunk count for another second and ensure
    // it doesn't go above 1 (the upsert dedup key is doing its job).
    std::thread::sleep(Duration::from_secs(1));
    let chunks_after = query_one_i64(&count_sql).unwrap().unwrap();
    assert_eq!(
        chunks_after, 1,
        "watch mode should not duplicate chunk rows"
    );

    cleanup_pipeline(PIPELINE_NAME);
    cleanup_dataset(DATASET_PIPELINE);
}
