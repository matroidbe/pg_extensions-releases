//! Verifies `pgx.register_file` with an explicit `time_axis` argument
//! actually populates `time_range` on every chunk, and that a temporal
//! predicate prunes via the catalog (not via decode).

mod common;

use crate::common::{
    cleanup_dataset, query_one_i64, query_one_string, write_zarr_with_time_axis, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;

const DATASET: &str = "e2e-register-file-time";

#[test]
fn register_file_time_axis_populates_time_range() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_register_file_time");
    // 4 time slices, 1 chunk per slice. Times = 0, 1, 2, 3 hours since
    // 2024-01-01 → absolute timestamps 2024-01-01 00:00 ... 03:00.
    let times: Vec<f64> = vec![0.0, 1.0, 2.0, 3.0];
    write_zarr_with_time_axis(
        &store.root,
        "t2m",
        N_LAT,
        N_LON,
        &times,
        "hours since 2024-01-01 00:00:00",
        50.0,
        0.0,
    );

    cleanup_dataset(DATASET);

    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, 'valid_time', NULL, NULL, NULL, NULL, true\
         )::bigint",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();

    // 4 time slices → 4 chunks.
    assert_eq!(count, times.len() as i64, "register_file returned count");

    // Every chunk must have a non-NULL time_range.
    let null_time = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.time_range IS NULL",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        null_time, 0,
        "every chunk should carry a time_range when time_axis was provided"
    );

    // bbox is still populated alongside time.
    let null_bbox = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.bbox_envelope IS NULL",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(null_bbox, 0, "bbox should also be populated");

    // Spot-check: chunk at t=2 covers 2024-01-01 02:00:00.
    let lower = query_one_string(&format!(
        "SELECT to_char(lower(c.time_range) AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') \
         FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.chunk_key = 't2m/c/2/0/0'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(lower, "2024-01-01 02:00:00");

    // Temporal pushdown: query at exactly 2024-01-01 01:30:00 should
    // hit exactly the chunk whose time_range contains it (the t=1
    // slice covers [2024-01-01 01:00, 01:00] — for a single-time chunk
    // the range collapses to a point so we instead test the t=1 boundary).
    let candidates_at = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{ds}' \
           AND c.time_range @> '2024-01-01 01:00:00+00'::timestamptz",
        ds = DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        candidates_at, 1,
        "exactly one chunk should contain 01:00:00; got {}",
        candidates_at
    );

    cleanup_dataset(DATASET);
}
