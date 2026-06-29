//! Phase B round-trip: a 3-D Zarr store with a Z (level/depth/altitude)
//! axis registers with `pgx.register_file(..., z_axis := 'level')`,
//! populates `pgx.chunks.level_range`, and `pgx.fetch` with
//! `level_from`/`level_to` prunes via that range.

mod common;

use crate::common::{
    cleanup_dataset, query_one_i64, query_one_string, write_zarr_with_z_axis, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;

const DATASET: &str = "e2e-zarr-z-axis";

#[test]
fn z_axis_populates_level_range_and_prunes() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_zarr_z_axis");
    // 4 z levels at pressures 1000 / 850 / 500 / 250 hPa — one chunk
    // per level (chunk_shape = [1, n_lat, n_lon] in the fixture).
    let z_values: Vec<f64> = vec![1000.0, 850.0, 500.0, 250.0];
    write_zarr_with_z_axis(&store.root, "t", &z_values, N_LAT, N_LON, 50.0, 0.0);

    cleanup_dataset(DATASET);

    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't', '{uri}', 'zarr', \
            NULL, NULL, NULL, 'level', NULL, NULL, NULL, true\
         )::bigint",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        count, 4,
        "4 z-levels × 1 chunk each → 4 chunk rows registered"
    );

    // Every chunk should carry a non-NULL level_range.
    let null_levels = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.level_range IS NULL",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        null_levels, 0,
        "Phase B should populate level_range on every chunk"
    );

    // Spot-check: the chunk at k=2 (the 500 hPa level) should have
    // level_range with both bounds = 500. Single-point ranges work
    // because we use `'[]'` (inclusive both ends) — same fix as
    // tstzrange for single-time chunks.
    let lo_str = query_one_string(&format!(
        "SELECT lower(c.level_range)::text FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.chunk_key = 't/c/2/0/0'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    let lo: f64 = lo_str.parse().expect("level_range lower parses as f64");
    assert!(
        (lo - 500.0).abs() < 1e-9,
        "chunk t/c/2/0/0 should be at 500 hPa, got {lo}"
    );

    // Pruning: a `level BETWEEN 600 AND 900` query should match
    // ONLY the chunk at k=1 (level = 850). The other three are
    // outside the requested slab.
    let candidates = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND c.level_range && numrange(600, 900, '[]')",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        candidates, 1,
        "level_range && [600, 900] should match exactly the 850 hPa chunk"
    );

    // End-to-end via `pgx.fetch`: pass level_from / level_to and
    // assert the count comes out at exactly the cells of one Z slice
    // (N_LAT * N_LON = 12).
    let cell_count = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch(\
            '{ds}', 't', NULL, \
            'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))', \
            600.0, 900.0\
         )",
        ds = DATASET,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        cell_count,
        (N_LAT * N_LON) as i64,
        "pgx.fetch with level_from=600/level_to=900 should return exactly one slice"
    );

    cleanup_dataset(DATASET);
}
