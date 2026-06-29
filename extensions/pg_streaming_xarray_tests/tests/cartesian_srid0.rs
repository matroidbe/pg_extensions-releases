//! Phase A round-trip: a non-geographic (SRID 0) Zarr store registers,
//! stores its bbox in SRID 0, and `pgx.fetch` interprets the user's
//! bbox WKT in the variable's SRID — no implicit lat/lon assumption.
//!
//! The fixture uses small Cartesian coordinates (x ∈ [0..3], y ∈ [0..2])
//! so the walker's `> 180 → -360` longitude normalisation doesn't
//! trigger. The walker writes the WKT as raw lat/lon-shaped coords
//! (which here is actually y/x), and `register_chunk_impl` tags it
//! with SRID 0 because that's the variable's effective SRID.
//!
//! Asserts:
//!   1. `pgx.datasets.default_srid = 0` after registration.
//!   2. The stored `bbox_envelope` carries `ST_SRID = 0`.
//!   3. A `pgx.fetch` call with `bbox_wkt` in SRID 0 actually returns
//!      cells (the GIST `&&` only works when SRIDs match).
//!   4. The fetched value is correct (orthogonal regression check;
//!      proves Phase 0 still works under Phase A).

mod common;

use crate::common::{
    cleanup_dataset, query_one_f64, query_one_i64, write_synthetic_zarr_2d, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;

const DATASET: &str = "e2e-cartesian-srid0";

#[test]
fn cartesian_srid0_round_trips() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_cartesian_srid0");
    // The fixture writer still uses lat/lon-named axes inside the Zarr
    // store — that's fine, the dimension_names default. What matters
    // is that the catalog tags the bbox with SRID 0. Values are
    // x_start=0..3, y_start=0..2 (i.e., small Cartesian, not lat/lon).
    write_synthetic_zarr_2d(&store.root, "t2m", N_LAT, N_LON, 0.0, 0.0);

    cleanup_dataset(DATASET);

    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, 0, NULL, NULL, true\
         )::bigint",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(count, 1, "single-chunk store registers 1 row");

    // 1. default_srid propagated.
    let ds_srid = query_one_i64(&format!(
        "SELECT default_srid::bigint FROM pgx.datasets WHERE name = '{}'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(ds_srid, 0, "dataset.default_srid should be 0");

    // 2. variable.srid propagated.
    let var_srid = query_one_i64(&format!(
        "SELECT v.srid::bigint FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(var_srid, 0, "variable.srid should be 0");

    // 3. bbox_envelope tagged with SRID 0.
    let bbox_srid = query_one_i64(&format!(
        "SELECT ST_SRID(c.bbox_envelope)::bigint \
         FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        bbox_srid, 0,
        "bbox_envelope SRID should be 0 (got {bbox_srid})"
    );

    // 4. Pushdown query works: a tight bbox in Cartesian space hits
    //    our single chunk. PostGIS `&&` requires matching SRIDs —
    //    if fetch_impl were still hardcoded to 4326 this would
    //    return zero rows.
    let bbox = "POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))";
    let total = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         )",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        total,
        (N_LAT * N_LON) as i64,
        "SRID-0 bbox query should return every cell in the chunk"
    );

    // 5. Specific cell sanity: value[j=1][i=1] = j*N_LON + i = 5.
    //    (The fixture writes "lat" = y_start..y_start+N_LAT, "lon" =
    //    x_start..x_start+N_LON; we use it as cartesian here.)
    let v = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         ) WHERE lat = 1.0 AND lon = 1.0",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert!((v - 5.0).abs() < 1e-6, "cell (1,1) should be 5, got {v}");

    cleanup_dataset(DATASET);
}
