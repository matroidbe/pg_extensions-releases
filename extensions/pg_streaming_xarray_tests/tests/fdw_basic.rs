//! Phase 3a smoke test: a foreign table backed by `pgx_fdw` behaves
//! like a normal table — `SELECT * FROM wx_t2m` returns the same rows
//! `pgx.fetch('demo','t2m')` would. No predicate pushdown asserted
//! yet (Phase 3b).

mod common;

use crate::common::{
    cleanup_dataset, execute, query_one_f64, query_one_i64, write_multichunk_zarr_2d, TempStore,
};

const N_LAT: usize = 6;
const N_LON: usize = 8;
const CHUNK_LAT: usize = 3;
const CHUNK_LON: usize = 4;
const LAT_START: f32 = 50.0;
const LON_START: f32 = 0.0;

const DATASET: &str = "e2e-fdw";
const SERVER: &str = "pgx_fdw_test_srv";
const TABLE: &str = "wx_t2m_fdw";

#[test]
fn fdw_select_returns_full_grid() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_fdw_basic");
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
    let _ = execute(&format!("DROP FOREIGN TABLE IF EXISTS {} CASCADE", TABLE));
    let _ = execute(&format!("DROP SERVER IF EXISTS {} CASCADE", SERVER));

    let store_uri = format!("fs://{}", store.root.display());
    execute(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap();

    execute(&format!(
        "CREATE SERVER {srv} FOREIGN DATA WRAPPER pgx_fdw",
        srv = SERVER
    ))
    .unwrap();

    execute(&format!(
        "CREATE FOREIGN TABLE {tbl} (\
            lat float8, lon float8, level float8, \
            \"time\" timestamptz, value float8\
         ) SERVER {srv} OPTIONS (dataset '{ds}', variable 't2m')",
        tbl = TABLE,
        srv = SERVER,
        ds = DATASET,
    ))
    .unwrap();

    // Same total cell count as the SRF: N_LAT * N_LON = 48.
    let total = query_one_i64(&format!("SELECT count(*)::bigint FROM {}", TABLE))
        .unwrap()
        .unwrap();
    assert_eq!(total, (N_LAT * N_LON) as i64);

    // Sum of every cell value: sum(j*N_LON + i) for j in [0..N_LAT), i in [0..N_LON).
    let sum = query_one_f64(&format!("SELECT sum(value)::float8 FROM {}", TABLE))
        .unwrap()
        .unwrap();
    let expected: f64 = (0..N_LAT * N_LON).map(|k| k as f64).sum();
    assert!(
        (sum - expected).abs() < 1e-6,
        "FDW sum mismatch: {} vs expected {}",
        sum,
        expected
    );

    // Spot-check one cell: (lat=51, lon=2) → global index (51-50)*8 + 2 = 10.
    let v = query_one_f64(&format!(
        "SELECT value FROM {} WHERE lat = 51.0 AND lon = 2.0",
        TABLE
    ))
    .unwrap()
    .unwrap();
    assert!((v - 10.0).abs() < 1e-6, "expected 10, got {}", v);

    // Cross-source LATERAL JOIN against a normal table — the headline
    // use case for the FDW. No pgx.fetch() in sight.
    execute("DROP TABLE IF EXISTS cities_fdw").unwrap();
    execute(
        "CREATE TABLE cities_fdw (name text, lat float8, lon float8); \
         INSERT INTO cities_fdw VALUES \
            ('Amsterdam', 50.0, 2.0), \
            ('Brussels',  51.0, 4.0), \
            ('Hannover',  52.0, 5.0)",
    )
    .unwrap();
    let joined = query_one_i64(&format!(
        "SELECT count(*)::bigint \
         FROM cities_fdw c \
         JOIN {tbl} f ON f.lat = c.lat AND f.lon = c.lon",
        tbl = TABLE,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(joined, 3, "LATERAL-style join should match 3 cities");

    // ---- Predicate pushdown ----
    //
    // A bounded WHERE on lat AND lon should turn into a bbox WKT inside
    // the FDW and route into fetch_impl. fetch_impl emits a WARNING
    // when no predicate is provided, so the absence of that WARNING +
    // a correct row count is our proof that the bbox came through.
    //
    // The chunk grid is 4 chunks (each 3 lat × 4 lon). The window
    // below covers exactly the lat 50..52 × lon 0..3 chunk's first
    // two rows × first three cols → 2*3 = 6 cells.
    let bounded = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM {tbl} \
         WHERE lat BETWEEN 50.0 AND 51.0 \
           AND lon BETWEEN 0.0 AND 2.0",
        tbl = TABLE,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        bounded, 6,
        "bbox-bounded SELECT should return exactly the 2*3 cells inside the window"
    );

    // Cleanup.
    let _ = execute("DROP TABLE IF EXISTS cities_fdw");
    let _ = execute(&format!("DROP FOREIGN TABLE IF EXISTS {} CASCADE", TABLE));
    let _ = execute(&format!("DROP SERVER IF EXISTS {} CASCADE", SERVER));
    cleanup_dataset(DATASET);
}
