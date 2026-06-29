//! End-to-end test: an int16 Zarr v3 store with CF-convention
//! `scale_factor` + `add_offset` + `_FillValue` (the most common
//! storage shape for real climate datasets like ERA5) round-trips
//! through `pgx.register_file` + `pgx.fetch` correctly.
//!
//! Asserts:
//!   1. `pgx.variables` row populated with the CF columns from the
//!      Zarr file's own attribute bag (`dtype`, `units`,
//!      `standard_name`, `long_name`, `scale_factor`, `add_offset`,
//!      `fill_value`).
//!   2. `pgx.fetch` returns PHYSICAL float values (stored × scale +
//!      offset), not the raw int16 bytes.
//!   3. The cell whose stored value equals `_FillValue` becomes
//!      `f64::NAN` — matches xarray / numpy convention.

mod common;

use crate::common::{
    cleanup_dataset, execute, query_one_f64, query_one_i64, query_one_string,
    write_int16_zarr_with_packing, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;
const LAT_START: f32 = 50.0;
const LON_START: f32 = 0.0;

const SCALE: f64 = 0.01;
const OFFSET: f64 = 10.0;
const FILL: i16 = -9999;

const DATASET: &str = "e2e-int16-cf";

#[test]
fn int16_with_scale_offset_fill_round_trips() {
    skip_if_not_running!();

    let store = TempStore::new("e2e_int16_cf");
    write_int16_zarr_with_packing(
        &store.root,
        "t2m",
        N_LAT,
        N_LON,
        LAT_START,
        LON_START,
        SCALE,
        OFFSET,
        FILL,
        "K",
        "air_temperature",
        "2-metre temperature",
    );

    cleanup_dataset(DATASET);

    let store_uri = format!("fs://{}", store.root.display());
    let count = query_one_i64(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 't2m', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )::bigint",
        ds = DATASET,
        uri = store_uri.replace('\'', "''"),
    ))
    .unwrap()
    .unwrap();
    assert_eq!(count, 1, "single-chunk store registers 1 chunk row");

    // ---- 1. Catalog columns populated from the file's attributes. ----
    let dtype = query_one_string(&format!(
        "SELECT v.dtype FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(dtype, "int16", "dtype came from the file's data_type");

    let units = query_one_string(&format!(
        "SELECT v.units FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(units, "K");

    let standard_name = query_one_string(&format!(
        "SELECT v.standard_name FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(standard_name, "air_temperature");

    let long_name = query_one_string(&format!(
        "SELECT v.long_name FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(long_name, "2-metre temperature");

    let scale = query_one_f64(&format!(
        "SELECT v.scale_factor FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert!(
        (scale - SCALE).abs() < 1e-12,
        "scale_factor stored: {scale}"
    );

    let offset = query_one_f64(&format!(
        "SELECT v.add_offset FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert!(
        (offset - OFFSET).abs() < 1e-12,
        "add_offset stored: {offset}"
    );

    let fill = query_one_f64(&format!(
        "SELECT v.fill_value FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 't2m'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert!(
        (fill - FILL as f64).abs() < 1e-9,
        "fill_value stored: {fill}"
    );

    // ---- 2. Physical decode through pgx.fetch. ----
    // Stored at (1, 1) = j*n_lon+i = 1*4+1 = 5 → physical = 5*0.01 + 10 = 10.05
    let bbox = "POLYGON((0.9 50.9, 1.1 50.9, 1.1 51.1, 0.9 51.1, 0.9 50.9))";
    let v = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         ) WHERE lat = 51.0 AND lon = 1.0",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert!(
        (v - 10.05).abs() < 1e-9,
        "expected physical 10.05 at (51,1), got {v}"
    );

    // ---- 3. Fill value → NaN. The cell at (lat=50, lon=0) was
    //         stored as -9999 (the sentinel). pgx.fetch should
    //         return NaN there. SQL's `value <> 'NaN'::float8` would
    //         exclude NaN from a normal filter; here we explicitly
    //         look for it.
    let fill_bbox = "POLYGON((-0.1 49.9, 0.1 49.9, 0.1 50.1, -0.1 50.1, -0.1 49.9))";
    let fill_count = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch(\
            '{ds}', 't2m', NULL, '{wkt}'\
         ) WHERE lat = 50.0 AND lon = 0.0 AND value = 'NaN'::float8",
        ds = DATASET,
        wkt = fill_bbox,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        fill_count, 1,
        "fill-valued cell at (50, 0) should decode to NaN exactly once"
    );

    // Belt-and-suspenders: a non-NaN sum across the whole window
    // includes the other 11 cells but NOT the NaN one. Stored values
    // 1..11 sum to 66; physical sum is 66*0.01 + 11*10 = 110.66.
    let sum = query_one_f64(&format!(
        "SELECT sum(value)::float8 FROM pgx.fetch(\
            '{ds}', 't2m', NULL, \
            'POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))'\
         ) WHERE value <> 'NaN'::float8",
        ds = DATASET
    ))
    .unwrap()
    .unwrap();
    assert!(
        (sum - 110.66).abs() < 1e-9,
        "non-NaN physical sum: {sum} (expected 110.66)"
    );

    // Cleanup.
    let _ = execute(&format!(
        "DELETE FROM pgx.datasets WHERE name = '{}'",
        DATASET
    ));
}
