//! Phase C round-trip: register two scalar Zarr variables (`u`, `v`),
//! declare a composite `wind = [u, v]` via
//! `pgx.register_variable(..., components := ARRAY['u','v'])`, and
//! query it with `pgx.fetch_vec` — get back `values float8[]` rows
//! with the components in declared order.
//!
//! The fixture writes two separate 3×4 Zarr stores under the same
//! root, one per component, so we exercise the real "components live
//! as independent scalar variables" model.

mod common;

use crate::common::{
    cleanup_dataset, execute, query_one_f64, query_one_i64, write_synthetic_zarr_2d, TempStore,
};

const N_LAT: usize = 3;
const N_LON: usize = 4;
const LAT_START: f32 = 50.0;
const LON_START: f32 = 0.0;

const DATASET: &str = "e2e-vector-wind";

#[test]
fn vector_components_zip_into_values_array() {
    skip_if_not_running!();

    // Two Zarr stores in the same tempdir — one for u, one for v. They
    // share the lat/lon grid so cells line up perfectly when zipped.
    let store = TempStore::new("e2e_vector_wind");
    let u_root = store.root.join("u");
    let v_root = store.root.join("v");
    std::fs::create_dir_all(&u_root).unwrap();
    std::fs::create_dir_all(&v_root).unwrap();

    // u: values[j][i] = j*N_LON + i      → 0..11
    // v: values[j][i] = -(j*N_LON + i)   → 0..-11 (sign-flip so we
    //    can tell components apart in fetch_vec output)
    write_synthetic_zarr_2d(&u_root, "u", N_LAT, N_LON, LAT_START, LON_START);
    write_v_zarr(&v_root, "v", N_LAT, N_LON, LAT_START, LON_START);

    cleanup_dataset(DATASET);

    let u_uri = format!("fs://{}", u_root.display());
    let v_uri = format!("fs://{}", v_root.display());

    // Register both scalars via register_file (auto-creates the
    // dataset + each scalar variable, populates bbox).
    execute(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 'u', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )",
        ds = DATASET,
        uri = u_uri.replace('\'', "''"),
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 'v', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, true\
         )",
        ds = DATASET,
        uri = v_uri.replace('\'', "''"),
    ))
    .unwrap();

    // Now declare the composite. components := ARRAY['u','v'] →
    // pgx.variable_components gains two rows pointing at the scalar
    // variable rows we just registered.
    execute(&format!(
        "SELECT pgx.register_variable(\
            '{ds}', 'wind', \
            NULL, NULL, NULL, NULL, NULL, NULL, \
            ARRAY['u','v']::text[]\
         )",
        ds = DATASET,
    ))
    .unwrap();

    // ---- 1. Catalog wiring is right. ----
    let n_components = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.variable_components vc \
         JOIN pgx.variables v ON v.id = vc.composite_variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 'wind'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(n_components, 2, "wind should have 2 components");

    // ---- 2. fetch_vec returns the right row count + value pairs. ----
    let bbox = "POLYGON((-1 49, 5 49, 5 53, -1 53, -1 49))";
    let total = query_one_i64(&format!(
        "SELECT count(*)::bigint FROM pgx.fetch_vec(\
            '{ds}', 'wind', NULL, '{wkt}'\
         )",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(
        total,
        (N_LAT * N_LON) as i64,
        "fetch_vec should return one row per coord shared by u + v"
    );

    // ---- 3. The cell at (lat=51, lon=2): u=1*4+2=6, v=-6. ----
    let vx = query_one_f64(&format!(
        "SELECT values[1]::float8 FROM pgx.fetch_vec(\
            '{ds}', 'wind', NULL, '{wkt}'\
         ) WHERE lat = 51.0 AND lon = 2.0",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert!((vx - 6.0).abs() < 1e-6, "u (values[1]) at (51,2): {vx}");

    let vy = query_one_f64(&format!(
        "SELECT values[2]::float8 FROM pgx.fetch_vec(\
            '{ds}', 'wind', NULL, '{wkt}'\
         ) WHERE lat = 51.0 AND lon = 2.0",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert!((vy - (-6.0)).abs() < 1e-6, "v (values[2]) at (51,2): {vy}");

    // ---- 4. array_length matches the components count. ----
    let len = query_one_i64(&format!(
        "SELECT array_length(values, 1)::bigint FROM pgx.fetch_vec(\
            '{ds}', 'wind', NULL, '{wkt}'\
         ) LIMIT 1",
        ds = DATASET,
        wkt = bbox,
    ))
    .unwrap()
    .unwrap();
    assert_eq!(len, 2, "values array length should equal component count");

    // ---- 5. fetch_vec on a scalar errors clearly. ----
    let err = execute(&format!(
        "SELECT count(*) FROM pgx.fetch_vec('{}', 'u', NULL, '{}')",
        DATASET, bbox,
    ));
    assert!(
        err.is_err(),
        "fetch_vec on a scalar variable should error, not return rows"
    );

    cleanup_dataset(DATASET);
}

/// Write a 2-D single-chunk Zarr v3 store like `write_synthetic_zarr_2d`
/// but with sign-flipped values, so we can tell components apart.
fn write_v_zarr(
    store_root: &std::path::Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    lat_start: f32,
    lon_start: f32,
) {
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_lat}, {n_lon}],
            "data_type":   "float32",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{n_lat}, {n_lon}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}],
            "dimension_names": ["latitude", "longitude"]
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    let mut bytes = Vec::with_capacity(n_lat * n_lon * 4);
    for j in 0..n_lat {
        for i in 0..n_lon {
            let v = -((j * n_lon + i) as f32);
            bytes.extend_from_slice(&v.to_le_bytes());
        }
    }
    let chunk_dir = var_dir.join("c").join("0");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();

    write_axis_helper(store_root, "latitude", n_lat, lat_start);
    write_axis_helper(store_root, "longitude", n_lon, lon_start);
}

fn write_axis_helper(store_root: &std::path::Path, name: &str, n: usize, start: f32) {
    let dir = store_root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n}],
            "data_type":   "float32",
            "chunk_grid":  {{"name":"regular","configuration":{{"chunk_shape":[{n}]}}}},
            "chunk_key_encoding": {{"name":"default","configuration":{{"separator":"/"}}}},
            "fill_value": 0,
            "codecs": [{{"name":"bytes","configuration":{{"endian":"little"}}}}]
        }}"#
    );
    std::fs::write(dir.join("zarr.json"), meta).unwrap();
    let mut bytes = Vec::with_capacity(n * 4);
    for k in 0..n {
        let v = start + k as f32;
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    let chunk_dir = dir.join("c");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();
}
