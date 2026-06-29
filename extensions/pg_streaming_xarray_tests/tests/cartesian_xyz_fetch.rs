//! Phase D round-trip: a Cartesian (SRID 0) dataset registered with
//! the `x_axis`/`y_axis` aliases (instead of `lat_axis`/`lon_axis`),
//! queried via `pgx.fetch_xyz` returning `(x, y, z, time, value)`
//! columns, and a vector flavour via `pgx.fetch_xyz_vec`.
//!
//! The walker still reads the same underlying coord arrays — Phase D
//! is purely a semantic relabel at register-time and SRF boundary —
//! so this test passes today by virtue of Phase A (SRID 0 catalog)
//! + Phase B (Z indexing) + Phase C (composite vars).

mod common;

use crate::common::{
    cleanup_dataset, execute, query_one_f64, query_one_i64, query_one_string,
    write_synthetic_zarr_2d, TempStore,
};

const N_Y: usize = 3;
const N_X: usize = 4;

const DATASET: &str = "e2e-xyz-fetch";

#[test]
fn fetch_xyz_renames_columns_and_fetch_xyz_vec_works() {
    skip_if_not_running!();

    // Two SRID-0 Zarr stores, one per component — simulates a CFD-style
    // (vx, vy) velocity field in model coords. Coordinates are small
    // Cartesian (x ∈ 0..3, y ∈ 0..2) so PostGIS's geographic
    // longitude normalisation doesn't trigger.
    let store = TempStore::new("e2e_xyz_fetch");
    let vx_root = store.root.join("vx");
    let vy_root = store.root.join("vy");
    std::fs::create_dir_all(&vx_root).unwrap();
    std::fs::create_dir_all(&vy_root).unwrap();
    write_synthetic_zarr_2d(&vx_root, "vx", N_Y, N_X, 0.0, 0.0);
    write_vy_zarr(&vy_root, "vy", N_Y, N_X, 0.0, 0.0);

    cleanup_dataset(DATASET);

    let vx_uri = format!("fs://{}", vx_root.display());
    let vy_uri = format!("fs://{}", vy_root.display());

    // Register vx using the x_axis / y_axis aliases — these resolve
    // to the same internal effective_lon / effective_lat the walker
    // uses, but read clearer for Cartesian data.
    execute(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 'vx', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, 0, \
            'longitude', 'latitude', \
            true\
         )",
        ds = DATASET,
        uri = vx_uri.replace('\'', "''"),
    ))
    .unwrap();
    execute(&format!(
        "SELECT pgx.register_file(\
            '{ds}', 'vy', '{uri}', 'zarr', \
            NULL, NULL, NULL, NULL, 0, \
            'longitude', 'latitude', \
            true\
         )",
        ds = DATASET,
        uri = vy_uri.replace('\'', "''"),
    ))
    .unwrap();

    // Declare the composite as in Phase C.
    execute(&format!(
        "SELECT pgx.register_variable(\
            '{ds}', 'velocity', \
            NULL, NULL, NULL, NULL, NULL, NULL, \
            ARRAY['vx','vy']::text[]\
         )",
        ds = DATASET,
    ))
    .unwrap();

    // ---- 1. fetch_xyz: column names should be (x, y, z, time, value). ----
    //
    // The catalog stores (lat, lon, level) internally; fetch_xyz just
    // relabels at the boundary. attname check via information_schema
    // would require a temp table, so we instead create a view from the
    // SRF and inspect column names — psql-style.
    execute(&format!(
        "DROP VIEW IF EXISTS xyz_view; \
         CREATE VIEW xyz_view AS \
         SELECT * FROM pgx.fetch_xyz(\
             '{ds}', 'vx', NULL, \
             'POLYGON((-1 -1, 4 -1, 4 3, -1 3, -1 -1))'\
         )",
        ds = DATASET,
    ))
    .unwrap();
    let cols = query_one_string(
        "SELECT string_agg(column_name::text, ',' ORDER BY ordinal_position) \
         FROM information_schema.columns \
         WHERE table_name = 'xyz_view'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        cols, "x,y,z,time,value",
        "fetch_xyz should expose (x, y, z, time, value) — got '{cols}'"
    );

    // ---- 2. Cell at (x=1, y=1): vx = j*N_X + i = 5. ----
    let v = query_one_f64(&format!(
        "SELECT value FROM pgx.fetch_xyz(\
            '{ds}', 'vx', NULL, \
            'POLYGON((-1 -1, 4 -1, 4 3, -1 3, -1 -1))'\
         ) WHERE x = 1.0 AND y = 1.0",
        ds = DATASET,
    ))
    .unwrap()
    .unwrap();
    assert!((v - 5.0).abs() < 1e-6, "vx at (1,1) should be 5: {v}");

    // ---- 3. fetch_xyz_vec: columns are (x, y, z, time, values). ----
    execute(
        "DROP VIEW IF EXISTS xyz_vec_view; \
         CREATE VIEW xyz_vec_view AS \
         SELECT * FROM pgx.fetch_xyz_vec(\
             'e2e-xyz-fetch', 'velocity', NULL, \
             'POLYGON((-1 -1, 4 -1, 4 3, -1 3, -1 -1))'\
         )",
    )
    .unwrap();
    let cols_vec = query_one_string(
        "SELECT string_agg(column_name::text, ',' ORDER BY ordinal_position) \
         FROM information_schema.columns \
         WHERE table_name = 'xyz_vec_view'",
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        cols_vec, "x,y,z,time,values",
        "fetch_xyz_vec should expose (x, y, z, time, values) — got '{cols_vec}'"
    );

    // ---- 4. Composite query: at (x=1, y=1) → vx=5, vy=-5. ----
    let vxc = query_one_f64(&format!(
        "SELECT values[1]::float8 FROM pgx.fetch_xyz_vec(\
            '{ds}', 'velocity', NULL, \
            'POLYGON((-1 -1, 4 -1, 4 3, -1 3, -1 -1))'\
         ) WHERE x = 1.0 AND y = 1.0",
        ds = DATASET,
    ))
    .unwrap()
    .unwrap();
    assert!((vxc - 5.0).abs() < 1e-6, "values[1] (vx) at (1,1): {vxc}");
    let vyc = query_one_f64(&format!(
        "SELECT values[2]::float8 FROM pgx.fetch_xyz_vec(\
            '{ds}', 'velocity', NULL, \
            'POLYGON((-1 -1, 4 -1, 4 3, -1 3, -1 -1))'\
         ) WHERE x = 1.0 AND y = 1.0",
        ds = DATASET,
    ))
    .unwrap()
    .unwrap();
    assert!(
        (vyc - (-5.0)).abs() < 1e-6,
        "values[2] (vy) at (1,1): {vyc}"
    );

    // ---- 5. The SRID is still 0 (we used x_axis/y_axis + srid=0). ----
    let bbox_srid = query_one_i64(&format!(
        "SELECT ST_SRID(c.bbox_envelope)::bigint \
         FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets  d ON d.id = v.dataset_id \
         WHERE d.name = '{}' AND v.name = 'vx'",
        DATASET
    ))
    .unwrap()
    .unwrap();
    assert_eq!(bbox_srid, 0, "vx bbox should be SRID 0");

    let _ = execute("DROP VIEW IF EXISTS xyz_view");
    let _ = execute("DROP VIEW IF EXISTS xyz_vec_view");
    cleanup_dataset(DATASET);
}

fn write_vy_zarr(
    store_root: &std::path::Path,
    var: &str,
    n_y: usize,
    n_x: usize,
    y_start: f32,
    x_start: f32,
) {
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_y}, {n_x}],
            "data_type":   "float32",
            "chunk_grid":  {{"name":"regular","configuration":{{"chunk_shape":[{n_y}, {n_x}]}}}},
            "chunk_key_encoding": {{"name":"default","configuration":{{"separator":"/"}}}},
            "fill_value": 0,
            "codecs": [{{"name":"bytes","configuration":{{"endian":"little"}}}}],
            "dimension_names": ["latitude", "longitude"]
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    let mut bytes = Vec::with_capacity(n_y * n_x * 4);
    for j in 0..n_y {
        for i in 0..n_x {
            let v = -((j * n_x + i) as f32);
            bytes.extend_from_slice(&v.to_le_bytes());
        }
    }
    let chunk_dir = var_dir.join("c").join("0");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();

    write_axis_helper(store_root, "latitude", n_y, y_start);
    write_axis_helper(store_root, "longitude", n_x, x_start);
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
