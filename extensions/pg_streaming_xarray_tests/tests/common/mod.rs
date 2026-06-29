//! Test harness for the pg_streaming + pg_xarray + pg_streaming_xarray
//! integration tests.
//!
//! Connects to the pgrx-managed Postgres started by `test.sh` via
//! `tokio-postgres` (auto-commit), so background workers can observe
//! state written by the test (and vice versa).

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tokio_postgres::NoTls;

pub const PG_HOST: &str = "localhost";
pub fn pg_port() -> u16 {
    // test.sh sets PG_PORT explicitly. The default here matches
    // the 298xx range test.sh uses (29816 for PG 16) to avoid
    // shmem collisions with the canonical 288xx pgrx port.
    std::env::var("PG_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(29816)
}
pub fn pg_db() -> String {
    std::env::var("PG_DB").unwrap_or_else(|_| "pg_streaming_xarray".to_string())
}

fn runtime() -> Runtime {
    Runtime::new().expect("Failed to create tokio runtime")
}

pub fn is_pg_running() -> bool {
    runtime().block_on(async {
        let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), pg_db());
        tokio_postgres::connect(&conn_str, NoTls).await.is_ok()
    })
}

async fn connect() -> Result<
    (
        tokio_postgres::Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    String,
> {
    let conn_str = format!("host={} port={} dbname={}", PG_HOST, pg_port(), pg_db());
    tokio_postgres::connect(&conn_str, NoTls)
        .await
        .map_err(|e| format!("Failed to connect to PostgreSQL: {}", e))
}

pub fn execute(sql: &str) -> Result<(), String> {
    runtime().block_on(async {
        let (client, conn) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        client
            .batch_execute(sql)
            .await
            .map_err(|e| format!("SQL error in `{}`: {}", sql, e))
    })
}

pub fn query_one_i64(sql: &str) -> Result<Option<i64>, String> {
    runtime().block_on(async {
        let (client, conn) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error in `{}`: {}", sql, e))?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(rows[0].try_get::<_, i64>(0).ok())
    })
}

pub fn query_one_f64(sql: &str) -> Result<Option<f64>, String> {
    runtime().block_on(async {
        let (client, conn) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error in `{}`: {}", sql, e))?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(rows[0]
            .try_get::<_, f64>(0)
            .ok()
            .or_else(|| rows[0].try_get::<_, i64>(0).ok().map(|v| v as f64)))
    })
}

pub fn query_one_string(sql: &str) -> Result<Option<String>, String> {
    runtime().block_on(async {
        let (client, conn) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error in `{}`: {}", sql, e))?;
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(rows[0].try_get::<_, String>(0).ok())
    })
}

pub fn query_all_f64(sql: &str) -> Result<Vec<Vec<f64>>, String> {
    runtime().block_on(async {
        let (client, conn) = connect().await?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });
        let rows = client
            .query(sql, &[])
            .await
            .map_err(|e| format!("SQL error in `{}`: {}", sql, e))?;
        let mut out = Vec::new();
        for row in &rows {
            let mut cols = Vec::new();
            for i in 0..row.len() {
                let v = row
                    .try_get::<_, f64>(i)
                    .ok()
                    .or_else(|| row.try_get::<_, i64>(i).ok().map(|x| x as f64))
                    .unwrap_or(f64::NAN);
                cols.push(v);
            }
            out.push(cols);
        }
        Ok(out)
    })
}

/// Poll a numeric SQL expression until it satisfies `is_ok(value)`, or
/// fail after `timeout`. Returns the final accepted value.
pub fn wait_for_count<F: Fn(i64) -> bool>(
    description: &str,
    sql: &str,
    is_ok: F,
    timeout: Duration,
) -> Result<i64, String> {
    let start = Instant::now();
    let poll = Duration::from_millis(100);
    let mut last: i64 = -1;
    while start.elapsed() < timeout {
        if let Ok(Some(v)) = query_one_i64(sql) {
            last = v;
            if is_ok(v) {
                return Ok(v);
            }
        }
        std::thread::sleep(poll);
    }
    Err(format!(
        "Timeout waiting for `{}` ({}): last seen {} after {:?}",
        description, sql, last, timeout
    ))
}

/// Stop + drop a pipeline; ignore errors so this is safe in setup teardown.
pub fn cleanup_pipeline(name: &str) {
    let _ = execute(&format!("SELECT pgstreams.stop('{}')", name));
    let _ = execute(&format!("SELECT pgstreams.drop_pipeline('{}')", name));
}

/// Drop a pg_xarray dataset; cascade clears variables, mesh, chunks.
pub fn cleanup_dataset(name: &str) {
    let _ = execute(&format!(
        "DELETE FROM pgx.datasets WHERE name = '{}'",
        name.replace('\'', "''")
    ));
}

// =============================================================================
// Synthetic Zarr v3 store builder — used by end_to_end_zarr.rs
// =============================================================================

/// A tempdir guard that removes itself on drop. Uses /tmp and the
/// process pid + a monotonic nanosecond counter for uniqueness so
/// parallel test runs don't collide.
pub struct TempStore {
    pub root: PathBuf,
}

impl TempStore {
    pub fn new(prefix: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "{prefix}_{pid}_{nanos}",
            prefix = prefix,
            pid = std::process::id(),
            nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
        ));
        std::fs::create_dir_all(&root).expect("create tempdir");
        Self { root }
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Write a 2-D Zarr v3 store consisting of a single chunk + matching
/// latitude/longitude coordinate arrays. Returns the path to the store
/// root (the directory that contains `<var>/zarr.json`).
///
/// Layout produced (for `var="t2m"`, n_lat=3, n_lon=4):
/// ```text
/// store_root/
/// ├── t2m/
/// │   ├── zarr.json                    # shape=[3,4], chunk_shape=[3,4],
/// │   │                                # f32, bytes codec, little-endian
/// │   └── c/0/0                        # 48 bytes of f32 values
/// ├── latitude/
/// │   ├── zarr.json                    # shape=[3], chunk_shape=[3], f32
/// │   └── c/0                          # 12 bytes
/// └── longitude/
///     ├── zarr.json                    # shape=[4], chunk_shape=[4], f32
///     └── c/0                          # 16 bytes
/// ```
///
/// Cell values are deterministic: `values[j][i] = (j * n_lon + i) as f32`.
/// Lats start at `lat_start` and increment by 1.0; same for lons.
pub fn write_synthetic_zarr_2d(
    store_root: &Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    lat_start: f32,
    lon_start: f32,
) {
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();

    // zarr.json for the variable
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

    // Chunk bytes — deterministic increasing f32s.
    let mut bytes = Vec::with_capacity(n_lat * n_lon * 4);
    for j in 0..n_lat {
        for i in 0..n_lon {
            let v = (j * n_lon + i) as f32;
            bytes.extend_from_slice(&v.to_le_bytes());
        }
    }
    let chunk_dir = var_dir.join("c").join("0");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();

    // Coordinate arrays.
    write_axis(store_root, "latitude", n_lat, lat_start);
    write_axis(store_root, "longitude", n_lon, lon_start);
}

/// Write a 3-D Zarr v3 store with shape `[n_z, n_lat, n_lon]` and a
/// `z` coord array carrying user-supplied physical values (no CF
/// units required — Z is read raw). One chunk per Z slice so the
/// catalog gets one `level_range` per chunk and `pgx.fetch`'s
/// `level_from`/`level_to` args actually prune.
///
/// Cell layout (z=k, j=row, i=col):
///   value[k][j][i] = k * 1000 + j * n_lon + i   (deterministic)
pub fn write_zarr_with_z_axis(
    store_root: &Path,
    var: &str,
    z_values: &[f64],
    n_lat: usize,
    n_lon: usize,
    lat_start: f32,
    lon_start: f32,
) {
    let n_z = z_values.len();
    assert!(n_z > 0);
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_z}, {n_lat}, {n_lon}],
            "data_type":   "float32",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [1, {n_lat}, {n_lon}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}],
            "dimension_names": ["level", "latitude", "longitude"]
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    // One file per Z chunk.
    for k in 0..n_z {
        let mut bytes = Vec::with_capacity(n_lat * n_lon * 4);
        for j in 0..n_lat {
            for i in 0..n_lon {
                let v = (k * 1000 + j * n_lon + i) as f32;
                bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        let chunk_dir = var_dir.join("c").join(k.to_string()).join("0");
        std::fs::create_dir_all(&chunk_dir).unwrap();
        std::fs::write(chunk_dir.join("0"), &bytes).unwrap();
    }

    write_axis(store_root, "latitude", n_lat, lat_start);
    write_axis(store_root, "longitude", n_lon, lon_start);

    // Z axis: f64 values, no chunking (one chunk for the whole axis).
    let z_dir = store_root.join("level");
    std::fs::create_dir_all(&z_dir).unwrap();
    let z_meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_z}],
            "data_type":   "float64",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{n_z}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}]
        }}"#
    );
    std::fs::write(z_dir.join("zarr.json"), z_meta).unwrap();
    let mut z_bytes = Vec::with_capacity(n_z * 8);
    for &v in z_values {
        z_bytes.extend_from_slice(&v.to_le_bytes());
    }
    let z_chunk = z_dir.join("c");
    std::fs::create_dir_all(&z_chunk).unwrap();
    std::fs::write(z_chunk.join("0"), &z_bytes).unwrap();
}

/// Write a 3-D Zarr v3 store with shape `[n_time, n_lat, n_lon]` and
/// a time coord array carrying a CF-style `units` attribute (e.g.
/// `"hours since 2024-01-01 00:00:00"`). The data is split into chunks
/// along the time dim only (one chunk per time slice) so we can test
/// time-axis indexing in isolation.
///
/// `time_values` are interpreted as offsets in `time_units` from
/// `time_ref` (which must be one of the CF reference dates the walker
/// supports: ISO 8601 or `YYYY-MM-DD HH:MM:SS`).
pub fn write_zarr_with_time_axis(
    store_root: &Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    time_values: &[f64],
    time_units: &str,
    lat_start: f32,
    lon_start: f32,
) {
    let n_time = time_values.len();
    assert!(n_time > 0);
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();

    // Chunk shape [1, n_lat, n_lon] — one chunk per time slice.
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_time}, {n_lat}, {n_lon}],
            "data_type":   "float32",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [1, {n_lat}, {n_lon}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}],
            "dimension_names": ["valid_time", "latitude", "longitude"]
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    // One chunk per time slice. Cell values: value[t][j][i] = t*100 + j*10 + i.
    for t in 0..n_time {
        let mut bytes = Vec::with_capacity(n_lat * n_lon * 4);
        for j in 0..n_lat {
            for i in 0..n_lon {
                let v = (t * 100 + j * 10 + i) as f32;
                bytes.extend_from_slice(&v.to_le_bytes());
            }
        }
        let chunk_dir = var_dir.join("c").join(t.to_string()).join("0");
        std::fs::create_dir_all(&chunk_dir).unwrap();
        std::fs::write(chunk_dir.join("0"), &bytes).unwrap();
    }

    // Spatial coords.
    write_axis(store_root, "latitude", n_lat, lat_start);
    write_axis(store_root, "longitude", n_lon, lon_start);

    // Time coord — needs a "units" attribute so the walker can decode.
    write_time_axis(store_root, "valid_time", time_values, time_units);
}

fn write_time_axis(store_root: &Path, name: &str, values: &[f64], units: &str) {
    let dir = store_root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let n = values.len();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n}],
            "data_type":   "float64",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{n}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}],
            "attributes": {{"units": "{units}"}}
        }}"#
    );
    std::fs::write(dir.join("zarr.json"), meta).unwrap();
    let mut bytes = Vec::with_capacity(n * 8);
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    let chunk_dir = dir.join("c");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();
}

/// Write a 2-D Zarr v3 store that is split into multiple chunks along
/// both dims. `shape = [n_lat, n_lon]`, `chunk_shape = [chunk_lat,
/// chunk_lon]` — the chunk grid is `ceil(n_lat/chunk_lat) ×
/// ceil(n_lon/chunk_lon)` chunks. Each chunk file holds `chunk_lat ×
/// chunk_lon` f32 cells (last chunk along each dim may be short).
///
/// Cell values are still deterministic: `value[j][i] = j * n_lon + i`
/// in the global frame, so any chunk file's bytes are derivable from
/// its position in the grid.
pub fn write_multichunk_zarr_2d(
    store_root: &Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    chunk_lat: usize,
    chunk_lon: usize,
    lat_start: f32,
    lon_start: f32,
) {
    assert!(chunk_lat > 0 && chunk_lon > 0);
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
                "configuration": {{"chunk_shape": [{chunk_lat}, {chunk_lon}]}}
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

    let n_chunks_lat = n_lat.div_ceil(chunk_lat);
    let n_chunks_lon = n_lon.div_ceil(chunk_lon);
    for cj in 0..n_chunks_lat {
        for ci in 0..n_chunks_lon {
            let lat0 = cj * chunk_lat;
            let lon0 = ci * chunk_lon;
            let lat_end = (lat0 + chunk_lat).min(n_lat);
            let lon_end = (lon0 + chunk_lon).min(n_lon);
            let mut bytes = Vec::with_capacity((lat_end - lat0) * (lon_end - lon0) * 4);
            // Zarr v3 stores the FULL chunk_shape worth of cells per chunk,
            // padding short chunks with fill_value (0). Match that.
            for j in 0..chunk_lat {
                for i in 0..chunk_lon {
                    let gj = lat0 + j;
                    let gi = lon0 + i;
                    let v = if gj < n_lat && gi < n_lon {
                        (gj * n_lon + gi) as f32
                    } else {
                        0.0_f32
                    };
                    bytes.extend_from_slice(&v.to_le_bytes());
                }
            }
            let chunk_dir = var_dir.join("c").join(cj.to_string());
            std::fs::create_dir_all(&chunk_dir).unwrap();
            std::fs::write(chunk_dir.join(ci.to_string()), &bytes).unwrap();
        }
    }
    write_axis_chunked(store_root, "latitude", n_lat, chunk_lat, lat_start);
    write_axis_chunked(store_root, "longitude", n_lon, chunk_lon, lon_start);
}

fn write_axis_chunked(store_root: &Path, name: &str, n: usize, chunk_size: usize, start: f32) {
    let dir = store_root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n}],
            "data_type":   "float32",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{chunk_size}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}]
        }}"#
    );
    std::fs::write(dir.join("zarr.json"), meta).unwrap();
    let n_chunks = n.div_ceil(chunk_size);
    for ci in 0..n_chunks {
        let mut bytes = Vec::with_capacity(chunk_size * 4);
        for k in 0..chunk_size {
            let gk = ci * chunk_size + k;
            let v = if gk < n { start + gk as f32 } else { 0.0_f32 };
            bytes.extend_from_slice(&v.to_le_bytes());
        }
        let chunk_dir = dir.join("c");
        std::fs::create_dir_all(&chunk_dir).unwrap();
        std::fs::write(chunk_dir.join(ci.to_string()), &bytes).unwrap();
    }
}

/// Write a 2-D single-chunk Zarr v3 store whose chunk bytes are
/// compressed with the named codec (`"gzip"` or `"zstd"`). Used to
/// verify that the shared `pgx_zarr_walker::decode_chunk_values`
/// path walks the codec chain correctly.
///
/// The variable name + value scheme is the same as
/// `write_synthetic_zarr_2d`: `value[j][i] = j * n_lon + i`.
pub fn write_compressed_zarr_2d(
    store_root: &Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    lat_start: f32,
    lon_start: f32,
    codec: &str,
) {
    assert!(codec == "gzip" || codec == "zstd");
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
            "codecs": [
                {{"name": "bytes", "configuration": {{"endian": "little"}}}},
                {{"name": "{codec}", "configuration": {{}}}}
            ],
            "dimension_names": ["latitude", "longitude"]
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    // Raw little-endian f32 cells, then run through the codec.
    let mut raw = Vec::with_capacity(n_lat * n_lon * 4);
    for j in 0..n_lat {
        for i in 0..n_lon {
            let v = (j * n_lon + i) as f32;
            raw.extend_from_slice(&v.to_le_bytes());
        }
    }
    let compressed = match codec {
        "gzip" => {
            use std::io::Write;
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(&raw).unwrap();
            enc.finish().unwrap()
        }
        "zstd" => zstd::stream::encode_all(&raw[..], 3).unwrap(),
        _ => unreachable!(),
    };

    let chunk_dir = var_dir.join("c").join("0");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &compressed).unwrap();

    write_axis(store_root, "latitude", n_lat, lat_start);
    write_axis(store_root, "longitude", n_lon, lon_start);
}

/// Write a 2-D single-chunk Zarr v3 store whose `t2m` variable is
/// int16 with CF data packing: physical = stored * scale + offset,
/// fill_value = -9999 (mapped to NaN at decode). Also stamps the
/// variable's `attributes` with `units` / `standard_name` /
/// `long_name` so the metadata-surfacing path has something to read.
///
/// Stored cell layout (j=row, i=col):
///   stored[j][i] = j * n_lon + i   — small non-negative ints
/// One cell is forced to the fill sentinel so we can assert NaN
/// propagation:
///   stored[0][0] = -9999
///
/// Physical values for the rest: physical = stored * 0.01 + 10.0
///   stored 0..N → 10.0, 10.01, 10.02, ...
pub fn write_int16_zarr_with_packing(
    store_root: &Path,
    var: &str,
    n_lat: usize,
    n_lon: usize,
    lat_start: f32,
    lon_start: f32,
    scale_factor: f64,
    add_offset: f64,
    fill_value: i16,
    units: &str,
    standard_name: &str,
    long_name: &str,
) {
    let var_dir = store_root.join(var);
    std::fs::create_dir_all(&var_dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n_lat}, {n_lon}],
            "data_type":   "int16",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{n_lat}, {n_lon}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": {fill_value},
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}],
            "dimension_names": ["latitude", "longitude"],
            "attributes": {{
                "units": "{units}",
                "standard_name": "{standard_name}",
                "long_name": "{long_name}",
                "scale_factor": {scale_factor},
                "add_offset": {add_offset},
                "_FillValue": {fill_value}
            }}
        }}"#
    );
    std::fs::write(var_dir.join("zarr.json"), meta).unwrap();

    let mut bytes = Vec::with_capacity(n_lat * n_lon * 2);
    for j in 0..n_lat {
        for i in 0..n_lon {
            let stored: i16 = if j == 0 && i == 0 {
                fill_value
            } else {
                (j * n_lon + i) as i16
            };
            bytes.extend_from_slice(&stored.to_le_bytes());
        }
    }
    let chunk_dir = var_dir.join("c").join("0");
    std::fs::create_dir_all(&chunk_dir).unwrap();
    std::fs::write(chunk_dir.join("0"), &bytes).unwrap();

    write_axis(store_root, "latitude", n_lat, lat_start);
    write_axis(store_root, "longitude", n_lon, lon_start);
}

fn write_axis(store_root: &Path, name: &str, n: usize, start: f32) {
    let dir = store_root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    let meta = format!(
        r#"{{
            "zarr_format": 3,
            "node_type":   "array",
            "shape":       [{n}],
            "data_type":   "float32",
            "chunk_grid":  {{
                "name": "regular",
                "configuration": {{"chunk_shape": [{n}]}}
            }},
            "chunk_key_encoding": {{
                "name": "default",
                "configuration": {{"separator": "/"}}
            }},
            "fill_value": 0,
            "codecs": [{{"name": "bytes", "configuration": {{"endian": "little"}}}}]
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

/// Macro to skip a test cleanly when the PG instance isn't running
/// (e.g., devs invoking `cargo test` directly without `./test.sh`).
#[macro_export]
macro_rules! skip_if_not_running {
    () => {
        if !$crate::common::is_pg_running() {
            eprintln!(
                "SKIPPED: pg_streaming_xarray DB not running at {}:{} (run ./test.sh)",
                $crate::common::PG_HOST,
                $crate::common::pg_port()
            );
            return;
        }
    };
}
