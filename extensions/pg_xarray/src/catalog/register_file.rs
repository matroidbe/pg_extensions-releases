//! `pgx.register_file` — open a file URI, walk its header, register one
//! catalog chunk per file-chunk in a single SQL call.
//!
//! This is the no-pg_streaming-pipeline path. The bbox is computed by
//! `header::enumerate_zarr_chunks`, so GIST pruning works for subsequent
//! `pgx.fetch` calls — unlike `register_chunk` invoked with NULL bbox.

use crate::catalog::crud::{register_dataset_impl, register_variable_impl, VariableCfExtras};
use crate::header;
use pgrx::prelude::*;
use pgx_zarr_walker::{DimensionMapping, VariableMeta};
use std::sync::OnceLock;
use tokio::runtime::Runtime;

pub fn register_file_impl(
    dataset: &str,
    variable: &str,
    uri: &str,
    format: &str,
    lat_axis: Option<&str>,
    lon_axis: Option<&str>,
    time_axis: Option<&str>,
    z_axis: Option<&str>,
    srid: Option<i32>,
    auto_create: bool,
) -> i64 {
    if dataset.is_empty() || variable.is_empty() || uri.is_empty() {
        pgrx::error!("register_file: dataset, variable, and uri are required");
    }

    if auto_create {
        // Idempotent: register_dataset_impl is the only one we need
        // up front. The variable upsert is deferred to the per-walk
        // loop below so we can populate units/standard_name/dtype/CF
        // packing from the file's own metadata in one pass.
        //
        // When `srid` is supplied, propagate it to `default_srid` so
        // chunks registered against this (possibly fresh) dataset get
        // the right SRID even if `register_variable_impl` doesn't
        // override it.
        register_dataset_impl(dataset, format, None, None, srid);
    }

    let dims = DimensionMapping {
        lat_axis: lat_axis.map(String::from),
        lon_axis: lon_axis.map(String::from),
        time_axis: time_axis.map(String::from),
        z_axis: z_axis.map(String::from),
    };

    // SELAFIN has its own register path because the walker also
    // populates mesh_nodes + mesh_cells — handle it before falling
    // through to the variable-only formats below.
    if matches!(format, "selafin" | "slf") {
        return register_selafin(dataset, variable, uri, srid, auto_create);
    }

    let walks = match format {
        "zarr" => walk_zarr(uri, variable, &dims),
        "netcdf" | "nc" => walk_netcdf_one(uri, variable, &dims),
        "grib" | "grib2" => walk_grib_one(uri, variable, &dims),
        other => pgrx::error!(
            "register_file: format '{}' not supported (zarr, netcdf, grib2, selafin for now)",
            other
        ),
    };

    let mut count: i64 = 0;
    for walk in walks {
        // Upsert the variable row with everything the file told us.
        // ON CONFLICT(dataset_id, name) COALESCEs, so re-registering
        // the same file is idempotent and an explicit prior
        // `pgx.register_variable` call wins over auto-populated NULLs.
        if auto_create {
            upsert_variable_from_meta(dataset, variable, &walk.meta, srid);
        }
        for c in walk.chunks {
            // Format times as RFC 3339 and let the SQL CAST handle the
            // chrono → timestamptz conversion.
            let time_from = c.time_from.map(|dt| dt.to_rfc3339());
            let time_to = c.time_to.map(|dt| dt.to_rfc3339());
            let (z_min, z_max) = match c.z_range {
                Some((lo, hi)) => (Some(lo), Some(hi)),
                None => (None, None),
            };
            insert_chunk_via_spi(
                dataset,
                variable,
                &c.uri,
                time_from.as_deref(),
                time_to.as_deref(),
                c.bbox_wkt.as_deref(),
                c.byte_offset,
                c.byte_length,
                Some(c.chunk_key.as_str()),
                z_min,
                z_max,
            );
            count += 1;
        }
    }
    count
}

/// Mirrors the SQL `xarray_index` sink uses — does the text→timestamptz
/// cast inline so we don't need to fight pgrx's epoch conventions.
fn insert_chunk_via_spi(
    dataset: &str,
    variable: &str,
    uri: &str,
    time_from: Option<&str>,
    time_to: Option<&str>,
    bbox_wkt: Option<&str>,
    byte_offset: Option<i64>,
    byte_length: Option<i64>,
    chunk_key: Option<&str>,
    level_from: Option<f64>,
    level_to: Option<f64>,
) {
    let level_from_num: Option<pgrx::AnyNumeric> =
        level_from.and_then(|v| pgrx::AnyNumeric::try_from(v).ok());
    let level_to_num: Option<pgrx::AnyNumeric> =
        level_to.and_then(|v| pgrx::AnyNumeric::try_from(v).ok());
    let sql = "
        SELECT pgx.register_chunk(
            $1::text, $2::text, $3::text,
            CASE WHEN $4::text IS NULL THEN NULL ELSE $4::text::timestamptz END,
            CASE WHEN $5::text IS NULL THEN NULL ELSE $5::text::timestamptz END,
            $6::text,
            $7::bigint, $8::bigint, $9::text,
            NULL, NULL,
            $10::float8, $11::float8
        )
    ";
    Spi::run_with_args(
        sql,
        &[
            dataset.into(),
            variable.into(),
            uri.into(),
            time_from.into(),
            time_to.into(),
            bbox_wkt.into(),
            byte_offset.into(),
            byte_length.into(),
            chunk_key.into(),
            level_from_num.into(),
            level_to_num.into(),
        ],
    )
    .unwrap_or_else(|e| pgrx::error!("register_file: register_chunk SPI failed: {}", e));
}

/// Turn a `VariableMeta` from the walker into the typed columns the
/// catalog wants, plus a JSONB blob of residual attributes. Any field
/// the file didn't carry stays NULL — the `ON CONFLICT ... COALESCE`
/// in `register_variable_impl` keeps prior non-NULL values.
fn upsert_variable_from_meta(
    dataset: &str,
    variable: &str,
    meta: &VariableMeta,
    srid: Option<i32>,
) {
    let cf = VariableCfExtras {
        long_name: meta.long_name.clone(),
        scale_factor: meta.packing.as_ref().map(|p| p.scale),
        add_offset: meta.packing.as_ref().map(|p| p.offset),
        fill_value: meta.packing.as_ref().and_then(|p| p.fill_value),
        valid_min: meta.valid_min,
        valid_max: meta.valid_max,
    };
    // Dim order: `Vec<Option<String>>` from the walker → Vec<String>
    // for the catalog. Unnamed dims (None) become empty strings —
    // dim names are diagnostic, not load-bearing.
    let dim_order: Option<Vec<String>> = if meta.dim_order.is_empty() {
        None
    } else {
        Some(
            meta.dim_order
                .iter()
                .map(|d| d.clone().unwrap_or_default())
                .collect(),
        )
    };
    // Stash unknown attributes as JSONB so nothing is lost.
    let metadata = if meta.raw_attrs.is_object()
        && !meta
            .raw_attrs
            .as_object()
            .map(|o| o.is_empty())
            .unwrap_or(true)
    {
        Some(pgrx::JsonB(meta.raw_attrs.clone()))
    } else {
        None
    };
    register_variable_impl(
        dataset,
        variable,
        meta.standard_name.as_deref(),
        meta.units.as_deref(),
        meta.dtype.as_deref(),
        dim_order,
        metadata,
        &cf,
        srid,
        None, // register_file always upserts scalar variables; the
              // composite is a separate `pgx.register_variable` call.
    );
}

fn walk_zarr(
    uri: &str,
    variable: &str,
    dims: &DimensionMapping,
) -> Vec<pgx_zarr_walker::VariableWalk> {
    let rt = runtime();
    let vars = vec![variable.to_string()];
    match rt.block_on(header::enumerate_zarr_chunks(uri, &vars, dims)) {
        Ok(walks) => walks,
        Err(e) => pgrx::error!("register_file: {}", e),
    }
}

/// NetCDF walker — V1 emits one VariableWalk per call (whole-variable
/// chunk). Sync; the netcdf crate is blocking.
fn walk_netcdf_one(
    uri: &str,
    variable: &str,
    dims: &DimensionMapping,
) -> Vec<pgx_zarr_walker::VariableWalk> {
    match crate::reader::netcdf::walk_netcdf(uri, variable, dims) {
        Ok(walk) => vec![walk],
        Err(e) => pgrx::error!("register_file: {}", e),
    }
}

/// GRIB2 walker — scans the file for messages matching `variable`,
/// emits one VariableWalk with one ChunkRecord per match. Each chunk
/// row's byte_offset/byte_length points at the message slab so SRF
/// reads are a single range fetch.
///
/// The walker uses OpenDAL, so `uri` can be `fs:///path/to.grib2`,
/// `https://...`, `s3://...`, `gs://...` — any backend OpenDAL is
/// linked against. For HTTPS / S3 the one-time scan does a single
/// GET for the whole file (or a series of range GETs depending on
/// backend); subsequent pgx.fetch calls only range-read the matched
/// messages.
fn walk_grib_one(
    uri: &str,
    variable: &str,
    dims: &DimensionMapping,
) -> Vec<pgx_zarr_walker::VariableWalk> {
    let rt = runtime();
    match rt.block_on(crate::reader::grib::walk_grib(uri, variable, dims)) {
        Ok(walk) => vec![walk],
        Err(e) => pgrx::error!("register_file: {}", e),
    }
}

/// SELAFIN registration path — parses the file's mesh + variables in
/// one pass, then:
///   1. auto-creates the dataset's mesh (kind = `ugrid_triangle`,
///      motion = `fixed`) on first call.
///   2. finds-or-creates a mesh_version covering the file's time span.
///   3. upserts every node + cell into `pgx.mesh_nodes` /
///      `pgx.mesh_cells` (idempotent on subsequent calls — same
///      file → same node IDs → upsert hits the existing rows).
///   4. registers one chunk per (variable, timestep) with byte_offset
///      + byte_length pointing at the SELAFIN data record.
///
/// Returns total chunk rows registered. SELAFIN files almost always
/// contain multiple variables; a second register_file call with a
/// different variable on the same file skips steps 1–3 cheaply via
/// the existing upsert / find-or-create paths.
fn register_selafin(
    dataset: &str,
    variable: &str,
    uri: &str,
    srid: Option<i32>,
    auto_create: bool,
) -> i64 {
    let walk = match crate::reader::selafin::walk_selafin(uri, variable) {
        Ok(w) => w,
        Err(e) => pgrx::error!("register_file: {}", e),
    };

    // 1) mesh (upsert on dataset_id) + 2) mesh_version (find-or-create).
    if auto_create {
        let n_nodes = walk.nodes.len() as i64;
        let n_cells = walk.cells.len() as i64;
        crate::catalog::crud::register_mesh_impl(
            dataset,
            "ugrid_triangle",
            "fixed",
            srid,
            Some(&walk.mesh_extent_wkt),
            Some(n_nodes),
            Some(n_cells),
            None,
        );
    }

    // mesh_version: re-use any existing one for this mesh; else create.
    // SELAFIN meshes are fixed → one version per dataset, valid over
    // the file's full time span.
    let mesh_version_id =
        find_or_create_mesh_version(dataset, walk.time_from, walk.time_to, &walk.mesh_extent_wkt);

    // 3) Upsert nodes (point geometry) and cells (centroid + node IDs).
    for node in &walk.nodes {
        let geom_wkt = format!("POINT({} {})", node.x, node.y);
        crate::catalog::crud::register_mesh_node_impl(
            mesh_version_id,
            node.node_id,
            &geom_wkt,
            None,
        );
    }
    for cell in &walk.cells {
        let centroid_wkt = format!("POINT({} {})", cell.centroid_x, cell.centroid_y);
        crate::catalog::crud::register_mesh_cell_impl(
            mesh_version_id,
            cell.cell_id,
            cell.node_ids.clone(),
            &centroid_wkt,
            None,
        );
    }

    // 4) Upsert the variable + register one chunk per timestep.
    if auto_create {
        upsert_variable_from_meta(dataset, variable, &walk.variable_walk.meta, srid);
    }

    let mut count: i64 = 0;
    for c in &walk.variable_walk.chunks {
        let time_from = c.time_from.map(|dt| dt.to_rfc3339());
        let time_to = c.time_to.map(|dt| dt.to_rfc3339());
        insert_chunk_via_spi(
            dataset,
            variable,
            &c.uri,
            time_from.as_deref(),
            time_to.as_deref(),
            None, // unstructured: per-cell bbox is on mesh_cells.centroid
            c.byte_offset,
            c.byte_length,
            Some(c.chunk_key.as_str()),
            None,
            None,
        );
        // Stamp the chunk with the mesh_version_id so pgx.fetch_mesh
        // can JOIN through it. Done with a follow-up UPDATE since
        // `pgx.register_chunk` doesn't currently take a mesh_version_id
        // for the file-driven path.
        let sql = "UPDATE pgx.chunks c \
                   SET mesh_version_id = $1 \
                   FROM pgx.variables v JOIN pgx.datasets d ON d.id = v.dataset_id \
                   WHERE c.variable_id = v.id \
                     AND d.name = $2 AND v.name = $3 \
                     AND c.chunk_key = $4";
        Spi::run_with_args(
            sql,
            &[
                mesh_version_id.into(),
                dataset.into(),
                variable.into(),
                c.chunk_key.as_str().into(),
            ],
        )
        .unwrap_or_else(|e| pgrx::error!("register_file (selafin): stamp mesh_version_id: {}", e));
        count += 1;
    }
    count
}

/// Look up the first mesh_version for the dataset's mesh; create one
/// if absent. Returns the mesh_version_id.
fn find_or_create_mesh_version(
    dataset: &str,
    time_from: Option<chrono::DateTime<chrono::Utc>>,
    time_to: Option<chrono::DateTime<chrono::Utc>>,
    extent_wkt: &str,
) -> i64 {
    let sql = "SELECT mv.id \
               FROM pgx.mesh_versions mv \
               JOIN pgx.meshes m   ON m.id = mv.mesh_id \
               JOIN pgx.datasets d ON d.id = m.dataset_id \
               WHERE d.name = $1 \
               ORDER BY mv.id LIMIT 1";
    let existing: Option<i64> = Spi::get_one_with_args::<i64>(sql, &[dataset.into()])
        .ok()
        .flatten();
    if let Some(id) = existing {
        return id;
    }
    // No version yet — create one covering the file's time span. For
    // fixed meshes we use a wide outer bound (epoch ... +50yr) when
    // the file has no DATE record so the catalog `tstzrange &&` over
    // any query window still hits.
    let valid_from_str = time_from
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01 00:00:00+00".to_string());
    let valid_to_str = time_to
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| "2100-01-01 00:00:00+00".to_string());
    let sql = "
        WITH mesh AS (
            SELECT m.id, COALESCE(m.crs, d.default_srid, 4326) AS srid
            FROM pgx.meshes m JOIN pgx.datasets d ON d.id = m.dataset_id
            WHERE d.name = $1
        )
        INSERT INTO pgx.mesh_versions (mesh_id, valid_time, extent)
        VALUES (
            (SELECT id   FROM mesh),
            tstzrange($2::text::timestamptz, $3::text::timestamptz, '[]'),
            public.ST_GeomFromText($4, (SELECT srid FROM mesh))
        )
        RETURNING id
    ";
    Spi::get_one_with_args::<i64>(
        sql,
        &[
            dataset.into(),
            valid_from_str.as_str().into(),
            valid_to_str.as_str().into(),
            extent_wkt.into(),
        ],
    )
    .unwrap_or_else(|e| {
        pgrx::error!(
            "register_file (selafin): find_or_create_mesh_version SPI failed: {}",
            e
        )
    })
    .unwrap_or_else(|| pgrx::error!("register_file (selafin): mesh_version INSERT returned no id"))
}

/// Discover every array directly under a Zarr store root — one entry
/// per `<root>/<name>/zarr.json`. The caller decides whether to filter
/// to rank-≥2 data variables or look at the full list (rank-1 = coord
/// axes). Blocks on the shared tokio runtime so SPI callers can stay
/// synchronous.
pub fn list_zarr_store(uri: &str) -> Vec<pgx_zarr_walker::StoreVariable> {
    let rt = runtime();
    match rt.block_on(pgx_zarr_walker::list_store_variables(uri)) {
        Ok(vars) => vars,
        Err(e) => pgrx::error!("list_zarr_store: {}", e),
    }
}

/// Bulk register every data variable (rank ≥ 2) found at a Zarr store
/// root. Calls [`register_file_impl`] under the hood for each — same
/// catalog rows, same per-chunk bbox, same dim auto-detection.
///
/// Returns `(n_variables_registered, total_chunks)`. Coord-axis rank-1
/// arrays at the root are skipped.
pub fn register_zarr_store_impl(
    dataset: &str,
    uri: &str,
    lat_axis: Option<&str>,
    lon_axis: Option<&str>,
    time_axis: Option<&str>,
    z_axis: Option<&str>,
    srid: Option<i32>,
) -> (i64, i64) {
    if dataset.is_empty() || uri.is_empty() {
        pgrx::error!("register_zarr_store: dataset and uri are required");
    }
    let store_vars = list_zarr_store(uri);
    let mut n_vars: i64 = 0;
    let mut total_chunks: i64 = 0;
    for sv in store_vars {
        if !sv.is_data_variable() {
            continue;
        }
        let chunks = register_file_impl(
            dataset, &sv.name, uri, "zarr", lat_axis, lon_axis, time_axis, z_axis, srid, true,
        );
        n_vars += 1;
        total_chunks += chunks;
    }
    (n_vars, total_chunks)
}

fn runtime() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .thread_name("pgx_register_file_rt")
            .build()
            .expect("register_file: failed to build tokio runtime")
    })
}
