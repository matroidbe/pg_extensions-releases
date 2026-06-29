//! pg_xarray — catalog + query layer for chunked scientific arrays
//!
//! See `design/pg_xarray/{README,indexing,integration}.md` for the
//! architecture. This crate implements:
//!
//! - **Catalog schema** in the `pgx` Postgres schema: `datasets`,
//!   `variables`, `meshes`, `mesh_versions`, `chunks` (this file's
//!   bootstrap SQL).
//! - **Catalog CRUD** SQL functions: `pgx.register_dataset`,
//!   `pgx.register_variable`, `pgx.register_mesh`,
//!   `pgx.register_mesh_version`, `pgx.register_chunk`, and `list_*`
//!   counterparts (`src/catalog/crud.rs`).
//! - **`ChunkReader` trait** + per-format readers
//!   (`src/reader/{mod,memory,zarr}.rs`).
//! - **`pgx.fetch` SRF** that ties the catalog to the readers
//!   (`src/srf/fetch.rs`).
//! - **`pgx_fdw` Foreign Data Wrapper** so users can
//!   `CREATE FOREIGN TABLE` over a dataset/variable pair and `SELECT`
//!   from it like a regular table, with `lat`/`lon`/`time`/`level`
//!   WHERE clauses pushed into the catalog (`src/fdw/mod.rs`).

#![allow(unexpected_cfgs)]
#![allow(clippy::too_many_arguments)]

use pgrx::prelude::*;

mod catalog;
mod fdw;
mod glb;
mod header;
mod raster;
mod reader;
mod server;
mod srf;

// Re-export the WMS bgworker entry point so its symbol lands in the
// .so's dynamic table — `BackgroundWorkerBuilder::set_function(...)`
// uses dlsym() to find it at startup.
pub use server::pg_xarray_wms_worker_main;

// =============================================================================
// SQL — Catalog CRUD
// =============================================================================

/// Register (or upsert) a dataset. Returns its id.
///
/// `default_srid` sets the spatial reference system for chunks /
/// meshes registered against this dataset when the variable doesn't
/// override it. Defaults to 4326 (WGS84 geographic). Pass `0` for
/// Cartesian / non-spatial-reference data (XYZ engineering coords,
/// simulation-space results), or any EPSG code for a custom CRS.
#[pg_extern]
fn register_dataset(
    name: &str,
    format: &str,
    conventions: default!(Option<Vec<String>>, "NULL"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
    default_srid: default!(Option<i32>, "NULL"),
) -> i64 {
    catalog::crud::register_dataset_impl(name, format, conventions, metadata, default_srid)
}

/// Register (or upsert) a variable on a dataset. Returns its id.
///
/// `srid` overrides the dataset's `default_srid` per-variable —
/// useful when one Zarr store mixes geographic and model-space
/// variables. `NULL` means "inherit dataset default".
///
/// `components` — when supplied, turns this row into a COMPOSITE
/// (vector / tensor) variable referencing existing scalar variables
/// of the same dataset, in the given order. Each name must already
/// be registered; `pgx.fetch_vec(dataset, name, ...)` then returns
/// `values float8[]` with the components in order.
#[pg_extern]
fn register_variable(
    dataset: &str,
    name: &str,
    standard_name: default!(Option<&str>, "NULL"),
    units: default!(Option<&str>, "NULL"),
    dtype: default!(Option<&str>, "NULL"),
    dim_order: default!(Option<Vec<String>>, "NULL"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
    srid: default!(Option<i32>, "NULL"),
    components: default!(Option<Vec<String>>, "NULL"),
) -> i64 {
    catalog::crud::register_variable_impl(
        dataset,
        name,
        standard_name,
        units,
        dtype,
        dim_order,
        metadata,
        &Default::default(),
        srid,
        components,
    )
}

/// Register (or upsert) a mesh on a dataset. Returns its id.
/// `kind` must be one of: regular_grid, curvilinear, ugrid_triangle,
/// ugrid_polygon, fem_tetra, fem_hex, voronoi, particle_cloud,
/// time_series, profile.
/// `motion` must be one of: fixed, versioned, deforming, lagrangian.
#[pg_extern]
fn register_mesh(
    dataset: &str,
    kind: &str,
    motion: default!(&str, "'fixed'"),
    crs: default!(Option<i32>, "NULL"),
    extent_wkt: default!(Option<&str>, "NULL"),
    node_count: default!(Option<i64>, "NULL"),
    cell_count: default!(Option<i64>, "NULL"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    catalog::crud::register_mesh_impl(
        dataset, kind, motion, crs, extent_wkt, node_count, cell_count, metadata,
    )
}

/// Register a mesh version. Only used for `versioned` or `deforming` meshes.
#[pg_extern]
fn register_mesh_version(
    dataset: &str,
    valid_from: pgrx::datum::TimestampWithTimeZone,
    valid_to: pgrx::datum::TimestampWithTimeZone,
    extent_wkt: &str,
    uri: default!(Option<&str>, "NULL"),
    byte_offset: default!(Option<i64>, "NULL"),
    byte_length: default!(Option<i64>, "NULL"),
    chunk_key: default!(Option<&str>, "NULL"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    catalog::crud::register_mesh_version_impl(
        dataset,
        valid_from,
        valid_to,
        extent_wkt,
        uri,
        byte_offset,
        byte_length,
        chunk_key,
        metadata,
    )
}

/// Register one node of an unstructured mesh. `geom_wkt` is parsed in
/// the mesh version's effective SRID. Returns the row id; idempotent
/// on `(mesh_version_id, node_id)`.
#[pg_extern]
fn register_mesh_node(
    mesh_version_id: i64,
    node_id: i64,
    geom_wkt: &str,
    attrs: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    catalog::crud::register_mesh_node_impl(mesh_version_id, node_id, geom_wkt, attrs)
}

/// Register one cell of an unstructured mesh. `node_ids` is the
/// ordered list of node ids bounding the cell. `centroid_wkt` is the
/// precomputed centroid in the mesh's effective SRID. Idempotent on
/// `(mesh_version_id, cell_id)`.
#[pg_extern]
fn register_mesh_cell(
    mesh_version_id: i64,
    cell_id: i64,
    node_ids: Vec<i64>,
    centroid_wkt: &str,
    attrs: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    catalog::crud::register_mesh_cell_impl(mesh_version_id, cell_id, node_ids, centroid_wkt, attrs)
}

/// Register a chunk in the catalog. Returns its id. Idempotent on
/// `(variable_id, time_range, uri, byte_offset)`.
///
/// `level_from`/`level_to` populate `pgx.chunks.level_range` (the
/// vertical / depth / altitude / Z extent). When both are NULL the
/// column stays NULL and `pgx.fetch`'s level filter doesn't prune.
#[pg_extern]
fn register_chunk(
    dataset: &str,
    variable: &str,
    uri: &str,
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    byte_offset: default!(Option<i64>, "NULL"),
    byte_length: default!(Option<i64>, "NULL"),
    chunk_key: default!(Option<&str>, "NULL"),
    mesh_version_id: default!(Option<i64>, "NULL"),
    metadata: default!(Option<pgrx::JsonB>, "NULL"),
    level_from: default!(Option<f64>, "NULL"),
    level_to: default!(Option<f64>, "NULL"),
) -> i64 {
    catalog::crud::register_chunk_impl(
        dataset,
        variable,
        uri,
        time_from,
        time_to,
        bbox_wkt,
        byte_offset,
        byte_length,
        chunk_key,
        mesh_version_id,
        metadata,
        level_from,
        level_to,
    )
}

/// Register every chunk of a file in one shot — the no-pipeline path.
///
/// Opens `uri` via OpenDAL, reads the file header (currently Zarr v3
/// only), enumerates its chunk grid, and for each chunk computes a
/// real `bbox_wkt` from the variable's lat/lon coord arrays — and a
/// real `time_range` when `time_axis` is given and the axis carries a
/// CF-style `"units": "<unit> since <date>"` attribute. Each chunk is
/// then upserted into `pgx.chunks` so the GIST + range indexes can
/// prune for subsequent `pgx.fetch` calls.
///
/// Axis mapping (all NULL by default — auto-detect from
/// `dimension_names` in zarr.json):
///   * `lat_axis`   — top-level coord group for latitude (e.g. `lat`,
///     `latitude`, `y`). When NULL, falls back to `dimension_names[rank-2]`.
///   * `lon_axis`   — same for longitude.
///   * `time_axis`  — when set, also computes per-chunk `time_from` /
///     `time_to`. When NULL, no temporal indexing is done (the
///     time_range column stays NULL — chunk is unprunable by time).
///
/// `srid` sets the spatial reference for the dataset's `default_srid`
/// (when auto-creating) and for the variable's `srid`. Defaults to
/// NULL → 4326 (WGS84) for the geographic case. Pass `0` for
/// Cartesian XYZ data (engineering/simulation coords) — bboxes are
/// then stored and queried in SRID 0 end-to-end.
///
/// `x_axis` / `y_axis` are aliases for `lon_axis` / `lat_axis`
/// (first/second horizontal coord-group names). For Cartesian /
/// non-geographic datasets the x/y/z spelling is clearer; under the
/// hood the walker reads the same coord arrays. When both an
/// x_axis and a lon_axis are supplied, x_axis wins; same for y/lat.
///
/// `auto_create` (default true): if the dataset/variable don't exist
/// yet, create them with the given `format` + `srid`. The "register
/// a Zarr store" UX is therefore a single SQL call.
///
/// Returns the number of chunk rows registered.
#[pg_extern]
fn register_file(
    dataset: &str,
    variable: &str,
    uri: &str,
    format: default!(&str, "'zarr'"),
    lat_axis: default!(Option<&str>, "NULL"),
    lon_axis: default!(Option<&str>, "NULL"),
    time_axis: default!(Option<&str>, "NULL"),
    z_axis: default!(Option<&str>, "NULL"),
    srid: default!(Option<i32>, "NULL"),
    x_axis: default!(Option<&str>, "NULL"),
    y_axis: default!(Option<&str>, "NULL"),
    auto_create: default!(bool, true),
) -> i64 {
    // x_axis wins over lon_axis (both mean "horizontal axis 1"); same
    // for y over lat. Keeping both spellings live lets a Cartesian
    // dataset call register_file with the spelling that matches the
    // file's dim names without forcing users of the geographic API
    // to switch.
    let effective_lon = x_axis.or(lon_axis);
    let effective_lat = y_axis.or(lat_axis);
    catalog::register_file::register_file_impl(
        dataset,
        variable,
        uri,
        format,
        effective_lat,
        effective_lon,
        time_axis,
        z_axis,
        srid,
        auto_create,
    )
}

/// Discover every array under a Zarr store root WITHOUT touching the
/// catalog. Returns one row per `<store>/<name>/zarr.json` so users can
/// see what's in a store before deciding what to register.
///
/// `is_data_variable` is `true` for rank-≥2 arrays (the variables you
/// typically want to fetch); rank-1 entries are coord axes (lat / lon /
/// level / time) — they're still listed because callers occasionally
/// want to introspect them.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_zarr_variables(
    uri: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(shape, Vec<i64>),
        name!(dimension_names, Vec<String>),
        name!(dtype, String),
        name!(is_data_variable, bool),
    ),
> {
    let store_vars = catalog::register_file::list_zarr_store(uri);
    let rows: Vec<_> = store_vars
        .into_iter()
        .map(|sv| {
            let shape: Vec<i64> = sv.shape.iter().map(|&n| n as i64).collect();
            let dim_names: Vec<String> = sv
                .dimension_names
                .iter()
                .map(|d| d.clone().unwrap_or_default())
                .collect();
            let is_data = sv.is_data_variable();
            (sv.name, shape, dim_names, sv.data_type, is_data)
        })
        .collect();
    TableIterator::new(rows)
}

/// Bulk-register every data variable (rank ≥ 2) under a Zarr store
/// root. One SPI round-trip per variable; idempotent — re-running
/// against the same store updates existing rows.
///
/// Returns `(n_variables, n_chunks)` so callers can sanity-check what
/// landed in the catalog. Coord-axis arrays (rank 1) are skipped —
/// they're metadata for the data variables, not data themselves.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn register_zarr_store(
    dataset: &str,
    uri: &str,
    lat_axis: default!(Option<&str>, "NULL"),
    lon_axis: default!(Option<&str>, "NULL"),
    time_axis: default!(Option<&str>, "NULL"),
    z_axis: default!(Option<&str>, "NULL"),
    srid: default!(Option<i32>, "NULL"),
    x_axis: default!(Option<&str>, "NULL"),
    y_axis: default!(Option<&str>, "NULL"),
) -> TableIterator<'static, (name!(n_variables, i64), name!(n_chunks, i64))> {
    let effective_lon = x_axis.or(lon_axis);
    let effective_lat = y_axis.or(lat_axis);
    let (n_vars, n_chunks) = catalog::register_file::register_zarr_store_impl(
        dataset,
        uri,
        effective_lat,
        effective_lon,
        time_axis,
        z_axis,
        srid,
    );
    TableIterator::new(vec![(n_vars, n_chunks)])
}

// =============================================================================
// SQL — Query (SRF)
// =============================================================================

/// Fetch cells matching a dataset/variable/(time, bbox, level) predicate.
/// Picks chunks from the catalog, dispatches to the format reader, returns
/// rows. Defensive cap `max_cells` (default 1,000,000) prevents accidental
/// whole-grid blowouts.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn fetch(
    dataset: &str,
    variable: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    level_from: default!(Option<f64>, "NULL"),
    level_to: default!(Option<f64>, "NULL"),
    max_cells: default!(i32, 1000000),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(lat, Option<f64>),
        name!(lon, Option<f64>),
        name!(level, Option<f64>),
        name!(time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(value, f64),
    ),
> {
    TableIterator::new(srf::fetch::fetch_impl(
        dataset, variable, at_time, bbox_wkt, level_from, level_to, max_cells, time_from, time_to,
    ))
}

/// Cartesian / non-geographic flavour of `pgx.fetch`. Same row data,
/// but the coord columns are named `(x, y, z, time, value)` instead
/// of `(lat, lon, level, time, value)` — semantically clear for
/// engineering / simulation / model-space datasets where the
/// horizontal axes aren't latitude / longitude.
///
/// Under the hood: identical to `pgx.fetch`. The catalog stores
/// "first horizontal" + "second horizontal" coords regardless of
/// SRS; this SRF just gives them clearer names at the boundary.
/// Use `pgx.fetch` for geographic data, this for everything else.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn fetch_xyz(
    dataset: &str,
    variable: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    z_from: default!(Option<f64>, "NULL"),
    z_to: default!(Option<f64>, "NULL"),
    max_cells: default!(i32, 1000000),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(x, Option<f64>),
        name!(y, Option<f64>),
        name!(z, Option<f64>),
        name!(time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(value, f64),
    ),
> {
    TableIterator::new(srf::fetch::fetch_impl(
        dataset, variable, at_time, bbox_wkt, z_from, z_to, max_cells, time_from, time_to,
    ))
}

/// Fetch cells of a COMPOSITE (vector / tensor / RGB) variable. The
/// composite is declared via `pgx.register_variable(..., components
/// := ARRAY[...])`; `values float8[]` carries the components in the
/// position order recorded in `pgx.variable_components`.
///
/// Errors when `variable` isn't registered as a composite — use the
/// scalar `pgx.fetch` for non-vector variables.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn fetch_vec(
    dataset: &str,
    variable: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    level_from: default!(Option<f64>, "NULL"),
    level_to: default!(Option<f64>, "NULL"),
    max_cells: default!(i32, 1000000),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(lat, Option<f64>),
        name!(lon, Option<f64>),
        name!(level, Option<f64>),
        name!(time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(values, Vec<f64>),
    ),
> {
    TableIterator::new(srf::fetch_vec::fetch_vec_impl(
        dataset, variable, at_time, bbox_wkt, level_from, level_to, max_cells, time_from, time_to,
    ))
}

/// Cartesian flavour of `pgx.fetch_vec`. Returns rows shaped
/// `(x, y, z, time, values float8[])` — same data, semantic column
/// names for engineering / simulation / model-space datasets.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn fetch_xyz_vec(
    dataset: &str,
    variable: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    z_from: default!(Option<f64>, "NULL"),
    z_to: default!(Option<f64>, "NULL"),
    max_cells: default!(i32, 1000000),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(x, Option<f64>),
        name!(y, Option<f64>),
        name!(z, Option<f64>),
        name!(time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(values, Vec<f64>),
    ),
> {
    TableIterator::new(srf::fetch_vec::fetch_vec_impl(
        dataset, variable, at_time, bbox_wkt, z_from, z_to, max_cells, time_from, time_to,
    ))
}

/// Fetch cells of a variable indexed against an unstructured mesh.
/// The variable's `dim_order` decides whether values are per-node
/// (joined to `pgx.mesh_nodes.geom`) or per-cell (joined to
/// `pgx.mesh_cells.centroid`); default is per-node.
///
/// Returns `(node_id, cell_id, geom_wkt, time, value)`. Exactly one of
/// `node_id` / `cell_id` is populated per row; clients typically call
/// `ST_GeomFromText(geom_wkt, srid)` to lift geom back to a PostGIS
/// geometry, e.g.:
///
/// ```sql
/// SELECT ST_GeomFromText(geom_wkt, 0) AS geom, value
///   FROM pgx.fetch_mesh('crash','stress', bbox_wkt := 'POLYGON((...))');
/// ```
#[pg_extern]
#[allow(clippy::type_complexity)]
fn fetch_mesh(
    dataset: &str,
    variable: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    max_cells: default!(i32, 1000000),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(node_id, Option<i64>),
        name!(cell_id, Option<i64>),
        name!(geom_wkt, Option<String>),
        name!(time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(value, f64),
    ),
> {
    TableIterator::new(srf::fetch_mesh::fetch_mesh_impl(
        dataset, variable, at_time, bbox_wkt, max_cells, time_from, time_to,
    ))
}

/// Export an indexed dataset as glTF 2.0 Binary (GLB) for visualisation.
///
/// Builds a triangulated surface displaced by `surface_var`, vertex-coloured
/// by `color_var` (defaults to `surface_var`) via the `colormap` LUT. When
/// `flow_uv` is provided (1- or 2-element array of velocity component
/// variable names), a LINES primitive is added carrying flow arrows. When
/// the time series has more than one timestep, the GLB carries a STEP
/// animation that morphs POSITION (Z) and COLOR_0 between timesteps.
///
/// `options` jsonb knobs:
///   - `vmin` / `vmax`: colormap value range overrides (defaults to
///     `pgx.variables.valid_min/max` then computed from data)
///   - `arrow_scale`: multiplier on (u, v) for arrow length (default 1.0)
///   - `max_cells`: per-`pgx.fetch_mesh` row cap (default 1_000_000)
///
/// Coordinates are emitted in the mesh's native CRS — no reprojection.
/// The only transform applied is `z_scale` on the surface Z axis.
///
/// `target_srid` (default 4326, matching pg_solid's `solid_to_glb`) is written
/// into `asset.extras.srid` + `asset.extras.axis_order` so the downstream
/// viewer knows what CRS the vertex coords live in. If the source mesh's SRID
/// differs from `target_srid`, a `WARNING` is raised — pg_xarray does NOT
/// reproject (use PostGIS `ST_Transform` on the dataset if you need to change
/// CRS).
#[pg_extern]
fn xarray_to_glb(
    dataset: &str,
    surface_var: &str,
    color_var: default!(Option<&str>, "NULL"),
    flow_uv: default!(Option<Vec<String>>, "NULL"),
    time_from: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    time_to: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    colormap: default!(&str, "'viridis'"),
    z_scale: default!(f64, 1.0),
    options: default!(Option<pgrx::JsonB>, "NULL"),
    target_srid: default!(i32, 4326),
) -> Vec<u8> {
    glb::xarray_to_glb_impl(
        dataset,
        surface_var,
        color_var,
        flow_uv.as_deref(),
        time_from,
        time_to,
        bbox_wkt,
        colormap,
        z_scale,
        options,
        target_srid,
    )
}

/// Render a single timestep of an indexed dataset as a PNG raster.
///
/// 2D companion to `xarray_to_glb`: builds a colourmapped image of the
/// scalar field at `at_time`, clipped to `bbox_wkt`. Output is consumed
/// directly (`\copy ... TO file`) or via the WMS bgworker (`?REQUEST=GetMap`).
///
/// Coordinates are emitted in the dataset's native CRS — no reprojection.
/// `options` jsonb knobs: `vmin`/`vmax` (colormap range), `background_color`
/// (`#RRGGBB` or `#RRGGBBAA`, default transparent), `max_cells`.
#[pg_extern]
fn xarray_to_png(
    dataset: &str,
    surface_var: &str,
    at_time: default!(Option<pgrx::datum::TimestampWithTimeZone>, "NULL"),
    bbox_wkt: default!(Option<&str>, "NULL"),
    width: default!(i32, 512),
    height: default!(i32, 512),
    colormap: default!(&str, "'viridis'"),
    options: default!(Option<pgrx::JsonB>, "NULL"),
) -> Vec<u8> {
    raster::xarray_to_png_impl(
        dataset,
        surface_var,
        at_time,
        bbox_wkt,
        width,
        height,
        colormap,
        options,
    )
}

// =============================================================================
// SQL — Catalog listing
// =============================================================================

/// List datasets in the catalog.
#[pg_extern]
fn list_datasets() -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(name, String),
        name!(format, String),
        name!(created_at, pgrx::datum::TimestampWithTimeZone),
    ),
> {
    TableIterator::new(catalog::crud::list_datasets_impl())
}

/// Count chunks for a given dataset (cheap diagnostic).
#[pg_extern]
fn chunk_count(dataset: &str) -> i64 {
    catalog::crud::chunk_count_impl(dataset)
}

/// List variables of a dataset with their catalog-level metadata.
///
/// One row per `pgx.variables` row matching `dataset`, sorted by
/// variable name. `srid` is the effective SRID (variable's own if
/// set, else dataset's `default_srid`). `n_components` is 0 for
/// scalar variables; `n_chunks` is the number of `pgx.chunks` rows.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_variables(
    dataset: &str,
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(name, String),
        name!(dtype, Option<String>),
        name!(units, Option<String>),
        name!(standard_name, Option<String>),
        name!(long_name, Option<String>),
        name!(srid, Option<i32>),
        name!(n_components, i64),
        name!(n_chunks, i64),
    ),
> {
    TableIterator::new(catalog::crud::list_variables_impl(dataset))
}

/// List chunks of a (dataset, variable) — extents + file pointers,
/// one row per `pgx.chunks` row. Use it to debug "what does pgx.fetch
/// see for my variable?" without reaching into the internal tables.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn list_chunks(
    dataset: &str,
    variable: &str,
) -> TableIterator<
    'static,
    (
        name!(id, i64),
        name!(bbox_wkt, Option<String>),
        name!(time_lo, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(time_hi, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(level_lo, Option<f64>),
        name!(level_hi, Option<f64>),
        name!(uri, String),
        name!(byte_offset, Option<i64>),
        name!(byte_length, Option<i64>),
        name!(chunk_key, Option<String>),
    ),
> {
    TableIterator::new(catalog::crud::list_chunks_impl(dataset, variable))
}

/// One-row summary of a dataset — variable count, chunk count, and
/// the aggregate spatial / temporal / vertical extents. Returns no
/// rows when the dataset doesn't exist.
#[pg_extern]
#[allow(clippy::type_complexity)]
fn dataset_summary(
    dataset: &str,
) -> TableIterator<
    'static,
    (
        name!(dataset, String),
        name!(format, String),
        name!(default_srid, i32),
        name!(n_variables, i64),
        name!(n_composite_variables, i64),
        name!(n_chunks, i64),
        name!(total_bbox_wkt, Option<String>),
        name!(earliest_time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(latest_time, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(min_level, Option<f64>),
        name!(max_level, Option<f64>),
    ),
> {
    TableIterator::new(catalog::crud::dataset_summary_impl(dataset))
}

// =============================================================================
// Bootstrap SQL — catalog tables
// =============================================================================

pgrx::extension_sql!(
    r#"
-- Datasets ===========================================================
-- `default_srid` is the spatial reference system used when registering
-- new chunks/meshes for this dataset, unless the variable overrides
-- it. Defaults to 4326 (WGS84 geographic) for back-compat — every
-- geographic dataset registered before Phase A behaves identically.
-- SRID 0 means "no spatial reference" (Cartesian XYZ, engineering
-- coords, simulation-space data). Custom EPSG codes (e.g., 3857 web
-- mercator, 28992 Dutch RD) are accepted; PostGIS does the math.
CREATE TABLE pgx.datasets (
    id           BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    format       TEXT NOT NULL
                 CHECK (format IN ('zarr', 'netcdf', 'hdf5', 'grib', 'grib2',
                                   'cog', 'selafin', 'med', 'cgns', 'fits',
                                   'memory')),
    conventions  TEXT[],
    default_srid INT NOT NULL DEFAULT 4326,
    metadata     JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Variables ==========================================================
-- CF-aware columns:
--   * standard_name / units / long_name come from the file's
--     attribute bag (the CF "self-describing" promise)
--   * scale_factor / add_offset / fill_value are the CF "data packing"
--     triple — physical = stored * scale + offset, with bytes-equal-to-
--     fill_value mapped to NaN at decode time
--   * valid_min / valid_max are the CF physical-value envelope (we
--     store but don't enforce; queries can filter on it)
-- `srid` overrides `datasets.default_srid` per-variable. NULL means
-- "inherit". Useful when a single Zarr store mixes geographic
-- coordinate axes (lat/lon, SRID 4326) with model-space data
-- (SRID 0) — register each variable with its own SRID.
CREATE TABLE pgx.variables (
    id            BIGSERIAL PRIMARY KEY,
    dataset_id    BIGINT NOT NULL REFERENCES pgx.datasets(id) ON DELETE CASCADE,
    name          TEXT NOT NULL,
    standard_name TEXT,
    long_name     TEXT,
    units         TEXT,
    dtype         TEXT,
    dim_order     TEXT[],
    srid          INT,
    scale_factor  DOUBLE PRECISION,
    add_offset    DOUBLE PRECISION,
    fill_value    DOUBLE PRECISION,
    valid_min     DOUBLE PRECISION,
    valid_max     DOUBLE PRECISION,
    metadata      JSONB,
    UNIQUE (dataset_id, name)
);

-- Variable components =================================================
-- Composite (vector / tensor) variables — `velocity = [u, v, w]`,
-- `rgb = [r, g, b]`, `stress = [sxx, syy, szz, sxy, sxz, syz]`. The
-- composite is itself a row in `pgx.variables` (with no chunks of its
-- own); each component is another `pgx.variables` row (typically with
-- its own chunks). `position` is 1-indexed and gives the array
-- ordering returned by `pgx.fetch_vec`.
CREATE TABLE pgx.variable_components (
    id                    BIGSERIAL PRIMARY KEY,
    composite_variable_id BIGINT NOT NULL REFERENCES pgx.variables(id)
                          ON DELETE CASCADE,
    position              INT    NOT NULL CHECK (position >= 1),
    component_name        TEXT   NOT NULL,
    component_variable_id BIGINT NOT NULL REFERENCES pgx.variables(id)
                          ON DELETE CASCADE,
    UNIQUE (composite_variable_id, position),
    UNIQUE (composite_variable_id, component_name)
);

CREATE INDEX variable_components_composite
    ON pgx.variable_components (composite_variable_id, position);

-- Meshes =============================================================
-- One row per logical mesh on a dataset. For fixed meshes that's one
-- row; for versioned/deforming meshes additional rows in
-- pgx.mesh_versions describe per-time-window mesh states.
CREATE TABLE pgx.meshes (
    id          BIGSERIAL PRIMARY KEY,
    dataset_id  BIGINT NOT NULL REFERENCES pgx.datasets(id) ON DELETE CASCADE,
    kind        TEXT NOT NULL
                CHECK (kind IN ('regular_grid', 'curvilinear',
                                'ugrid_triangle', 'ugrid_polygon',
                                'fem_tetra', 'fem_hex', 'voronoi',
                                'particle_cloud', 'time_series', 'profile')),
    motion      TEXT NOT NULL DEFAULT 'fixed'
                CHECK (motion IN ('fixed', 'versioned', 'deforming', 'lagrangian')),
    crs         INT,
    extent      geometry(POLYGON),
    node_count  BIGINT,
    cell_count  BIGINT,
    metadata    JSONB,
    UNIQUE (dataset_id)
);

CREATE INDEX meshes_extent_gist ON pgx.meshes USING GIST (extent);

-- Mesh versions ======================================================
-- Only populated when motion != 'fixed'.
CREATE TABLE pgx.mesh_versions (
    id           BIGSERIAL PRIMARY KEY,
    mesh_id      BIGINT NOT NULL REFERENCES pgx.meshes(id) ON DELETE CASCADE,
    valid_time   TSTZRANGE NOT NULL,
    extent       geometry(POLYGON) NOT NULL,
    uri          TEXT,
    byte_offset  BIGINT,
    byte_length  BIGINT,
    chunk_key    TEXT,
    metadata     JSONB
);

CREATE INDEX mesh_versions_time_gist ON pgx.mesh_versions USING GIST (valid_time);
CREATE INDEX mesh_versions_ext_gist  ON pgx.mesh_versions USING GIST (extent);

-- Mesh nodes ==========================================================
-- Unstructured mesh vertices. One row per file-native node, stamped
-- with the mesh_version it belongs to so deforming meshes can carry
-- multiple geometric realisations of the same node.
--
-- `geom` is PostGIS Point — stored as Point (not PointZ) because
-- PostGIS GIST indexes work fine over Z-coords stuffed into the X/Y
-- envelope via ST_3DEnvelope; for the typical 2D mesh case we just
-- store Point and let `attrs` carry vertical/Z metadata when needed.
-- Real unstructured surface meshes (UGRID polygons, SELAFIN triangles)
-- are 2D and this stays simple. 3D FEM volumes will need PointZ + a
-- dedicated 3D GIST index later (Phase E.2).
CREATE TABLE pgx.mesh_nodes (
    id              BIGSERIAL PRIMARY KEY,
    mesh_version_id BIGINT NOT NULL REFERENCES pgx.mesh_versions(id)
                    ON DELETE CASCADE,
    node_id         BIGINT NOT NULL,
    geom            geometry(POINT) NOT NULL,
    attrs           JSONB,
    UNIQUE (mesh_version_id, node_id)
);

CREATE INDEX mesh_nodes_geom_gist   ON pgx.mesh_nodes USING GIST (geom);
CREATE INDEX mesh_nodes_version_idx ON pgx.mesh_nodes (mesh_version_id);

-- Mesh cells ==========================================================
-- Unstructured mesh elements (triangles, quads, polygons, tetrahedra).
-- `node_ids` is the file-native node-id list bounding the cell, in the
-- order the file uses (CW / CCW preserved). `centroid` is precomputed
-- so spatial queries can prune via GIST without dereferencing each
-- cell's nodes — the typical "values are per-cell" case.
CREATE TABLE pgx.mesh_cells (
    id              BIGSERIAL PRIMARY KEY,
    mesh_version_id BIGINT NOT NULL REFERENCES pgx.mesh_versions(id)
                    ON DELETE CASCADE,
    cell_id         BIGINT NOT NULL,
    node_ids        BIGINT[] NOT NULL,
    centroid        geometry(POINT) NOT NULL,
    attrs           JSONB,
    UNIQUE (mesh_version_id, cell_id)
);

CREATE INDEX mesh_cells_centroid_gist ON pgx.mesh_cells USING GIST (centroid);
CREATE INDEX mesh_cells_version_idx   ON pgx.mesh_cells (mesh_version_id);

-- Chunks =============================================================
-- The hot table. ~1M-100M rows typical.
CREATE TABLE pgx.chunks (
    id              BIGSERIAL PRIMARY KEY,
    variable_id     BIGINT NOT NULL REFERENCES pgx.variables(id) ON DELETE CASCADE,
    mesh_version_id BIGINT REFERENCES pgx.mesh_versions(id),
    time_range      TSTZRANGE,
    level_range     NUMRANGE,
    bbox_envelope   geometry(POLYGON),
    node_range      INT8RANGE,
    member_id       INT,
    uri             TEXT NOT NULL,
    byte_offset     BIGINT,
    byte_length     BIGINT,
    chunk_key       TEXT,
    estimated_cells BIGINT,
    metadata        JSONB,
    indexed_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX chunks_variable     ON pgx.chunks (variable_id);
CREATE INDEX chunks_time_gist    ON pgx.chunks USING GIST (time_range);
CREATE INDEX chunks_bbox_gist    ON pgx.chunks USING GIST (bbox_envelope);
CREATE INDEX chunks_node_gist    ON pgx.chunks USING GIST (node_range);
CREATE INDEX chunks_member       ON pgx.chunks (member_id) WHERE member_id IS NOT NULL;

-- Idempotency for register_chunk: same (variable, time, uri, byte_offset)
-- represents the same chunk. Used by the xarray_index sink's upsert.
-- NULLS NOT DISTINCT (PG15+) so chunks without a byte_offset / time_range
-- (e.g. SELAFIN time-stepped chunks) still dedupe instead of multiplying.
CREATE UNIQUE INDEX chunks_dedupe_idx
    ON pgx.chunks (variable_id, uri, byte_offset, time_range) NULLS NOT DISTINCT;
"#,
    name = "bootstrap_catalog",
    bootstrap
);

// FDW bootstrap — runs AFTER the catalog tables so the FDW C-symbols
// linked into pg_xarray.so are wrapped in SQL handlers and registered
// as a foreign data wrapper. Users then:
//
//   CREATE SERVER my_pgx FOREIGN DATA WRAPPER pgx_fdw;
//   CREATE FOREIGN TABLE wx_t2m (
//       lat float8, lon float8, level float8,
//       time timestamptz, value float8
//   ) SERVER my_pgx OPTIONS (dataset 'era5', variable 't2m');
//   SELECT * FROM wx_t2m;
//
pgrx::extension_sql!(
    r#"
CREATE FUNCTION pgx.fdw_handler() RETURNS fdw_handler
    AS '$libdir/pg_xarray', 'pgx_fdw_handler_wrapper'
    LANGUAGE C STRICT;

CREATE FUNCTION pgx.fdw_validator(options text[], catalog oid) RETURNS void
    AS '$libdir/pg_xarray', 'pgx_fdw_validator_wrapper'
    LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER pgx_fdw
    HANDLER pgx.fdw_handler
    VALIDATOR pgx.fdw_validator;
"#,
    name = "fdw_bootstrap",
    requires = ["bootstrap_catalog"]
);

pgrx::pg_module_magic!();

/// Extension init: register GUCs + the WMS bgworker. The worker is
/// always registered so users can toggle `pg_xarray.wms_enabled`
/// without a server restart; when disabled it sits idle on the
/// bgworker latch.
#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    server::init();
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_datasets_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.datasets");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_variables_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.variables");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_meshes_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.meshes");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_mesh_versions_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.mesh_versions");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_chunks_table_exists() {
        let count = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.chunks");
        assert_eq!(count, Ok(Some(0)));
    }

    #[pg_test]
    fn test_dataset_format_constraint() {
        // pgrx 0.16 surfaces PG ERRORs as panics, not Result::Err.
        let result = std::panic::catch_unwind(|| {
            Spi::run("INSERT INTO pgx.datasets (name, format) VALUES ('bad', 'pdf')")
        });
        assert!(result.is_err(), "expected CHECK to reject unknown format");
    }

    #[pg_test]
    fn test_mesh_kind_constraint() {
        // First create a dataset to reference.
        Spi::run("INSERT INTO pgx.datasets (name, format) VALUES ('d1', 'memory')").unwrap();
        let dataset_id = Spi::get_one::<i64>("SELECT id FROM pgx.datasets WHERE name='d1'")
            .unwrap()
            .unwrap();
        let result = std::panic::catch_unwind(|| {
            Spi::run_with_args(
                "INSERT INTO pgx.meshes (dataset_id, kind) VALUES ($1, 'martian')",
                &[dataset_id.into()],
            )
        });
        assert!(
            result.is_err(),
            "expected CHECK to reject unknown mesh kind"
        );
    }

    #[pg_test]
    fn test_chunks_dedupe_unique_index_exists() {
        let exists = Spi::get_one::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_indexes \
             WHERE schemaname='pgx' AND indexname='chunks_dedupe_idx')",
        );
        assert_eq!(exists, Ok(Some(true)));
    }

    // =========================================================================
    // Catalog CRUD round-trip
    // =========================================================================

    #[pg_test]
    fn test_register_dataset_returns_id() {
        let id = Spi::get_one::<i64>("SELECT pgx.register_dataset('era5', 'memory')")
            .unwrap()
            .unwrap();
        assert!(id > 0);
    }

    #[pg_test]
    fn test_register_dataset_is_upsert() {
        let id1 = Spi::get_one::<i64>("SELECT pgx.register_dataset('twice', 'memory')")
            .unwrap()
            .unwrap();
        let id2 = Spi::get_one::<i64>("SELECT pgx.register_dataset('twice', 'memory')")
            .unwrap()
            .unwrap();
        assert_eq!(id1, id2, "second call must return same id");
    }

    #[pg_test]
    fn test_register_variable_requires_dataset() {
        // Variable on a missing dataset must fail with a clear message.
        // pgrx 0.16 propagates PG ERROR via panic, not Result::Err — catch it.
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<i64>("SELECT pgx.register_variable('ghost', 'v')")
        });
        assert!(result.is_err(), "expected a panic from PG ERROR");
    }

    #[pg_test]
    fn test_register_variable_roundtrip() {
        Spi::run("SELECT pgx.register_dataset('d', 'memory')").unwrap();
        let var_id = Spi::get_one::<i64>(
            "SELECT pgx.register_variable('d', 't2m', 'air_temperature', 'K', 'float32')",
        )
        .unwrap()
        .unwrap();
        assert!(var_id > 0);
        let cnt =
            Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.variables WHERE name='t2m'")
                .unwrap()
                .unwrap();
        assert_eq!(cnt, 1);
    }

    #[pg_test]
    fn test_register_chunk_roundtrip_and_chunk_count() {
        Spi::run("SELECT pgx.register_dataset('d2', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('d2', 'v')").unwrap();
        Spi::run(
            "SELECT pgx.register_chunk(\
                 'd2', 'v', 'memory://grid?nx=3&ny=3&value=1', \
                 NULL, NULL, 'POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))', \
                 NULL, NULL, NULL, NULL, NULL\
              )",
        )
        .unwrap();

        let cnt = Spi::get_one::<i64>("SELECT pgx.chunk_count('d2')")
            .unwrap()
            .unwrap();
        assert_eq!(cnt, 1);
    }

    #[pg_test]
    fn test_register_chunk_is_idempotent() {
        Spi::run("SELECT pgx.register_dataset('d3', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('d3', 'v')").unwrap();
        Spi::run(
            "SELECT pgx.register_chunk('d3', 'v', 'memory://x', \
              NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .unwrap();
        Spi::run(
            "SELECT pgx.register_chunk('d3', 'v', 'memory://x', \
              NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
        )
        .unwrap();
        let cnt = Spi::get_one::<i64>("SELECT pgx.chunk_count('d3')")
            .unwrap()
            .unwrap();
        assert_eq!(cnt, 1, "duplicate register_chunk should not insert twice");
    }

    #[pg_test]
    fn test_list_datasets_returns_registered() {
        Spi::run("SELECT pgx.register_dataset('a-ds', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_dataset('b-ds', 'memory')").unwrap();
        let cnt = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgx.list_datasets() WHERE name IN ('a-ds','b-ds')",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cnt, 2);
    }

    #[pg_test]
    fn test_register_mesh_roundtrip() {
        Spi::run("SELECT pgx.register_dataset('m1', 'memory')").unwrap();
        let mesh_id = Spi::get_one::<i64>(
            "SELECT pgx.register_mesh('m1', 'regular_grid', 'fixed', 4326, \
                                       'POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))')",
        )
        .unwrap()
        .unwrap();
        assert!(mesh_id > 0);
    }

    #[pg_test]
    fn test_register_mesh_invalid_kind_errors() {
        Spi::run("SELECT pgx.register_dataset('m2', 'memory')").unwrap();
        let result = std::panic::catch_unwind(|| {
            Spi::get_one::<i64>("SELECT pgx.register_mesh('m2', 'martian-tetrahedral')")
        });
        assert!(result.is_err());
    }

    // =========================================================================
    // End-to-end: catalog + memory reader + fetch SRF
    // =========================================================================

    #[pg_test]
    fn test_fetch_end_to_end_memory_reader() {
        // 1. Register catalog entries pointing at an in-process memory reader.
        Spi::run("SELECT pgx.register_dataset('synthetic', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('synthetic', 'v')").unwrap();
        Spi::run(
            "SELECT pgx.register_chunk(\
                 'synthetic', 'v', 'memory://grid?nx=3&ny=3&value=7', \
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL\
              )",
        )
        .unwrap();

        // 2. Query: no bbox filter → expect all 9 cells, all value=7.
        let cnt = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.fetch('synthetic', 'v')")
            .unwrap()
            .unwrap();
        assert_eq!(cnt, 9);

        let value_sum = Spi::get_one::<f64>(
            "SELECT sum(value)::double precision FROM pgx.fetch('synthetic', 'v')",
        )
        .unwrap()
        .unwrap();
        assert!((value_sum - 63.0).abs() < 1e-9, "expected 9 * 7 = 63");
    }

    #[pg_test]
    fn test_fetch_with_bbox_filter() {
        Spi::run("SELECT pgx.register_dataset('synth-bbox', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('synth-bbox', 'v')").unwrap();
        Spi::run(
            "SELECT pgx.register_chunk(\
                 'synth-bbox', 'v', 'memory://grid?nx=5&ny=5&value=1', \
                 NULL, NULL, 'POLYGON((-2 -2, 2 -2, 2 2, -2 2, -2 -2))', \
                 NULL, NULL, NULL, NULL, NULL\
              )",
        )
        .unwrap();

        // bbox keeps lat,lon ∈ [0, 2] → 3x3 = 9 cells.
        let cnt = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgx.fetch(\
                 'synth-bbox', 'v', NULL, \
                 'POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'\
             )",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cnt, 9);
    }

    #[pg_test]
    fn test_fetch_max_cells_cap() {
        Spi::run("SELECT pgx.register_dataset('cap', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('cap', 'v')").unwrap();
        Spi::run(
            "SELECT pgx.register_chunk(\
                 'cap', 'v', 'memory://grid?nx=10&ny=10&value=1', \
                 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL\
              )",
        )
        .unwrap();

        let cnt = Spi::get_one::<i64>(
            "SELECT count(*)::bigint FROM pgx.fetch(\
                 'cap', 'v', NULL, NULL, NULL, NULL, 5\
             )",
        )
        .unwrap()
        .unwrap();
        assert_eq!(cnt, 5);
    }

    #[pg_test]
    fn test_fetch_no_chunks_returns_empty() {
        Spi::run("SELECT pgx.register_dataset('empty', 'memory')").unwrap();
        Spi::run("SELECT pgx.register_variable('empty', 'v')").unwrap();
        let cnt = Spi::get_one::<i64>("SELECT count(*)::bigint FROM pgx.fetch('empty', 'v')")
            .unwrap()
            .unwrap();
        assert_eq!(cnt, 0);
    }

    // =========================================================================
    // xarray_to_glb — GLB export end-to-end against memory-backed ugrid mesh.
    // =========================================================================

    #[pg_test]
    fn test_xarray_to_glb_ugrid_triangle() {
        // Build a tiny SELAFIN-like setup: 3 nodes forming 1 triangle, one
        // chunk pointing at the memory reader's nodes URI.
        Spi::run("SELECT pgx.register_dataset('glb-tri', 'memory', default_srid := 4326)").unwrap();
        Spi::run("SELECT pgx.register_variable('glb-tri', 'depth')").unwrap();
        Spi::run(
            "SELECT pgx.register_mesh('glb-tri', 'ugrid_triangle', 'fixed', 0, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))', 3, 1)",
        )
        .unwrap();
        let mv = Spi::get_one::<i64>(
            "SELECT pgx.register_mesh_version('glb-tri', \
             '2024-01-01'::timestamptz, '2024-12-31'::timestamptz, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))')",
        )
        .unwrap()
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_mesh_node($1, 1, 'POINT(0 0)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_mesh_node($1, 2, 'POINT(1 0)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_mesh_node($1, 3, 'POINT(0 1)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_mesh_cell($1, 1, ARRAY[1,2,3]::bigint[], 'POINT(0.33 0.33)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_chunk(\
                 'glb-tri', 'depth', 'memory://nodes?ids=1,2,3&values=10,20,30', \
                 NULL, NULL, NULL, NULL, NULL, NULL, $1, NULL\
             )",
            &[mv.into()],
        )
        .unwrap();

        let bytes = Spi::get_one::<Vec<u8>>("SELECT pgx.xarray_to_glb('glb-tri', 'depth')")
            .unwrap()
            .unwrap();

        assert!(bytes.len() > 12, "GLB too short: {} bytes", bytes.len());
        assert_eq!(&bytes[0..4], b"glTF", "missing GLB magic");
        assert_eq!(
            u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            2,
            "wrong GLB version"
        );
        assert_eq!(
            u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize,
            bytes.len(),
            "GLB length header mismatches actual length"
        );

        // Pull the JSON chunk out and verify scene structure.
        let (json, _bin) = crate::glb::encoder::parse_glb(&bytes).expect("parse GLB");
        assert_eq!(json["asset"]["version"], "2.0");
        assert_eq!(json["meshes"][0]["primitives"][0]["mode"], 4); // TRIANGLES
        assert!(
            json["accessors"].as_array().unwrap().len() >= 4,
            "expected POSITION, NORMAL, COLOR_0, indices accessors"
        );
        // Memory reader emits time:None → 1 keyframe → no animations.
        assert!(
            json.get("animations").is_none(),
            "static dataset should not emit animations: {}",
            json
        );
        // Asset extras carry the colormap + range so callers can render a legend.
        assert_eq!(json["asset"]["extras"]["dataset"], "glb-tri");
        assert_eq!(json["asset"]["extras"]["surface_var"], "depth");
        assert_eq!(json["asset"]["extras"]["colormap"], "viridis");
        // Default target_srid = 4326; source SRID = 4326 (registered above).
        // Both source_srid and srid should be 4326 — no warning.
        assert_eq!(json["asset"]["extras"]["srid"], 4326);
        assert_eq!(json["asset"]["extras"]["source_srid"], 4326);
        assert_eq!(
            json["asset"]["extras"]["axis_order"],
            "longitude_deg,latitude_deg,ellipsoidal_height_m"
        );
    }

    #[pg_test]
    fn test_xarray_to_glb_target_srid_overrides_extras() {
        // Source SRID = 4326 (via default_srid). Pass target_srid := 32631 —
        // extras.srid should be 32631; extras.source_srid stays 4326; the
        // axis_order hint switches to projected-CRS form.
        Spi::run("SELECT pgx.register_dataset('glb-tri-tgt', 'memory', default_srid := 4326)")
            .unwrap();
        Spi::run("SELECT pgx.register_variable('glb-tri-tgt', 'depth')").unwrap();
        Spi::run(
            "SELECT pgx.register_mesh('glb-tri-tgt', 'ugrid_triangle', 'fixed', 4326, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))', 3, 1)",
        )
        .unwrap();
        let mv = Spi::get_one::<i64>(
            "SELECT pgx.register_mesh_version('glb-tri-tgt', \
             '2024-01-01'::timestamptz, '2024-12-31'::timestamptz, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))')",
        )
        .unwrap()
        .unwrap();
        for (id, pt) in [(1, "POINT(0 0)"), (2, "POINT(1 0)"), (3, "POINT(0 1)")] {
            Spi::run_with_args(
                "SELECT pgx.register_mesh_node($1, $2, $3)",
                &[mv.into(), id.into(), pt.into()],
            )
            .unwrap();
        }
        Spi::run_with_args(
            "SELECT pgx.register_mesh_cell($1, 1, ARRAY[1,2,3]::bigint[], 'POINT(0.33 0.33)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_chunk(\
                 'glb-tri-tgt', 'depth', 'memory://nodes?ids=1,2,3&values=10,20,30', \
                 NULL, NULL, NULL, NULL, NULL, NULL, $1, NULL\
             )",
            &[mv.into()],
        )
        .unwrap();

        let bytes = Spi::get_one::<Vec<u8>>(
            "SELECT pgx.xarray_to_glb('glb-tri-tgt', 'depth', \
                 NULL, NULL, NULL, NULL, NULL, 'viridis', 1.0, NULL, 32631)",
        )
        .unwrap()
        .unwrap();

        let (json, _bin) = crate::glb::encoder::parse_glb(&bytes).expect("parse GLB");
        assert_eq!(
            json["asset"]["extras"]["srid"], 32631,
            "target_srid must override extras.srid"
        );
        assert_eq!(
            json["asset"]["extras"]["source_srid"], 4326,
            "source SRID must be preserved as 4326"
        );
        assert_eq!(
            json["asset"]["extras"]["axis_order"], "easting_m,northing_m,height_m",
            "axis_order hint must follow target_srid"
        );
    }

    // =========================================================================
    // xarray_to_png — 2D raster export end-to-end against memory-backed ugrid.
    // =========================================================================

    #[pg_test]
    fn test_xarray_to_png_ugrid_triangle() {
        Spi::run("SELECT pgx.register_dataset('png-tri', 'memory', default_srid := 0)").unwrap();
        Spi::run("SELECT pgx.register_variable('png-tri', 'depth')").unwrap();
        Spi::run(
            "SELECT pgx.register_mesh('png-tri', 'ugrid_triangle', 'fixed', 0, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))', 3, 1)",
        )
        .unwrap();
        let mv = Spi::get_one::<i64>(
            "SELECT pgx.register_mesh_version('png-tri', \
             '2024-01-01'::timestamptz, '2024-12-31'::timestamptz, \
             'POLYGON((0 0, 1 0, 0 1, 0 0))')",
        )
        .unwrap()
        .unwrap();
        for (id, pt) in [(1, "POINT(0 0)"), (2, "POINT(1 0)"), (3, "POINT(0 1)")] {
            Spi::run_with_args(
                "SELECT pgx.register_mesh_node($1, $2, $3)",
                &[mv.into(), id.into(), pt.into()],
            )
            .unwrap();
        }
        Spi::run_with_args(
            "SELECT pgx.register_mesh_cell($1, 1, ARRAY[1,2,3]::bigint[], 'POINT(0.33 0.33)')",
            &[mv.into()],
        )
        .unwrap();
        Spi::run_with_args(
            "SELECT pgx.register_chunk(\
                 'png-tri', 'depth', 'memory://nodes?ids=1,2,3&values=10,20,30', \
                 NULL, NULL, NULL, NULL, NULL, NULL, $1, NULL\
             )",
            &[mv.into()],
        )
        .unwrap();

        let bytes = Spi::get_one_with_args::<Vec<u8>>(
            "SELECT pgx.xarray_to_png('png-tri', 'depth', NULL, NULL, $1, $2)",
            &[64.into(), 64.into()],
        )
        .unwrap()
        .unwrap();

        // PNG file signature.
        assert!(bytes.len() >= 8, "PNG too short: {} bytes", bytes.len());
        assert_eq!(
            &bytes[0..8],
            &[0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A],
            "missing PNG magic"
        );
        // IHDR chunk: bytes 8..12 = length (always 13 for IHDR), 12..16 = "IHDR".
        assert_eq!(&bytes[12..16], b"IHDR");
        // Width and height live big-endian at bytes 16..20 and 20..24.
        let w = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        let h = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
        assert_eq!(w, 64);
        assert_eq!(h, 64);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // PostGIS is required for geometry columns; in test envs we
        // rely on the harness having PostGIS installed.
        vec![]
    }
}
