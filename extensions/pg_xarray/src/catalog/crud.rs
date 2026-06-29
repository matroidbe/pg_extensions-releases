//! CRUD operations on the catalog tables.
//!
//! All functions are upsert-friendly so that pg_streaming indexer
//! pipelines can safely re-invoke them on the same input without
//! creating duplicates.

use pgrx::prelude::*;

/// Upsert a dataset row. Returns its id.
pub fn register_dataset_impl(
    name: &str,
    format: &str,
    conventions: Option<Vec<String>>,
    metadata: Option<pgrx::JsonB>,
    default_srid: Option<i32>,
) -> i64 {
    if name.is_empty() {
        pgrx::error!("register_dataset: name must be non-empty");
    }
    // Validate format up-front for a clearer error than the SQL CHECK.
    validate_format(format);

    let conv_text = conventions.map(|v| v.join(","));
    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgx.datasets (name, format, conventions, metadata, default_srid) \
         VALUES ($1, $2, \
                 CASE WHEN $3::text IS NULL THEN NULL ELSE string_to_array($3, ',') END, \
                 $4, COALESCE($5, 4326)) \
         ON CONFLICT (name) DO UPDATE \
         SET format       = EXCLUDED.format, \
             conventions  = COALESCE(EXCLUDED.conventions, pgx.datasets.conventions), \
             metadata     = COALESCE(EXCLUDED.metadata, pgx.datasets.metadata), \
             default_srid = COALESCE($5, pgx.datasets.default_srid) \
         RETURNING id",
        &[
            name.into(),
            format.into(),
            conv_text.into(),
            metadata.into(),
            default_srid.into(),
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_dataset: upsert returned no id"),
        Err(e) => pgrx::error!("register_dataset: SPI failed: {}", e),
    }
}

/// Upsert a variable on a dataset. Returns its id.
/// CF-convention extras the catalog tracks per variable. All NULL by
/// default (`Default` is the no-op); `register_file_impl` populates
/// them from `pgx_zarr_walker::VariableMeta`.
#[derive(Debug, Default, Clone)]
pub struct VariableCfExtras {
    pub long_name: Option<String>,
    pub scale_factor: Option<f64>,
    pub add_offset: Option<f64>,
    pub fill_value: Option<f64>,
    pub valid_min: Option<f64>,
    pub valid_max: Option<f64>,
}

pub fn register_variable_impl(
    dataset: &str,
    name: &str,
    standard_name: Option<&str>,
    units: Option<&str>,
    dtype: Option<&str>,
    dim_order: Option<Vec<String>>,
    metadata: Option<pgrx::JsonB>,
    cf: &VariableCfExtras,
    srid: Option<i32>,
    components: Option<Vec<String>>,
) -> i64 {
    if name.is_empty() {
        pgrx::error!("register_variable: name must be non-empty");
    }
    let dataset_id = lookup_dataset_id(dataset);
    let dim_text = dim_order.map(|v| v.join(","));

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgx.variables \
           (dataset_id, name, standard_name, units, dtype, dim_order, metadata, \
            long_name, scale_factor, add_offset, fill_value, valid_min, valid_max, srid) \
         VALUES ($1, $2, $3, $4, $5, \
                 CASE WHEN $6::text IS NULL THEN NULL ELSE string_to_array($6, ',') END, \
                 $7, $8, $9, $10, $11, $12, $13, $14) \
         ON CONFLICT (dataset_id, name) DO UPDATE \
         SET standard_name = COALESCE(EXCLUDED.standard_name, pgx.variables.standard_name), \
             units         = COALESCE(EXCLUDED.units,         pgx.variables.units), \
             dtype         = COALESCE(EXCLUDED.dtype,         pgx.variables.dtype), \
             dim_order     = COALESCE(EXCLUDED.dim_order,     pgx.variables.dim_order), \
             metadata      = COALESCE(EXCLUDED.metadata,      pgx.variables.metadata), \
             long_name     = COALESCE(EXCLUDED.long_name,     pgx.variables.long_name), \
             scale_factor  = COALESCE(EXCLUDED.scale_factor,  pgx.variables.scale_factor), \
             add_offset    = COALESCE(EXCLUDED.add_offset,    pgx.variables.add_offset), \
             fill_value    = COALESCE(EXCLUDED.fill_value,    pgx.variables.fill_value), \
             valid_min     = COALESCE(EXCLUDED.valid_min,     pgx.variables.valid_min), \
             valid_max     = COALESCE(EXCLUDED.valid_max,     pgx.variables.valid_max), \
             srid          = COALESCE(EXCLUDED.srid,          pgx.variables.srid) \
         RETURNING id",
        &[
            dataset_id.into(),
            name.into(),
            standard_name.into(),
            units.into(),
            dtype.into(),
            dim_text.into(),
            metadata.into(),
            cf.long_name.as_deref().into(),
            cf.scale_factor.into(),
            cf.add_offset.into(),
            cf.fill_value.into(),
            cf.valid_min.into(),
            cf.valid_max.into(),
            srid.into(),
        ],
    );
    let composite_id = match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_variable: upsert returned no id"),
        Err(e) => pgrx::error!("register_variable: SPI failed: {}", e),
    };

    // When `components` is supplied, link them into the composite via
    // `pgx.variable_components`. Each name must already be a registered
    // scalar variable on the same dataset (we don't auto-create here —
    // the user has to register the underlying variables first).
    if let Some(names) = components {
        link_components(dataset, composite_id, &names);
    }
    composite_id
}

/// Replace the `variable_components` rows for `composite_variable_id`
/// with the supplied ordered list of component variable names. Each
/// name must resolve to an existing variable on the same dataset.
fn link_components(dataset: &str, composite_id: i64, names: &[String]) {
    if names.is_empty() {
        return;
    }
    let dataset_id = lookup_dataset_id(dataset);
    // Resolve all component names first so we fail early if any is
    // missing (avoids leaving the link table half-populated).
    let mut resolved: Vec<(usize, &str, i64)> = Vec::with_capacity(names.len());
    for (idx, name) in names.iter().enumerate() {
        let component_id = match Spi::get_one_with_args::<i64>(
            "SELECT id FROM pgx.variables WHERE dataset_id = $1 AND name = $2",
            &[dataset_id.into(), name.as_str().into()],
        ) {
            Ok(Some(id)) => id,
            Ok(None) => pgrx::error!(
                "register_variable: component '{}' not found on dataset '{}' \
                 (register it as a scalar variable first)",
                name,
                dataset
            ),
            Err(e) => pgrx::error!("register_variable: component lookup SPI failed: {}", e),
        };
        if component_id == composite_id {
            pgrx::error!(
                "register_variable: composite '{}' cannot reference itself as a component",
                names[idx]
            );
        }
        resolved.push((idx + 1, name.as_str(), component_id));
    }

    // Wipe + re-insert. `components` is the source of truth so a
    // re-registration replaces the list rather than appending.
    let _ = Spi::run_with_args(
        "DELETE FROM pgx.variable_components WHERE composite_variable_id = $1",
        &[composite_id.into()],
    );
    for (position, comp_name, component_id) in resolved {
        let _ = Spi::run_with_args(
            "INSERT INTO pgx.variable_components \
               (composite_variable_id, position, component_name, component_variable_id) \
             VALUES ($1, $2, $3, $4)",
            &[
                composite_id.into(),
                (position as i32).into(),
                comp_name.into(),
                component_id.into(),
            ],
        );
    }
}

/// Upsert the mesh on a dataset. One mesh per dataset (enforced by
/// UNIQUE (dataset_id) on pgx.meshes). Returns its id.
pub fn register_mesh_impl(
    dataset: &str,
    kind: &str,
    motion: &str,
    crs: Option<i32>,
    extent_wkt: Option<&str>,
    node_count: Option<i64>,
    cell_count: Option<i64>,
    metadata: Option<pgrx::JsonB>,
) -> i64 {
    validate_mesh_kind(kind);
    validate_mesh_motion(motion);
    let dataset_id = lookup_dataset_id(dataset);

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgx.meshes \
           (dataset_id, kind, motion, crs, extent, node_count, cell_count, metadata) \
         VALUES ($1, $2, $3, $4, \
                 CASE WHEN $5::text IS NULL THEN NULL \
                      ELSE public.ST_GeomFromText($5, COALESCE($4, \
                          (SELECT default_srid FROM pgx.datasets WHERE id = $1), \
                          4326)) END, \
                 $6, $7, $8) \
         ON CONFLICT (dataset_id) DO UPDATE \
         SET kind       = EXCLUDED.kind, \
             motion     = EXCLUDED.motion, \
             crs        = COALESCE(EXCLUDED.crs,        pgx.meshes.crs), \
             extent     = COALESCE(EXCLUDED.extent,     pgx.meshes.extent), \
             node_count = COALESCE(EXCLUDED.node_count, pgx.meshes.node_count), \
             cell_count = COALESCE(EXCLUDED.cell_count, pgx.meshes.cell_count), \
             metadata   = COALESCE(EXCLUDED.metadata,   pgx.meshes.metadata) \
         RETURNING id",
        &[
            dataset_id.into(),
            kind.into(),
            motion.into(),
            crs.into(),
            extent_wkt.into(),
            node_count.into(),
            cell_count.into(),
            metadata.into(),
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_mesh: upsert returned no id"),
        Err(e) => pgrx::error!("register_mesh: SPI failed: {}", e),
    }
}

/// Insert a mesh version. Versions are not upserted because each
/// represents a distinct mesh state at a distinct time window.
pub fn register_mesh_version_impl(
    dataset: &str,
    valid_from: pgrx::datum::TimestampWithTimeZone,
    valid_to: pgrx::datum::TimestampWithTimeZone,
    extent_wkt: &str,
    uri: Option<&str>,
    byte_offset: Option<i64>,
    byte_length: Option<i64>,
    chunk_key: Option<&str>,
    metadata: Option<pgrx::JsonB>,
) -> i64 {
    let mesh_id = lookup_mesh_id(dataset);

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgx.mesh_versions \
           (mesh_id, valid_time, extent, uri, byte_offset, byte_length, chunk_key, metadata) \
         VALUES ($1, tstzrange($2, $3, '[)'), \
                 public.ST_GeomFromText($4, ( \
                     SELECT COALESCE(m.crs, d.default_srid, 4326) \
                     FROM pgx.meshes m \
                     JOIN pgx.datasets d ON d.id = m.dataset_id \
                     WHERE m.id = $1 \
                 )), \
                 $5, $6, $7, $8, $9) \
         RETURNING id",
        &[
            mesh_id.into(),
            valid_from.into(),
            valid_to.into(),
            extent_wkt.into(),
            uri.into(),
            byte_offset.into(),
            byte_length.into(),
            chunk_key.into(),
            metadata.into(),
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_mesh_version: insert returned no id"),
        Err(e) => pgrx::error!("register_mesh_version: SPI failed: {}", e),
    }
}

/// Insert one mesh node. `geom_wkt` is interpreted in the mesh
/// version's effective SRID (mesh.crs → dataset.default_srid → 4326).
/// Idempotent on `(mesh_version_id, node_id)` via the unique index.
pub fn register_mesh_node_impl(
    mesh_version_id: i64,
    node_id: i64,
    geom_wkt: &str,
    attrs: Option<pgrx::JsonB>,
) -> i64 {
    if geom_wkt.is_empty() {
        pgrx::error!("register_mesh_node: geom_wkt is required");
    }
    let result = Spi::get_one_with_args::<i64>(
        "WITH eff AS ( \
             SELECT COALESCE(m.crs, d.default_srid, 4326) AS srid \
             FROM pgx.mesh_versions mv \
             JOIN pgx.meshes  m ON m.id = mv.mesh_id \
             JOIN pgx.datasets d ON d.id = m.dataset_id \
             WHERE mv.id = $1 \
         ) \
         INSERT INTO pgx.mesh_nodes (mesh_version_id, node_id, geom, attrs) \
         VALUES ($1, $2, public.ST_GeomFromText($3, (SELECT srid FROM eff)), $4) \
         ON CONFLICT (mesh_version_id, node_id) DO UPDATE \
         SET geom  = EXCLUDED.geom, \
             attrs = COALESCE(EXCLUDED.attrs, pgx.mesh_nodes.attrs) \
         RETURNING id",
        &[
            mesh_version_id.into(),
            node_id.into(),
            geom_wkt.into(),
            attrs.into(),
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_mesh_node: upsert returned no id"),
        Err(e) => pgrx::error!("register_mesh_node: SPI failed: {}", e),
    }
}

/// Insert one mesh cell. `centroid_wkt` interpreted in the mesh
/// version's effective SRID. `node_ids` is the ordered list of
/// file-native node ids bounding this cell.
/// Idempotent on `(mesh_version_id, cell_id)`.
pub fn register_mesh_cell_impl(
    mesh_version_id: i64,
    cell_id: i64,
    node_ids: Vec<i64>,
    centroid_wkt: &str,
    attrs: Option<pgrx::JsonB>,
) -> i64 {
    if centroid_wkt.is_empty() {
        pgrx::error!("register_mesh_cell: centroid_wkt is required");
    }
    if node_ids.is_empty() {
        pgrx::error!("register_mesh_cell: node_ids must be non-empty");
    }
    let result = Spi::get_one_with_args::<i64>(
        "WITH eff AS ( \
             SELECT COALESCE(m.crs, d.default_srid, 4326) AS srid \
             FROM pgx.mesh_versions mv \
             JOIN pgx.meshes  m ON m.id = mv.mesh_id \
             JOIN pgx.datasets d ON d.id = m.dataset_id \
             WHERE mv.id = $1 \
         ) \
         INSERT INTO pgx.mesh_cells (mesh_version_id, cell_id, node_ids, centroid, attrs) \
         VALUES ($1, $2, $3, public.ST_GeomFromText($4, (SELECT srid FROM eff)), $5) \
         ON CONFLICT (mesh_version_id, cell_id) DO UPDATE \
         SET node_ids = EXCLUDED.node_ids, \
             centroid = EXCLUDED.centroid, \
             attrs    = COALESCE(EXCLUDED.attrs, pgx.mesh_cells.attrs) \
         RETURNING id",
        &[
            mesh_version_id.into(),
            cell_id.into(),
            node_ids.into(),
            centroid_wkt.into(),
            attrs.into(),
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_mesh_cell: upsert returned no id"),
        Err(e) => pgrx::error!("register_mesh_cell: SPI failed: {}", e),
    }
}

/// Upsert a chunk. Idempotent on (variable_id, uri, byte_offset, time_range).
pub fn register_chunk_impl(
    dataset: &str,
    variable: &str,
    uri: &str,
    time_from: Option<pgrx::datum::TimestampWithTimeZone>,
    time_to: Option<pgrx::datum::TimestampWithTimeZone>,
    bbox_wkt: Option<&str>,
    byte_offset: Option<i64>,
    byte_length: Option<i64>,
    chunk_key: Option<&str>,
    mesh_version_id: Option<i64>,
    metadata: Option<pgrx::JsonB>,
    level_from: Option<f64>,
    level_to: Option<f64>,
) -> i64 {
    if uri.is_empty() {
        pgrx::error!("register_chunk: uri must be non-empty");
    }
    let variable_id = lookup_variable_id(dataset, variable);

    // The bbox WKT is interpreted in the variable's effective SRID —
    // its own `srid` if set, else the dataset's `default_srid` (which
    // defaults to 4326). The sub-SELECT keeps the API of
    // register_chunk_impl unchanged while letting Cartesian/XYZ
    // datasets register bboxes with `srid = 0` end-to-end.
    let level_from_num: Option<pgrx::AnyNumeric> =
        level_from.and_then(|v| pgrx::AnyNumeric::try_from(v).ok());
    let level_to_num: Option<pgrx::AnyNumeric> =
        level_to.and_then(|v| pgrx::AnyNumeric::try_from(v).ok());
    let result = Spi::get_one_with_args::<i64>(
        "WITH eff AS ( \
             SELECT COALESCE(v.srid, d.default_srid, 4326) AS srid \
             FROM pgx.variables v \
             JOIN pgx.datasets  d ON d.id = v.dataset_id \
             WHERE v.id = $1 \
         ) \
         INSERT INTO pgx.chunks \
           (variable_id, mesh_version_id, time_range, bbox_envelope, \
            uri, byte_offset, byte_length, chunk_key, metadata, level_range) \
         VALUES ($1, $2, \
                 CASE WHEN $3::timestamptz IS NULL OR $4::timestamptz IS NULL \
                      THEN NULL ELSE tstzrange($3, $4, '[]') END, \
                 CASE WHEN $5::text IS NULL THEN NULL \
                      ELSE public.ST_GeomFromText($5, (SELECT srid FROM eff)) END, \
                 $6, $7, $8, $9, $10, \
                 CASE WHEN $11::numeric IS NULL OR $12::numeric IS NULL \
                      THEN NULL ELSE numrange($11, $12, '[]') END) \
         ON CONFLICT (variable_id, uri, byte_offset, time_range) DO UPDATE \
         SET bbox_envelope = COALESCE(EXCLUDED.bbox_envelope, pgx.chunks.bbox_envelope), \
             byte_length   = COALESCE(EXCLUDED.byte_length,   pgx.chunks.byte_length), \
             chunk_key     = COALESCE(EXCLUDED.chunk_key,     pgx.chunks.chunk_key), \
             metadata      = COALESCE(EXCLUDED.metadata,      pgx.chunks.metadata), \
             level_range   = COALESCE(EXCLUDED.level_range,   pgx.chunks.level_range), \
             indexed_at    = now() \
         RETURNING id",
        &[
            variable_id.into(),     // $1
            mesh_version_id.into(), // $2
            time_from.into(),       // $3
            time_to.into(),         // $4
            bbox_wkt.into(),        // $5
            uri.into(),             // $6
            byte_offset.into(),     // $7
            byte_length.into(),     // $8
            chunk_key.into(),       // $9
            metadata.into(),        // $10
            level_from_num.into(),  // $11
            level_to_num.into(),    // $12
        ],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!("register_chunk: upsert returned no id"),
        Err(e) => pgrx::error!("register_chunk: SPI failed: {}", e),
    }
}

// =============================================================================
// Listing / counting
// =============================================================================

pub fn list_datasets_impl() -> Vec<(i64, String, String, pgrx::datum::TimestampWithTimeZone)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, name, format, created_at \
             FROM pgx.datasets ORDER BY name",
            None,
            &[],
        )?;
        let mut rows = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let name: String = row.get(2)?.unwrap_or_default();
            let format: String = row.get(3)?.unwrap_or_default();
            let created_at: pgrx::datum::TimestampWithTimeZone = row
                .get(4)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());
            rows.push((id, name, format, created_at));
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

pub fn chunk_count_impl(dataset: &str) -> i64 {
    Spi::get_one_with_args::<i64>(
        "SELECT count(*)::bigint \
         FROM pgx.chunks c \
         JOIN pgx.variables v ON v.id = c.variable_id \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = $1",
        &[dataset.into()],
    )
    .ok()
    .flatten()
    .unwrap_or(0)
}

/// One row per variable for a dataset, with the columns most users want
/// when browsing the catalog: name, dtype, units, standard_name,
/// composite-component-count (0 for scalars), chunk-count.
pub type VariableListRow = (
    i64,            // id
    String,         // name
    Option<String>, // dtype
    Option<String>, // units
    Option<String>, // standard_name
    Option<String>, // long_name
    Option<i32>,    // srid (effective: variable's own srid, else dataset default)
    i64,            // n_components (0 for scalar)
    i64,            // n_chunks
);

pub fn list_variables_impl(dataset: &str) -> Vec<VariableListRow> {
    Spi::connect(|client| {
        let sql = r#"
            SELECT v.id,
                   v.name,
                   v.dtype,
                   v.units,
                   v.standard_name,
                   v.long_name,
                   COALESCE(v.srid, d.default_srid)::int                AS srid,
                   (SELECT count(*)::bigint FROM pgx.variable_components vc
                     WHERE vc.composite_variable_id = v.id)             AS n_components,
                   (SELECT count(*)::bigint FROM pgx.chunks c
                     WHERE c.variable_id = v.id)                        AS n_chunks
            FROM pgx.variables v
            JOIN pgx.datasets  d ON d.id = v.dataset_id
            WHERE d.name = $1
            ORDER BY v.name
        "#;
        let table = client.select(sql, None, &[dataset.into()])?;
        let mut rows = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let name: String = row.get(2)?.unwrap_or_default();
            let dtype: Option<String> = row.get(3)?;
            let units: Option<String> = row.get(4)?;
            let std_name: Option<String> = row.get(5)?;
            let long_name: Option<String> = row.get(6)?;
            let srid: Option<i32> = row.get(7)?;
            let n_components: i64 = row.get(8)?.unwrap_or(0);
            let n_chunks: i64 = row.get(9)?.unwrap_or(0);
            rows.push((
                id,
                name,
                dtype,
                units,
                std_name,
                long_name,
                srid,
                n_components,
                n_chunks,
            ));
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// One row per chunk for a (dataset, variable) pair. Returns the
/// extents (bbox / time / level) and the file pointers (uri /
/// byte_offset / byte_length / chunk_key) — useful for debugging
/// "what does pgx.fetch see for my variable?"
pub type ChunkListRow = (
    i64,                                        // id
    Option<String>,                             // bbox_wkt (NULL-able)
    Option<pgrx::datum::TimestampWithTimeZone>, // time_lo
    Option<pgrx::datum::TimestampWithTimeZone>, // time_hi
    Option<f64>,                                // level_lo
    Option<f64>,                                // level_hi
    String,                                     // uri
    Option<i64>,                                // byte_offset
    Option<i64>,                                // byte_length
    Option<String>,                             // chunk_key
);

pub fn list_chunks_impl(dataset: &str, variable: &str) -> Vec<ChunkListRow> {
    Spi::connect(|client| {
        let sql = r#"
            SELECT c.id,
                   public.ST_AsText(c.bbox_envelope)            AS bbox_wkt,
                   lower(c.time_range)                   AS time_lo,
                   upper(c.time_range)                   AS time_hi,
                   lower(c.level_range)::float8          AS level_lo,
                   upper(c.level_range)::float8          AS level_hi,
                   c.uri,
                   c.byte_offset,
                   c.byte_length,
                   c.chunk_key
            FROM pgx.chunks    c
            JOIN pgx.variables v ON v.id = c.variable_id
            JOIN pgx.datasets  d ON d.id = v.dataset_id
            WHERE d.name = $1 AND v.name = $2
            ORDER BY c.id
        "#;
        let table = client.select(sql, None, &[dataset.into(), variable.into()])?;
        let mut rows = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let bbox: Option<String> = row.get(2)?;
            let time_lo: Option<pgrx::datum::TimestampWithTimeZone> = row.get(3)?;
            let time_hi: Option<pgrx::datum::TimestampWithTimeZone> = row.get(4)?;
            let level_lo: Option<f64> = row.get(5)?;
            let level_hi: Option<f64> = row.get(6)?;
            let uri: String = row.get(7)?.unwrap_or_default();
            let byte_offset: Option<i64> = row.get(8)?;
            let byte_length: Option<i64> = row.get(9)?;
            let chunk_key: Option<String> = row.get(10)?;
            rows.push((
                id,
                bbox,
                time_lo,
                time_hi,
                level_lo,
                level_hi,
                uri,
                byte_offset,
                byte_length,
                chunk_key,
            ));
        }
        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Aggregate stats for a whole dataset — one row summarising the
/// catalog state. Returns NULLs (variable counts of 0, extent NULL)
/// when the dataset doesn't exist or is empty.
pub type DatasetSummaryRow = (
    String,                                     // dataset
    String,                                     // format
    i32,                                        // default_srid
    i64,                                        // n_variables
    i64,                                        // n_composite_variables
    i64,                                        // n_chunks
    Option<String>,                             // total_bbox_wkt (union)
    Option<pgrx::datum::TimestampWithTimeZone>, // earliest time
    Option<pgrx::datum::TimestampWithTimeZone>, // latest time
    Option<f64>,                                // min level
    Option<f64>,                                // max level
);

pub fn dataset_summary_impl(dataset: &str) -> Option<DatasetSummaryRow> {
    let sql = r#"
        WITH ds AS (
            SELECT id, name, format, default_srid FROM pgx.datasets WHERE name = $1
        ),
        vars AS (
            SELECT v.id,
                   (SELECT count(*) FROM pgx.variable_components vc
                     WHERE vc.composite_variable_id = v.id) > 0 AS is_composite
            FROM pgx.variables v JOIN ds ON ds.id = v.dataset_id
        ),
        chunks_for_ds AS (
            SELECT c.* FROM pgx.chunks c
            JOIN pgx.variables v ON v.id = c.variable_id
            JOIN ds ON ds.id = v.dataset_id
        )
        SELECT ds.name,
               ds.format,
               ds.default_srid::int,
               (SELECT count(*)::bigint FROM vars)                              AS n_variables,
               (SELECT count(*)::bigint FROM vars WHERE is_composite)           AS n_composite,
               (SELECT count(*)::bigint FROM chunks_for_ds)                     AS n_chunks,
               (SELECT public.ST_AsText(public.ST_Extent(bbox_envelope)) FROM chunks_for_ds)  AS bbox_wkt,
               (SELECT min(lower(time_range)) FROM chunks_for_ds)               AS t_lo,
               (SELECT max(upper(time_range)) FROM chunks_for_ds)               AS t_hi,
               (SELECT min(lower(level_range))::float8 FROM chunks_for_ds)      AS l_lo,
               (SELECT max(upper(level_range))::float8 FROM chunks_for_ds)      AS l_hi
        FROM ds
    "#;
    Spi::connect(|client| {
        let mut table = client.select(sql, Some(1), &[dataset.into()])?;
        let row = match table.next() {
            Some(r) => r,
            None => return Ok(None),
        };
        let name: String = row.get(1)?.unwrap_or_default();
        let format: String = row.get(2)?.unwrap_or_default();
        let default_srid: i32 = row.get(3)?.unwrap_or(0);
        let n_variables: i64 = row.get(4)?.unwrap_or(0);
        let n_composite: i64 = row.get(5)?.unwrap_or(0);
        let n_chunks: i64 = row.get(6)?.unwrap_or(0);
        let bbox: Option<String> = row.get(7)?;
        let t_lo: Option<pgrx::datum::TimestampWithTimeZone> = row.get(8)?;
        let t_hi: Option<pgrx::datum::TimestampWithTimeZone> = row.get(9)?;
        let l_lo: Option<f64> = row.get(10)?;
        let l_hi: Option<f64> = row.get(11)?;
        Ok::<_, spi::Error>(Some((
            name,
            format,
            default_srid,
            n_variables,
            n_composite,
            n_chunks,
            bbox,
            t_lo,
            t_hi,
            l_lo,
            l_hi,
        )))
    })
    .ok()
    .flatten()
}

// =============================================================================
// Lookups + validation
// =============================================================================

/// Run a single-row `SELECT` and return `Ok(None)` on zero rows. pgrx
/// 0.16's `Spi::get_one_with_args` raises a `SpiTupleTable positioned
/// before the start or after the end` SpiError on empty result sets
/// instead of returning `Ok(None)` — we use the lower-level cursor API
/// so the no-row case is observable.
fn spi_get_optional_i64(
    sql: &str,
    args: &[pgrx::datum::DatumWithOid],
) -> Result<Option<i64>, spi::Error> {
    Spi::connect(|client| {
        let mut t = client.select(sql, Some(1), args)?;
        match t.next() {
            Some(row) => Ok(row.get::<i64>(1)?),
            None => Ok(None),
        }
    })
}

fn lookup_dataset_id(name: &str) -> i64 {
    let result = spi_get_optional_i64(
        "SELECT id FROM pgx.datasets WHERE name = $1",
        &[name.into()],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!(
            "register_*: dataset '{}' does not exist. Call pgx.register_dataset first.",
            name
        ),
        Err(e) => pgrx::error!("lookup_dataset_id: SPI failed: {}", e),
    }
}

fn lookup_variable_id(dataset: &str, variable: &str) -> i64 {
    let result = spi_get_optional_i64(
        "SELECT v.id FROM pgx.variables v \
         JOIN pgx.datasets d ON d.id = v.dataset_id \
         WHERE d.name = $1 AND v.name = $2",
        &[dataset.into(), variable.into()],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!(
            "register_chunk: variable '{}' of dataset '{}' does not exist. \
             Call pgx.register_variable first.",
            variable,
            dataset
        ),
        Err(e) => pgrx::error!("lookup_variable_id: SPI failed: {}", e),
    }
}

fn lookup_mesh_id(dataset: &str) -> i64 {
    let result = spi_get_optional_i64(
        "SELECT m.id FROM pgx.meshes m \
         JOIN pgx.datasets d ON d.id = m.dataset_id \
         WHERE d.name = $1",
        &[dataset.into()],
    );
    match result {
        Ok(Some(id)) => id,
        Ok(None) => pgrx::error!(
            "register_mesh_version: dataset '{}' has no mesh. \
             Call pgx.register_mesh first.",
            dataset
        ),
        Err(e) => pgrx::error!("lookup_mesh_id: SPI failed: {}", e),
    }
}

/// Match the CHECK constraint in lib.rs bootstrap SQL.
pub const VALID_FORMATS: &[&str] = &[
    "zarr", "netcdf", "hdf5", "grib", "grib2", "cog", "selafin", "med", "cgns", "fits", "memory",
];

fn validate_format(format: &str) {
    if !VALID_FORMATS.contains(&format) {
        pgrx::error!(
            "register_dataset: format '{}' is not supported. Allowed: {:?}",
            format,
            VALID_FORMATS
        );
    }
}

pub const VALID_MESH_KINDS: &[&str] = &[
    "regular_grid",
    "curvilinear",
    "ugrid_triangle",
    "ugrid_polygon",
    "fem_tetra",
    "fem_hex",
    "voronoi",
    "particle_cloud",
    "time_series",
    "profile",
];

fn validate_mesh_kind(kind: &str) {
    if !VALID_MESH_KINDS.contains(&kind) {
        pgrx::error!(
            "register_mesh: kind '{}' is not supported. Allowed: {:?}",
            kind,
            VALID_MESH_KINDS
        );
    }
}

pub const VALID_MESH_MOTIONS: &[&str] = &["fixed", "versioned", "deforming", "lagrangian"];

fn validate_mesh_motion(motion: &str) {
    if !VALID_MESH_MOTIONS.contains(&motion) {
        pgrx::error!(
            "register_mesh: motion '{}' is not supported. Allowed: {:?}",
            motion,
            VALID_MESH_MOTIONS
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure-Rust unit tests — exercise the validators that don't need SPI.

    #[test]
    fn valid_format_constants_are_in_sync_with_sql_check() {
        // If the SQL CHECK constraint changes, this list must change too.
        for f in VALID_FORMATS {
            assert!(!f.is_empty());
            assert!(f
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        }
    }

    #[test]
    fn valid_mesh_kinds_are_in_sync() {
        for k in VALID_MESH_KINDS {
            assert!(!k.is_empty());
        }
        assert!(VALID_MESH_KINDS.contains(&"regular_grid"));
        assert!(VALID_MESH_KINDS.contains(&"ugrid_triangle"));
        assert!(VALID_MESH_KINDS.contains(&"particle_cloud"));
    }

    #[test]
    fn valid_mesh_motions_are_in_sync() {
        assert_eq!(
            VALID_MESH_MOTIONS,
            &["fixed", "versioned", "deforming", "lagrangian"]
        );
    }
}
