//! Deep Feature Synthesis (DFS) implementation
//!
//! The core algorithm for automated feature generation from relational data.
//! Generates features by traversing relationships and applying primitives.
//! Generation itself (`generate_features`) is pure: it works on a
//! `SchemaInfo` snapshot loaded up front.

use crate::primitives::STACKABLE_OUTER;
use crate::primitives::{get_primitives_by_name, is_applicable, is_numeric, Input, Prim};
use crate::relationships::{get_all_relationships, Relationship};
use crate::schema::{
    get_primary_key_columns, get_table_columns, parse_table_ref, quote_identifier, ColumnInfo,
    PgType, TableRef,
};
use crate::sql_gen::{CutoffSpec, IfExists, SqlGenerator};
use crate::time_indexes::get_time_index;
use crate::{get_max_features, PgFeatureError};
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;
use std::collections::{HashMap, HashSet};

/// PostgreSQL's identifier length limit (NAMEDATALEN - 1)
const MAX_IDENTIFIER_BYTES: usize = 63;

/// Escape a string literal for use in SQL
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// A generated feature specification
#[derive(Debug, Clone)]
pub struct FeatureSpec {
    /// Name of the feature (e.g., "orders_COUNT", "orders_SUM_amount")
    pub name: String,
    /// The primitive applied last (the outer one at depth 2)
    pub primitive: Prim,
    /// The grandchild-level primitive of a depth-2 feature
    pub inner: Option<Prim>,
    /// Table the base column lives in
    pub source_table: TableRef,
    /// Base column (the key column for row counts, the time index for time primitives)
    pub source_column: String,
    /// Relationships from the target down to `source_table`; empty for transforms
    pub path: Vec<Relationship>,
    /// Depth at which this feature was generated
    pub depth: i32,
    /// Human-readable description
    pub description: String,
    /// Rolling window interval (e.g., "30 days", "90 days") - None means all-time
    pub window: Option<String>,
}

impl FeatureSpec {
    /// "aggregation" or "transform"
    pub fn feature_type(&self) -> &'static str {
        if self.path.is_empty() {
            "transform"
        } else {
            "aggregation"
        }
    }

    /// Primitive as stored in the catalog: `mean(sum)` for stacked features
    pub fn primitive_label(&self) -> String {
        match self.inner {
            Some(inner) => format!("{}({})", self.primitive.name(), inner.name()),
            None => self.primitive.name().to_string(),
        }
    }
}

/// Columns, keys and time indexes of the tables DFS touches
#[derive(Debug, Default)]
pub struct SchemaInfo {
    pub columns: HashMap<TableRef, Vec<ColumnInfo>>,
    pub primary_keys: HashMap<TableRef, Vec<String>>,
    pub time_indexes: HashMap<TableRef, String>,
}

impl SchemaInfo {
    fn columns(&self, table: &TableRef) -> Result<&[ColumnInfo], PgFeatureError> {
        self.columns
            .get(table)
            .map(Vec::as_slice)
            .ok_or_else(|| PgFeatureError::TableNotFound(table.qualified_name()))
    }

    /// Columns that identify rows rather than describe them: the primary key
    /// and both sides of every relationship touching the table
    fn key_columns(&self, table: &TableRef, rels: &[Relationship]) -> HashSet<String> {
        let mut keys: HashSet<String> = self
            .primary_keys
            .get(table)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for r in rels {
            if &r.child == table {
                keys.insert(r.child_column.clone());
            }
            if &r.parent == table {
                keys.insert(r.parent_column.clone());
            }
        }
        keys
    }
}

/// Result of feature synthesis
#[derive(Debug)]
pub struct SynthesisResult {
    pub project_name: String,
    pub feature_count: i32,
    pub row_count: i64,
    pub output_name: String,
}

/// Main entry point for feature synthesis
#[allow(clippy::too_many_arguments)]
pub(crate) fn synthesize_features_impl(
    project_name: &str,
    target_table: &str,
    target_id: &str,
    cutoff_time: Option<TimestampWithTimeZone>,
    cutoff_times_table: Option<&str>,
    cutoff_time_column: &str,
    cutoff_interval: Option<&str>,
    windows: Option<&[String]>,
    tables: Option<&[String]>,
    agg_primitives: &[String],
    trans_primitives: &[String],
    max_depth: i32,
    output_name: &str,
    if_exists: &str,
) -> Result<SynthesisResult, PgFeatureError> {
    let target_ref = parse_table_ref(target_table)?;
    let output_ref = parse_table_ref(output_name)?;

    let exists_behavior = IfExists::from_str(if_exists).ok_or_else(|| {
        PgFeatureError::InvalidParameter(format!("Invalid if_exists: {}", if_exists))
    })?;
    if exists_behavior == IfExists::Error && relation_exists(&output_ref)? {
        return Err(PgFeatureError::InvalidParameter(format!(
            "{} already exists (use if_exists => 'replace')",
            output_ref.qualified_name()
        )));
    }

    let agg_prims =
        get_primitives_by_name(agg_primitives, true).map_err(PgFeatureError::InvalidParameter)?;
    let trans_prims = get_primitives_by_name(trans_primitives, false)
        .map_err(PgFeatureError::InvalidParameter)?;

    check_cutoff_modes(
        cutoff_time.is_some(),
        cutoff_times_table.is_some(),
        cutoff_interval.is_some(),
    )?;
    if let Some(interval) = cutoff_interval {
        if !is_positive_interval(interval)? {
            return Err(PgFeatureError::InvalidParameter(format!(
                "cutoff_interval must be a positive interval, got '{}'",
                interval
            )));
        }
    }
    for window in windows.unwrap_or(&[]) {
        if !is_positive_interval(window)? {
            return Err(PgFeatureError::InvalidParameter(format!(
                "window '{}' is not a positive interval",
                window
            )));
        }
    }

    let relationships = filter_relationships(&target_ref, get_all_relationships()?, tables)?;

    let schema = load_schema(&target_ref, &relationships)?;
    require_column(&target_ref, schema.columns(&target_ref)?, target_id)?;

    // Build cutoff specification
    let mut cutoffs_table_used = cutoff_times_table.map(str::to_string);
    let cutoff_spec = if let Some(cutoff_table) = cutoff_times_table {
        let table = parse_table_ref(cutoff_table)?;
        let columns = get_table_columns(&table)?;
        require_column(&table, &columns, target_id)?;
        require_column(&table, &columns, cutoff_time_column)?;
        CutoffSpec::PerEntity {
            table,
            id_column: target_id.to_string(),
            time_column: cutoff_time_column.to_string(),
        }
    } else if let Some(interval) = cutoff_interval {
        // Kept after the build: the matrix reads from it, and REFRESH needs it
        let table = TableRef::new("pgft", &format!("_cutoffs_{}", project_name));
        generate_auto_cutoffs(
            &target_ref,
            target_id,
            interval,
            &relationships,
            &schema.time_indexes,
            &table,
        )?;
        cutoffs_table_used = Some(table.qualified_name());
        CutoffSpec::PerEntity {
            table,
            id_column: target_id.to_string(),
            time_column: "cutoff_time".to_string(),
        }
    } else if let Some(cutoff) = cutoff_time {
        CutoffSpec::Single(format!("'{}'::timestamptz", cutoff.to_iso_string()))
    } else {
        CutoffSpec::None
    };

    let features = generate_features(
        &target_ref,
        target_id,
        &relationships,
        &schema,
        &agg_prims,
        &trans_prims,
        max_depth,
        windows.unwrap_or(&[]),
    )?;

    let max_features = get_max_features();
    if features.len() > max_features as usize {
        return Err(PgFeatureError::FeatureGenError(format!(
            "Generated {} features, exceeds maximum of {}. Reduce max_depth or primitives.",
            features.len(),
            max_features
        )));
    }

    let generator = SqlGenerator {
        target: &target_ref,
        target_id,
        features: &features,
        time_indexes: &schema.time_indexes,
    };

    for sql in generator.matrix_statements(&output_ref, &cutoff_spec, exists_behavior) {
        Spi::run(&sql)?;
    }
    let row_count = Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM {}",
        output_ref.sql_identifier()
    ))?
    .unwrap_or(0);

    // _train and _live views when using per-entity cutoffs
    if matches!(cutoff_spec, CutoffSpec::PerEntity { .. }) {
        for suffix in ["_train", "_live"] {
            Spi::run(&format!(
                "DROP VIEW IF EXISTS {}.{}",
                quote_identifier(&output_ref.schema),
                quote_identifier(&format!("{}{}", output_ref.name, suffix))
            ))?;
        }
        Spi::run(&generator.train_view_sql(&output_ref))?;
        Spi::run(&generator.live_view_sql(&output_ref))?;
    }

    store_feature_definitions(project_name, &features)?;
    store_feature_matrix_metadata(
        project_name,
        &target_ref,
        target_id,
        cutoff_time,
        cutoffs_table_used.as_deref(),
        cutoff_interval,
        windows,
        &output_ref,
        features.len() as i32,
        row_count,
    )?;

    Ok(SynthesisResult {
        project_name: project_name.to_string(),
        feature_count: features.len() as i32,
        row_count,
        output_name: output_ref.qualified_name(),
    })
}

/// Only one way of choosing cutoffs may be given
fn check_cutoff_modes(single: bool, table: bool, interval: bool) -> Result<(), PgFeatureError> {
    if [single, table, interval].iter().filter(|m| **m).count() > 1 {
        return Err(PgFeatureError::InvalidParameter(
            "pass only one of cutoff_time, cutoff_times_table and cutoff_interval".to_string(),
        ));
    }
    Ok(())
}

/// Keep the relationships among the target and the listed tables; the target is
/// always included, so `tables` only needs to name the related tables
fn filter_relationships(
    target: &TableRef,
    relationships: Vec<Relationship>,
    tables: Option<&[String]>,
) -> Result<Vec<Relationship>, PgFeatureError> {
    let Some(tables) = tables else {
        return Ok(relationships);
    };
    let mut included = vec![target.clone()];
    for t in tables {
        included.push(parse_table_ref(t)?);
    }
    Ok(relationships
        .into_iter()
        .filter(|r| included.contains(&r.parent) && included.contains(&r.child))
        .collect())
}

/// Whether `value` is an interval greater than zero; a value that is not an
/// interval at all raises PostgreSQL's own input error
fn is_positive_interval(value: &str) -> Result<bool, PgFeatureError> {
    Ok(
        Spi::get_one_with_args::<bool>("SELECT $1::interval > interval '0'", &[value.into()])?
            .unwrap_or(false),
    )
}

fn require_column(
    table: &TableRef,
    columns: &[ColumnInfo],
    column: &str,
) -> Result<(), PgFeatureError> {
    if columns.iter().any(|c| c.name == column) {
        Ok(())
    } else {
        Err(PgFeatureError::ColumnNotFound(
            table.qualified_name(),
            column.to_string(),
        ))
    }
}

fn relation_exists(table: &TableRef) -> Result<bool, PgFeatureError> {
    Ok(Spi::get_one_with_args::<bool>(
        "SELECT to_regclass($1) IS NOT NULL",
        &[table.sql_identifier().into()],
    )?
    .unwrap_or(false))
}

/// Load columns, primary keys and time indexes for the target, its children
/// and its grandchildren
fn load_schema(
    target: &TableRef,
    relationships: &[Relationship],
) -> Result<SchemaInfo, PgFeatureError> {
    let mut tables = vec![target.clone()];
    for rel in relationships.iter().filter(|r| &r.parent == target) {
        tables.push(rel.child.clone());
        for gc in relationships.iter().filter(|r| r.parent == rel.child) {
            tables.push(gc.child.clone());
        }
    }

    let mut schema = SchemaInfo::default();
    for table in tables {
        if schema.columns.contains_key(&table) {
            continue;
        }
        schema
            .columns
            .insert(table.clone(), get_table_columns(&table)?);
        schema
            .primary_keys
            .insert(table.clone(), get_primary_key_columns(&table)?);
        if let Some(tc) = get_time_index(&table)? {
            schema.time_indexes.insert(table, tc);
        }
    }
    Ok(schema)
}

/// Window configurations: all-time, plus each window if the rows have a time index
fn window_configs(windows: &[String], has_time_index: bool) -> Vec<Option<String>> {
    let mut configs = vec![None];
    if has_time_index {
        configs.extend(windows.iter().map(|w| Some(w.clone())));
    }
    configs
}

/// Name for a related table in feature names; disambiguated by the foreign key
/// when the parent has several relationships to the same table
fn table_label(rel: &Relationship, rels: &[Relationship]) -> String {
    let parallel = rels
        .iter()
        .filter(|r| r.parent == rel.parent && r.child == rel.child)
        .count();
    if parallel > 1 {
        format!("{}_{}", rel.child.name, rel.child_column)
    } else {
        rel.child.name.clone()
    }
}

fn with_window(name: String, window: &Option<String>) -> String {
    match window {
        None => name,
        Some(w) => format!("{}_{}", name, window_to_suffix(w)),
    }
}

fn window_phrase(window: &Option<String>) -> String {
    match window {
        None => String::new(),
        Some(w) => format!(" in last {}", w),
    }
}

/// An input to an aggregation at one level: (primitive, column, column type, name part)
struct AggInput {
    prim: Prim,
    column: String,
    column_type: PgType,
    /// "" for row/time inputs, "_{column}" for column inputs
    name_part: String,
}

/// Every aggregation applicable to a table, in feature order:
/// row/time primitives first, then per column, per primitive
fn aggregation_inputs(
    table: &TableRef,
    key_column: &str,
    schema: &SchemaInfo,
    rels: &[Relationship],
    aggs: &[Prim],
) -> Result<Vec<AggInput>, PgFeatureError> {
    let time = schema.time_indexes.get(table);
    let mut inputs = Vec::new();
    for &p in aggs {
        match p.input() {
            Input::Rows => inputs.push(AggInput {
                prim: p,
                column: key_column.to_string(),
                column_type: PgType::Other,
                name_part: String::new(),
            }),
            Input::TimeIndex => {
                if let Some(tc) = time {
                    inputs.push(AggInput {
                        prim: p,
                        column: tc.clone(),
                        column_type: PgType::Timestamp,
                        name_part: String::new(),
                    })
                }
            }
            Input::Column => {}
        }
    }
    let keys = schema.key_columns(table, rels);
    for column in schema.columns(table)? {
        if keys.contains(&column.name) {
            continue;
        }
        for &p in aggs {
            if is_applicable(p, column.pg_type) && (!p.needs_time_index() || time.is_some()) {
                inputs.push(AggInput {
                    prim: p,
                    column: column.name.clone(),
                    column_type: column.pg_type,
                    name_part: format!("_{}", column.name),
                });
            }
        }
    }
    Ok(inputs)
}

/// Generate features using the DFS algorithm
#[allow(clippy::too_many_arguments)]
pub(crate) fn generate_features(
    target: &TableRef,
    target_id: &str,
    relationships: &[Relationship],
    schema: &SchemaInfo,
    agg_primitives: &[Prim],
    trans_primitives: &[Prim],
    max_depth: i32,
    windows: &[String],
) -> Result<Vec<FeatureSpec>, PgFeatureError> {
    let mut features = Vec::new();

    // Depth 0: transforms of target columns
    for column in schema.columns(target)? {
        if column.name == target_id {
            continue;
        }
        for &primitive in trans_primitives {
            if is_applicable(primitive, column.pg_type) {
                features.push(FeatureSpec {
                    name: format!("{}_{}", column.name, primitive.name().to_uppercase()),
                    primitive,
                    inner: None,
                    source_table: target.clone(),
                    source_column: column.name.clone(),
                    path: vec![],
                    depth: 0,
                    description: format!(
                        "{} of {} from {}",
                        primitive.description(),
                        column.name,
                        target.name
                    ),
                    window: None,
                });
            }
        }
    }

    let child_rels: Vec<&Relationship> = relationships
        .iter()
        .filter(|r| &r.parent == target)
        .collect();

    // Depth 1: aggregations of each child
    if max_depth >= 1 {
        for rel in &child_rels {
            let label = table_label(rel, relationships);
            let has_time = schema.time_indexes.contains_key(&rel.child);
            let inputs = aggregation_inputs(
                &rel.child,
                &rel.child_column,
                schema,
                relationships,
                agg_primitives,
            )?;
            for input in &inputs {
                for window in window_configs(windows, has_time) {
                    features.push(FeatureSpec {
                        name: with_window(
                            format!(
                                "{}_{}{}",
                                label,
                                input.prim.name().to_uppercase(),
                                input.name_part
                            ),
                            &window,
                        ),
                        primitive: input.prim,
                        inner: None,
                        source_table: rel.child.clone(),
                        source_column: input.column.clone(),
                        path: vec![(*rel).clone()],
                        depth: 1,
                        description: format!(
                            "{} of {} from {}{} grouped by {}",
                            input.prim.description(),
                            input.column,
                            rel.child.name,
                            window_phrase(&window),
                            rel.parent.name
                        ),
                        window,
                    });
                }
            }
        }
    }

    // Depth 2: aggregate each child's aggregation of its grandchildren
    if max_depth >= 2 {
        let outers: Vec<Prim> = agg_primitives
            .iter()
            .copied()
            .filter(|p| STACKABLE_OUTER.contains(&p.name()))
            .collect();

        for rel in &child_rels {
            let label = table_label(rel, relationships);
            let grandchild_rels = relationships
                .iter()
                .filter(|r| r.parent == rel.child && r.child != *target && r.id != rel.id);
            for gc_rel in grandchild_rels {
                let gc_label = table_label(gc_rel, relationships);
                let has_time = schema.time_indexes.contains_key(&rel.child)
                    || schema.time_indexes.contains_key(&gc_rel.child);
                let inputs = aggregation_inputs(
                    &gc_rel.child,
                    &gc_rel.child_column,
                    schema,
                    relationships,
                    agg_primitives,
                )?;
                for input in inputs
                    .iter()
                    .filter(|i| is_numeric(i.prim.output_type(i.column_type)))
                {
                    for &outer in &outers {
                        for window in window_configs(windows, has_time) {
                            features.push(FeatureSpec {
                                name: with_window(
                                    format!(
                                        "{}_{}_{}_{}{}",
                                        label,
                                        outer.name().to_uppercase(),
                                        gc_label,
                                        input.prim.name().to_uppercase(),
                                        input.name_part
                                    ),
                                    &window,
                                ),
                                primitive: outer,
                                inner: Some(input.prim),
                                source_table: gc_rel.child.clone(),
                                source_column: input.column.clone(),
                                path: vec![(*rel).clone(), gc_rel.clone()],
                                depth: 2,
                                description: format!(
                                    "{} of ({} of {} from {}) from {}{} grouped by {}",
                                    outer.description(),
                                    input.prim.description(),
                                    input.column,
                                    gc_rel.child.name,
                                    rel.child.name,
                                    window_phrase(&window),
                                    target.name
                                ),
                                window,
                            });
                        }
                    }
                }
            }
        }
    }

    finalize_names(&mut features, target_id)?;
    Ok(features)
}

/// FNV-1a, for stable short suffixes
fn fnv1a(s: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in s.bytes() {
        h ^= b as u32;
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Fit a name into PostgreSQL's identifier limit, keeping it unique
pub fn fit_identifier(name: &str) -> String {
    if name.len() <= MAX_IDENTIFIER_BYTES {
        return name.to_string();
    }
    let suffix = format!("_{:08x}", fnv1a(name));
    let mut cut = MAX_IDENTIFIER_BYTES - suffix.len();
    while !name.is_char_boundary(cut) {
        cut -= 1;
    }
    format!("{}{}", &name[..cut], suffix)
}

/// Shorten over-long names and reject duplicates
fn finalize_names(features: &mut [FeatureSpec], target_id: &str) -> Result<(), PgFeatureError> {
    let mut seen: HashSet<String> = [target_id.to_string(), "cutoff_time".to_string()]
        .into_iter()
        .collect();
    for f in features.iter_mut() {
        if f.name.len() > MAX_IDENTIFIER_BYTES {
            f.description = format!("{} [full name: {}]", f.description, f.name);
            f.name = fit_identifier(&f.name);
        }
        if !seen.insert(f.name.clone()) {
            return Err(PgFeatureError::FeatureGenError(format!(
                "duplicate feature name '{}'; rename the column or restrict tables/primitives",
                f.name
            )));
        }
    }
    Ok(())
}

/// Convert a window interval string to a suffix (e.g., "30 days" -> "30d")
pub fn window_to_suffix(window: &str) -> String {
    window
        .trim()
        .to_lowercase()
        .replace(" days", "d")
        .replace(" day", "d")
        .replace(" months", "m")
        .replace(" month", "m")
        .replace(" weeks", "w")
        .replace(" week", "w")
        .replace(" years", "y")
        .replace(" year", "y")
        .replace(' ', "")
}

/// Store feature definitions in the catalog
fn store_feature_definitions(
    project_name: &str,
    features: &[FeatureSpec],
) -> Result<(), PgFeatureError> {
    let delete_sql = format!(
        "DELETE FROM pgft.feature_definitions WHERE project_name = '{}'",
        escape_literal(project_name)
    );
    Spi::run(&delete_sql)?;

    for feature in features {
        let insert_sql = format!(
            r#"
            INSERT INTO pgft.feature_definitions
                (project_name, feature_name, feature_type, primitive, base_column, source_table, depth, description)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}')
            "#,
            escape_literal(project_name),
            escape_literal(&feature.name),
            feature.feature_type(),
            escape_literal(&feature.primitive_label()),
            escape_literal(&feature.source_column),
            escape_literal(&feature.source_table.qualified_name()),
            feature.depth,
            escape_literal(&feature.description)
        );
        Spi::run(&insert_sql)?;
    }
    Ok(())
}

/// Store feature matrix metadata
#[allow(clippy::too_many_arguments)]
fn store_feature_matrix_metadata(
    project_name: &str,
    target: &TableRef,
    target_id: &str,
    cutoff_time: Option<TimestampWithTimeZone>,
    cutoff_times_table: Option<&str>,
    cutoff_interval: Option<&str>,
    windows: Option<&[String]>,
    output: &TableRef,
    feature_count: i32,
    row_count: i64,
) -> Result<(), PgFeatureError> {
    let cutoff_time_sql = match cutoff_time {
        Some(ts) => format!("'{}'::timestamptz", ts.to_iso_string()),
        None => "NULL".to_string(),
    };
    let cutoff_table_sql = match cutoff_times_table {
        Some(table) => format!("'{}'", escape_literal(table)),
        None => "NULL".to_string(),
    };
    let cutoff_interval_sql = match cutoff_interval {
        Some(interval) => format!("'{}'", escape_literal(interval)),
        None => "NULL".to_string(),
    };
    let windows_sql = match windows {
        Some(wins) if !wins.is_empty() => {
            let escaped: Vec<String> = wins.iter().map(|w| escape_literal(w)).collect();
            format!("ARRAY['{}']", escaped.join("','"))
        }
        _ => "NULL".to_string(),
    };

    let sql = format!(
        r#"
        INSERT INTO pgft.feature_matrices
            (project_name, target_schema, target_table, target_id_column,
             cutoff_time, cutoff_times_table, cutoff_interval, windows,
             output_schema, output_name, output_type, feature_count, row_count)
        VALUES ('{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', 'materialized_view', {}, {})
        ON CONFLICT (project_name)
        DO UPDATE SET
            target_schema = EXCLUDED.target_schema,
            target_table = EXCLUDED.target_table,
            target_id_column = EXCLUDED.target_id_column,
            cutoff_time = EXCLUDED.cutoff_time,
            cutoff_times_table = EXCLUDED.cutoff_times_table,
            cutoff_interval = EXCLUDED.cutoff_interval,
            windows = EXCLUDED.windows,
            output_schema = EXCLUDED.output_schema,
            output_name = EXCLUDED.output_name,
            output_type = EXCLUDED.output_type,
            feature_count = EXCLUDED.feature_count,
            row_count = EXCLUDED.row_count,
            created_at = NOW()
        "#,
        escape_literal(project_name),
        escape_literal(&target.schema),
        escape_literal(&target.name),
        escape_literal(target_id),
        cutoff_time_sql,
        cutoff_table_sql,
        cutoff_interval_sql,
        windows_sql,
        escape_literal(&output.schema),
        escape_literal(&output.name),
        feature_count,
        row_count
    );
    Spi::run(&sql)?;
    Ok(())
}

/// Create temporal cutoffs table
pub(crate) fn make_temporal_cutoffs_impl(
    target_table: &str,
    target_id: &str,
    start_time: TimestampWithTimeZone,
    end_time: Option<TimestampWithTimeZone>,
    window_size: &str,
    num_windows: Option<i32>,
    output_table: &str,
) -> Result<i64, PgFeatureError> {
    let target_ref = parse_table_ref(target_table)?;
    let output_ref = parse_table_ref(output_table)?;

    if num_windows.is_some_and(|n| n < 1) {
        return Err(PgFeatureError::InvalidParameter(
            "num_windows must be at least 1".to_string(),
        ));
    }
    if !is_positive_interval(window_size)? {
        return Err(PgFeatureError::InvalidParameter(format!(
            "window_size must be a positive interval, got '{}'",
            window_size
        )));
    }

    let series_clause = if let Some(end) = end_time {
        format!(
            "generate_series('{}'::timestamptz, '{}'::timestamptz, '{}'::interval)",
            start_time.to_iso_string(),
            end.to_iso_string(),
            escape_literal(window_size)
        )
    } else if let Some(n) = num_windows {
        format!(
            "generate_series('{}'::timestamptz, '{}'::timestamptz + ({} - 1) * '{}'::interval, '{}'::interval)",
            start_time.to_iso_string(),
            start_time.to_iso_string(),
            n,
            escape_literal(window_size),
            escape_literal(window_size)
        )
    } else {
        return Err(PgFeatureError::InvalidParameter(
            "Must specify either end_time or num_windows".to_string(),
        ));
    };

    // No CASCADE: a feature matrix built on these cutoffs reads from the table,
    // so replacing it must fail rather than silently drop the matrix
    Spi::run(&format!(
        "DROP TABLE IF EXISTS {}",
        output_ref.sql_identifier()
    ))?;
    let id = quote_identifier(target_id);
    Spi::run(&format!(
        "CREATE TABLE {} AS
         SELECT t.{} AS {}, s AS cutoff_time
         FROM {} t
         CROSS JOIN {} s",
        output_ref.sql_identifier(),
        id,
        id,
        target_ref.sql_identifier(),
        series_clause
    ))?;

    Ok(Spi::get_one::<i64>(&format!(
        "SELECT COUNT(*) FROM {}",
        output_ref.sql_identifier()
    ))?
    .unwrap_or(0))
}

/// Auto-generate cutoff times from data
///
/// Queries the min/max timestamps from time-indexed child tables and writes
/// every target id × every `interval` step between them to `output`.
fn generate_auto_cutoffs(
    target: &TableRef,
    target_id: &str,
    interval: &str,
    relationships: &[Relationship],
    time_indexes: &HashMap<TableRef, String>,
    output: &TableRef,
) -> Result<i64, PgFeatureError> {
    let union_parts: Vec<String> = relationships
        .iter()
        .filter(|r| &r.parent == target)
        .filter_map(|r| {
            time_indexes.get(&r.child).map(|tc| {
                format!(
                    "SELECT MIN({c})::timestamptz AS min_ts, MAX({c})::timestamptz AS max_ts FROM {}",
                    r.child.sql_identifier(),
                    c = quote_identifier(tc)
                )
            })
        })
        .collect();

    if union_parts.is_empty() {
        return Err(PgFeatureError::InvalidParameter(
            "No time-indexed tables found for auto-cutoff generation. Use set_time_index() first."
                .to_string(),
        ));
    }

    let bounds_sql = format!(
        "SELECT MIN(min_ts), MAX(max_ts) FROM ({}) b",
        union_parts.join(" UNION ALL ")
    );
    let (min_ts, max_ts) =
        Spi::get_two::<TimestampWithTimeZone, TimestampWithTimeZone>(&bounds_sql)?;
    let (min_ts, max_ts) = match (min_ts, max_ts) {
        (Some(min), Some(max)) => (min, max),
        _ => {
            return Err(PgFeatureError::InvalidParameter(
                "Could not determine time bounds from data".to_string(),
            ))
        }
    };

    // Replacing the previous run's cutoffs also drops the matrix built on them
    Spi::run(&format!(
        "DROP TABLE IF EXISTS {} CASCADE",
        output.sql_identifier()
    ))?;
    let id = quote_identifier(target_id);
    Spi::run(&format!(
        "CREATE TABLE {} AS
         SELECT t.{} AS {}, s AS cutoff_time
         FROM {} t
         CROSS JOIN generate_series('{}'::timestamptz, '{}'::timestamptz, '{}'::interval) s",
        output.sql_identifier(),
        id,
        id,
        target.sql_identifier(),
        min_ts.to_iso_string(),
        max_ts.to_iso_string(),
        escape_literal(interval)
    ))?;

    Ok(
        Spi::get_one::<i64>(&format!("SELECT COUNT(*) FROM {}", output.sql_identifier()))?
            .unwrap_or(0),
    )
}

/// List features for a project
#[allow(clippy::type_complexity)]
pub(crate) fn list_features_impl(
    project_name: &str,
) -> Result<
    Vec<(
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        i32,
        Option<String>,
    )>,
    PgFeatureError,
> {
    let features = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT feature_name, feature_type, primitive, base_column, source_table, depth, description
            FROM pgft.feature_definitions
            WHERE project_name = '{}'
            ORDER BY id
            "#,
            escape_literal(project_name)
        );
        let result = client.select(&sql, None, &[])?;

        let mut features = Vec::new();
        for row in result {
            let feature_name: String = row.get(1)?.unwrap_or_default();
            let feature_type: String = row.get(2)?.unwrap_or_default();
            let primitive: String = row.get(3)?.unwrap_or_default();
            let base_column: Option<String> = row.get(4)?;
            let source_table: Option<String> = row.get(5)?;
            let depth: i32 = row.get(6)?.unwrap_or(0);
            let description: Option<String> = row.get(7)?;
            features.push((
                feature_name,
                feature_type,
                primitive,
                base_column,
                source_table,
                depth,
                description,
            ));
        }
        Ok::<_, pgrx::spi::Error>(features)
    })?;

    Ok(features)
}

/// Explain how a feature was derived
pub(crate) fn explain_feature_impl(
    project_name: &str,
    feature_name: &str,
) -> Result<String, PgFeatureError> {
    let explanation = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT description, primitive, base_column, source_table, depth
            FROM pgft.feature_definitions
            WHERE project_name = '{}' AND feature_name = '{}'
            "#,
            escape_literal(project_name),
            escape_literal(feature_name)
        );
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let description: Option<String> = row.get(1)?;
            let primitive: String = row.get(2)?.unwrap_or_default();
            let base_column: Option<String> = row.get(3)?;
            let source_table: Option<String> = row.get(4)?;
            let depth: i32 = row.get(5)?.unwrap_or(0);

            let explanation = format!(
                "Feature: {}\nPrimitive: {}\nSource Table: {}\nBase Column: {}\nDepth: {}\n\n{}",
                feature_name,
                primitive,
                source_table.unwrap_or_else(|| "N/A".to_string()),
                base_column.unwrap_or_else(|| "N/A".to_string()),
                depth,
                description.unwrap_or_else(|| "No description available".to_string())
            );
            Ok(explanation)
        } else {
            Err(PgFeatureError::FeatureGenError(format!(
                "Feature '{}' not found in project '{}'",
                feature_name, project_name
            )))
        }
    })?;

    Ok(explanation)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::get_primitives_by_name;

    fn t(name: &str) -> TableRef {
        TableRef::new("public", name)
    }

    fn col(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            pg_type: PgType::from_data_type(data_type),
            is_nullable: true,
            ordinal_position: 0,
        }
    }

    fn rel(id: i64, parent: &str, pcol: &str, child: &str, ccol: &str) -> Relationship {
        Relationship {
            id,
            parent: t(parent),
            parent_column: pcol.to_string(),
            child: t(child),
            child_column: ccol.to_string(),
            name: None,
        }
    }

    fn prims(names: &[&str], agg: bool) -> Vec<Prim> {
        let names: Vec<String> = names.iter().map(|s| s.to_string()).collect();
        get_primitives_by_name(&names, agg).unwrap()
    }

    /// customers <- orders <- items; orders has a time index
    fn shop() -> (Vec<Relationship>, SchemaInfo) {
        let rels = vec![
            rel(1, "customers", "id", "orders", "customer_id"),
            rel(2, "orders", "id", "items", "order_id"),
        ];
        let mut s = SchemaInfo::default();
        s.columns.insert(
            t("customers"),
            vec![
                col("id", "integer"),
                col("signup", "date"),
                col("name", "text"),
            ],
        );
        s.columns.insert(
            t("orders"),
            vec![
                col("id", "integer"),
                col("customer_id", "integer"),
                col("amount", "numeric"),
                col("ordered_at", "timestamp with time zone"),
                col("channel", "text"),
            ],
        );
        s.columns.insert(
            t("items"),
            vec![
                col("id", "bigint"),
                col("order_id", "integer"),
                col("qty", "integer"),
            ],
        );
        s.primary_keys.insert(t("orders"), vec!["id".to_string()]);
        s.primary_keys.insert(t("items"), vec!["id".to_string()]);
        s.time_indexes.insert(t("orders"), "ordered_at".to_string());
        (rels, s)
    }

    fn names(features: &[FeatureSpec]) -> Vec<&str> {
        features.iter().map(|f| f.name.as_str()).collect()
    }

    fn generate(depth: i32, aggs: &[&str], trans: &[&str], windows: &[&str]) -> Vec<FeatureSpec> {
        let (rels, schema) = shop();
        let windows: Vec<String> = windows.iter().map(|s| s.to_string()).collect();
        generate_features(
            &t("customers"),
            "id",
            &rels,
            &schema,
            &prims(aggs, true),
            &prims(trans, false),
            depth,
            &windows,
        )
        .unwrap()
    }

    #[test]
    fn test_tables_filter_keeps_target() {
        let (rels, _) = shop();
        let kept =
            filter_relationships(&t("customers"), rels, Some(&["orders".to_string()])).unwrap();
        // customers <- orders is kept although customers is not listed; orders <- items is not
        assert_eq!(kept.iter().map(|r| r.id).collect::<Vec<_>>(), vec![1]);
    }

    #[test]
    fn test_tables_filter_rejects_bad_names() {
        let (rels, _) = shop();
        assert!(filter_relationships(&t("customers"), rels, Some(&["a.b.c".to_string()])).is_err());
    }

    #[test]
    fn test_only_one_cutoff_mode() {
        assert!(check_cutoff_modes(false, false, false).is_ok());
        assert!(check_cutoff_modes(true, false, false).is_ok());
        let err = check_cutoff_modes(true, false, true)
            .unwrap_err()
            .to_string();
        assert!(err.contains("only one of cutoff_time, cutoff_times_table and cutoff_interval"));
    }

    #[test]
    fn test_count_is_one_feature_per_relationship() {
        let f = generate(1, &["count"], &[], &[]);
        assert_eq!(names(&f), vec!["orders_COUNT"]);
        assert_eq!(f[0].source_column, "customer_id");
    }

    #[test]
    fn test_key_columns_are_not_aggregated() {
        let f = generate(1, &["sum", "max"], &[], &[]);
        // orders.id (pk) and orders.customer_id (fk) are keys; only amount is numeric
        assert_eq!(names(&f), vec!["orders_SUM_amount", "orders_MAX_amount"]);
    }

    #[test]
    fn test_transforms_skip_target_id() {
        let f = generate(0, &["count"], &["year", "num_characters"], &[]);
        assert_eq!(names(&f), vec!["signup_YEAR", "name_NUM_CHARACTERS"]);
        assert!(f.iter().all(|f| f.feature_type() == "transform"));
    }

    #[test]
    fn test_windows_only_for_time_indexed_children() {
        let f = generate(2, &["count", "sum"], &[], &["30 days"]);
        assert!(names(&f).contains(&"orders_SUM_amount_30d"));
        assert!(names(&f).contains(&"orders_COUNT_30d"));
        // depth-2 window: orders has a time index, so the path does
        assert!(names(&f).contains(&"orders_SUM_items_SUM_qty_30d"));
    }

    #[test]
    fn test_windows_skipped_without_time_index() {
        let (mut rels, mut schema) = shop();
        schema.time_indexes.clear();
        rels.truncate(1);
        let f = generate_features(
            &t("customers"),
            "id",
            &rels,
            &schema,
            &prims(&["count"], true),
            &[],
            1,
            &["30 days".to_string()],
        )
        .unwrap();
        assert_eq!(names(&f), vec!["orders_COUNT"]);
    }

    #[test]
    fn test_depth2_stacks_numeric_inner_under_numeric_outer() {
        let f = generate(2, &["count", "sum", "mean", "mode"], &[], &[]);
        let d2: Vec<&str> = f
            .iter()
            .filter(|f| f.depth == 2)
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(
            d2,
            vec![
                "orders_SUM_items_COUNT",
                "orders_MEAN_items_COUNT",
                "orders_SUM_items_SUM_qty",
                "orders_MEAN_items_SUM_qty",
                "orders_SUM_items_MEAN_qty",
                "orders_MEAN_items_MEAN_qty",
                // the mode of an integer column is numeric, so it stacks too
                "orders_SUM_items_MODE_qty",
                "orders_MEAN_items_MODE_qty",
            ]
        );
        let stacked = f
            .iter()
            .find(|f| f.name == "orders_MEAN_items_SUM_qty")
            .unwrap();
        assert_eq!(stacked.primitive_label(), "mean(sum)");
        assert_eq!(stacked.path.len(), 2);
        assert_eq!(stacked.source_table, t("items"));
        // count/mode are never outer
        assert!(!d2
            .iter()
            .any(|n| n.starts_with("orders_COUNT_") || n.starts_with("orders_MODE_")));
    }

    #[test]
    fn test_depth_one_has_no_depth2() {
        let f = generate(1, &["count", "sum"], &[], &[]);
        assert!(f.iter().all(|f| f.depth <= 1));
    }

    #[test]
    fn test_time_primitives_need_time_index() {
        let f = generate(2, &["time_since_last", "last", "mean"], &[], &[]);
        let n = names(&f);
        assert!(n.contains(&"orders_TIME_SINCE_LAST"));
        assert!(n.contains(&"orders_LAST_amount"));
        assert!(n.contains(&"orders_LAST_channel"));
        // items has no time index: no time-based inner features
        assert!(!n
            .iter()
            .any(|n| n.contains("items_TIME_SINCE_LAST") || n.contains("items_LAST")));
    }

    #[test]
    fn test_generation_is_deterministic() {
        let a = generate(
            2,
            &["count", "sum", "mean", "max"],
            &["year"],
            &["30 days", "90 days"],
        );
        let b = generate(
            2,
            &["count", "sum", "mean", "max"],
            &["year"],
            &["30 days", "90 days"],
        );
        assert_eq!(names(&a), names(&b));
    }

    #[test]
    fn test_parallel_relationships_are_disambiguated() {
        let rels = vec![
            rel(1, "accounts", "id", "transfers", "from_account"),
            rel(2, "accounts", "id", "transfers", "to_account"),
        ];
        let mut s = SchemaInfo::default();
        s.columns.insert(t("accounts"), vec![col("id", "integer")]);
        s.columns.insert(
            t("transfers"),
            vec![
                col("from_account", "integer"),
                col("to_account", "integer"),
                col("amount", "numeric"),
            ],
        );
        let f = generate_features(
            &t("accounts"),
            "id",
            &rels,
            &s,
            &prims(&["count", "sum"], true),
            &[],
            2,
            &[],
        )
        .unwrap();
        assert_eq!(
            names(&f),
            vec![
                "transfers_from_account_COUNT",
                "transfers_from_account_SUM_amount",
                "transfers_to_account_COUNT",
                "transfers_to_account_SUM_amount",
            ]
        );
    }

    #[test]
    fn test_fit_identifier() {
        assert_eq!(fit_identifier("short"), "short");
        let long = "a".repeat(80);
        let fitted = fit_identifier(&long);
        assert_eq!(fitted.len(), 63);
        assert_ne!(fit_identifier(&"a".repeat(81)), fitted);
        assert_eq!(fit_identifier(&long), fitted);
    }

    #[test]
    fn test_duplicate_names_rejected() {
        let year = prims(&["year"], false)[0];
        let spec = |name: &str| FeatureSpec {
            name: name.into(),
            primitive: year,
            inner: None,
            source_table: t("c"),
            source_column: "d".into(),
            path: vec![],
            depth: 0,
            description: String::new(),
            window: None,
        };
        assert!(finalize_names(&mut [spec("a"), spec("b")], "id").is_ok());
        assert!(finalize_names(&mut [spec("a"), spec("a")], "id").is_err());
        // may not shadow the id or cutoff_time columns
        assert!(finalize_names(&mut [spec("cutoff_time")], "id").is_err());
        assert!(finalize_names(&mut [spec("id")], "id").is_err());
    }

    #[test]
    fn test_long_names_are_fitted() {
        let year = prims(&["year"], false)[0];
        let mut f = [FeatureSpec {
            name: "x".repeat(70),
            primitive: year,
            inner: None,
            source_table: t("c"),
            source_column: "d".into(),
            path: vec![],
            depth: 0,
            description: "desc".into(),
            window: None,
        }];
        finalize_names(&mut f, "id").unwrap();
        assert_eq!(f[0].name.len(), 63);
        assert!(f[0].description.contains(&"x".repeat(70)));
    }

    #[test]
    fn test_window_suffix() {
        assert_eq!(window_to_suffix("30 days"), "30d");
        assert_eq!(window_to_suffix("1 month"), "1m");
        assert_eq!(window_to_suffix("3 months"), "3m");
        assert_eq!(window_to_suffix("1 week"), "1w");
        assert_eq!(window_to_suffix("1 year"), "1y");
    }
}
