//! Deep Feature Synthesis (DFS) implementation
//!
//! The core algorithm for automated feature generation from relational data.
//! Generates features by traversing relationships and applying primitives.

use crate::primitives::{get_primitives_by_name, is_applicable, Primitive};
use crate::relationships::{get_all_relationships, Relationship};
use crate::schema::{get_table_columns, parse_table_ref, PgType, TableRef};
use crate::sql_gen::{CutoffSpec, IfExists, SqlGenerator};
use crate::time_indexes::get_time_index;
use crate::{get_max_features, PgFeatureError};
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;
use std::collections::HashMap;
use std::fmt;

/// Escape a string literal for use in SQL
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// A generated feature specification
#[derive(Clone)]
pub struct FeatureSpec {
    /// Name of the feature (e.g., "orders_COUNT", "orders_SUM_amount")
    pub name: String,
    /// The primitive used to generate this feature
    pub primitive: Box<dyn Primitive>,
    /// Source table for the feature
    pub source_table: TableRef,
    /// Source column for the feature
    pub source_column: String,
    /// Depth at which this feature was generated
    pub depth: i32,
    /// Human-readable description
    pub description: String,
    /// Feature type ("aggregation" or "transform")
    pub feature_type: String,
    /// Rolling window interval (e.g., "30 days", "90 days") - None means all-time
    pub window: Option<String>,
}

// Manual Debug implementation since Box<dyn Primitive> doesn't implement Debug
impl fmt::Debug for FeatureSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FeatureSpec")
            .field("name", &self.name)
            .field("primitive", &self.primitive.name())
            .field("source_table", &self.source_table)
            .field("source_column", &self.source_column)
            .field("depth", &self.depth)
            .field("description", &self.description)
            .field("feature_type", &self.feature_type)
            .field("window", &self.window)
            .finish()
    }
}

// Implement Clone for Box<dyn Primitive>
impl Clone for Box<dyn Primitive> {
    fn clone(&self) -> Self {
        // Since we can't clone trait objects directly, we need to use a workaround
        // We'll recreate the primitive based on its name
        let name = self.name();
        get_primitives_by_name(&[name.to_string()], self.is_aggregation())
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                // Fallback: create a Count primitive
                Box::new(crate::primitives::Count)
            })
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
    // Parse target table
    let target_ref = parse_table_ref(target_table)?;

    // Parse output table
    let output_ref = parse_table_ref(output_name)?;

    // Parse if_exists
    let exists_behavior = IfExists::from_str(if_exists).ok_or_else(|| {
        PgFeatureError::InvalidParameter(format!("Invalid if_exists: {}", if_exists))
    })?;

    // Get primitives (needed early for relationships)
    let agg_prims = get_primitives_by_name(agg_primitives, true);
    let trans_prims = get_primitives_by_name(trans_primitives, false);

    // Get relationships (needed for auto-cutoff generation)
    let all_relationships = get_all_relationships()?;

    // Filter relationships to included tables if specified
    let relationships: Vec<Relationship> = if let Some(included_tables) = tables {
        let included_refs: Vec<TableRef> = included_tables
            .iter()
            .filter_map(|t| parse_table_ref(t).ok())
            .collect();

        all_relationships
            .into_iter()
            .filter(|r| {
                included_refs.iter().any(|t| t == &r.parent)
                    && included_refs.iter().any(|t| t == &r.child)
            })
            .collect()
    } else {
        all_relationships
    };

    // Build time index map
    let mut time_indexes: HashMap<String, String> = HashMap::new();
    for rel in &relationships {
        if let Ok(Some(time_col)) = get_time_index(&rel.child) {
            time_indexes.insert(rel.child.qualified_name(), time_col);
        }
    }

    // Track if we created a temp cutoff table (for cleanup)
    let mut temp_cutoff_table: Option<String> = None;

    // Build cutoff specification
    let cutoff_spec = if let Some(cutoff_table) = cutoff_times_table {
        let table_ref = parse_table_ref(cutoff_table)?;
        CutoffSpec::PerEntity {
            table: table_ref,
            id_column: target_id.to_string(),
            time_column: cutoff_time_column.to_string(),
        }
    } else if let Some(interval) = cutoff_interval {
        // Auto-generate cutoffs from data
        let temp_table_name = format!("pgft._cutoffs_{}", project_name.replace('-', "_"));
        temp_cutoff_table = Some(temp_table_name.clone());

        // Generate cutoffs using time bounds from related tables
        generate_auto_cutoffs(
            &target_ref,
            target_id,
            interval,
            &relationships,
            &time_indexes,
            &temp_table_name,
        )?;

        let table_ref = parse_table_ref(&temp_table_name)?;
        CutoffSpec::PerEntity {
            table: table_ref,
            id_column: target_id.to_string(),
            time_column: "cutoff_time".to_string(),
        }
    } else if let Some(cutoff) = cutoff_time {
        // Format timestamp for SQL
        let cutoff_str = format!("'{}'", cutoff.to_iso_string());
        CutoffSpec::Single(cutoff_str)
    } else {
        CutoffSpec::None
    };

    // Generate features using DFS algorithm (now with windows support)
    let features = generate_features(
        &target_ref,
        target_id,
        &relationships,
        &agg_prims,
        &trans_prims,
        max_depth,
        windows,
    )?;

    // Check feature limit
    let max_features = get_max_features();
    if features.len() > max_features as usize {
        return Err(PgFeatureError::FeatureGenError(format!(
            "Generated {} features, exceeds maximum of {}. Reduce max_depth or primitives.",
            features.len(),
            max_features
        )));
    }

    // Determine if we should generate _train and _live views
    let generate_views = matches!(cutoff_spec, CutoffSpec::PerEntity { .. });

    // Build SQL generator
    let generator = SqlGenerator {
        target: target_ref.clone(),
        target_id: target_id.to_string(),
        features: features.clone(),
        relationships: relationships.clone(),
        time_indexes: time_indexes.clone(),
        cutoff: cutoff_spec.clone(),
        output: output_ref.clone(),
        if_exists: exists_behavior,
    };

    // Generate and execute SQL
    let statements = generator.generate();
    let mut row_count = 0i64;

    Spi::connect_mut(|client| {
        for sql in &statements {
            Spi::run(sql)?;
        }

        // Get row count from the output
        let count_query = format!("SELECT COUNT(*) FROM {}", output_ref.sql_identifier());
        let mut result = client.select(&count_query, None, &[])?;
        if let Some(row) = result.next() {
            row_count = row.get::<i64>(1)?.unwrap_or(0);
        }

        Ok::<_, PgFeatureError>(())
    })?;

    // Generate _train and _live views if using per-entity cutoffs
    if generate_views {
        generate_auxiliary_views(
            &target_ref,
            target_id,
            &features,
            &relationships,
            &time_indexes,
            &output_ref,
        )?;
    }

    // Clean up temp cutoff table if we created one
    if let Some(temp_table) = temp_cutoff_table {
        let drop_sql = format!("DROP TABLE IF EXISTS {} CASCADE", temp_table);
        Spi::run(&drop_sql)?;
    }

    // Store feature definitions
    store_feature_definitions(project_name, &features)?;

    // Store feature matrix metadata
    store_feature_matrix_metadata(
        project_name,
        &target_ref,
        target_id,
        cutoff_time,
        cutoff_times_table,
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

/// Generate features using DFS algorithm
fn generate_features(
    target: &TableRef,
    target_id: &str,
    relationships: &[Relationship],
    agg_primitives: &[Box<dyn Primitive>],
    trans_primitives: &[Box<dyn Primitive>],
    max_depth: i32,
    windows: Option<&[String]>,
) -> Result<Vec<FeatureSpec>, PgFeatureError> {
    let mut features = Vec::new();

    // Get target table columns for transform features
    let target_columns = get_table_columns(target)?;

    // Generate transform features on target table (no window support for transforms)
    for column in &target_columns {
        // Skip the primary key
        if column.name == target_id {
            continue;
        }

        for primitive in trans_primitives {
            if is_applicable(primitive.as_ref(), column.pg_type) {
                let feature_name = format!("{}_{}", column.name, primitive.name().to_uppercase());
                let description = format!(
                    "{} of {} from {}",
                    primitive.description(),
                    column.name,
                    target.name
                );

                features.push(FeatureSpec {
                    name: feature_name,
                    primitive: primitive.clone(),
                    source_table: target.clone(),
                    source_column: column.name.clone(),
                    depth: 0,
                    description,
                    feature_type: "transform".to_string(),
                    window: None,
                });
            }
        }
    }

    // Generate aggregation features at each depth level
    for depth in 1..=max_depth {
        let new_features = generate_depth_features(
            target,
            relationships,
            agg_primitives,
            trans_primitives,
            depth,
            windows,
        )?;
        features.extend(new_features);
    }

    Ok(features)
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

/// Generate features at a specific depth level
fn generate_depth_features(
    target: &TableRef,
    relationships: &[Relationship],
    agg_primitives: &[Box<dyn Primitive>],
    _trans_primitives: &[Box<dyn Primitive>],
    depth: i32,
    windows: Option<&[String]>,
) -> Result<Vec<FeatureSpec>, PgFeatureError> {
    let mut features = Vec::new();

    // Build list of windows to generate (None = all-time, Some = specific window)
    let window_configs: Vec<Option<String>> = {
        let mut configs = vec![None]; // Always include all-time aggregation
        if let Some(wins) = windows {
            for w in wins {
                configs.push(Some(w.clone()));
            }
        }
        configs
    };

    // For depth 1, generate features from direct children
    if depth == 1 {
        // Find relationships where target is the parent
        let child_rels: Vec<&Relationship> = relationships
            .iter()
            .filter(|r| &r.parent == target)
            .collect();

        for rel in child_rels {
            let child_columns = get_table_columns(&rel.child)?;

            for column in &child_columns {
                // Skip foreign key column
                if column.name == rel.child_column {
                    continue;
                }

                for primitive in agg_primitives {
                    if is_applicable(primitive.as_ref(), column.pg_type) {
                        // Generate feature for each window configuration
                        for window in &window_configs {
                            let base_name = format!(
                                "{}_{}{}",
                                rel.child.name,
                                primitive.name().to_uppercase(),
                                if column.name != rel.child.name {
                                    format!("_{}", column.name)
                                } else {
                                    String::new()
                                }
                            );

                            let feature_name = match window {
                                None => base_name,
                                Some(w) => format!("{}_{}", base_name, window_to_suffix(w)),
                            };

                            let description = match window {
                                None => format!(
                                    "{} of {} from {} grouped by {}",
                                    primitive.description(),
                                    column.name,
                                    rel.child.name,
                                    rel.parent.name
                                ),
                                Some(w) => format!(
                                    "{} of {} from {} in last {} grouped by {}",
                                    primitive.description(),
                                    column.name,
                                    rel.child.name,
                                    w,
                                    rel.parent.name
                                ),
                            };

                            features.push(FeatureSpec {
                                name: feature_name,
                                primitive: primitive.clone(),
                                source_table: rel.child.clone(),
                                source_column: column.name.clone(),
                                depth,
                                description,
                                feature_type: "aggregation".to_string(),
                                window: window.clone(),
                            });
                        }
                    }
                }
            }
        }
    }

    // For depth > 1, generate features by aggregating over existing features
    // This is more complex and requires stacking primitives
    // For MVP, we support depth 1 and 2
    if depth == 2 {
        // Find grandchild relationships (child of child)
        let child_rels: Vec<&Relationship> = relationships
            .iter()
            .filter(|r| &r.parent == target)
            .collect();

        for rel in &child_rels {
            // Find relationships where our child is the parent
            let grandchild_rels: Vec<&Relationship> = relationships
                .iter()
                .filter(|r| r.parent == rel.child)
                .collect();

            for gc_rel in grandchild_rels {
                let gc_columns = get_table_columns(&gc_rel.child)?;

                for column in &gc_columns {
                    if column.name == gc_rel.child_column {
                        continue;
                    }

                    // For depth 2, we apply two aggregations
                    // First aggregate from grandchild to child, then from child to target
                    for inner_prim in agg_primitives {
                        if !is_applicable(inner_prim.as_ref(), column.pg_type) {
                            continue;
                        }

                        // The inner aggregation produces a numeric result
                        for outer_prim in agg_primitives {
                            // Outer primitive must work on numeric types
                            if !is_applicable(outer_prim.as_ref(), PgType::Numeric) {
                                continue;
                            }

                            // Generate feature for each window configuration
                            for window in &window_configs {
                                let base_name = format!(
                                    "{}_{}_{}_{}_{}",
                                    rel.child.name,
                                    outer_prim.name().to_uppercase(),
                                    gc_rel.child.name,
                                    inner_prim.name().to_uppercase(),
                                    column.name
                                );

                                let feature_name = match window {
                                    None => base_name,
                                    Some(w) => format!("{}_{}", base_name, window_to_suffix(w)),
                                };

                                let description = match window {
                                    None => format!(
                                        "{} of ({} of {} from {}) from {} grouped by {}",
                                        outer_prim.description(),
                                        inner_prim.description(),
                                        column.name,
                                        gc_rel.child.name,
                                        rel.child.name,
                                        target.name
                                    ),
                                    Some(w) => format!(
                                        "{} of ({} of {} from {}) from {} in last {} grouped by {}",
                                        outer_prim.description(),
                                        inner_prim.description(),
                                        column.name,
                                        gc_rel.child.name,
                                        rel.child.name,
                                        w,
                                        target.name
                                    ),
                                };

                                features.push(FeatureSpec {
                                    name: feature_name,
                                    primitive: outer_prim.clone(),
                                    source_table: gc_rel.child.clone(),
                                    source_column: column.name.clone(),
                                    depth,
                                    description,
                                    feature_type: "aggregation".to_string(),
                                    window: window.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(features)
}

/// Store feature definitions in the catalog
fn store_feature_definitions(
    project_name: &str,
    features: &[FeatureSpec],
) -> Result<(), PgFeatureError> {
    // Clear existing definitions for this project
    let delete_sql = format!(
        "DELETE FROM pgft.feature_definitions WHERE project_name = '{}'",
        escape_literal(project_name)
    );
    Spi::run(&delete_sql)?;

    // Insert new definitions
    for feature in features {
        let insert_sql = format!(
            r#"
            INSERT INTO pgft.feature_definitions
                (project_name, feature_name, feature_type, primitive, base_column, source_table, depth, description)
            VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, '{}')
            "#,
            escape_literal(project_name),
            escape_literal(&feature.name),
            escape_literal(&feature.feature_type),
            escape_literal(feature.primitive.name()),
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
        Some(wins) => {
            let escaped: Vec<String> = wins.iter().map(|w| escape_literal(w)).collect();
            format!("ARRAY['{}']", escaped.join("','"))
        }
        None => "NULL".to_string(),
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

    let count = Spi::connect_mut(|client| {
        // Drop existing table if exists
        let drop_sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            output_ref.sql_identifier()
        );
        Spi::run(&drop_sql)?;

        // Build the generate_series based on parameters
        let series_clause = if let Some(end) = end_time {
            format!(
                "generate_series('{}'::timestamptz, '{}'::timestamptz, '{}'::interval)",
                start_time.to_iso_string(),
                end.to_iso_string(),
                escape_literal(window_size)
            )
        } else if let Some(n) = num_windows {
            format!(
                "generate_series('{}'::timestamptz, '{}' + ({} - 1) * '{}'::interval, '{}'::interval)",
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

        // Create the cutoffs table with cross join
        let create_sql = format!(
            "CREATE TABLE {} AS
             SELECT t.{} AS {}, s AS cutoff_time
             FROM {} t
             CROSS JOIN {} s",
            output_ref.sql_identifier(),
            target_id,
            target_id,
            target_ref.sql_identifier(),
            series_clause
        );
        Spi::run(&create_sql)?;

        // Get row count
        let count_sql = format!("SELECT COUNT(*) FROM {}", output_ref.sql_identifier());
        let mut result = client.select(&count_sql, None, &[])?;
        let count = if let Some(row) = result.next() {
            row.get::<i64>(1)?.unwrap_or(0)
        } else {
            0
        };

        Ok(count)
    })?;

    Ok(count)
}

/// Auto-generate cutoff times from data
///
/// Queries the min/max timestamps from time-indexed tables and generates
/// evenly-spaced cutoff times at the specified interval.
fn generate_auto_cutoffs(
    target: &TableRef,
    target_id: &str,
    interval: &str,
    relationships: &[Relationship],
    time_indexes: &HashMap<String, String>,
    output_table: &str,
) -> Result<i64, PgFeatureError> {
    use crate::schema::quote_identifier;

    // Find min/max timestamps from all time-indexed child tables
    let mut union_parts: Vec<String> = Vec::new();

    for rel in relationships {
        if &rel.parent == target {
            if let Some(time_col) = time_indexes.get(&rel.child.qualified_name()) {
                union_parts.push(format!(
                    "SELECT MIN({}) AS min_ts, MAX({}) AS max_ts FROM {}",
                    quote_identifier(time_col),
                    quote_identifier(time_col),
                    rel.child.sql_identifier()
                ));
            }
        }
    }

    if union_parts.is_empty() {
        return Err(PgFeatureError::InvalidParameter(
            "No time-indexed tables found for auto-cutoff generation. Use set_time_index() first."
                .to_string(),
        ));
    }

    let output_ref = parse_table_ref(output_table)?;

    Spi::connect_mut(|client| {
        // Find global min/max
        let bounds_sql = format!(
            "SELECT MIN(min_ts), MAX(max_ts) FROM ({})",
            union_parts.join(" UNION ALL ")
        );

        let result = client.select(&bounds_sql, None, &[])?;
        let (min_ts, max_ts): (Option<TimestampWithTimeZone>, Option<TimestampWithTimeZone>) =
            if let Some(row) = result.into_iter().next() {
                (row.get(1)?, row.get(2)?)
            } else {
                (None, None)
            };

        let (min_ts, max_ts) = match (min_ts, max_ts) {
            (Some(min), Some(max)) => (min, max),
            _ => {
                return Err(PgFeatureError::InvalidParameter(
                    "Could not determine time bounds from data".to_string(),
                ))
            }
        };

        // Drop existing table if exists
        let drop_sql = format!(
            "DROP TABLE IF EXISTS {} CASCADE",
            output_ref.sql_identifier()
        );
        Spi::run(&drop_sql)?;

        // Create the cutoffs table with cross join
        let create_sql = format!(
            "CREATE TABLE {} AS
             SELECT t.{} AS {}, s AS cutoff_time
             FROM {} t
             CROSS JOIN generate_series('{}'::timestamptz, '{}'::timestamptz, '{}'::interval) s",
            output_ref.sql_identifier(),
            quote_identifier(target_id),
            quote_identifier(target_id),
            target.sql_identifier(),
            min_ts.to_iso_string(),
            max_ts.to_iso_string(),
            escape_literal(interval)
        );
        Spi::run(&create_sql)?;

        // Get row count
        let count_sql = format!("SELECT COUNT(*) FROM {}", output_ref.sql_identifier());
        let mut result = client.select(&count_sql, None, &[])?;
        let count = if let Some(row) = result.next() {
            row.get::<i64>(1)?.unwrap_or(0)
        } else {
            0
        };

        Ok(count)
    })
}

/// Generate auxiliary views (_train and _live) for ML workflows
///
/// Creates:
/// - {output}_train: View of main table excluding cutoff_time (ML-ready training data)
/// - {output}_live: View with cutoff = now() for inference
fn generate_auxiliary_views(
    target: &TableRef,
    target_id: &str,
    features: &[FeatureSpec],
    relationships: &[Relationship],
    time_indexes: &HashMap<String, String>,
    output: &TableRef,
) -> Result<(), PgFeatureError> {
    use crate::schema::quote_identifier;

    // Get list of feature columns (everything except cutoff_time)
    let mut feature_columns: Vec<String> = vec![quote_identifier(target_id)];
    for feature in features {
        feature_columns.push(quote_identifier(&feature.name));
    }
    let columns_sql = feature_columns.join(", ");

    // Create _train view (excludes cutoff_time)
    let train_view = format!("{}_train", output.name);
    let train_sql = format!(
        "CREATE OR REPLACE VIEW {}.{} AS SELECT {} FROM {}",
        quote_identifier(&output.schema),
        quote_identifier(&train_view),
        columns_sql,
        output.sql_identifier()
    );
    Spi::run(&train_sql)?;

    // Create _live view (uses now() as cutoff)
    // This requires regenerating the query with now() instead of cutoff table
    let live_view = format!("{}_live", output.name);

    // Build the live query using LATERAL joins with now() as cutoff
    let live_sql = generate_live_view_sql(
        target,
        target_id,
        features,
        relationships,
        time_indexes,
        &output.schema,
        &live_view,
    )?;
    Spi::run(&live_sql)?;

    Ok(())
}

/// Generate SQL for the _live view (cutoff = now())
fn generate_live_view_sql(
    target: &TableRef,
    target_id: &str,
    features: &[FeatureSpec],
    relationships: &[Relationship],
    time_indexes: &HashMap<String, String>,
    output_schema: &str,
    view_name: &str,
) -> Result<String, PgFeatureError> {
    use crate::schema::quote_identifier;

    // Collect child tables and their features grouped by window
    let mut child_features: HashMap<String, Vec<&FeatureSpec>> = HashMap::new();
    for feature in features {
        if feature.feature_type == "aggregation" && feature.depth == 1 {
            let key = format!(
                "{}:{}",
                feature.source_table.qualified_name(),
                feature.window.as_deref().unwrap_or("all")
            );
            child_features.entry(key).or_default().push(feature);
        }
    }

    // Build SELECT columns
    let mut select_cols: Vec<String> = vec![format!("t.{}", quote_identifier(target_id))];

    // Add transform features
    for feature in features {
        if feature.feature_type == "transform" {
            select_cols.push(format!(
                "{} AS {}",
                feature
                    .primitive
                    .to_sql(&format!("t.{}", quote_identifier(&feature.source_column))),
                quote_identifier(&feature.name)
            ));
        }
    }

    // Build LATERAL joins for aggregations
    let mut lateral_joins: Vec<String> = Vec::new();
    let mut lateral_idx = 0;

    // Group features by (source_table, window)
    let mut grouped: HashMap<(String, Option<String>), Vec<&FeatureSpec>> = HashMap::new();
    for feature in features {
        if feature.feature_type == "aggregation" && feature.depth == 1 {
            let key = (
                feature.source_table.qualified_name(),
                feature.window.clone(),
            );
            grouped.entry(key).or_default().push(feature);
        }
    }

    for ((table_name, window), feats) in grouped {
        let table_ref = parse_table_ref(&table_name)?;
        let rel = relationships
            .iter()
            .find(|r| r.child == table_ref)
            .ok_or_else(|| {
                PgFeatureError::TableNotFound(format!("Relationship for {} not found", table_name))
            })?;

        let time_col = time_indexes.get(&table_name);
        let alias = format!("agg_{}", lateral_idx);

        // Build aggregation expressions
        let mut agg_exprs: Vec<String> = Vec::new();
        for feat in &feats {
            let expr = if feat.primitive.name() == "count" {
                "COUNT(*)".to_string()
            } else {
                feat.primitive
                    .to_sql(&format!("child.{}", quote_identifier(&feat.source_column)))
            };
            agg_exprs.push(format!("{} AS {}", expr, quote_identifier(&feat.name)));
        }

        // Build WHERE clause
        let mut where_parts: Vec<String> = vec![format!(
            "child.{} = t.{}",
            quote_identifier(&rel.child_column),
            quote_identifier(&rel.parent_column)
        )];

        if let Some(tc) = time_col {
            // Add cutoff filter (now())
            where_parts.push(format!("child.{} <= now()", quote_identifier(tc)));

            // Add window filter if specified
            if let Some(w) = &window {
                where_parts.push(format!(
                    "child.{} > (now() - '{}'::interval)",
                    quote_identifier(tc),
                    escape_literal(w)
                ));
            }
        }

        let lateral_sql = format!(
            "LEFT JOIN LATERAL (\n    SELECT {}\n    FROM {} child\n    WHERE {}\n) {} ON true",
            agg_exprs.join(", "),
            table_ref.sql_identifier(),
            where_parts.join(" AND "),
            alias
        );
        lateral_joins.push(lateral_sql);

        // Add to SELECT with COALESCE for COUNT
        for feat in &feats {
            let col_ref = format!("{}.{}", alias, quote_identifier(&feat.name));
            if feat.primitive.name() == "count" {
                select_cols.push(format!(
                    "COALESCE({}, 0) AS {}",
                    col_ref,
                    quote_identifier(&feat.name)
                ));
            } else {
                select_cols.push(format!("{} AS {}", col_ref, quote_identifier(&feat.name)));
            }
        }

        lateral_idx += 1;
    }

    // Build final SQL
    let sql = format!(
        "CREATE OR REPLACE VIEW {}.{} AS\nSELECT {}\nFROM {} t\n{}",
        quote_identifier(output_schema),
        quote_identifier(view_name),
        select_cols.join(",\n       "),
        target.sql_identifier(),
        lateral_joins.join("\n")
    );

    Ok(sql)
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
            ORDER BY depth, feature_name
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
