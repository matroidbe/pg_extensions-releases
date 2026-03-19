//! SQL query generator for feature synthesis
//!
//! Generates SQL queries to compute features using CTEs,
//! handling cutoff times and output types (table/view/materialized view).

use crate::dfs::FeatureSpec;
use crate::relationships::Relationship;
use crate::schema::{quote_identifier, TableRef};

/// How to handle existing output object
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IfExists {
    Error,
    Replace,
}

impl IfExists {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "error" => Some(IfExists::Error),
            "replace" => Some(IfExists::Replace),
            _ => None,
        }
    }
}

/// Cutoff time specification
#[derive(Debug, Clone)]
pub enum CutoffSpec {
    /// No cutoff - use all data
    None,
    /// Single cutoff time for all entities
    Single(String), // Timestamp as SQL literal
    /// Per-entity cutoffs from a table
    PerEntity {
        table: TableRef,
        id_column: String,
        time_column: String,
    },
}

/// SQL generator for feature queries
pub struct SqlGenerator {
    /// Target table to generate features for
    pub target: TableRef,
    /// Primary key column in target
    pub target_id: String,
    /// Features to generate
    pub features: Vec<FeatureSpec>,
    /// Relationships in the schema
    pub relationships: Vec<Relationship>,
    /// Time index columns by table
    pub time_indexes: std::collections::HashMap<String, String>,
    /// Cutoff time specification
    pub cutoff: CutoffSpec,
    /// Output materialized view reference
    pub output: TableRef,
    /// How to handle existing output
    pub if_exists: IfExists,
}

impl SqlGenerator {
    /// Generate the complete SQL statement(s) to create the feature matrix
    pub fn generate(&self) -> Vec<String> {
        let mut statements = Vec::new();

        // Handle existing objects if needed
        if self.if_exists == IfExists::Replace {
            statements.push(self.generate_drop_statement());
        }

        // Generate the main CREATE statement
        let select_query = self.generate_select_query();
        statements.push(self.generate_create_statement(&select_query));

        statements
    }

    /// Generate DROP statement for replacing existing objects
    fn generate_drop_statement(&self) -> String {
        format!(
            "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE",
            self.output.sql_identifier()
        )
    }

    /// Generate CREATE MATERIALIZED VIEW AS SELECT statement
    fn generate_create_statement(&self, select_query: &str) -> String {
        format!(
            "CREATE MATERIALIZED VIEW {} AS {}",
            self.output.sql_identifier(),
            select_query
        )
    }

    /// Generate the SELECT query for feature computation
    pub fn generate_select_query(&self) -> String {
        match &self.cutoff {
            CutoffSpec::None => self.generate_simple_query(),
            CutoffSpec::Single(cutoff) => self.generate_single_cutoff_query(cutoff),
            CutoffSpec::PerEntity {
                table,
                id_column,
                time_column,
            } => self.generate_per_entity_cutoff_query(table, id_column, time_column),
        }
    }

    /// Generate query without cutoff times (simple aggregations)
    fn generate_simple_query(&self) -> String {
        let (ctes, feature_aliases) = self.generate_ctes_and_aliases(None);

        let mut sql = String::new();

        // Add CTEs if any
        if !ctes.is_empty() {
            sql.push_str("WITH ");
            sql.push_str(&ctes.join(",\n"));
            sql.push('\n');
        }

        // Main SELECT
        sql.push_str("SELECT\n");
        sql.push_str(&format!(
            "    t.{} AS {}",
            quote_identifier(&self.target_id),
            quote_identifier(&self.target_id)
        ));

        // Add feature columns
        for alias in &feature_aliases {
            sql.push_str(&format!(",\n    {}", alias));
        }

        // FROM clause
        sql.push_str(&format!("\nFROM {} t", self.target.sql_identifier()));

        // Join CTEs
        for cte_name in self.get_cte_names() {
            sql.push_str(&format!(
                "\nLEFT JOIN {} ON {}.{} = t.{}",
                cte_name, cte_name, self.target_id, self.target_id
            ));
        }

        sql
    }

    /// Generate query with a single cutoff time
    fn generate_single_cutoff_query(&self, cutoff: &str) -> String {
        let (ctes, feature_aliases) = self.generate_ctes_and_aliases(Some(cutoff));

        let mut sql = String::new();

        // Add CTEs
        if !ctes.is_empty() {
            sql.push_str("WITH ");
            sql.push_str(&ctes.join(",\n"));
            sql.push('\n');
        }

        // Main SELECT
        sql.push_str("SELECT\n");
        sql.push_str(&format!(
            "    t.{} AS {}",
            quote_identifier(&self.target_id),
            quote_identifier(&self.target_id)
        ));

        // Add feature columns
        for alias in &feature_aliases {
            sql.push_str(&format!(",\n    {}", alias));
        }

        // FROM clause
        sql.push_str(&format!("\nFROM {} t", self.target.sql_identifier()));

        // Join CTEs
        for cte_name in self.get_cte_names() {
            sql.push_str(&format!(
                "\nLEFT JOIN {} ON {}.{} = t.{}",
                cte_name, cte_name, self.target_id, self.target_id
            ));
        }

        sql
    }

    /// Generate query with per-entity cutoff times using LATERAL joins
    fn generate_per_entity_cutoff_query(
        &self,
        cutoff_table: &TableRef,
        cutoff_id_column: &str,
        cutoff_time_column: &str,
    ) -> String {
        use std::collections::HashMap;

        let mut sql = String::new();

        // Group features by (child_table, window) for LATERAL subqueries
        // Each unique combination gets its own LATERAL subquery
        let mut feature_groups: HashMap<(String, Option<String>), Vec<&FeatureSpec>> =
            HashMap::new();
        for feature in &self.features {
            if feature.feature_type == "aggregation" && feature.depth == 1 {
                let key = (
                    feature.source_table.qualified_name(),
                    feature.window.clone(),
                );
                feature_groups.entry(key).or_default().push(feature);
            }
        }

        // Main SELECT
        sql.push_str("SELECT\n");
        sql.push_str(&format!(
            "    ct.{} AS {}",
            quote_identifier(cutoff_id_column),
            quote_identifier(&self.target_id)
        ));
        sql.push_str(&format!(
            ",\n    ct.{} AS cutoff_time",
            quote_identifier(cutoff_time_column)
        ));

        // Note: We don't include ct.* to avoid column name conflicts.
        // Users should join back to cutoff table for labels if needed.

        // Generate transform features on target table
        let transform_features = self.get_transform_features_for_table(&self.target);
        for feature in &transform_features {
            let col_ref = format!("t.{}", quote_identifier(&feature.source_column));
            let expr = feature.primitive.to_sql(&col_ref);
            sql.push_str(&format!(
                ",\n    {} AS {}",
                expr,
                quote_identifier(&feature.name)
            ));
        }

        // Generate LATERAL subqueries for aggregations grouped by (table, window)
        let mut lateral_idx = 0;
        let mut lateral_aliases: Vec<(String, Vec<&FeatureSpec>)> = Vec::new();

        for ((_table_name, _window), features) in &feature_groups {
            let lateral_alias = format!("agg_{}", lateral_idx);
            lateral_aliases.push((lateral_alias.clone(), features.clone()));

            // Add aggregation feature columns
            for feature in features {
                // Only use COALESCE with 0 for COUNT (always returns integer)
                let col_expr = if feature.primitive.name() == "count" {
                    format!(
                        "COALESCE({}.{}, 0)",
                        lateral_alias,
                        quote_identifier(&feature.name)
                    )
                } else {
                    format!("{}.{}", lateral_alias, quote_identifier(&feature.name))
                };
                sql.push_str(&format!(
                    ",\n    {} AS {}",
                    col_expr,
                    quote_identifier(&feature.name)
                ));
            }

            lateral_idx += 1;
        }

        // FROM cutoff times table
        sql.push_str(&format!("\nFROM {} ct", cutoff_table.sql_identifier()));

        // JOIN target table
        sql.push_str(&format!(
            "\nJOIN {} t ON t.{} = ct.{}",
            self.target.sql_identifier(),
            quote_identifier(&self.target_id),
            quote_identifier(cutoff_id_column)
        ));

        // Add LATERAL joins for each (table, window) group
        lateral_idx = 0;
        for ((_table_name, window), features) in &feature_groups {
            if features.is_empty() {
                continue;
            }

            let child_table = &features[0].source_table;
            let lateral_alias = format!("agg_{}", lateral_idx);
            let rel = self.get_relationship_to_child(child_table);

            if let Some(relationship) = rel {
                let time_col = self.time_indexes.get(&child_table.qualified_name());

                // Build LATERAL subquery
                sql.push_str("\nLEFT JOIN LATERAL (");
                sql.push_str("\n    SELECT");

                for (j, feature) in features.iter().enumerate() {
                    let col_ref = format!("child.{}", quote_identifier(&feature.source_column));
                    let expr = feature.primitive.to_sql(&col_ref);
                    if j > 0 {
                        sql.push(',');
                    }
                    sql.push_str(&format!(
                        "\n        {} AS {}",
                        expr,
                        quote_identifier(&feature.name)
                    ));
                }

                sql.push_str(&format!(
                    "\n    FROM {} child",
                    child_table.sql_identifier()
                ));
                sql.push_str(&format!(
                    "\n    WHERE child.{} = ct.{}",
                    quote_identifier(&relationship.child_column),
                    quote_identifier(cutoff_id_column)
                ));

                // Add cutoff time filter if time index exists
                if let Some(time_column) = time_col {
                    sql.push_str(&format!(
                        "\n      AND child.{} <= ct.{}",
                        quote_identifier(time_column),
                        quote_identifier(cutoff_time_column)
                    ));

                    // Add window filter if specified
                    if let Some(w) = window {
                        sql.push_str(&format!(
                            "\n      AND child.{} > (ct.{} - '{}'::interval)",
                            quote_identifier(time_column),
                            quote_identifier(cutoff_time_column),
                            w.replace('\'', "''")
                        ));
                    }
                }

                sql.push_str(&format!("\n) {} ON true", lateral_alias));
            }

            lateral_idx += 1;
        }

        sql
    }

    /// Generate CTEs for aggregation features
    fn generate_ctes_and_aliases(&self, cutoff: Option<&str>) -> (Vec<String>, Vec<String>) {
        let mut ctes = Vec::new();
        let mut feature_aliases = Vec::new();

        // Transform features on target table (no CTE needed)
        let transform_features = self.get_transform_features_for_table(&self.target);
        for feature in &transform_features {
            let col_ref = format!("t.{}", quote_identifier(&feature.source_column));
            let expr = feature.primitive.to_sql(&col_ref);
            feature_aliases.push(format!("{} AS {}", expr, quote_identifier(&feature.name)));
        }

        // Aggregation features need CTEs
        let child_tables = self.get_child_tables();
        for child_table in &child_tables {
            let agg_features = self.get_aggregation_features_for_child(child_table);
            if agg_features.is_empty() {
                continue;
            }

            let rel = self.get_relationship_to_child(child_table);
            if rel.is_none() {
                continue;
            }
            let relationship = rel.unwrap();

            let cte_name = format!("{}_agg", child_table.name);
            let time_col = self.time_indexes.get(&child_table.qualified_name());

            // Build CTE
            let mut cte = format!(
                "{} AS (\n    SELECT\n        child.{} AS {}",
                cte_name,
                quote_identifier(&relationship.child_column),
                quote_identifier(&self.target_id)
            );

            for feature in &agg_features {
                let col_ref = format!("child.{}", quote_identifier(&feature.source_column));
                let expr = feature.primitive.to_sql(&col_ref);
                cte.push_str(&format!(
                    ",\n        {} AS {}",
                    expr,
                    quote_identifier(&feature.name)
                ));

                // Add to feature aliases
                // Only use COALESCE with 0 for COUNT (always returns integer)
                // Other aggregations (MIN, MAX, SUM, etc.) preserve NULL for missing rows
                let alias = if feature.primitive.name() == "count" {
                    format!(
                        "COALESCE({}.{}, 0) AS {}",
                        cte_name,
                        quote_identifier(&feature.name),
                        quote_identifier(&feature.name)
                    )
                } else {
                    format!(
                        "{}.{} AS {}",
                        cte_name,
                        quote_identifier(&feature.name),
                        quote_identifier(&feature.name)
                    )
                };
                feature_aliases.push(alias);
            }

            cte.push_str(&format!(
                "\n    FROM {} child",
                child_table.sql_identifier()
            ));

            // Add cutoff filter if specified
            if let (Some(cutoff_time), Some(time_column)) = (cutoff, time_col) {
                cte.push_str(&format!(
                    "\n    WHERE child.{} <= {}::timestamptz",
                    quote_identifier(time_column),
                    cutoff_time
                ));
            }

            cte.push_str(&format!(
                "\n    GROUP BY child.{}",
                quote_identifier(&relationship.child_column)
            ));
            cte.push_str("\n)");

            ctes.push(cte);
        }

        (ctes, feature_aliases)
    }

    /// Get list of CTE names for joining
    fn get_cte_names(&self) -> Vec<String> {
        let child_tables = self.get_child_tables();
        child_tables
            .iter()
            .filter(|t| !self.get_aggregation_features_for_child(t).is_empty())
            .map(|t| format!("{}_agg", t.name))
            .collect()
    }

    /// Get unique child tables from features
    fn get_child_tables(&self) -> Vec<TableRef> {
        let mut tables: Vec<TableRef> = self
            .features
            .iter()
            .filter(|f| f.primitive.is_aggregation() && f.source_table != self.target)
            .map(|f| f.source_table.clone())
            .collect();
        tables.sort_by_key(|a| a.qualified_name());
        tables.dedup_by(|a, b| a.qualified_name() == b.qualified_name());
        tables
    }

    /// Get transform features for a specific table
    fn get_transform_features_for_table(&self, table: &TableRef) -> Vec<&FeatureSpec> {
        self.features
            .iter()
            .filter(|f| !f.primitive.is_aggregation() && &f.source_table == table)
            .collect()
    }

    /// Get aggregation features for a specific child table
    fn get_aggregation_features_for_child(&self, child: &TableRef) -> Vec<&FeatureSpec> {
        self.features
            .iter()
            .filter(|f| f.primitive.is_aggregation() && &f.source_table == child)
            .collect()
    }

    /// Get relationship from target to a child table
    fn get_relationship_to_child(&self, child: &TableRef) -> Option<&Relationship> {
        self.relationships
            .iter()
            .find(|r| r.parent == self.target && &r.child == child)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_if_exists_from_str() {
        assert_eq!(IfExists::from_str("error"), Some(IfExists::Error));
        assert_eq!(IfExists::from_str("replace"), Some(IfExists::Replace));
        assert_eq!(IfExists::from_str("invalid"), None);
    }
}
