//! SQL query generator for feature synthesis
//!
//! One generator renders both the feature matrix and the `_live` view, so a
//! feature's SQL expression is produced the same way for training and serving.
//!
//! Every aggregation feature belongs to a *group* — one relationship path plus
//! one window — and each group becomes one subquery joined to the target:
//! - lateral style (per-entity cutoffs, `now()`): `LEFT JOIN LATERAL (...) ON true`
//! - grouped style (no cutoff, single cutoff): `LEFT JOIN (... GROUP BY key) ON key`

use crate::dfs::FeatureSpec;
use crate::primitives::SqlCtx;
use crate::relationships::Relationship;
use crate::schema::{quote_identifier, TableRef};
use std::collections::HashMap;

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

/// The moment features are computed "as of"
#[derive(Debug, Clone)]
pub enum CutoffSpec {
    /// No cutoff - use all data
    None,
    /// Single cutoff for all entities, as a SQL timestamptz expression
    Single(String),
    /// Per-entity cutoffs from a table
    PerEntity {
        table: TableRef,
        id_column: String,
        time_column: String,
    },
    /// `now()` - used for the `_live` view
    Now,
}

impl CutoffSpec {
    /// SQL expression rows are filtered against, if any
    fn filter_expr(&self) -> Option<String> {
        match self {
            CutoffSpec::None => None,
            CutoffSpec::Single(expr) => Some(expr.clone()),
            CutoffSpec::PerEntity { time_column, .. } => {
                Some(format!("ct.{}", quote_identifier(time_column)))
            }
            CutoffSpec::Now => Some("now()".to_string()),
        }
    }

    /// Reference point for windows and `time_since*` (`now()` without a cutoff)
    fn reference_expr(&self) -> String {
        self.filter_expr().unwrap_or_else(|| "now()".to_string())
    }

    /// Per-entity and live queries correlate per target row
    fn is_lateral(&self) -> bool {
        matches!(self, CutoffSpec::PerEntity { .. } | CutoffSpec::Now)
    }
}

/// SQL generator for feature queries
pub struct SqlGenerator<'a> {
    /// Target table to generate features for
    pub target: &'a TableRef,
    /// Primary key column in target
    pub target_id: &'a str,
    /// Features to generate, in output column order
    pub features: &'a [FeatureSpec],
    /// Time index columns by table
    pub time_indexes: &'a HashMap<TableRef, String>,
}

/// A set of aggregation features sharing a relationship path and window
struct Group<'f> {
    path: &'f [Relationship],
    window: Option<&'f str>,
    features: Vec<&'f FeatureSpec>,
}

fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

fn group_alias(idx: usize) -> String {
    format!("g{}", idx)
}

impl<'a> SqlGenerator<'a> {
    /// Statements that create the feature matrix materialized view
    pub fn matrix_statements(
        &self,
        output: &TableRef,
        cutoff: &CutoffSpec,
        if_exists: IfExists,
    ) -> Vec<String> {
        let mut statements = Vec::new();
        if if_exists == IfExists::Replace {
            statements.push(format!(
                "DROP MATERIALIZED VIEW IF EXISTS {} CASCADE",
                output.sql_identifier()
            ));
        }
        statements.push(format!(
            "CREATE MATERIALIZED VIEW {} AS\n{}",
            output.sql_identifier(),
            self.select_query(cutoff)
        ));
        statements
    }

    /// `{output}_train`: the matrix without `cutoff_time`
    pub fn train_view_sql(&self, output: &TableRef) -> String {
        let mut cols = vec![quote_identifier(self.target_id)];
        cols.extend(self.features.iter().map(|f| quote_identifier(&f.name)));
        format!(
            "CREATE VIEW {}.{} AS SELECT {} FROM {}",
            quote_identifier(&output.schema),
            quote_identifier(&format!("{}_train", output.name)),
            cols.join(", "),
            output.sql_identifier()
        )
    }

    /// `{output}_live`: the same features with cutoff `now()`
    pub fn live_view_sql(&self, output: &TableRef) -> String {
        format!(
            "CREATE VIEW {}.{} AS\n{}",
            quote_identifier(&output.schema),
            quote_identifier(&format!("{}_live", output.name)),
            self.select_query(&CutoffSpec::Now)
        )
    }

    /// The SELECT computing every feature as of `cutoff`
    pub fn select_query(&self, cutoff: &CutoffSpec) -> String {
        let filter = cutoff.filter_expr();
        let reference = cutoff.reference_expr();
        let groups = self.groups();

        // Feature -> index of its group
        let mut group_of: HashMap<*const FeatureSpec, usize> = HashMap::new();
        for (i, g) in groups.iter().enumerate() {
            for f in &g.features {
                group_of.insert(*f as *const FeatureSpec, i);
            }
        }

        let mut select = Vec::new();
        match cutoff {
            CutoffSpec::PerEntity {
                id_column,
                time_column,
                ..
            } => {
                select.push(format!(
                    "ct.{} AS {}",
                    quote_identifier(id_column),
                    quote_identifier(self.target_id)
                ));
                select.push(format!(
                    "ct.{} AS cutoff_time",
                    quote_identifier(time_column)
                ));
            }
            _ => select.push(format!(
                "t.{} AS {}",
                quote_identifier(self.target_id),
                quote_identifier(self.target_id)
            )),
        }

        for feature in self.features {
            let name = quote_identifier(&feature.name);
            let expr = match group_of.get(&(feature as *const FeatureSpec)) {
                None => {
                    let col = format!("t.{}", quote_identifier(&feature.source_column));
                    feature.primitive.to_sql(&SqlCtx {
                        column: &col,
                        time: None,
                        cutoff: &reference,
                    })
                }
                Some(&i) => {
                    let col = format!("{}.{}", group_alias(i), name);
                    if feature.inner.is_none() && feature.primitive.is_count() {
                        format!("COALESCE({}, 0)", col)
                    } else {
                        col
                    }
                }
            };
            select.push(format!("{} AS {}", expr, name));
        }

        let mut sql = format!("SELECT\n    {}", select.join(",\n    "));

        match cutoff {
            CutoffSpec::PerEntity {
                table, id_column, ..
            } => {
                sql.push_str(&format!(
                    "\nFROM {} ct\nJOIN {} t ON t.{} = ct.{}",
                    table.sql_identifier(),
                    self.target.sql_identifier(),
                    quote_identifier(self.target_id),
                    quote_identifier(id_column)
                ));
            }
            _ => sql.push_str(&format!("\nFROM {} t", self.target.sql_identifier())),
        }

        for (i, group) in groups.iter().enumerate() {
            sql.push('\n');
            sql.push_str(&self.group_join(
                group,
                &group_alias(i),
                cutoff.is_lateral(),
                filter.as_deref(),
                &reference,
            ));
        }

        // An entity only exists from its own time index on
        if let (Some(f), Some(tc)) = (&filter, self.time_indexes.get(self.target)) {
            sql.push_str(&format!("\nWHERE t.{} <= {}", quote_identifier(tc), f));
        }

        sql
    }

    /// Aggregation features grouped by (path, window), in first-appearance order
    fn groups(&self) -> Vec<Group<'a>> {
        let mut groups: Vec<Group<'a>> = Vec::new();
        for feature in self.features.iter().filter(|f| !f.path.is_empty()) {
            let window = feature.window.as_deref();
            let existing = groups.iter_mut().find(|g| {
                g.window == window
                    && g.path.len() == feature.path.len()
                    && g.path.iter().zip(&feature.path).all(|(a, b)| a.id == b.id)
            });
            match existing {
                Some(g) => g.features.push(feature),
                None => groups.push(Group {
                    path: &feature.path,
                    window,
                    features: vec![feature],
                }),
            }
        }
        groups
    }

    /// Time/window predicates for rows of `table` aliased `alias`
    fn row_filters(
        &self,
        table: &TableRef,
        alias: &str,
        filter: Option<&str>,
        reference: &str,
        window: Option<&str>,
    ) -> Vec<String> {
        let mut preds = Vec::new();
        if let Some(tc) = self.time_indexes.get(table) {
            let col = format!("{}.{}", alias, quote_identifier(tc));
            if let Some(f) = filter {
                preds.push(format!("{} <= {}", col, f));
            }
            if let Some(w) = window {
                preds.push(format!(
                    "{} > ({} - '{}'::interval)",
                    col,
                    reference,
                    escape_literal(w)
                ));
            }
        }
        preds
    }

    fn time_col(&self, table: &TableRef, alias: &str) -> Option<String> {
        self.time_indexes
            .get(table)
            .map(|tc| format!("{}.{}", alias, quote_identifier(tc)))
    }

    /// Render a primitive against a column of `table` aliased `alias`
    fn render(
        &self,
        prim: crate::primitives::Prim,
        table: &TableRef,
        alias: &str,
        column: &str,
        reference: &str,
    ) -> String {
        let col = format!("{}.{}", alias, quote_identifier(column));
        let time = self.time_col(table, alias);
        prim.to_sql(&SqlCtx {
            column: &col,
            time: time.as_deref(),
            cutoff: reference,
        })
    }

    /// The join for one aggregation group
    fn group_join(
        &self,
        group: &Group,
        alias: &str,
        lateral: bool,
        filter: Option<&str>,
        reference: &str,
    ) -> String {
        let rel = &group.path[0];
        let child = &rel.child;
        let fk = format!("child.{}", quote_identifier(&rel.child_column));
        let parent_key = format!("t.{}", quote_identifier(&rel.parent_column));

        let mut child_preds = Vec::new();
        if lateral {
            child_preds.push(format!("{} = {}", fk, parent_key));
        }
        child_preds.extend(self.row_filters(child, "child", filter, reference, group.window));
        let child_where = if child_preds.is_empty() {
            String::new()
        } else {
            format!("\n    WHERE {}", child_preds.join("\n      AND "))
        };

        let body = if group.path.len() == 1 {
            let mut cols = Vec::new();
            if !lateral {
                cols.push(format!("{} AS __key", fk));
            }
            for f in &group.features {
                cols.push(format!(
                    "{} AS {}",
                    self.render(f.primitive, child, "child", &f.source_column, reference),
                    quote_identifier(&f.name)
                ));
            }
            let mut body = format!(
                "SELECT {}\n    FROM {} child{}",
                cols.join(",\n           "),
                child.sql_identifier(),
                child_where
            );
            if !lateral {
                body.push_str(&format!("\n    GROUP BY {}", fk));
            }
            body
        } else {
            self.depth2_body(group, lateral, filter, reference, &fk, &child_where)
        };

        if lateral {
            format!("LEFT JOIN LATERAL (\n    {}\n) {} ON true", body, alias)
        } else {
            format!(
                "LEFT JOIN (\n    {}\n) {} ON {}.__key = {}",
                body, alias, alias, parent_key
            )
        }
    }

    /// Two-level aggregation: grandchild rows per child row, then per target row
    fn depth2_body(
        &self,
        group: &Group,
        lateral: bool,
        filter: Option<&str>,
        reference: &str,
        fk: &str,
        child_where: &str,
    ) -> String {
        let child = &group.path[0].child;
        let gc_rel = &group.path[1];
        let gc = &gc_rel.child;
        let child_key = format!("child.{}", quote_identifier(&gc_rel.parent_column));

        // Inner aggregations, deduplicated across outer primitives
        let mut inner: Vec<(String, String)> = Vec::new(); // (sql, alias)
        let mut inner_alias_of = Vec::new();
        for f in &group.features {
            let prim = f.inner.expect("depth-2 feature has an inner primitive");
            let sql = self.render(prim, gc, "gc", &f.source_column, reference);
            let idx = match inner.iter().position(|(s, _)| *s == sql) {
                Some(i) => i,
                None => {
                    inner.push((sql, format!("i{}", inner.len())));
                    inner.len() - 1
                }
            };
            inner_alias_of.push(idx);
        }

        let mut on = vec![format!(
            "gc.{} = {}",
            quote_identifier(&gc_rel.child_column),
            child_key
        )];
        on.extend(self.row_filters(gc, "gc", filter, reference, group.window));

        let mut inner_cols = Vec::new();
        if !lateral {
            inner_cols.push(format!("{} AS __key", fk));
        }
        inner_cols.extend(inner.iter().map(|(sql, a)| format!("{} AS {}", sql, a)));
        let mut group_by = vec![child_key.clone()];
        if !lateral {
            group_by.insert(0, fk.to_string());
        }
        let inner_sql = format!(
            "SELECT {}\n        FROM {} child\n        LEFT JOIN {} gc ON {}{}\n        GROUP BY {}",
            inner_cols.join(", "),
            child.sql_identifier(),
            gc.sql_identifier(),
            on.join(" AND "),
            child_where.replace("\n    ", "\n        "),
            group_by.join(", ")
        );

        let mut outer_cols = Vec::new();
        if !lateral {
            outer_cols.push("c.__key".to_string());
        }
        for (f, idx) in group.features.iter().zip(&inner_alias_of) {
            let col = format!("c.{}", inner[*idx].1);
            outer_cols.push(format!(
                "{} AS {}",
                f.primitive.to_sql(&SqlCtx {
                    column: &col,
                    time: None,
                    cutoff: reference,
                }),
                quote_identifier(&f.name)
            ));
        }
        let mut body = format!(
            "SELECT {}\n    FROM (\n        {}\n    ) c",
            outer_cols.join(",\n           "),
            inner_sql
        );
        if !lateral {
            body.push_str("\n    GROUP BY c.__key");
        }
        body
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::{get_primitives_by_name, Prim};

    fn prim(name: &str) -> Prim {
        get_primitives_by_name(&[name.to_string()], true)
            .or_else(|_| get_primitives_by_name(&[name.to_string()], false))
            .unwrap()[0]
    }

    fn rel(id: i64, parent: &str, pcol: &str, child: &str, ccol: &str) -> Relationship {
        Relationship {
            id,
            parent: TableRef::new("public", parent),
            parent_column: pcol.to_string(),
            child: TableRef::new("public", child),
            child_column: ccol.to_string(),
            name: None,
        }
    }

    fn feature(
        name: &str,
        p: &str,
        path: Vec<Relationship>,
        column: &str,
        window: Option<&str>,
    ) -> FeatureSpec {
        let depth = path.len() as i32;
        FeatureSpec {
            name: name.to_string(),
            primitive: prim(p),
            inner: None,
            source_table: path
                .last()
                .map(|r| r.child.clone())
                .unwrap_or_else(|| TableRef::new("public", "customers")),
            source_column: column.to_string(),
            path,
            depth,
            description: String::new(),
            window: window.map(str::to_string),
        }
    }

    fn orders() -> Relationship {
        rel(1, "customers", "id", "orders", "customer_id")
    }

    fn items() -> Relationship {
        rel(2, "orders", "order_id", "items", "order_id")
    }

    fn time_indexes() -> HashMap<TableRef, String> {
        let mut m = HashMap::new();
        m.insert(TableRef::new("public", "orders"), "ordered_at".to_string());
        m.insert(TableRef::new("public", "items"), "added_at".to_string());
        m
    }

    fn features() -> Vec<FeatureSpec> {
        let mut d2 = feature(
            "orders_MEAN_items_SUM_qty",
            "mean",
            vec![orders(), items()],
            "qty",
            None,
        );
        d2.inner = Some(prim("sum"));
        vec![
            feature("signup_YEAR", "year", vec![], "signup", None),
            feature("orders_COUNT", "count", vec![orders()], "customer_id", None),
            feature("orders_SUM_amount", "sum", vec![orders()], "amount", None),
            feature(
                "orders_SUM_amount_30d",
                "sum",
                vec![orders()],
                "amount",
                Some("30 days"),
            ),
            d2,
        ]
    }

    fn per_entity() -> CutoffSpec {
        CutoffSpec::PerEntity {
            table: TableRef::new("pgft", "_cutoffs_p"),
            id_column: "id".to_string(),
            time_column: "cutoff_time".to_string(),
        }
    }

    fn generator<'a>(
        target: &'a TableRef,
        features: &'a [FeatureSpec],
        ti: &'a HashMap<TableRef, String>,
    ) -> SqlGenerator<'a> {
        SqlGenerator {
            target,
            target_id: "id",
            features,
            time_indexes: ti,
        }
    }

    /// Column list of a SELECT, in order
    fn output_columns(sql: &str) -> Vec<String> {
        let head = sql.split("\nFROM").next().unwrap();
        head.lines()
            .skip(1)
            .map(|l| {
                let l = l.trim().trim_end_matches(',');
                l.rsplit(" AS ").next().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn test_if_exists_from_str() {
        assert_eq!(IfExists::from_str("error"), Some(IfExists::Error));
        assert_eq!(IfExists::from_str("replace"), Some(IfExists::Replace));
        assert_eq!(IfExists::from_str("invalid"), None);
    }

    #[test]
    fn test_matrix_and_live_share_aggregation_expressions() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let g = generator(&target, &feats, &ti);
        let matrix = g.select_query(&per_entity());
        let live = g.select_query(&CutoffSpec::Now);

        // Same expressions, only the cutoff reference differs
        assert_eq!(
            matrix.replace("ct.cutoff_time", "now()").replace(
                "SELECT\n    ct.id AS id,\n    now() AS cutoff_time,",
                "SELECT\n    t.id AS id,"
            ),
            live.replace(
                "\nFROM public.customers t",
                "\nFROM pgft._cutoffs_p ct\nJOIN public.customers t ON t.id = ct.id"
            )
        );
        // COUNT is a row count over the key, in both
        assert!(matrix.contains("COUNT(child.customer_id) AS \"orders_COUNT\""));
        assert!(!matrix.contains("COUNT(*)"));
    }

    #[test]
    fn test_column_order_follows_features() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let g = generator(&target, &feats, &ti);
        let expected = [
            "id",
            "cutoff_time",
            "\"signup_YEAR\"",
            "\"orders_COUNT\"",
            "\"orders_SUM_amount\"",
            "\"orders_SUM_amount_30d\"",
            "\"orders_MEAN_items_SUM_qty\"",
        ];
        assert_eq!(output_columns(&g.select_query(&per_entity())), expected);
        let live: Vec<_> = expected
            .iter()
            .copied()
            .filter(|c| *c != "cutoff_time")
            .collect();
        assert_eq!(output_columns(&g.select_query(&CutoffSpec::Now)), live);
        // Deterministic across runs
        assert_eq!(g.select_query(&per_entity()), g.select_query(&per_entity()));
    }

    #[test]
    fn test_depth2_features_are_computed() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let sql = generator(&target, &feats, &ti).select_query(&per_entity());
        assert!(
            sql.contains("AVG(c.i0) AS \"orders_MEAN_items_SUM_qty\""),
            "{sql}"
        );
        assert!(sql.contains("SUM(gc.qty) AS i0"), "{sql}");
        assert!(sql.contains("LEFT JOIN public.items gc ON gc.order_id = child.order_id"));
        // cutoff applies to both levels
        assert!(sql.contains("gc.added_at <= ct.cutoff_time"));
        assert!(sql.contains("child.ordered_at <= ct.cutoff_time"));
    }

    #[test]
    fn test_windows_apply_without_per_entity_cutoff() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let g = generator(&target, &feats, &ti);

        let single = g.select_query(&CutoffSpec::Single(
            "'2024-04-01T00:00:00Z'::timestamptz".to_string(),
        ));
        assert!(single.contains(
            "child.ordered_at > ('2024-04-01T00:00:00Z'::timestamptz - '30 days'::interval)"
        ));
        assert!(single.contains("child.ordered_at <= '2024-04-01T00:00:00Z'::timestamptz"));

        let none = g.select_query(&CutoffSpec::None);
        assert!(none.contains("child.ordered_at > (now() - '30 days'::interval)"));
        assert!(!none.contains("<= now()"));
    }

    #[test]
    fn test_grouped_style_joins_on_parent_column() {
        let target = TableRef::new("public", "customers");
        let r = rel(1, "customers", "customer_code", "orders", "cust_code");
        let feats = vec![feature("orders_SUM_amount", "sum", vec![r], "amount", None)];
        let ti = HashMap::new();
        let sql = generator(&target, &feats, &ti).select_query(&CutoffSpec::None);
        assert!(sql.contains("child.cust_code AS __key"), "{sql}");
        assert!(sql.contains("GROUP BY child.cust_code"));
        assert!(sql.contains(") g0 ON g0.__key = t.customer_code"), "{sql}");
        assert!(!sql.contains("LATERAL"));
    }

    #[test]
    fn test_count_coalesced_in_both_styles() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let g = generator(&target, &feats, &ti);
        for cutoff in [CutoffSpec::None, per_entity()] {
            let sql = g.select_query(&cutoff);
            assert!(sql.contains("COALESCE(g0.\"orders_COUNT\", 0) AS \"orders_COUNT\""));
            assert!(sql.contains("g0.\"orders_SUM_amount\" AS \"orders_SUM_amount\""));
        }
    }

    #[test]
    fn test_target_time_index_limits_rows_to_existing_entities() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let mut ti = time_indexes();
        ti.insert(target.clone(), "signup".to_string());
        let g = generator(&target, &feats, &ti);
        assert!(g
            .select_query(&per_entity())
            .ends_with("\nWHERE t.signup <= ct.cutoff_time"));
        assert!(!g.select_query(&CutoffSpec::None).contains("WHERE t.signup"));
    }

    #[test]
    fn test_time_since_transform_uses_cutoff() {
        let target = TableRef::new("public", "customers");
        let feats = vec![feature(
            "signup_TIME_SINCE",
            "time_since",
            vec![],
            "signup",
            None,
        )];
        let ti = HashMap::new();
        let sql = generator(&target, &feats, &ti).select_query(&per_entity());
        assert!(sql.contains("EXTRACT(EPOCH FROM (ct.cutoff_time - t.signup))"));
    }

    #[test]
    fn test_train_view_lists_features_in_order() {
        let target = TableRef::new("public", "customers");
        let feats = features();
        let ti = time_indexes();
        let sql = generator(&target, &feats, &ti).train_view_sql(&TableRef::new("ml", "f"));
        assert_eq!(
            sql,
            "CREATE VIEW ml.f_train AS SELECT id, \"signup_YEAR\", \"orders_COUNT\", \
             \"orders_SUM_amount\", \"orders_SUM_amount_30d\", \"orders_MEAN_items_SUM_qty\" \
             FROM ml.f"
        );
    }
}
