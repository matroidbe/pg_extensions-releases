//! Aggregate processor — incremental aggregation with state tables and rollups
//!
//! Maintains running aggregates (sum, count, avg, min, max) in a state table
//! using PostgreSQL's INSERT...ON CONFLICT DO UPDATE. Each batch updates the
//! accumulators in a single SQL statement — no per-record loops.
//!
//! Optionally creates time-bucketed rollup tables for multi-resolution analytics.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::collections::HashMap;

/// Parsed aggregate function
#[derive(Debug, Clone)]
enum AggFunc {
    /// sum(expr) — running sum accumulator
    Sum(String),
    /// count(*) — running count accumulator
    Count,
    /// avg(expr) — uses _sum + _count accumulators, computes ratio
    Avg(String),
    /// min(expr) — running minimum via LEAST
    Min(String),
    /// max(expr) — running maximum via GREATEST
    Max(String),
}

/// A parsed aggregate column definition
#[derive(Debug, Clone)]
struct ParsedColumn {
    /// User-visible output column name
    name: String,
    /// Parsed aggregate function
    func: AggFunc,
}

/// What the processor emits downstream
#[derive(Debug, Clone, PartialEq)]
enum EmitMode {
    /// Emit the updated aggregate rows
    UpdatedRows,
    /// Pass input records through unchanged
    Input,
    /// Emit nothing (sink-only)
    None,
}

#[cfg_attr(test, derive(Debug))]
pub struct AggregateProcessor {
    state_table: String,
    columns: Vec<ParsedColumn>,
    emit: EmitMode,
    /// SQL for the main state table upsert (includes input CTE + _agg CTE + INSERT)
    upsert_sql: String,
    /// SQL for each rollup table upsert
    rollup_sqls: Vec<String>,
    /// Rollup table names (for ensure_state_tables)
    rollup_tables: Vec<(String, String)>, // (table_name, interval)
}

// =============================================================================
// Parsing
// =============================================================================

/// Parse an aggregate expression like "sum(total)", "count(*)", "avg(amount)"
fn parse_aggregate(expr: &str) -> Result<AggFunc, String> {
    let expr = expr.trim();

    if expr == "count(*)" {
        return Ok(AggFunc::Count);
    }

    // Match function(inner_expr) pattern
    if let Some(paren) = expr.find('(') {
        let func_name = expr[..paren].trim().to_lowercase();
        let inner = expr[paren + 1..]
            .strip_suffix(')')
            .ok_or_else(|| format!("Malformed aggregate expression: {}", expr))?
            .trim()
            .to_string();

        if inner.is_empty() {
            return Err(format!("Empty argument in aggregate: {}", expr));
        }

        match func_name.as_str() {
            "sum" => Ok(AggFunc::Sum(inner)),
            "count" => Ok(AggFunc::Count), // count(expr) treated same as count(*)
            "avg" => Ok(AggFunc::Avg(inner)),
            "min" => Ok(AggFunc::Min(inner)),
            "max" => Ok(AggFunc::Max(inner)),
            _ => Err(format!(
                "Unknown aggregate function '{}'. Supported: sum, count, avg, min, max",
                func_name
            )),
        }
    } else {
        Err(format!(
            "Invalid aggregate expression '{}'. Expected format: func(expr)",
            expr
        ))
    }
}

impl ParsedColumn {
    /// DDL column definitions for the state table
    fn state_table_columns(&self) -> Vec<(String, String)> {
        // Returns: (column_name, type_with_default)
        match &self.func {
            AggFunc::Sum(_) => vec![(self.name.clone(), "NUMERIC NOT NULL DEFAULT 0".to_string())],
            AggFunc::Count => {
                vec![(self.name.clone(), "BIGINT NOT NULL DEFAULT 0".to_string())]
            }
            AggFunc::Avg(_) => vec![
                (
                    format!("_sum_{}", self.name),
                    "NUMERIC NOT NULL DEFAULT 0".to_string(),
                ),
                (
                    format!("_count_{}", self.name),
                    "BIGINT NOT NULL DEFAULT 0".to_string(),
                ),
                (self.name.clone(), "NUMERIC".to_string()),
            ],
            AggFunc::Min(_) => vec![(self.name.clone(), "NUMERIC".to_string())],
            AggFunc::Max(_) => vec![(self.name.clone(), "NUMERIC".to_string())],
        }
    }

    /// SELECT expressions for the _agg CTE (batch-level aggregation)
    fn batch_agg_exprs(&self) -> Vec<String> {
        match &self.func {
            AggFunc::Sum(expr) => vec![format!("sum({}) AS _batch_{}", expr, self.name)],
            AggFunc::Count => vec![format!("count(*) AS _batch_{}", self.name)],
            AggFunc::Avg(expr) => vec![
                format!("sum({}) AS _batch__sum_{}", expr, self.name),
                format!("count(*) AS _batch__count_{}", self.name),
            ],
            AggFunc::Min(expr) => vec![format!("min({}) AS _batch_{}", expr, self.name)],
            AggFunc::Max(expr) => vec![format!("max({}) AS _batch_{}", expr, self.name)],
        }
    }

    /// Column names for the INSERT statement
    fn insert_columns(&self) -> Vec<String> {
        match &self.func {
            AggFunc::Sum(_) | AggFunc::Count | AggFunc::Min(_) | AggFunc::Max(_) => {
                vec![self.name.clone()]
            }
            AggFunc::Avg(_) => vec![
                format!("_sum_{}", self.name),
                format!("_count_{}", self.name),
                self.name.clone(),
            ],
        }
    }

    /// Value expressions for the INSERT...SELECT FROM _agg
    fn insert_values(&self) -> Vec<String> {
        match &self.func {
            AggFunc::Sum(_) | AggFunc::Count | AggFunc::Min(_) | AggFunc::Max(_) => {
                vec![format!("_batch_{}", self.name)]
            }
            AggFunc::Avg(_) => vec![
                format!("_batch__sum_{}", self.name),
                format!("_batch__count_{}", self.name),
                format!(
                    "_batch__sum_{}::numeric / NULLIF(_batch__count_{}, 0)",
                    self.name, self.name
                ),
            ],
        }
    }

    /// ON CONFLICT DO UPDATE SET expressions
    fn conflict_updates(&self, table: &str) -> Vec<String> {
        match &self.func {
            AggFunc::Sum(_) | AggFunc::Count => vec![format!(
                "{n} = {t}.{n} + EXCLUDED.{n}",
                n = self.name,
                t = table
            )],
            AggFunc::Avg(_) => vec![
                format!(
                    "_sum_{n} = {t}._sum_{n} + EXCLUDED._sum_{n}",
                    n = self.name,
                    t = table
                ),
                format!(
                    "_count_{n} = {t}._count_{n} + EXCLUDED._count_{n}",
                    n = self.name,
                    t = table
                ),
                format!(
                    "{n} = ({t}._sum_{n} + EXCLUDED._sum_{n})::numeric / \
                     NULLIF({t}._count_{n} + EXCLUDED._count_{n}, 0)",
                    n = self.name,
                    t = table
                ),
            ],
            AggFunc::Min(_) => vec![format!(
                "{n} = LEAST({t}.{n}, EXCLUDED.{n})",
                n = self.name,
                t = table
            )],
            AggFunc::Max(_) => vec![format!(
                "{n} = GREATEST({t}.{n}, EXCLUDED.{n})",
                n = self.name,
                t = table
            )],
        }
    }
}

// =============================================================================
// Rollup helpers
// =============================================================================

/// Map a rollup interval string to a short table name suffix
fn interval_to_suffix(interval: &str) -> String {
    match interval.trim() {
        "1 minute" | "1 minutes" => "1m".to_string(),
        "5 minutes" => "5m".to_string(),
        "10 minutes" => "10m".to_string(),
        "15 minutes" => "15m".to_string(),
        "30 minutes" => "30m".to_string(),
        "1 hour" | "1 hours" => "1h".to_string(),
        "1 day" | "1 days" => "1d".to_string(),
        "1 week" | "1 weeks" => "1w".to_string(),
        "1 month" | "1 months" => "1mo".to_string(),
        "1 year" | "1 years" => "1y".to_string(),
        other => other.replace(' ', ""),
    }
}

/// Generate the SQL expression for time bucketing.
/// Uses date_trunc for standard intervals, epoch-based for arbitrary ones.
fn bucket_expr(interval: &str) -> String {
    match interval.trim() {
        "1 minute" | "1 minutes" => "date_trunc('minute', now())".to_string(),
        "1 hour" | "1 hours" => "date_trunc('hour', now())".to_string(),
        "1 day" | "1 days" => "date_trunc('day', now())".to_string(),
        "1 week" | "1 weeks" => "date_trunc('week', now())".to_string(),
        "1 month" | "1 months" => "date_trunc('month', now())".to_string(),
        "1 year" | "1 years" => "date_trunc('year', now())".to_string(),
        _ => {
            // Epoch-based bucketing for arbitrary intervals (e.g., "5 minutes")
            format!(
                "to_timestamp(floor(extract(epoch from now()) / \
                 extract(epoch from interval '{}')) * \
                 extract(epoch from interval '{}'))",
                interval, interval
            )
        }
    }
}

// =============================================================================
// SQL generation
// =============================================================================

/// Build the main upsert SQL for the state table.
/// Returns (upsert_sql, has_returning) — has_returning is true when emit=updated_rows.
fn build_upsert_sql(
    cte: &str,
    group_by: &str,
    columns: &[ParsedColumn],
    state_table: &str,
    emit: &EmitMode,
) -> String {
    // _agg CTE: aggregate the batch by group key
    let agg_exprs: Vec<String> = columns.iter().flat_map(|c| c.batch_agg_exprs()).collect();

    let agg_cte = format!(
        "_agg AS (SELECT ({})::text AS group_key, {} FROM _batch GROUP BY ({})::text)",
        group_by,
        agg_exprs.join(", "),
        group_by
    );

    // INSERT columns and values
    let mut insert_cols = vec!["group_key".to_string()];
    let mut insert_vals = vec!["group_key".to_string()];
    for col in columns {
        insert_cols.extend(col.insert_columns());
        insert_vals.extend(col.insert_values());
    }
    insert_cols.push("updated_at".to_string());
    insert_vals.push("now()".to_string());

    // ON CONFLICT DO UPDATE SET
    let mut conflict_sets: Vec<String> = columns
        .iter()
        .flat_map(|c| c.conflict_updates(state_table))
        .collect();
    conflict_sets.push("updated_at = now()".to_string());

    // RETURNING clause (only for emit=updated_rows)
    let returning = if *emit == EmitMode::UpdatedRows {
        let return_cols: Vec<String> = std::iter::once("'group_key', group_key".to_string())
            .chain(columns.iter().map(|c| format!("'{}', {}", c.name, c.name)))
            .chain(std::iter::once("'updated_at', updated_at".to_string()))
            .collect();
        format!(" RETURNING jsonb_build_object({})", return_cols.join(", "))
    } else {
        String::new()
    };

    if *emit == EmitMode::UpdatedRows {
        // Wrap in a CTE to get jsonb_agg of RETURNING rows
        format!(
            "{cte}, {agg_cte}, \
             _upsert AS (\
               INSERT INTO {table} ({cols}) \
               SELECT {vals} FROM _agg \
               ON CONFLICT (group_key) DO UPDATE SET {sets}{returning}\
             ) \
             SELECT COALESCE(jsonb_agg(_upsert), '[]'::jsonb) FROM _upsert",
            cte = cte.trim(),
            agg_cte = agg_cte,
            table = state_table,
            cols = insert_cols.join(", "),
            vals = insert_vals.join(", "),
            sets = conflict_sets.join(", "),
            returning = returning,
        )
    } else {
        format!(
            "{cte}, {agg_cte} \
             INSERT INTO {table} ({cols}) \
             SELECT {vals} FROM _agg \
             ON CONFLICT (group_key) DO UPDATE SET {sets}",
            cte = cte.trim(),
            agg_cte = agg_cte,
            table = state_table,
            cols = insert_cols.join(", "),
            vals = insert_vals.join(", "),
            sets = conflict_sets.join(", "),
        )
    }
}

/// Build the upsert SQL for a rollup table.
/// Same structure as main upsert but adds a `bucket` column.
fn build_rollup_sql(
    cte: &str,
    group_by: &str,
    columns: &[ParsedColumn],
    rollup_table: &str,
    interval: &str,
) -> String {
    let agg_exprs: Vec<String> = columns.iter().flat_map(|c| c.batch_agg_exprs()).collect();
    let bucket = bucket_expr(interval);

    let agg_cte = format!(
        "_agg AS (SELECT ({})::text AS group_key, {} FROM _batch GROUP BY ({})::text)",
        group_by,
        agg_exprs.join(", "),
        group_by
    );

    let mut insert_cols = vec!["group_key".to_string(), "bucket".to_string()];
    let mut insert_vals = vec!["group_key".to_string(), bucket];
    for col in columns {
        insert_cols.extend(col.insert_columns());
        insert_vals.extend(col.insert_values());
    }
    insert_cols.push("updated_at".to_string());
    insert_vals.push("now()".to_string());

    let mut conflict_sets: Vec<String> = columns
        .iter()
        .flat_map(|c| c.conflict_updates(rollup_table))
        .collect();
    conflict_sets.push("updated_at = now()".to_string());

    format!(
        "{cte}, {agg_cte} \
         INSERT INTO {table} ({cols}) \
         SELECT {vals} FROM _agg \
         ON CONFLICT (group_key, bucket) DO UPDATE SET {sets}",
        cte = cte.trim(),
        agg_cte = agg_cte,
        table = rollup_table,
        cols = insert_cols.join(", "),
        vals = insert_vals.join(", "),
        sets = conflict_sets.join(", "),
    )
}

// =============================================================================
// State table DDL
// =============================================================================

/// Generate CREATE TABLE DDL for the main state table
fn state_table_ddl(table: &str, columns: &[ParsedColumn]) -> String {
    let mut col_defs = vec!["group_key TEXT NOT NULL PRIMARY KEY".to_string()];
    for col in columns {
        for (name, type_def) in col.state_table_columns() {
            col_defs.push(format!("{} {}", name, type_def));
        }
    }
    col_defs.push("updated_at TIMESTAMPTZ NOT NULL DEFAULT now()".to_string());

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table,
        col_defs.join(", ")
    )
}

/// Generate CREATE TABLE DDL for a rollup table (adds bucket column + composite PK)
fn rollup_table_ddl(table: &str, columns: &[ParsedColumn]) -> String {
    let mut col_defs = vec![
        "group_key TEXT NOT NULL".to_string(),
        "bucket TIMESTAMPTZ NOT NULL".to_string(),
    ];
    for col in columns {
        for (name, type_def) in col.state_table_columns() {
            col_defs.push(format!("{} {}", name, type_def));
        }
    }
    col_defs.push("updated_at TIMESTAMPTZ NOT NULL DEFAULT now()".to_string());
    col_defs.push("PRIMARY KEY (group_key, bucket)".to_string());

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table,
        col_defs.join(", ")
    )
}

// =============================================================================
// AggregateProcessor implementation
// =============================================================================

impl AggregateProcessor {
    /// Create an aggregate processor.
    /// `state_table` must be the fully qualified name (e.g., "pgstreams.revenue_by_region").
    pub fn new(
        group_by: &str,
        column_defs: &HashMap<String, String>,
        state_table: &str,
        rollups: &[String],
        emit: &str,
        cte: &str,
    ) -> Result<Self, String> {
        if column_defs.is_empty() {
            return Err("Aggregate processor requires at least one column".to_string());
        }

        // Parse aggregate expressions (sort keys for deterministic SQL generation)
        let mut col_names: Vec<&String> = column_defs.keys().collect();
        col_names.sort();

        let mut columns = Vec::new();
        for name in col_names {
            let expr = &column_defs[name];
            let func = parse_aggregate(expr).map_err(|e| format!("Column '{}': {}", name, e))?;
            columns.push(ParsedColumn {
                name: name.clone(),
                func,
            });
        }

        let emit_mode = match emit {
            "input" => EmitMode::Input,
            "none" => EmitMode::None,
            _ => EmitMode::UpdatedRows,
        };

        // Build main upsert SQL
        let upsert_sql = build_upsert_sql(cte, group_by, &columns, state_table, &emit_mode);

        // Build rollup SQLs
        let mut rollup_tables = Vec::new();
        let mut rollup_sqls = Vec::new();
        for interval in rollups {
            let suffix = interval_to_suffix(interval);
            let rollup_table = format!("{}_{}", state_table, suffix);
            rollup_sqls.push(build_rollup_sql(
                cte,
                group_by,
                &columns,
                &rollup_table,
                interval,
            ));
            rollup_tables.push((rollup_table, interval.clone()));
        }

        Ok(Self {
            state_table: state_table.to_string(),
            columns,
            emit: emit_mode,
            upsert_sql,
            rollup_sqls,
            rollup_tables,
        })
    }

    /// Create state tables (main + rollups). Called during pipeline compilation.
    pub fn ensure_state_tables(&self) -> Result<(), String> {
        // Main state table
        let ddl = state_table_ddl(&self.state_table, &self.columns);
        Spi::run(&ddl).map_err(|e| {
            format!(
                "Failed to create aggregate state table '{}': {}",
                self.state_table, e
            )
        })?;

        // Index on updated_at for cleanup
        let idx = format!(
            "CREATE INDEX IF NOT EXISTS {}_updated_idx ON {} (updated_at)",
            self.state_table.replace('.', "_").replace("pgstreams_", ""),
            self.state_table
        );
        let _ = Spi::run(&idx);

        // Rollup tables
        for (rollup_table, _interval) in &self.rollup_tables {
            let ddl = rollup_table_ddl(rollup_table, &self.columns);
            Spi::run(&ddl)
                .map_err(|e| format!("Failed to create rollup table '{}': {}", rollup_table, e))?;

            let idx = format!(
                "CREATE INDEX IF NOT EXISTS {}_updated_idx ON {} (updated_at)",
                rollup_table.replace('.', "_").replace("pgstreams_", ""),
                rollup_table
            );
            let _ = Spi::run(&idx);
        }

        Ok(())
    }

    /// Get the generated upsert SQL (for testing)
    #[cfg(test)]
    pub fn upsert_sql(&self) -> &str {
        &self.upsert_sql
    }

    /// Get rollup SQLs (for testing)
    #[cfg(test)]
    pub fn rollup_sqls(&self) -> &[String] {
        &self.rollup_sqls
    }
}

impl Processor for AggregateProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch.clone());
        let batch_jsonb = pgrx::JsonB(batch_json);

        // Keep a clone of the raw batch for rollups and potential input emit
        let batch_raw = batch.clone();

        let result_batch = match self.emit {
            EmitMode::UpdatedRows => {
                // Run upsert with RETURNING → jsonb_agg
                let result =
                    Spi::get_one_with_args::<pgrx::JsonB>(&self.upsert_sql, &[batch_jsonb.into()]);

                let jsonb = result.map_err(|e| {
                    format!("Aggregate upsert failed on '{}': {}", self.state_table, e)
                })?;

                match jsonb {
                    Some(jb) => match jb.0.as_array() {
                        Some(arr) => arr.clone(),
                        None => vec![],
                    },
                    None => vec![],
                }
            }
            EmitMode::Input => {
                // Run upsert without RETURNING
                let _ = Spi::run_with_args(&self.upsert_sql, &[batch_jsonb.into()]);
                batch_raw.clone()
            }
            EmitMode::None => {
                // Run upsert without RETURNING
                let _ = Spi::run_with_args(&self.upsert_sql, &[batch_jsonb.into()]);
                vec![]
            }
        };

        // Run rollup upserts — create a fresh JsonB per rollup
        for rollup_sql in &self.rollup_sqls {
            let rjb = pgrx::JsonB(serde_json::Value::Array(batch_raw.clone()));
            let _ = Spi::run_with_args(rollup_sql, &[rjb.into()]);
        }

        Ok(result_batch)
    }

    fn name(&self) -> &str {
        "aggregate"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch_cte;

    fn make_columns(defs: &[(&str, &str)]) -> HashMap<String, String> {
        defs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_parse_aggregate_sum() {
        let f = parse_aggregate("sum(total)").unwrap();
        assert!(matches!(f, AggFunc::Sum(ref e) if e == "total"));
    }

    #[test]
    fn test_parse_aggregate_count() {
        let f = parse_aggregate("count(*)").unwrap();
        assert!(matches!(f, AggFunc::Count));
    }

    #[test]
    fn test_parse_aggregate_avg() {
        let f = parse_aggregate("avg(amount)").unwrap();
        assert!(matches!(f, AggFunc::Avg(ref e) if e == "amount"));
    }

    #[test]
    fn test_parse_aggregate_min_max() {
        let f = parse_aggregate("min(price)").unwrap();
        assert!(matches!(f, AggFunc::Min(ref e) if e == "price"));

        let f = parse_aggregate("max(price)").unwrap();
        assert!(matches!(f, AggFunc::Max(ref e) if e == "price"));
    }

    #[test]
    fn test_parse_aggregate_unknown_func() {
        let err = parse_aggregate("median(x)").unwrap_err();
        assert!(err.contains("Unknown aggregate function"));
    }

    #[test]
    fn test_parse_aggregate_malformed() {
        let err = parse_aggregate("not_a_func").unwrap_err();
        assert!(err.contains("Invalid aggregate expression"));
    }

    #[test]
    fn test_state_table_ddl_sum_count() {
        let columns = vec![
            ParsedColumn {
                name: "revenue".to_string(),
                func: AggFunc::Sum("total".to_string()),
            },
            ParsedColumn {
                name: "order_count".to_string(),
                func: AggFunc::Count,
            },
        ];
        let ddl = state_table_ddl("pgstreams.test_agg", &columns);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pgstreams.test_agg"));
        assert!(ddl.contains("group_key TEXT NOT NULL PRIMARY KEY"));
        assert!(ddl.contains("revenue NUMERIC NOT NULL DEFAULT 0"));
        assert!(ddl.contains("order_count BIGINT NOT NULL DEFAULT 0"));
        assert!(ddl.contains("updated_at TIMESTAMPTZ"));
    }

    #[test]
    fn test_state_table_ddl_avg() {
        let columns = vec![ParsedColumn {
            name: "avg_order".to_string(),
            func: AggFunc::Avg("total".to_string()),
        }];
        let ddl = state_table_ddl("pgstreams.test_agg", &columns);
        assert!(ddl.contains("_sum_avg_order NUMERIC NOT NULL DEFAULT 0"));
        assert!(ddl.contains("_count_avg_order BIGINT NOT NULL DEFAULT 0"));
        assert!(ddl.contains("avg_order NUMERIC"));
    }

    #[test]
    fn test_rollup_table_ddl() {
        let columns = vec![ParsedColumn {
            name: "total".to_string(),
            func: AggFunc::Sum("amount".to_string()),
        }];
        let ddl = rollup_table_ddl("pgstreams.test_agg_1h", &columns);
        assert!(ddl.contains("group_key TEXT NOT NULL"));
        assert!(ddl.contains("bucket TIMESTAMPTZ NOT NULL"));
        assert!(ddl.contains("total NUMERIC NOT NULL DEFAULT 0"));
        assert!(ddl.contains("PRIMARY KEY (group_key, bucket)"));
    }

    #[test]
    fn test_upsert_sql_generation() {
        let cols = make_columns(&[("revenue", "sum(total)"), ("order_count", "count(*)")]);
        let proc = AggregateProcessor::new(
            "region",
            &cols,
            "pgstreams.rev_by_region",
            &[],
            "updated_rows",
            batch_cte(),
        )
        .unwrap();

        let sql = proc.upsert_sql();
        // Should contain input CTE + _agg CTE + _upsert CTE + SELECT jsonb_agg
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("_agg AS"));
        assert!(sql.contains("(region)::text AS group_key"));
        assert!(sql.contains("sum(total) AS _batch_revenue"));
        assert!(sql.contains("count(*) AS _batch_order_count"));
        assert!(sql.contains("INSERT INTO pgstreams.rev_by_region"));
        assert!(sql.contains("ON CONFLICT (group_key) DO UPDATE SET"));
        assert!(sql.contains("RETURNING jsonb_build_object"));
        assert!(sql.contains("jsonb_agg"));
    }

    #[test]
    fn test_upsert_sql_no_returning_for_none_emit() {
        let cols = make_columns(&[("total", "sum(amount)")]);
        let proc = AggregateProcessor::new(
            "category",
            &cols,
            "pgstreams.totals",
            &[],
            "none",
            batch_cte(),
        )
        .unwrap();

        let sql = proc.upsert_sql();
        assert!(!sql.contains("RETURNING"));
        assert!(!sql.contains("jsonb_agg"));
        assert!(sql.contains("INSERT INTO pgstreams.totals"));
    }

    #[test]
    fn test_avg_accumulator_sql() {
        let cols = make_columns(&[("avg_order", "avg(total)")]);
        let proc = AggregateProcessor::new(
            "region",
            &cols,
            "pgstreams.test",
            &[],
            "updated_rows",
            batch_cte(),
        )
        .unwrap();

        let sql = proc.upsert_sql();
        // Should have internal accumulators
        assert!(sql.contains("_sum_avg_order"));
        assert!(sql.contains("_count_avg_order"));
        // Should compute avg from accumulators in conflict update
        assert!(sql.contains("NULLIF"));
    }

    #[test]
    fn test_min_max_sql() {
        let cols = make_columns(&[("min_price", "min(price)"), ("max_price", "max(price)")]);
        let proc = AggregateProcessor::new(
            "category",
            &cols,
            "pgstreams.test",
            &[],
            "none",
            batch_cte(),
        )
        .unwrap();

        let sql = proc.upsert_sql();
        assert!(sql.contains("LEAST"));
        assert!(sql.contains("GREATEST"));
    }

    #[test]
    fn test_rollup_sql_generation() {
        let cols = make_columns(&[("revenue", "sum(total)")]);
        let proc = AggregateProcessor::new(
            "region",
            &cols,
            "pgstreams.rev",
            &["5 minutes".to_string(), "1 hour".to_string()],
            "none",
            batch_cte(),
        )
        .unwrap();

        let rollups = proc.rollup_sqls();
        assert_eq!(rollups.len(), 2);

        // 5-minute rollup uses epoch-based bucketing
        assert!(rollups[0].contains("pgstreams.rev_5m"));
        assert!(rollups[0].contains("ON CONFLICT (group_key, bucket)"));
        assert!(rollups[0].contains("extract(epoch from"));

        // 1-hour rollup uses date_trunc
        assert!(rollups[1].contains("pgstreams.rev_1h"));
        assert!(rollups[1].contains("date_trunc('hour', now())"));
    }

    #[test]
    fn test_interval_to_suffix() {
        assert_eq!(interval_to_suffix("5 minutes"), "5m");
        assert_eq!(interval_to_suffix("1 hour"), "1h");
        assert_eq!(interval_to_suffix("1 day"), "1d");
        assert_eq!(interval_to_suffix("1 month"), "1mo");
        assert_eq!(interval_to_suffix("1 year"), "1y");
    }

    #[test]
    fn test_bucket_expr_standard() {
        assert_eq!(bucket_expr("1 hour"), "date_trunc('hour', now())");
        assert_eq!(bucket_expr("1 day"), "date_trunc('day', now())");
        assert_eq!(bucket_expr("1 month"), "date_trunc('month', now())");
    }

    #[test]
    fn test_bucket_expr_custom() {
        let expr = bucket_expr("5 minutes");
        assert!(expr.contains("extract(epoch from"));
        assert!(expr.contains("5 minutes"));
    }

    #[test]
    fn test_empty_batch_passthrough() {
        let cols = make_columns(&[("total", "sum(amount)")]);
        let proc =
            AggregateProcessor::new("key", &cols, "pgstreams.test", &[], "none", batch_cte())
                .unwrap();
        let result = proc.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_empty_columns_rejected() {
        let cols: HashMap<String, String> = HashMap::new();
        let result =
            AggregateProcessor::new("key", &cols, "pgstreams.test", &[], "none", batch_cte());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one column"));
    }
}
