//! Pure filter logic for the index path.
//!
//! Two responsibilities, both Postgres-free and unit-testable:
//!
//! 1. [`build_where_clause`] — turn a JSON filter into a tier-1 SQL `WHERE`
//!    fragment over `delta.files` (file-level pruning via `add.stats`).
//! 2. [`eval_row`] — apply the *exact* same filter to a decoded parquet row
//!    (tier 1 only narrows the file set; the reader still does the row test).
//!
//! Filter grammar (see `design/pg_delta/indexing.md` §"SQL UX"):
//! ```json
//! {"city": "BE",                                  // equality
//!  "ts":   {"gte": "2024-06-01", "lt": "..."},   // range
//!  "amount": {"gt": 1000}}
//! ```

use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;

/// Per-column metadata the SQL builder needs.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    /// Postgres cast type for typed comparisons, e.g. `timestamptz`, `numeric`,
    /// `bigint`, `text`.
    pub sql_type: String,
    /// True if this column is a Delta partition key (stored in
    /// `partition_values`, exact per file) rather than a stat column.
    pub is_partition: bool,
}

/// Column name → metadata, derived by the caller from a table's
/// `schema_snapshot` + `partition_keys`.
pub type TableSchema = HashMap<String, ColumnMeta>;

#[derive(Debug, Clone, Copy, PartialEq)]
enum Op {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

struct Pred {
    op: Op,
    value: Value,
}

/// Map a Delta `schema_snapshot` type string to the Postgres cast type used in
/// tier-1 comparisons.
pub fn delta_type_to_pg_cast(delta_type: &str) -> &'static str {
    match delta_type.to_ascii_lowercase().as_str() {
        "string" => "text",
        "long" => "bigint",
        "integer" => "integer",
        "short" => "smallint",
        "byte" => "smallint",
        "float" => "real",
        "double" => "double precision",
        "boolean" => "boolean",
        "date" => "date",
        "timestamp" | "timestamp_ntz" | "timestampntz" => "timestamptz",
        // decimal(p,s) and anything else → numeric / text fallback.
        t if t.starts_with("decimal") => "numeric",
        _ => "text",
    }
}

/// Build a [`TableSchema`] from a stored `schema_snapshot`
/// (`{"col": {"type": "long", ...}}`) plus the list of partition keys.
pub fn table_schema_from_snapshot(
    schema_snapshot: &Value,
    partition_keys: &[String],
) -> TableSchema {
    let mut schema = TableSchema::new();
    if let Some(obj) = schema_snapshot.as_object() {
        for (col, meta) in obj {
            let delta_type = meta.get("type").and_then(Value::as_str).unwrap_or("string");
            schema.insert(
                col.clone(),
                ColumnMeta {
                    sql_type: delta_type_to_pg_cast(delta_type).to_string(),
                    is_partition: partition_keys.iter().any(|k| k == col),
                },
            );
        }
    }
    schema
}

/// Build the tier-1 `WHERE` fragment (the part AND-ed onto the fixed
/// `table_id = .. AND delta_version <= ..` predicate). Returns `"TRUE"` for an
/// empty filter. Errors on an unknown column or malformed spec so typos surface
/// instead of silently scanning everything.
pub fn build_where_clause(filter: &Value, schema: &TableSchema) -> Result<String, String> {
    let obj = match filter {
        Value::Null => return Ok("TRUE".to_string()),
        Value::Object(o) if o.is_empty() => return Ok("TRUE".to_string()),
        Value::Object(o) => o,
        _ => return Err("filter must be a JSON object".to_string()),
    };

    let mut clauses: Vec<String> = Vec::new();
    for (col, spec) in obj {
        let meta = schema
            .get(col)
            .ok_or_else(|| format!("unknown column '{}' in filter", col))?;
        let preds = parse_spec(col, spec)?;
        for pred in preds {
            clauses.push(if meta.is_partition {
                partition_clause(col, &pred, &meta.sql_type)?
            } else {
                stat_clause(col, &pred, &meta.sql_type)?
            });
        }
    }

    if clauses.is_empty() {
        Ok("TRUE".to_string())
    } else {
        Ok(clauses.join(" AND "))
    }
}

/// Tier-2 (parquet row-group) prune: given a row group's per-column
/// `(min, max)` stats, can it possibly contain a row matching `filter`?
///
/// Conservative — returns `true` unless a predicate *proves* no overlap using
/// present, **same-typed** bounds. A type mismatch (e.g. an Int64 timestamp
/// stat vs an ISO-string filter value) is treated as "cannot prove" so a row
/// group is never wrongly pruned; the exact row test still runs afterward.
pub fn row_group_survives(
    filter: &Value,
    col_stats: &HashMap<String, (Option<Value>, Option<Value>)>,
) -> bool {
    let obj = match filter {
        Value::Object(o) => o,
        _ => return true,
    };

    for (col, spec) in obj {
        let preds = match parse_spec(col, spec) {
            Ok(p) => p,
            Err(_) => continue, // malformed → don't prune here
        };
        let (min, max) = match col_stats.get(col) {
            Some(mm) => mm,
            None => continue, // no stats for this column (or partition col)
        };
        for pred in &preds {
            if pred_prunes(&pred.op, &pred.value, min.as_ref(), max.as_ref()) {
                return false;
            }
        }
    }
    true
}

/// Does this predicate prove the row group cannot contain a match?
fn pred_prunes(op: &Op, v: &Value, min: Option<&Value>, max: Option<&Value>) -> bool {
    let below_min =
        |b: Option<&Value>| matches!(b.and_then(|m| typed_cmp(m, v)), Some(Ordering::Greater));
    let above_max =
        |b: Option<&Value>| matches!(b.and_then(|m| typed_cmp(m, v)), Some(Ordering::Less));
    match op {
        // v outside [min,max] → no row can equal v
        Op::Eq => above_max(max) || below_min(min),
        // need some value > v: impossible if max <= v
        Op::Gt => matches!(
            max.and_then(|m| typed_cmp(m, v)),
            Some(Ordering::Less | Ordering::Equal)
        ),
        Op::Gte => above_max(max),
        // need some value < v: impossible if min >= v
        Op::Lt => matches!(
            min.and_then(|m| typed_cmp(m, v)),
            Some(Ordering::Greater | Ordering::Equal)
        ),
        Op::Lte => below_min(min),
    }
}

/// Compare only when both values are the same JSON kind; otherwise `None`.
fn typed_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => json_cmp(a, b),
        (Value::String(_), Value::String(_)) => json_cmp(a, b),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Evaluate the exact filter against a decoded row (JSON object). A missing or
/// null column value fails any predicate, matching SQL `col <op> v` semantics.
pub fn eval_row(filter: &Value, row: &Value) -> bool {
    let obj = match filter {
        Value::Object(o) => o,
        _ => return true, // no/!object filter → keep everything
    };

    for (col, spec) in obj {
        let preds = match parse_spec(col, spec) {
            Ok(p) => p,
            Err(_) => return false,
        };
        let rv = match row.get(col) {
            Some(v) if !v.is_null() => v,
            _ => return false,
        };
        for pred in &preds {
            let ord = match json_cmp(rv, &pred.value) {
                Some(o) => o,
                None => return false,
            };
            let ok = match pred.op {
                Op::Eq => ord == Ordering::Equal,
                Op::Gt => ord == Ordering::Greater,
                Op::Gte => ord != Ordering::Less,
                Op::Lt => ord == Ordering::Less,
                Op::Lte => ord != Ordering::Greater,
            };
            if !ok {
                return false;
            }
        }
    }
    true
}

/// Normalize a column spec into a list of predicates. A scalar is equality; an
/// object maps `eq/gt/gte/lt/lte` keys to operators.
fn parse_spec(col: &str, spec: &Value) -> Result<Vec<Pred>, String> {
    match spec {
        Value::String(_) | Value::Number(_) | Value::Bool(_) => Ok(vec![Pred {
            op: Op::Eq,
            value: spec.clone(),
        }]),
        Value::Object(o) => {
            let mut preds = Vec::new();
            for (k, v) in o {
                let op = match k.as_str() {
                    "eq" => Op::Eq,
                    "gt" => Op::Gt,
                    "gte" => Op::Gte,
                    "lt" => Op::Lt,
                    "lte" => Op::Lte,
                    other => {
                        return Err(format!(
                            "unknown operator '{}' for column '{}' (use eq/gt/gte/lt/lte)",
                            other, col
                        ))
                    }
                };
                preds.push(Pred {
                    op,
                    value: v.clone(),
                });
            }
            Ok(preds)
        }
        _ => Err(format!("invalid filter spec for column '{}'", col)),
    }
}

/// Stat-column clause. Null-tolerant: a file lacking stats for the column is
/// never pruned (it falls through to the parquet footer — `indexing.md` pitfall
/// #1).
fn stat_clause(col: &str, pred: &Pred, ty: &str) -> Result<String, String> {
    let lit = render_literal(&pred.value)?;
    let min = format!("(column_stats->'{}'->>'min')", esc_ident(col));
    let max = format!("(column_stats->'{}'->>'max')", esc_ident(col));
    let clause = match pred.op {
        // file may contain v iff min <= v <= max
        Op::Eq => format!(
            "({min} IS NULL OR {min}::{ty} <= {lit}) AND ({max} IS NULL OR {max}::{ty} >= {lit})"
        ),
        // col > v reachable iff max > v
        Op::Gt => format!("({max} IS NULL OR {max}::{ty} > {lit})"),
        Op::Gte => format!("({max} IS NULL OR {max}::{ty} >= {lit})"),
        // col < v reachable iff min < v
        Op::Lt => format!("({min} IS NULL OR {min}::{ty} < {lit})"),
        Op::Lte => format!("({min} IS NULL OR {min}::{ty} <= {lit})"),
    };
    Ok(clause)
}

/// Partition-column clause. Equality uses JSONB containment (fast via the GIN
/// index); ranges compare the extracted text cast to the column type.
fn partition_clause(col: &str, pred: &Pred, ty: &str) -> Result<String, String> {
    if pred.op == Op::Eq {
        // Partition values are stored as JSON strings, so coerce to string form.
        let as_str = value_to_plain_string(&pred.value);
        let containment = serde_json::json!({ col: as_str }).to_string();
        return Ok(format!(
            "partition_values @> '{}'::jsonb",
            containment.replace('\'', "''")
        ));
    }
    let lit = render_literal(&pred.value)?;
    let extracted = format!("(partition_values->>'{}')", esc_ident(col));
    let op = match pred.op {
        Op::Gt => ">",
        Op::Gte => ">=",
        Op::Lt => "<",
        Op::Lte => "<=",
        Op::Eq => unreachable!(),
    };
    Ok(format!("{extracted}::{ty} {op} {lit}"))
}

/// Render a JSON scalar as a safe SQL literal. Strings are single-quoted and
/// escaped; numbers/bools are rendered directly.
fn render_literal(v: &Value) -> Result<String, String> {
    match v {
        Value::String(s) => Ok(format!("'{}'", s.replace('\'', "''"))),
        Value::Number(n) => Ok(n.to_string()),
        Value::Bool(b) => Ok(b.to_string()),
        _ => Err(format!("filter value must be a scalar, got {}", v)),
    }
}

fn value_to_plain_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Escape a JSON path key for embedding in `->'key'`. Column names are from the
/// table schema, but escape single quotes defensively.
fn esc_ident(s: &str) -> String {
    s.replace('\'', "''")
}

/// Compare two JSON scalars. Numbers (or numeric strings) compare numerically;
/// two RFC3339 timestamps compare as instants (so differing precision/zone
/// spellings like `...Z` vs `...000000Z` or `+00:00` agree); otherwise compare
/// as strings.
fn json_cmp(a: &Value, b: &Value) -> Option<Ordering> {
    if let (Some(x), Some(y)) = (as_f64(a), as_f64(b)) {
        return x.partial_cmp(&y);
    }
    let sa = as_string(a)?;
    let sb = as_string(b)?;
    if let (Ok(ta), Ok(tb)) = (
        chrono::DateTime::parse_from_rfc3339(&sa),
        chrono::DateTime::parse_from_rfc3339(&sb),
    ) {
        return Some(ta.cmp(&tb));
    }
    Some(sa.cmp(&sb))
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn as_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema() -> TableSchema {
        let mut s = TableSchema::new();
        s.insert(
            "city".to_string(),
            ColumnMeta {
                sql_type: "text".to_string(),
                is_partition: true,
            },
        );
        s.insert(
            "ts".to_string(),
            ColumnMeta {
                sql_type: "timestamptz".to_string(),
                is_partition: false,
            },
        );
        s.insert(
            "amount".to_string(),
            ColumnMeta {
                sql_type: "numeric".to_string(),
                is_partition: false,
            },
        );
        s
    }

    #[test]
    fn empty_filter_is_true() {
        assert_eq!(build_where_clause(&json!({}), &schema()).unwrap(), "TRUE");
        assert_eq!(build_where_clause(&Value::Null, &schema()).unwrap(), "TRUE");
    }

    #[test]
    fn partition_equality_uses_containment() {
        let sql = build_where_clause(&json!({"city": "BE"}), &schema()).unwrap();
        assert_eq!(sql, r#"partition_values @> '{"city":"BE"}'::jsonb"#);
    }

    #[test]
    fn stat_range_is_null_tolerant() {
        let sql = build_where_clause(
            &json!({"ts": {"gte": "2024-06-01", "lt": "2024-06-08"}}),
            &schema(),
        )
        .unwrap();
        // gte → max test; lt → min test; both null-tolerant.
        assert!(sql.contains("(column_stats->'ts'->>'max') IS NULL OR (column_stats->'ts'->>'max')::timestamptz >= '2024-06-01'"));
        assert!(sql.contains("(column_stats->'ts'->>'min') IS NULL OR (column_stats->'ts'->>'min')::timestamptz < '2024-06-08'"));
    }

    #[test]
    fn stat_equality_brackets_min_and_max() {
        let sql = build_where_clause(&json!({"amount": 1000}), &schema()).unwrap();
        assert!(sql.contains("(column_stats->'amount'->>'min')::numeric <= 1000"));
        assert!(sql.contains("(column_stats->'amount'->>'max')::numeric >= 1000"));
    }

    #[test]
    fn unknown_column_errors() {
        let err = build_where_clause(&json!({"nope": 1}), &schema()).unwrap_err();
        assert!(err.contains("unknown column 'nope'"));
    }

    #[test]
    fn unknown_operator_errors() {
        let err = build_where_clause(&json!({"amount": {"between": 5}}), &schema()).unwrap_err();
        assert!(err.contains("unknown operator 'between'"));
    }

    #[test]
    fn sql_injection_in_string_is_escaped() {
        let sql = build_where_clause(&json!({"city": "B'E"}), &schema()).unwrap();
        assert!(sql.contains("B''E"));
    }

    #[test]
    fn eval_row_equality_and_range() {
        let filter = json!({"city": "BE", "amount": {"gt": 1000}});
        assert!(eval_row(&filter, &json!({"city": "BE", "amount": 1500})));
        assert!(!eval_row(&filter, &json!({"city": "NL", "amount": 1500}))); // wrong city
        assert!(!eval_row(&filter, &json!({"city": "BE", "amount": 500}))); // amount too low
    }

    #[test]
    fn eval_row_timestamp_string_compare() {
        let filter = json!({"ts": {"gte": "2024-06-01T00:00:00Z", "lt": "2024-06-08T00:00:00Z"}});
        assert!(eval_row(&filter, &json!({"ts": "2024-06-03T12:00:00Z"})));
        assert!(!eval_row(&filter, &json!({"ts": "2024-06-09T00:00:00Z"})));
    }

    #[test]
    fn eval_row_timestamp_precision_mismatch() {
        // Reader emits microsecond RFC3339; a filter may use second precision.
        // gte at the exact instant must still match.
        let filter = json!({"ts": {"gte": "2024-01-02T00:00:00Z"}});
        assert!(eval_row(
            &filter,
            &json!({"ts": "2024-01-02T00:00:00.000000Z"})
        ));
        // and an offset spelling of the same instant
        let filter2 = json!({"ts": {"lt": "2024-06-02T00:00:00+00:00"}});
        assert!(eval_row(
            &filter2,
            &json!({"ts": "2024-06-01T00:00:00.000000Z"})
        ));
    }

    #[test]
    fn eval_row_missing_or_null_excluded() {
        let filter = json!({"amount": {"gt": 0}});
        assert!(!eval_row(&filter, &json!({"city": "BE"}))); // amount missing
        assert!(!eval_row(&filter, &json!({"amount": null})));
    }

    #[test]
    fn cast_mapping() {
        assert_eq!(delta_type_to_pg_cast("string"), "text");
        assert_eq!(delta_type_to_pg_cast("long"), "bigint");
        assert_eq!(delta_type_to_pg_cast("timestamp"), "timestamptz");
        assert_eq!(delta_type_to_pg_cast("decimal(10,2)"), "numeric");
    }

    fn stats(min: Value, max: Value) -> HashMap<String, (Option<Value>, Option<Value>)> {
        let mut m = HashMap::new();
        m.insert("amount".to_string(), (Some(min), Some(max)));
        m
    }

    #[test]
    fn row_group_pruned_when_range_disjoint() {
        let filter = json!({"amount": {"gt": 1000}});
        // max 500 < 1000 → cannot contain amount > 1000 → pruned
        assert!(!row_group_survives(&filter, &stats(json!(0), json!(500))));
        // max 1500 ≥ 1000 → may contain → survives
        assert!(row_group_survives(&filter, &stats(json!(0), json!(1500))));
    }

    #[test]
    fn row_group_equality_prune() {
        let filter = json!({"amount": 1000});
        assert!(!row_group_survives(
            &filter,
            &stats(json!(2000), json!(3000))
        )); // 1000 < min
        assert!(!row_group_survives(&filter, &stats(json!(0), json!(500)))); // 1000 > max
        assert!(row_group_survives(&filter, &stats(json!(0), json!(2000)))); // in range
    }

    #[test]
    fn type_mismatch_never_prunes() {
        // Int64 timestamp stat vs ISO-string filter → must NOT prune.
        let filter = json!({"amount": {"gte": "2024-06-01"}});
        assert!(row_group_survives(
            &filter,
            &stats(json!(1000), json!(2000))
        ));
    }

    #[test]
    fn missing_stats_never_prunes() {
        let filter = json!({"other": {"gt": 5}});
        assert!(row_group_survives(&filter, &stats(json!(0), json!(1))));
    }

    #[test]
    fn schema_from_snapshot_marks_partitions_and_types() {
        let snap = json!({
            "ts": {"type": "timestamp", "ordinal": 0},
            "city": {"type": "string", "ordinal": 1},
            "amount": {"type": "decimal(10,2)", "ordinal": 2}
        });
        let s = table_schema_from_snapshot(&snap, &["city".to_string()]);
        assert!(s["city"].is_partition);
        assert_eq!(s["city"].sql_type, "text");
        assert!(!s["ts"].is_partition);
        assert_eq!(s["ts"].sql_type, "timestamptz");
        assert_eq!(s["amount"].sql_type, "numeric");
    }
}
