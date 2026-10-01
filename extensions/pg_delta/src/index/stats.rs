//! Pure transforms of Delta `add.stats` and partition values into the catalog
//! JSON shapes stored in `delta.files`.
//!
//! These functions have no Postgres or Delta dependencies so they can be unit
//! tested with plain `#[test]` (CLAUDE.md TDD: unit tests first).

use serde_json::{Map, Value};
use std::collections::HashMap;

/// Result of parsing a Delta `add.stats` blob.
#[derive(Debug, Default, PartialEq)]
pub struct ParsedStats {
    /// `numRecords` from the stats blob, if present.
    pub num_records: Option<i64>,
    /// Per-column stats in the catalog shape:
    /// `{"col": {"min": .., "max": .., "null_count": N}}`.
    /// `None` when the file carried no stats at all (a stat-less file — see
    /// `indexing.md` pitfall #1: it can never be tier-1 pruned).
    pub column_stats: Option<Value>,
}

/// Transform a raw Delta `add.stats` JSON string into [`ParsedStats`].
///
/// The Delta protocol shape is:
/// ```json
/// {"numRecords": N,
///  "minValues": {"col": v, ...},
///  "maxValues": {"col": v, ...},
///  "nullCount": {"col": n, ...}}
/// ```
/// We pivot it into per-column objects. Only scalar min/max values are kept;
/// nested struct/list values (objects/arrays) are skipped for min/max because
/// they can't drive tier-1 SQL pruning (they may still prune at the parquet
/// footer). `null_count` is kept whenever it is a scalar number.
pub fn parse_add_stats(stats: Option<&str>) -> ParsedStats {
    let raw = match stats {
        Some(s) if !s.trim().is_empty() => s,
        _ => return ParsedStats::default(),
    };

    let parsed: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return ParsedStats::default(),
    };

    let num_records = parsed.get("numRecords").and_then(json_as_i64);

    let min_values = parsed.get("minValues").and_then(Value::as_object);
    let max_values = parsed.get("maxValues").and_then(Value::as_object);
    let null_count = parsed.get("nullCount").and_then(Value::as_object);

    // Union of all top-level column names that appear in any of the three maps.
    let mut columns: Vec<String> = Vec::new();
    for src in [min_values, max_values, null_count].into_iter().flatten() {
        for key in src.keys() {
            if !columns.iter().any(|c| c == key) {
                columns.push(key.clone());
            }
        }
    }

    let mut column_stats = Map::new();
    for col in columns {
        let mut entry = Map::new();

        if let Some(v) = min_values.and_then(|m| m.get(&col)) {
            if is_scalar(v) {
                entry.insert("min".to_string(), v.clone());
            }
        }
        if let Some(v) = max_values.and_then(|m| m.get(&col)) {
            if is_scalar(v) {
                entry.insert("max".to_string(), v.clone());
            }
        }
        if let Some(n) = null_count.and_then(|m| m.get(&col)).and_then(json_as_i64) {
            entry.insert("null_count".to_string(), Value::from(n));
        }

        if !entry.is_empty() {
            column_stats.insert(col, Value::Object(entry));
        }
    }

    let column_stats = if column_stats.is_empty() {
        // The blob parsed but yielded no usable per-column stats (e.g. only
        // numRecords present). Treat as "no column stats" for pruning purposes.
        None
    } else {
        Some(Value::Object(column_stats))
    };

    ParsedStats {
        num_records,
        column_stats,
    }
}

/// Normalize Delta partition values (`HashMap<String, Option<String>>`) into a
/// JSON object suitable for the `delta.files.partition_values` GIN index.
///
/// Delta stores partition values as strings; we keep them as JSON strings (or
/// `null`) so `partition_values @> '{"city":"BE"}'` containment works. Returns
/// `None` when the table is unpartitioned (empty map).
pub fn partition_values_to_json(values: &HashMap<String, Option<String>>) -> Option<Value> {
    if values.is_empty() {
        return None;
    }
    let mut obj = Map::new();
    for (k, v) in values {
        match v {
            Some(s) => obj.insert(k.clone(), Value::String(s.clone())),
            None => obj.insert(k.clone(), Value::Null),
        };
    }
    Some(Value::Object(obj))
}

/// A JSON value is "scalar" if it can be compared in a tier-1 SQL predicate:
/// string, number, or bool. Objects/arrays/null are not.
fn is_scalar(v: &Value) -> bool {
    matches!(v, Value::String(_) | Value::Number(_) | Value::Bool(_))
}

/// Coerce a JSON number (possibly encoded as a string) into an i64.
fn json_as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_full_stats_blob() {
        let blob = r#"{
            "numRecords": 1452318,
            "minValues": {"ts": "2024-06-01T00:00:14.000Z", "city": "AD", "user_id": 17, "amount": 0.01},
            "maxValues": {"ts": "2024-06-01T23:59:51.000Z", "city": "ZW", "user_id": 9999998, "amount": 49997.83},
            "nullCount": {"ts": 0, "city": 412, "user_id": 0, "amount": 23}
        }"#;

        let parsed = parse_add_stats(Some(blob));
        assert_eq!(parsed.num_records, Some(1452318));

        let cs = parsed.column_stats.expect("column stats present");
        assert_eq!(cs["city"]["min"], json!("AD"));
        assert_eq!(cs["city"]["max"], json!("ZW"));
        assert_eq!(cs["city"]["null_count"], json!(412));
        assert_eq!(cs["user_id"]["min"], json!(17));
        assert_eq!(cs["amount"]["max"], json!(49997.83));
        assert_eq!(cs["ts"]["null_count"], json!(0));
    }

    #[test]
    fn missing_stats_yields_empty() {
        assert_eq!(parse_add_stats(None), ParsedStats::default());
        assert_eq!(parse_add_stats(Some("")), ParsedStats::default());
        assert_eq!(parse_add_stats(Some("   ")), ParsedStats::default());
    }

    #[test]
    fn invalid_json_yields_empty() {
        assert_eq!(parse_add_stats(Some("{not json")), ParsedStats::default());
    }

    #[test]
    fn num_records_only_has_no_column_stats() {
        let parsed = parse_add_stats(Some(r#"{"numRecords": 100}"#));
        assert_eq!(parsed.num_records, Some(100));
        assert!(parsed.column_stats.is_none());
    }

    #[test]
    fn skips_nested_struct_min_max_but_keeps_null_count() {
        // Nested struct columns appear as objects in minValues/maxValues; those
        // cannot drive tier-1 pruning and must be dropped, but a scalar
        // null_count is still useful.
        let blob = r#"{
            "minValues": {"addr": {"city": "AD"}, "amount": 1},
            "maxValues": {"addr": {"city": "ZW"}, "amount": 9},
            "nullCount": {"addr": 5, "amount": 0}
        }"#;
        let cs = parse_add_stats(Some(blob)).column_stats.unwrap();
        // addr keeps only null_count (its min/max were objects, skipped).
        assert!(cs["addr"].get("min").is_none());
        assert!(cs["addr"].get("max").is_none());
        assert_eq!(cs["addr"]["null_count"], json!(5));
        // amount keeps everything.
        assert_eq!(cs["amount"]["min"], json!(1));
        assert_eq!(cs["amount"]["max"], json!(9));
    }

    #[test]
    fn null_count_as_string_is_coerced() {
        let blob = r#"{"nullCount": {"city": "7"}, "minValues": {"city": "A"}}"#;
        let cs = parse_add_stats(Some(blob)).column_stats.unwrap();
        assert_eq!(cs["city"]["null_count"], json!(7));
    }

    #[test]
    fn partition_values_roundtrip() {
        let mut pv = HashMap::new();
        pv.insert("city".to_string(), Some("BE".to_string()));
        pv.insert("missing".to_string(), None);

        let json = partition_values_to_json(&pv).unwrap();
        assert_eq!(json["city"], json!("BE"));
        assert_eq!(json["missing"], Value::Null);
    }

    #[test]
    fn partition_values_empty_is_none() {
        assert!(partition_values_to_json(&HashMap::new()).is_none());
    }
}
