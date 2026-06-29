//! Postgres table → polars DataFrame ingestion.
//!
//! Uses SPI to fetch rows and build column-wise typed buffers. Timestamp,
//! date, time, interval, and JSON(B) columns are cast to ::text inside
//! the SQL so pgrx can extract them as `String`.

use crate::error::AugurPgError;
use crate::models::quote_literal;
use pgrx::prelude::*;
use polars::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq)]
enum ColKind {
    Bool,
    Int,
    Float,
    /// numeric/decimal: pgrx can't extract as f64 directly, needs ::float8 cast in SQL
    FloatCast,
    Text,
    /// Types that we cast to ::text in SQL (timestamp, date, interval, jsonb, ...)
    TextCast,
}

fn classify_pg_type(pg_type: &str) -> ColKind {
    let t = pg_type.to_ascii_lowercase();
    match t.as_str() {
        "boolean" | "bool" => ColKind::Bool,
        "smallint" | "integer" | "bigint" | "int2" | "int4" | "int8" => ColKind::Int,
        "real" | "double precision" | "float4" | "float8" => ColKind::Float,
        "numeric" | "decimal" => ColKind::FloatCast,
        "text" | "character varying" | "varchar" | "character" | "char" | "uuid" | "name"
        | "citext" => ColKind::Text,
        // Needs ::text cast to be readable as Rust String via pgrx SPI
        _ => ColKind::TextCast,
    }
}

/// A typed column description inferred from information_schema.
#[derive(Debug, Clone)]
struct ColumnMeta {
    name: String,
    kind: ColKind,
}

fn list_columns(
    schema: Option<&str>,
    table: &str,
    exclude: &[&str],
) -> Result<Vec<ColumnMeta>, AugurPgError> {
    let schema_filter = match schema {
        Some(s) => format!("table_schema = {}", quote_literal(s)),
        None => "table_schema NOT IN ('pg_catalog','information_schema')".to_string(),
    };
    let sql = format!(
        "SELECT column_name::text, data_type::text, ordinal_position
         FROM information_schema.columns
         WHERE {} AND table_name = {}
         ORDER BY ordinal_position",
        schema_filter,
        quote_literal(table),
    );

    let mut cols = Vec::<ColumnMeta>::new();
    Spi::connect(|client| -> Result<(), pgrx::spi::SpiError> {
        let result = client.select(&sql, None, &[])?;
        for row in result {
            let name: String = row.get(1)?.unwrap_or_default();
            let pg_type: String = row.get(2)?.unwrap_or_default();
            if exclude.iter().any(|e| e.eq_ignore_ascii_case(&name)) {
                continue;
            }
            cols.push(ColumnMeta {
                name,
                kind: classify_pg_type(&pg_type),
            });
        }
        Ok(())
    })
    .map_err(|e| AugurPgError::Spi(format!("list_columns: {e}")))?;

    if cols.is_empty() {
        return Err(AugurPgError::InvalidRelation(format!(
            "no columns for {}{}",
            schema.map(|s| format!("{}.", s)).unwrap_or_default(),
            table
        )));
    }
    Ok(cols)
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Load a Postgres table into a Polars DataFrame.
///
/// Excluded columns are dropped before returning. Unknown PG types are
/// coerced to TEXT via `::text` cast.
pub fn load_table(
    schema: Option<&str>,
    table: &str,
    exclude_columns: &[&str],
) -> Result<DataFrame, AugurPgError> {
    let cols = list_columns(schema, table, exclude_columns)?;

    // Build typed column buffers.
    let mut bools: Vec<Vec<Option<bool>>> = Vec::new();
    let mut ints: Vec<Vec<Option<i64>>> = Vec::new();
    let mut floats: Vec<Vec<Option<f64>>> = Vec::new();
    let mut texts: Vec<Vec<Option<String>>> = Vec::new();

    // per-column index into the appropriate buffer vec, preserving order.
    let mut slots: Vec<(usize, ColKind)> = Vec::with_capacity(cols.len());
    for c in &cols {
        match c.kind {
            ColKind::Bool => {
                slots.push((bools.len(), ColKind::Bool));
                bools.push(Vec::new());
            }
            ColKind::Int => {
                slots.push((ints.len(), ColKind::Int));
                ints.push(Vec::new());
            }
            ColKind::Float | ColKind::FloatCast => {
                slots.push((floats.len(), ColKind::Float));
                floats.push(Vec::new());
            }
            ColKind::Text | ColKind::TextCast => {
                slots.push((texts.len(), ColKind::Text));
                texts.push(Vec::new());
            }
        }
    }

    // Build the SELECT list, casting non-native types to ::text.
    let select_items: Vec<String> = cols
        .iter()
        .map(|c| match c.kind {
            ColKind::FloatCast => format!("{}::float8", quote_ident(&c.name)),
            ColKind::TextCast => format!("{}::text", quote_ident(&c.name)),
            _ => quote_ident(&c.name),
        })
        .collect();
    let qualified = match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    };
    let sql = format!("SELECT {} FROM {}", select_items.join(", "), qualified);

    Spi::connect(|client| -> Result<(), pgrx::spi::SpiError> {
        let result = client.select(&sql, None, &[])?;
        for row in result {
            for (col_idx, (slot, kind)) in slots.iter().enumerate() {
                let pos = col_idx + 1;
                match kind {
                    ColKind::Bool => {
                        let v: Option<bool> = row.get(pos)?;
                        bools[*slot].push(v);
                    }
                    ColKind::Int => {
                        let v: Option<i64> = row
                            .get(pos)
                            .or_else(|_| row.get::<i32>(pos).map(|o| o.map(|x| x as i64)))?;
                        ints[*slot].push(v);
                    }
                    ColKind::Float => {
                        let v: Option<f64> = row
                            .get(pos)
                            .or_else(|_| row.get::<f32>(pos).map(|o| o.map(|x| x as f64)))?;
                        floats[*slot].push(v);
                    }
                    ColKind::Text => {
                        let v: Option<String> = row.get(pos)?;
                        texts[*slot].push(v);
                    }
                    ColKind::FloatCast => unreachable!("FloatCast was mapped to Float slot"),
                    ColKind::TextCast => unreachable!("TextCast was mapped to Text slot"),
                }
            }
        }
        Ok(())
    })
    .map_err(|e| AugurPgError::Spi(format!("load_table SELECT: {e}")))?;

    // Reassemble as polars columns in original order.
    let mut polars_cols: Vec<Column> = Vec::with_capacity(cols.len());
    for (c, (slot, kind)) in cols.iter().zip(slots.iter()) {
        let series = match kind {
            ColKind::Bool => Series::new(c.name.as_str().into(), &bools[*slot]),
            ColKind::Int => Series::new(c.name.as_str().into(), &ints[*slot]),
            ColKind::Float => Series::new(c.name.as_str().into(), &floats[*slot]),
            ColKind::Text => {
                let refs: Vec<Option<&str>> = texts[*slot].iter().map(|o| o.as_deref()).collect();
                Series::new(c.name.as_str().into(), refs)
            }
            ColKind::FloatCast => unreachable!("FloatCast was mapped to Float slot"),
            ColKind::TextCast => unreachable!("TextCast was mapped to Text slot"),
        };
        polars_cols.push(series.into_column());
    }

    let df = DataFrame::new(polars_cols).map_err(|e| AugurPgError::Polars(e.to_string()))?;
    if df.height() == 0 {
        return Err(AugurPgError::EmptyTable);
    }
    Ok(df)
}

/// Load only specific columns from a Postgres table into a Polars DataFrame.
///
/// This is used by the FDW feature view to load only the columns declared
/// in the foreign table definition, skipping columns like id, created_at, etc.
pub fn load_table_columns(
    schema: Option<&str>,
    table: &str,
    include_columns: &[String],
) -> Result<DataFrame, AugurPgError> {
    // Get all columns from the source table, then exclude those not in include_columns
    let all_cols = list_columns(schema, table, &[])?;
    let include_set: std::collections::HashSet<&str> =
        include_columns.iter().map(|s| s.as_str()).collect();
    let exclude: Vec<&str> = all_cols
        .iter()
        .filter(|c| !include_set.contains(c.name.as_str()))
        .map(|c| c.name.as_str())
        .collect();
    load_table(schema, table, &exclude)
}

/// Parse `"schema.table"` or `"table"` into `(Option<schema>, table)`.
pub fn parse_relation(s: &str) -> Result<(Option<String>, String), AugurPgError> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err(AugurPgError::InvalidRelation(s.to_string()));
    }
    // Very simple parser — does not handle quoted identifiers with dots.
    let parts: Vec<&str> = trimmed.split('.').collect();
    match parts.as_slice() {
        [t] => Ok((None, (*t).to_string())),
        [s, t] => Ok((Some((*s).to_string()), (*t).to_string())),
        _ => Err(AugurPgError::InvalidRelation(s.to_string())),
    }
}

/// Build a single-row DataFrame from a JSON object keyed by column name.
/// Uses the provided feature-column list to determine the column order.
pub fn row_from_jsonb(
    value: &serde_json::Value,
    feature_columns: &[String],
) -> Result<DataFrame, AugurPgError> {
    let obj = value
        .as_object()
        .ok_or_else(|| AugurPgError::Json("predict_row expects a JSON object".to_string()))?;

    let mut columns: Vec<Column> = Vec::with_capacity(feature_columns.len());
    for col in feature_columns {
        let v = obj.get(col);
        let series = match v {
            None | Some(serde_json::Value::Null) => {
                Series::new(col.as_str().into(), &[None::<f64>])
            }
            Some(serde_json::Value::Bool(b)) => Series::new(col.as_str().into(), &[Some(*b)]),
            Some(serde_json::Value::Number(n)) => {
                if let Some(f) = n.as_f64() {
                    Series::new(col.as_str().into(), &[Some(f)])
                } else if let Some(i) = n.as_i64() {
                    Series::new(col.as_str().into(), &[Some(i)])
                } else {
                    Series::new(col.as_str().into(), &[None::<f64>])
                }
            }
            Some(serde_json::Value::String(s)) => {
                let refs: &[Option<&str>] = &[Some(s.as_str())];
                Series::new(col.as_str().into(), refs)
            }
            Some(other) => {
                let s = other.to_string();
                let refs: &[Option<&str>] = &[Some(s.as_str())];
                Series::new(col.as_str().into(), refs)
            }
        };
        columns.push(series.into_column());
    }

    DataFrame::new(columns).map_err(|e| AugurPgError::Polars(e.to_string()))
}

/// Build a single-row DataFrame from an ordered array of float features.
/// Uses feature-column names to label each column.
pub fn row_from_floats(
    features: &[f64],
    feature_columns: &[String],
) -> Result<DataFrame, AugurPgError> {
    if features.len() != feature_columns.len() {
        return Err(AugurPgError::FeatureMismatch {
            expected: feature_columns.len(),
            actual: features.len(),
        });
    }
    let mut columns: Vec<Column> = Vec::with_capacity(features.len());
    for (name, value) in feature_columns.iter().zip(features.iter()) {
        let series = Series::new(name.as_str().into(), &[Some(*value)]);
        columns.push(series.into_column());
    }
    DataFrame::new(columns).map_err(|e| AugurPgError::Polars(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_types() {
        assert_eq!(classify_pg_type("boolean"), ColKind::Bool);
        assert_eq!(classify_pg_type("integer"), ColKind::Int);
        assert_eq!(classify_pg_type("double precision"), ColKind::Float);
        assert_eq!(classify_pg_type("numeric"), ColKind::FloatCast);
        assert_eq!(classify_pg_type("decimal"), ColKind::FloatCast);
        assert_eq!(classify_pg_type("text"), ColKind::Text);
        assert_eq!(
            classify_pg_type("timestamp with time zone"),
            ColKind::TextCast
        );
        assert_eq!(classify_pg_type("jsonb"), ColKind::TextCast);
    }

    #[test]
    fn parse_relations() {
        assert_eq!(parse_relation("foo").unwrap(), (None, "foo".to_string()));
        assert_eq!(
            parse_relation("public.foo").unwrap(),
            (Some("public".to_string()), "foo".to_string())
        );
        assert!(parse_relation("a.b.c").is_err());
        assert!(parse_relation("").is_err());
    }

    #[test]
    fn row_from_floats_builds_df() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let df = row_from_floats(&[1.0, 2.0, 3.0], &cols).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 3);
    }

    #[test]
    fn row_from_floats_length_mismatch() {
        let cols = vec!["a".to_string(), "b".to_string()];
        assert!(row_from_floats(&[1.0, 2.0, 3.0], &cols).is_err());
    }

    #[test]
    fn row_from_jsonb_handles_types() {
        let cols = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let val = serde_json::json!({"a": 1.5, "b": "hello", "c": true});
        let df = row_from_jsonb(&val, &cols).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 3);
    }
}
