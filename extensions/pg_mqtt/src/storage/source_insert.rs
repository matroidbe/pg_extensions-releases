//! Source table INSERT module for pg_mqtt typed topics
//!
//! Handles inserting MQTT payloads into typed topic backing tables
//! by decomposing JSON into table columns.

use super::spi_bridge::{ColumnType, SpiError, SpiParam, SpiRow};

/// Source insert error
#[derive(Debug, thiserror::Error)]
pub enum SourceInsertError {
    #[error("SPI error: {0}")]
    Spi(#[from] SpiError),
    #[error("missing required column: {0}")]
    MissingColumn(String),
    #[error("JSON parse error: {0}")]
    JsonParse(String),
}

/// Column information from a table
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
    pub column_default: Option<String>,
    /// Schema of the column's underlying type (e.g. `pg_catalog`)
    pub udt_schema: String,
    /// Name of the column's underlying type (e.g. `int4`, `timestamptz`, `_text`)
    pub udt_name: String,
}

/// The columns of a table, in rows `columns_from_rows` reads.
/// $1: schema, $2: table.
pub const TABLE_COLUMNS_SQL: &str =
    "SELECT column_name::text, data_type::text, is_nullable::text, column_default::text,
            udt_schema::text, udt_name::text
     FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = $2
     ORDER BY ordinal_position";

/// Column types of TABLE_COLUMNS_SQL's rows
pub const TABLE_COLUMNS_TYPES: [ColumnType; 6] = [ColumnType::Text; 6];

/// The parameters of TABLE_COLUMNS_SQL for a (possibly schema-qualified) table
pub fn table_columns_params(source_table: &str) -> Vec<SpiParam> {
    let (schema, table) = parse_table_name(source_table);
    vec![
        SpiParam::Text(Some(schema.to_string())),
        SpiParam::Text(Some(table.to_string())),
    ]
}

/// Read TABLE_COLUMNS_SQL's rows
pub fn columns_from_rows(rows: &[SpiRow]) -> Vec<ColumnInfo> {
    rows.iter()
        .map(|row| ColumnInfo {
            name: row.get_string(0).unwrap_or_default(),
            data_type: row.get_string(1).unwrap_or_default(),
            is_nullable: row.get_string(2).map(|s| s == "YES").unwrap_or(true),
            column_default: row.get_string(3),
            udt_schema: row.get_string(4).unwrap_or_default(),
            udt_name: row.get_string(5).unwrap_or_default(),
        })
        .collect()
}

/// The INSERT that stores a JSON message in a typed topic's backing table:
/// each top-level key becomes the column of the same name.
pub fn build_insert(
    source_table: &str,
    columns: &[ColumnInfo],
    json_value: &serde_json::Value,
) -> Result<(String, Vec<SpiParam>), SourceInsertError> {
    let json_obj = json_value
        .as_object()
        .ok_or_else(|| SourceInsertError::JsonParse("payload must be a JSON object".to_string()))?;

    let mut insert_columns = Vec::new();
    let mut insert_values = Vec::new();
    let mut params = Vec::new();

    for col in columns {
        // Skip auto-increment columns (serial/bigserial)
        if col
            .column_default
            .as_ref()
            .map(|d| d.contains("nextval"))
            .unwrap_or(false)
        {
            continue;
        }

        if let Some(val) = json_obj.get(&col.name) {
            insert_columns.push(quote_ident(&col.name));
            insert_values.push(typed_placeholder(params.len() + 1, Some(col)));
            params.push(json_value_to_spi_param(val, &col.data_type));
        } else if !col.is_nullable && col.column_default.is_none() {
            return Err(SourceInsertError::MissingColumn(col.name.clone()));
        }
    }

    if insert_columns.is_empty() {
        return Err(SourceInsertError::JsonParse(
            "no matching columns found in JSON".to_string(),
        ));
    }

    let query = format!(
        "INSERT INTO {} ({}) VALUES ({})",
        source_table,
        insert_columns.join(", "),
        insert_values.join(", ")
    );
    Ok((query, params))
}

/// Parse a table name into (schema, table)
fn parse_table_name(table_name: &str) -> (&str, &str) {
    if let Some(dot_pos) = table_name.find('.') {
        (&table_name[..dot_pos], &table_name[dot_pos + 1..])
    } else {
        ("public", table_name)
    }
}

/// Quote a SQL identifier (double quotes, embedded quotes doubled).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a `$N::<column type>` placeholder.
///
/// pg_spi binds parameters as *typed* SQL arguments (a `SpiParam::Text` is a
/// `text` value, not an untyped literal), so a text parameter would not be
/// assignment-coerced into a `uuid`, `timestamptz`, `numeric`, `boolean` or
/// `jsonb` column. Casting the placeholder to the column's declared type
/// restores the literal-style coercion while keeping the value bound (the
/// value never enters the SQL text).
fn typed_placeholder(idx: usize, col: Option<&ColumnInfo>) -> String {
    match col {
        Some(c) if !c.udt_name.is_empty() => {
            let schema = if c.udt_schema.is_empty() {
                "pg_catalog"
            } else {
                c.udt_schema.as_str()
            };
            format!(
                "${}::{}.{}",
                idx,
                quote_ident(schema),
                quote_ident(&c.udt_name)
            )
        }
        _ => format!("${}", idx),
    }
}

/// Convert a JSON value to an SPI parameter
fn json_value_to_spi_param(value: &serde_json::Value, data_type: &str) -> SpiParam {
    match value {
        serde_json::Value::Null => SpiParam::Text(None),
        serde_json::Value::Bool(b) => SpiParam::Text(Some(b.to_string())),
        serde_json::Value::Number(n) => {
            if data_type.contains("int") {
                if let Some(i) = n.as_i64() {
                    SpiParam::Int8(Some(i))
                } else {
                    SpiParam::Text(Some(n.to_string()))
                }
            } else {
                SpiParam::Text(Some(n.to_string()))
            }
        }
        serde_json::Value::String(s) => SpiParam::Text(Some(s.clone())),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            SpiParam::Text(Some(value.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("pgmqtt.sensors");
        assert_eq!(schema, "pgmqtt");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("sensors");
        assert_eq!(schema, "public");
        assert_eq!(table, "sensors");
    }

    #[test]
    fn test_json_value_to_spi_param_string() {
        let value = json!("hello");
        let param = json_value_to_spi_param(&value, "text");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert_eq!(s, "hello");
    }

    #[test]
    fn test_json_value_to_spi_param_number_int() {
        let value = json!(42);
        let param = json_value_to_spi_param(&value, "bigint");
        let SpiParam::Int8(Some(n)) = param else {
            panic!("expected Int8");
        };
        assert_eq!(n, 42);
    }

    #[test]
    fn test_json_value_to_spi_param_number_numeric() {
        // Not 3.14 — clippy::approx_constant reads that as a botched PI.
        let value = json!(2.75);
        let param = json_value_to_spi_param(&value, "numeric");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert!(s.starts_with("2.75"));
    }

    #[test]
    fn test_json_value_to_spi_param_null() {
        let value = json!(null);
        let param = json_value_to_spi_param(&value, "text");
        assert!(matches!(param, SpiParam::Text(None)));
    }

    #[test]
    fn test_json_value_to_spi_param_bool() {
        let value = json!(true);
        let param = json_value_to_spi_param(&value, "boolean");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert_eq!(s, "true");
    }

    #[test]
    fn test_json_value_to_spi_param_object() {
        let value = json!({"nested": "object"});
        let param = json_value_to_spi_param(&value, "jsonb");
        let SpiParam::Text(Some(s)) = param else {
            panic!("expected Text");
        };
        assert!(s.contains("nested"));
    }

    fn col(name: &str, udt_schema: &str, udt_name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: String::new(),
            is_nullable: true,
            column_default: None,
            udt_schema: udt_schema.to_string(),
            udt_name: udt_name.to_string(),
        }
    }

    #[test]
    fn test_typed_placeholder_casts_to_column_type() {
        assert_eq!(
            typed_placeholder(3, Some(&col("recorded_at", "pg_catalog", "timestamptz"))),
            "$3::\"pg_catalog\".\"timestamptz\""
        );
        assert_eq!(
            typed_placeholder(2, Some(&col("active", "pg_catalog", "bool"))),
            "$2::\"pg_catalog\".\"bool\""
        );
    }

    #[test]
    fn test_typed_placeholder_without_type_info() {
        assert_eq!(typed_placeholder(4, None), "$4");
        assert_eq!(typed_placeholder(5, Some(&col("x", "", ""))), "$5");
    }

    #[test]
    fn test_build_insert() {
        let mut id = col("id", "pg_catalog", "int8");
        id.column_default = Some("nextval('t_id_seq'::regclass)".into());
        let mut required = col("v", "pg_catalog", "float8");
        required.is_nullable = false;
        let columns = [id, required, col("note", "pg_catalog", "text")];

        let (sql, params) =
            build_insert("public.t", &columns, &json!({"v": 1.5, "other": 1})).unwrap();
        assert_eq!(
            sql,
            "INSERT INTO public.t (\"v\") VALUES ($1::\"pg_catalog\".\"float8\")"
        );
        assert_eq!(params.len(), 1);

        // A required column missing from the JSON
        assert!(matches!(
            build_insert("public.t", &columns, &json!({"note": "x"})),
            Err(SourceInsertError::MissingColumn(c)) if c == "v"
        ));
        assert!(build_insert("public.t", &columns, &json!([1])).is_err());
    }

    #[test]
    fn test_source_insert_error_display() {
        let err = SourceInsertError::MissingColumn("name".to_string());
        assert!(err.to_string().contains("missing required column"));

        let err = SourceInsertError::JsonParse("bad json".to_string());
        assert!(err.to_string().contains("JSON parse error"));
    }
}
