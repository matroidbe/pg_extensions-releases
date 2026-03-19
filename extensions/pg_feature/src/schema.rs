//! Schema introspection utilities for pg_feature
//!
//! Provides helpers for parsing table references, querying column information,
//! and validating schema elements.

use crate::PgFeatureError;
use pgrx::prelude::*;

/// Represents a fully-qualified table reference
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub schema: String,
    pub name: String,
}

impl TableRef {
    pub fn new(schema: &str, name: &str) -> Self {
        Self {
            schema: schema.to_string(),
            name: name.to_string(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.schema, self.name)
    }

    /// Returns the identifier for use in SQL (properly quoted if needed)
    pub fn sql_identifier(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.name)
        )
    }
}

impl std::fmt::Display for TableRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.schema, self.name)
    }
}

/// Column information from schema introspection
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub pg_type: PgType,
    pub is_nullable: bool,
    pub ordinal_position: i32,
}

/// PostgreSQL type categories for primitive applicability
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    Integer,
    Numeric,
    Boolean,
    Text,
    Timestamp,
    Date,
    Time,
    Json,
    Array,
    Other,
}

impl PgType {
    /// Determine the type category from a data_type string
    pub fn from_data_type(data_type: &str) -> Self {
        let dt = data_type.to_lowercase();
        if dt.contains("int") || dt == "serial" || dt == "bigserial" || dt == "smallserial" {
            PgType::Integer
        } else if dt.contains("numeric")
            || dt.contains("decimal")
            || dt.contains("real")
            || dt.contains("double")
            || dt.contains("float")
        {
            PgType::Numeric
        } else if dt.contains("bool") {
            PgType::Boolean
        } else if dt.contains("char")
            || dt.contains("text")
            || dt == "name"
            || dt.contains("varchar")
        {
            PgType::Text
        } else if dt.contains("timestamp") {
            PgType::Timestamp
        } else if dt == "date" {
            PgType::Date
        } else if dt.contains("time") && !dt.contains("timestamp") {
            PgType::Time
        } else if dt.contains("json") {
            PgType::Json
        } else if dt.starts_with("_") || dt.contains("[]") || dt == "array" {
            PgType::Array
        } else {
            PgType::Other
        }
    }

    /// Check if this type is numeric (for aggregation primitives)
    pub fn is_numeric(&self) -> bool {
        matches!(self, PgType::Integer | PgType::Numeric)
    }

    /// Check if this type is temporal (for time-based transforms)
    pub fn is_temporal(&self) -> bool {
        matches!(self, PgType::Timestamp | PgType::Date | PgType::Time)
    }
}

/// Parse a table name into a TableRef
///
/// Handles both qualified ('schema.table') and unqualified ('table') names.
/// Unqualified names default to 'public' schema.
pub fn parse_table_ref(table_name: &str) -> Result<TableRef, PgFeatureError> {
    let parts: Vec<&str> = table_name.split('.').collect();
    match parts.len() {
        1 => Ok(TableRef::new("public", parts[0])),
        2 => Ok(TableRef::new(parts[0], parts[1])),
        _ => Err(PgFeatureError::InvalidParameter(format!(
            "Invalid table name '{}': expected 'schema.table' or 'table'",
            table_name
        ))),
    }
}

/// Quote an identifier if it needs quoting
pub fn quote_identifier(name: &str) -> String {
    // Check if the identifier needs quoting
    let needs_quoting = name
        .chars()
        .any(|c| !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '_')
        || is_reserved_word(name);

    if needs_quoting {
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Check if a word is a SQL reserved word (simplified check)
fn is_reserved_word(word: &str) -> bool {
    let reserved = [
        "select",
        "from",
        "where",
        "and",
        "or",
        "not",
        "in",
        "is",
        "null",
        "true",
        "false",
        "table",
        "column",
        "index",
        "create",
        "drop",
        "alter",
        "insert",
        "update",
        "delete",
        "join",
        "left",
        "right",
        "inner",
        "outer",
        "on",
        "as",
        "order",
        "by",
        "group",
        "having",
        "limit",
        "offset",
        "union",
        "except",
        "intersect",
        "case",
        "when",
        "then",
        "else",
        "end",
        "primary",
        "key",
        "foreign",
        "references",
        "unique",
        "check",
        "default",
        "constraint",
        "user",
        "role",
        "grant",
        "revoke",
        "all",
        "any",
        "between",
        "like",
        "ilike",
        "similar",
    ];
    reserved.contains(&word.to_lowercase().as_str())
}

/// Verify that a table exists
pub fn verify_table_exists(table_ref: &TableRef) -> Result<bool, PgFeatureError> {
    let exists = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = '{}' AND table_name = '{}'
            )
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name)
        );
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            Ok::<_, pgrx::spi::Error>(row.get::<bool>(1)?.unwrap_or(false))
        } else {
            Ok::<_, pgrx::spi::Error>(false)
        }
    })?;

    Ok(exists)
}

/// Escape a string literal for use in SQL
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Get column information for a table
pub fn get_table_columns(table_ref: &TableRef) -> Result<Vec<ColumnInfo>, PgFeatureError> {
    let columns = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT
                column_name::text,
                data_type::text,
                is_nullable::text,
                ordinal_position
            FROM information_schema.columns
            WHERE table_schema = '{}' AND table_name = '{}'
            ORDER BY ordinal_position
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name)
        );
        let result = client.select(&sql, None, &[])?;

        let mut columns = Vec::new();
        for row in result {
            let name: String = row.get(1)?.unwrap_or_default();
            let data_type: String = row.get(2)?.unwrap_or_default();
            let is_nullable_str: String = row.get(3)?.unwrap_or_else(|| "YES".to_string());
            let ordinal_position: i32 = row.get(4)?.unwrap_or(0);

            columns.push(ColumnInfo {
                name,
                pg_type: PgType::from_data_type(&data_type),
                data_type,
                is_nullable: is_nullable_str == "YES",
                ordinal_position,
            });
        }
        Ok::<_, pgrx::spi::Error>(columns)
    })?;

    if columns.is_empty() {
        return Err(PgFeatureError::TableNotFound(table_ref.qualified_name()));
    }

    Ok(columns)
}

/// Get the primary key column(s) for a table
#[allow(dead_code)]
pub fn get_primary_key_columns(table_ref: &TableRef) -> Result<Vec<String>, PgFeatureError> {
    let pk_columns = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
              AND n.nspname = '{}'
              AND c.relname = '{}'
            ORDER BY array_position(i.indkey, a.attnum)
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name)
        );
        let result = client.select(&sql, None, &[])?;

        let mut columns = Vec::new();
        for row in result {
            if let Some(name) = row.get::<String>(1)? {
                columns.push(name);
            }
        }
        Ok::<_, pgrx::spi::Error>(columns)
    })?;

    Ok(pk_columns)
}

/// Get the row count for a table
#[allow(dead_code)]
pub fn get_table_row_count(table_ref: &TableRef) -> Result<i64, PgFeatureError> {
    let count = Spi::connect(|client| {
        let query = format!("SELECT COUNT(*) FROM {}", table_ref.sql_identifier());
        let mut result = client.select(&query, None, &[])?;

        if let Some(row) = result.next() {
            Ok::<_, pgrx::spi::Error>(row.get::<i64>(1)?.unwrap_or(0))
        } else {
            Ok::<_, pgrx::spi::Error>(0)
        }
    })?;

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_ref_qualified() {
        let result = parse_table_ref("myschema.mytable").unwrap();
        assert_eq!(result.schema, "myschema");
        assert_eq!(result.name, "mytable");
    }

    #[test]
    fn test_parse_table_ref_unqualified() {
        let result = parse_table_ref("mytable").unwrap();
        assert_eq!(result.schema, "public");
        assert_eq!(result.name, "mytable");
    }

    #[test]
    fn test_parse_table_ref_invalid() {
        let result = parse_table_ref("a.b.c");
        assert!(result.is_err());
    }

    #[test]
    fn test_pg_type_from_data_type() {
        assert_eq!(PgType::from_data_type("integer"), PgType::Integer);
        assert_eq!(PgType::from_data_type("bigint"), PgType::Integer);
        assert_eq!(PgType::from_data_type("numeric(10,2)"), PgType::Numeric);
        assert_eq!(PgType::from_data_type("double precision"), PgType::Numeric);
        assert_eq!(PgType::from_data_type("boolean"), PgType::Boolean);
        assert_eq!(PgType::from_data_type("text"), PgType::Text);
        assert_eq!(PgType::from_data_type("character varying"), PgType::Text);
        assert_eq!(
            PgType::from_data_type("timestamp without time zone"),
            PgType::Timestamp
        );
        assert_eq!(
            PgType::from_data_type("timestamp with time zone"),
            PgType::Timestamp
        );
        assert_eq!(PgType::from_data_type("date"), PgType::Date);
        assert_eq!(PgType::from_data_type("jsonb"), PgType::Json);
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "simple");
        assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
        assert_eq!(quote_identifier("with space"), "\"with space\"");
        assert_eq!(quote_identifier("select"), "\"select\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn test_table_ref_sql_identifier() {
        let t = TableRef::new("public", "users");
        assert_eq!(t.sql_identifier(), "public.users");

        let t2 = TableRef::new("My Schema", "User Table");
        assert_eq!(t2.sql_identifier(), "\"My Schema\".\"User Table\"");
    }
}
