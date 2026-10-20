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
    ///
    /// Matches whole type names (`information_schema.columns.data_type` spellings
    /// and their common aliases), so e.g. `interval` and `point` are not integers.
    pub fn from_data_type(data_type: &str) -> Self {
        let dt = data_type.trim().to_lowercase();
        // Strip a type modifier: numeric(10,2), character varying(20)
        let base = dt.split('(').next().unwrap_or("").trim();
        match base {
            "smallint" | "integer" | "bigint" | "int" | "int2" | "int4" | "int8" | "serial"
            | "smallserial" | "bigserial" => PgType::Integer,
            "numeric" | "decimal" | "real" | "double precision" | "float4" | "float8" => {
                PgType::Numeric
            }
            "boolean" | "bool" => PgType::Boolean,
            "text" | "character varying" | "varchar" | "character" | "char" | "bpchar" | "name"
            | "citext" => PgType::Text,
            "timestamp" | "timestamptz" => PgType::Timestamp,
            "date" => PgType::Date,
            "time" | "timetz" => PgType::Time,
            "json" | "jsonb" => PgType::Json,
            "array" => PgType::Array,
            _ if base.starts_with("timestamp") => PgType::Timestamp,
            _ if base.starts_with("time ") => PgType::Time,
            _ if base.starts_with('_') || base.ends_with("[]") => PgType::Array,
            _ => PgType::Other,
        }
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
///
/// Same rule as PostgreSQL's `quote_ident`: a name stays bare only if it is a
/// lowercase identifier that does not start with a digit and is not a keyword
/// that needs quoting.
pub fn quote_identifier(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');

    if plain && !is_reserved_word(name) {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// Keywords that cannot be used as a bare column name: every PostgreSQL keyword
/// outside the unreserved category (`SELECT word FROM pg_get_keywords() WHERE
/// catcode <> 'U'`, PostgreSQL 18)
const QUOTED_KEYWORDS: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "between",
    "bigint",
    "binary",
    "bit",
    "boolean",
    "both",
    "case",
    "cast",
    "char",
    "character",
    "check",
    "coalesce",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "dec",
    "decimal",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "exists",
    "extract",
    "false",
    "fetch",
    "float",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "greatest",
    "group",
    "grouping",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "inout",
    "int",
    "integer",
    "intersect",
    "interval",
    "into",
    "is",
    "isnull",
    "join",
    "json",
    "json_array",
    "json_arrayagg",
    "json_exists",
    "json_object",
    "json_objectagg",
    "json_query",
    "json_scalar",
    "json_serialize",
    "json_table",
    "json_value",
    "lateral",
    "leading",
    "least",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "merge_action",
    "national",
    "natural",
    "nchar",
    "none",
    "normalize",
    "not",
    "notnull",
    "null",
    "nullif",
    "numeric",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "out",
    "outer",
    "overlaps",
    "overlay",
    "placing",
    "position",
    "precision",
    "primary",
    "real",
    "references",
    "returning",
    "right",
    "row",
    "select",
    "session_user",
    "setof",
    "similar",
    "smallint",
    "some",
    "substring",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "time",
    "timestamp",
    "to",
    "trailing",
    "treat",
    "trim",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "values",
    "varchar",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
    "xmlattributes",
    "xmlconcat",
    "xmlelement",
    "xmlexists",
    "xmlforest",
    "xmlnamespaces",
    "xmlparse",
    "xmlpi",
    "xmlroot",
    "xmlserialize",
    "xmltable",
];

/// Check if a word is a keyword that must be quoted
fn is_reserved_word(word: &str) -> bool {
    QUOTED_KEYWORDS.contains(&word)
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
pub fn get_primary_key_columns(table_ref: &TableRef) -> Result<Vec<String>, PgFeatureError> {
    let pk_columns = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT a.attname::text
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
        assert_eq!(
            PgType::from_data_type("time without time zone"),
            PgType::Time
        );
        assert_eq!(PgType::from_data_type("ARRAY"), PgType::Array);
    }

    #[test]
    fn test_pg_type_not_fooled_by_substrings() {
        // "interval" and "point" contain "int"; neither is an integer
        assert_eq!(PgType::from_data_type("interval"), PgType::Other);
        assert_eq!(PgType::from_data_type("point"), PgType::Other);
        assert_eq!(PgType::from_data_type("tsrange"), PgType::Other);
        assert_eq!(PgType::from_data_type("USER-DEFINED"), PgType::Other);
        assert_eq!(PgType::from_data_type("uuid"), PgType::Other);
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "simple");
        assert_eq!(quote_identifier("MixedCase"), "\"MixedCase\"");
        assert_eq!(quote_identifier("with space"), "\"with space\"");
        assert_eq!(quote_identifier("select"), "\"select\"");
        assert_eq!(quote_identifier("has\"quote"), "\"has\"\"quote\"");
        // Keywords the old hand-written list missed
        assert_eq!(quote_identifier("desc"), "\"desc\"");
        assert_eq!(quote_identifier("do"), "\"do\"");
        assert_eq!(quote_identifier("window"), "\"window\"");
        // Unreserved keywords stay bare, as with quote_ident
        assert_eq!(quote_identifier("key"), "key");
        assert_eq!(quote_identifier("index"), "index");
        // A leading digit needs quoting
        assert_eq!(quote_identifier("1st"), "\"1st\"");
        assert_eq!(quote_identifier("_x1"), "_x1");
    }

    #[test]
    fn test_table_ref_sql_identifier() {
        let t = TableRef::new("public", "users");
        assert_eq!(t.sql_identifier(), "public.users");

        let t2 = TableRef::new("My Schema", "User Table");
        assert_eq!(t2.sql_identifier(), "\"My Schema\".\"User Table\"");
    }
}
