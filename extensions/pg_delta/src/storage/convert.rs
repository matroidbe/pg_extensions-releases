//! Type conversion between Postgres and Arrow.

#![allow(dead_code)] // Utility functions for future use

use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use pgrx::prelude::*;

/// Convert Arrow DataType to Postgres type name.
pub fn arrow_type_to_pg_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 | DataType::UInt16 => "INTEGER",
        DataType::UInt32 => "BIGINT",
        DataType::UInt64 => "NUMERIC",
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMPTZ",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "NUMERIC",
        DataType::List(_) | DataType::LargeList(_) => "JSONB", // Serialize as JSON
        DataType::Struct(_) => "JSONB",
        DataType::Map(_, _) => "JSONB",
        _ => "TEXT", // Fallback
    }
}

/// Convert Postgres type OID to Arrow DataType.
pub fn pg_type_to_arrow_type(type_oid: pgrx::pg_sys::Oid) -> DataType {
    use pgrx::pg_sys::*;

    match type_oid {
        BOOLOID => DataType::Boolean,
        INT2OID => DataType::Int16,
        INT4OID => DataType::Int32,
        INT8OID => DataType::Int64,
        FLOAT4OID => DataType::Float32,
        FLOAT8OID => DataType::Float64,
        TEXTOID | VARCHAROID | BPCHAROID => DataType::Utf8,
        BYTEAOID => DataType::Binary,
        DATEOID => DataType::Date32,
        TIMESTAMPOID | TIMESTAMPTZOID => DataType::Timestamp(TimeUnit::Microsecond, None),
        NUMERICOID => DataType::Float64, // Simplified - could use Decimal128
        JSONBOID | JSONOID => DataType::Utf8,
        UUIDOID => DataType::Utf8, // Store as string
        _ => DataType::Utf8,       // Fallback to text
    }
}

/// Build Arrow schema from Postgres table metadata.
pub fn build_arrow_schema_from_pg_table(table_name: &str) -> Result<ArrowSchema, String> {
    // Parse schema.table format
    let (schema_name, table_only) = parse_table_name(table_name);

    let query = format!(
        r#"
        SELECT column_name::text, udt_name::text, is_nullable::text
        FROM information_schema.columns
        WHERE table_schema = '{}' AND table_name = '{}'
        ORDER BY ordinal_position
        "#,
        schema_name, table_only
    );

    let mut fields = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(&query, None, &[])
            .map_err(|e| format!("SPI select error: {:?}", e))?;

        for row in result {
            // Columns: column_name, udt_name, is_nullable (positions 1, 2, 3)
            let col_name: String = row
                .get::<String>(1)
                .map_err(|e| format!("Failed to get column_name: {}", e))?
                .ok_or("column_name is null")?;

            let udt_name: String = row
                .get::<String>(2)
                .map_err(|e| format!("Failed to get udt_name: {}", e))?
                .ok_or("udt_name is null")?;

            let is_nullable: String = row
                .get::<String>(3)
                .map_err(|e| format!("Failed to get is_nullable: {}", e))?
                .ok_or("is_nullable is null")?;

            let arrow_type = pg_udt_to_arrow_type(&udt_name);
            let nullable = is_nullable == "YES";

            fields.push(Field::new(&col_name, arrow_type, nullable));
        }

        Ok::<(), String>(())
    })
    .map_err(|e| format!("SPI error: {:?}", e))?;

    if fields.is_empty() {
        return Err(format!("Table {} not found or has no columns", table_name));
    }

    Ok(ArrowSchema::new(fields))
}

/// Convert Postgres udt_name to Arrow DataType.
pub fn pg_udt_to_arrow_type(udt_name: &str) -> DataType {
    match udt_name {
        "bool" => DataType::Boolean,
        "int2" => DataType::Int16,
        "int4" => DataType::Int32,
        "int8" => DataType::Int64,
        "float4" => DataType::Float32,
        "float8" => DataType::Float64,
        "numeric" => DataType::Float64,
        "text" | "varchar" | "bpchar" | "name" => DataType::Utf8,
        "bytea" => DataType::Binary,
        "date" => DataType::Date32,
        // Use UTC timezone for timestamps - Delta Lake requires timezone for writer v7+
        "timestamp" | "timestamptz" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        // Delta Lake doesn't support Time type, store as string
        "time" | "timetz" => DataType::Utf8,
        "json" | "jsonb" => DataType::Utf8,
        "uuid" => DataType::Utf8,
        // Array types (prefixed with _) and unknown types fallback to text
        _ => DataType::Utf8,
    }
}

/// Parse schema.table into (schema, table).
pub fn parse_table_name(name: &str) -> (String, String) {
    if let Some(dot_pos) = name.find('.') {
        let schema = &name[..dot_pos];
        let table = &name[dot_pos + 1..];
        (schema.to_string(), table.to_string())
    } else {
        ("public".to_string(), name.to_string())
    }
}

/// Generate CREATE TABLE statement from Arrow schema.
pub fn generate_create_table_sql(table_name: &str, schema: &ArrowSchema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| {
            let pg_type = arrow_type_to_pg_type(f.data_type());
            let nullable = if f.is_nullable() { "" } else { " NOT NULL" };
            format!("\"{}\" {}{}", f.name(), pg_type, nullable)
        })
        .collect();

    format!(
        "CREATE TABLE IF NOT EXISTS {} (\n    {}\n)",
        table_name,
        columns.join(",\n    ")
    )
}

/// Generate INSERT statement for Arrow RecordBatch.
pub fn generate_insert_sql(table_name: &str, schema: &ArrowSchema) -> String {
    let columns: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("\"{}\"", f.name()))
        .collect();

    let placeholders: Vec<String> = (1..=schema.fields().len())
        .map(|i| format!("${}", i))
        .collect();

    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        table_name,
        columns.join(", "),
        placeholders.join(", ")
    )
}

// Note: arrow_value_to_datum was removed because pgrx 0.16 changed the Datum type.
// We now use SQL literal formatting for inserts instead of parameterized queries.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name_with_schema() {
        let (schema, table) = parse_table_name("myschema.mytable");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_parse_table_name_without_schema() {
        let (schema, table) = parse_table_name("mytable");
        assert_eq!(schema, "public");
        assert_eq!(table, "mytable");
    }

    #[test]
    fn test_arrow_type_to_pg_type() {
        assert_eq!(arrow_type_to_pg_type(&DataType::Boolean), "BOOLEAN");
        assert_eq!(arrow_type_to_pg_type(&DataType::Int32), "INTEGER");
        assert_eq!(arrow_type_to_pg_type(&DataType::Int64), "BIGINT");
        assert_eq!(
            arrow_type_to_pg_type(&DataType::Float64),
            "DOUBLE PRECISION"
        );
        assert_eq!(arrow_type_to_pg_type(&DataType::Utf8), "TEXT");
    }

    #[test]
    fn test_generate_create_table_sql() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, false),
        ]);

        let sql = generate_create_table_sql("public.test", &schema);
        assert!(sql.contains("\"id\" BIGINT NOT NULL"));
        assert!(sql.contains("\"name\" TEXT"));
        assert!(sql.contains("\"active\" BOOLEAN NOT NULL"));
    }

    #[test]
    fn test_generate_insert_sql() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let sql = generate_insert_sql("public.test", &schema);
        assert_eq!(
            sql,
            "INSERT INTO public.test (\"id\", \"name\") VALUES ($1, $2)"
        );
    }
}
