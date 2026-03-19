//! Time index management for pg_feature
//!
//! Time indexes specify which column in a table represents "when data became known".
//! This is critical for preventing data leakage in ML by filtering data based on
//! cutoff times during feature generation.

use crate::schema::{parse_table_ref, TableRef};
use crate::PgFeatureError;
use pgrx::prelude::*;

/// Escape a string literal for use in SQL
fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Set a time index for a table
pub(crate) fn set_time_index_impl(
    table_name: &str,
    time_column: &str,
) -> Result<i64, PgFeatureError> {
    let table_ref = parse_table_ref(table_name)?;

    // First verify the column exists and is a timestamp/date type
    let data_type = Spi::get_one_with_args::<String>(
        r#"
        SELECT data_type::text
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
        "#,
        &[
            table_ref.schema.clone().into(),
            table_ref.name.clone().into(),
            time_column.into(),
        ],
    )
    .map_err(|e| PgFeatureError::SqlError(e.to_string()))?;

    // Validate the column type
    match data_type {
        Some(dt) => {
            let valid_types = [
                "timestamp without time zone",
                "timestamp with time zone",
                "date",
                "time without time zone",
                "time with time zone",
            ];
            if !valid_types.iter().any(|t| dt.to_lowercase().contains(t)) {
                return Err(PgFeatureError::InvalidParameter(format!(
                    "Column '{}' has type '{}', expected a timestamp or date type",
                    time_column, dt
                )));
            }
        }
        None => {
            return Err(PgFeatureError::ColumnNotFound(
                format!("{}.{}", table_ref.schema, table_ref.name),
                time_column.to_string(),
            ));
        }
    }

    // Insert or update the time index using Spi::get_one_with_args
    let id = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgft.time_indexes (schema_name, table_name, time_column)
        VALUES ($1, $2, $3)
        ON CONFLICT (schema_name, table_name)
        DO UPDATE SET time_column = EXCLUDED.time_column, created_at = NOW()
        RETURNING id
        "#,
        &[
            table_ref.schema.into(),
            table_ref.name.into(),
            time_column.into(),
        ],
    )
    .map_err(|e| PgFeatureError::SqlError(e.to_string()))?
    .ok_or_else(|| PgFeatureError::SqlError("Insert returned no id".to_string()))?;

    Ok(id)
}

/// Verify that a column exists and is a timestamp/date type
#[allow(dead_code)]
fn verify_time_column(table_ref: &TableRef, time_column: &str) -> Result<(), PgFeatureError> {
    // Use Spi::connect_mut to avoid read-only mode that would affect subsequent writes
    let column_exists = Spi::connect_mut(|client| {
        let sql = format!(
            r#"
            SELECT data_type::text
            FROM information_schema.columns
            WHERE table_schema = '{}'
              AND table_name = '{}'
              AND column_name = '{}'
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name),
            escape_literal(time_column)
        );
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let data_type: Option<String> = row.get(1)?;
            Ok::<_, pgrx::spi::Error>(data_type)
        } else {
            Ok::<_, pgrx::spi::Error>(None)
        }
    })?;

    match column_exists {
        Some(data_type) => {
            let valid_types = [
                "timestamp without time zone",
                "timestamp with time zone",
                "date",
                "time without time zone",
                "time with time zone",
            ];
            if !valid_types
                .iter()
                .any(|t| data_type.to_lowercase().contains(t))
            {
                return Err(PgFeatureError::InvalidParameter(format!(
                    "Column '{}' has type '{}', expected a timestamp or date type",
                    time_column, data_type
                )));
            }
            Ok(())
        }
        None => Err(PgFeatureError::ColumnNotFound(
            format!("{}.{}", table_ref.schema, table_ref.name),
            time_column.to_string(),
        )),
    }
}

/// List all registered time indexes
pub(crate) fn list_time_indexes_impl() -> Result<Vec<(i64, String, String, String)>, PgFeatureError>
{
    let indexes = Spi::connect(|client| {
        let result = client.select(
            "SELECT id, schema_name, table_name, time_column FROM pgft.time_indexes ORDER BY id",
            None,
            &[],
        )?;

        let mut indexes = Vec::new();
        for row in result {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let schema_name: String = row.get(2)?.unwrap_or_default();
            let table_name: String = row.get(3)?.unwrap_or_default();
            let time_column: String = row.get(4)?.unwrap_or_default();
            indexes.push((id, schema_name, table_name, time_column));
        }
        Ok::<_, pgrx::spi::Error>(indexes)
    })?;

    Ok(indexes)
}

/// Remove a time index for a table
pub(crate) fn remove_time_index_impl(table_name: &str) -> Result<bool, PgFeatureError> {
    let table_ref = parse_table_ref(table_name)?;

    let deleted = Spi::connect_mut(|client| {
        let sql = format!(
            r#"
            DELETE FROM pgft.time_indexes
            WHERE schema_name = '{}' AND table_name = '{}'
            RETURNING id
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name)
        );
        let result = client.select(&sql, None, &[])?;

        // Count the rows returned
        let mut count = 0;
        for _ in result {
            count += 1;
        }
        Ok::<_, pgrx::spi::Error>(count > 0)
    })?;

    Ok(deleted)
}

/// Auto-discover time indexes by looking for timestamp columns
pub(crate) fn discover_time_indexes_impl(schema_name: &str) -> Result<i32, PgFeatureError> {
    // Common time column names to look for
    let time_column_patterns = [
        "created_at",
        "updated_at",
        "timestamp",
        "event_time",
        "event_timestamp",
        "date",
        "datetime",
        "time",
        "occurred_at",
        "transaction_time",
        "order_date",
        "purchase_date",
    ];

    let discovered = Spi::connect_mut(|client| {
        // Find tables with timestamp columns matching common patterns
        // Note: Cast sql_identifier columns to text for Rust compatibility
        let query = format!(
            r#"
            SELECT DISTINCT
                c.table_schema::text,
                c.table_name::text,
                c.column_name::text
            FROM information_schema.columns c
            JOIN information_schema.tables t
                ON c.table_schema = t.table_schema AND c.table_name = t.table_name
            WHERE c.table_schema = '{}'
              AND t.table_type = 'BASE TABLE'
              AND (c.data_type LIKE '%timestamp%' OR c.data_type = 'date')
              AND (
                  {}
              )
              AND NOT EXISTS (
                  SELECT 1 FROM pgft.time_indexes ti
                  WHERE ti.schema_name = c.table_schema::text AND ti.table_name = c.table_name::text
              )
            ORDER BY c.table_name, c.column_name
            "#,
            escape_literal(schema_name),
            time_column_patterns
                .iter()
                .map(|p| format!("LOWER(c.column_name::text) = '{}'", p))
                .collect::<Vec<_>>()
                .join(" OR ")
        );

        let result = client.select(&query, None, &[])?;

        // Collect rows first since we need mutable client for updates
        let mut rows = Vec::new();
        for row in result {
            let table_schema: String = row.get(1)?.unwrap_or_default();
            let table_name: String = row.get(2)?.unwrap_or_default();
            let column_name: String = row.get(3)?.unwrap_or_default();
            rows.push((table_schema, table_name, column_name));
        }

        let mut count = 0;
        let mut seen_tables = std::collections::HashSet::new();

        for (table_schema, table_name, column_name) in rows {
            // Only register one time index per table (first match wins)
            let table_key = format!("{}.{}", table_schema, table_name);
            if seen_tables.contains(&table_key) {
                continue;
            }
            seen_tables.insert(table_key);

            // Insert the time index
            let insert_sql = format!(
                r#"
                INSERT INTO pgft.time_indexes (schema_name, table_name, time_column)
                VALUES ('{}', '{}', '{}')
                ON CONFLICT (schema_name, table_name) DO NOTHING
                "#,
                escape_literal(&table_schema),
                escape_literal(&table_name),
                escape_literal(&column_name)
            );
            Spi::run(&insert_sql)?;
            count += 1;
        }

        Ok::<_, pgrx::spi::Error>(count)
    })?;

    Ok(discovered)
}

/// Get the time index column for a table, if registered
pub(crate) fn get_time_index(table_ref: &TableRef) -> Result<Option<String>, PgFeatureError> {
    let time_column = Spi::connect(|client| {
        let sql = format!(
            r#"
            SELECT time_column
            FROM pgft.time_indexes
            WHERE schema_name = '{}' AND table_name = '{}'
            "#,
            escape_literal(&table_ref.schema),
            escape_literal(&table_ref.name)
        );
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            Ok::<_, pgrx::spi::Error>(row.get::<String>(1)?)
        } else {
            Ok::<_, pgrx::spi::Error>(None)
        }
    })?;

    Ok(time_column)
}

#[cfg(test)]
mod tests {
    // Unit tests for table parsing are in schema.rs
}
