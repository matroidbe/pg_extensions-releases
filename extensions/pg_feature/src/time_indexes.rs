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
            // A time of day cannot be compared with a cutoff timestamp
            let valid_types = [
                "timestamp without time zone",
                "timestamp with time zone",
                "date",
            ];
            if !valid_types.contains(&dt.to_lowercase().as_str()) {
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
    // A read-write SPI call: a select would be read-only, and fail, whenever this
    // is the transaction's first write
    let deleted = Spi::get_one_with_args::<bool>(
        "WITH d AS (
            DELETE FROM pgft.time_indexes WHERE schema_name = $1 AND table_name = $2 RETURNING 1
         )
         SELECT COUNT(*) > 0 FROM d",
        &[
            table_ref.schema.as_str().into(),
            table_ref.name.as_str().into(),
        ],
    )?;
    Ok(deleted.unwrap_or(false))
}

/// Common time column names, most preferred first. `updated_at` comes last: it
/// moves when a row changes, so it says little about when the row became known.
const TIME_COLUMN_NAMES: &[&str] = &[
    "created_at",
    "occurred_at",
    "event_time",
    "event_timestamp",
    "transaction_time",
    "order_date",
    "purchase_date",
    "timestamp",
    "datetime",
    "date",
    "time",
    "updated_at",
];

/// Auto-discover time indexes by looking for timestamp/date columns with a
/// common time column name; one per table, the most preferred name wins
pub(crate) fn discover_time_indexes_impl(schema_name: &str) -> Result<i32, PgFeatureError> {
    let names: Vec<String> = TIME_COLUMN_NAMES.iter().map(|n| n.to_string()).collect();
    let discovered = Spi::get_one_with_args::<i64>(
        r#"
        WITH candidates AS (
            SELECT DISTINCT ON (c.table_name)
                   c.table_schema::text AS schema_name,
                   c.table_name::text AS table_name,
                   c.column_name::text AS time_column
            FROM information_schema.columns c
            JOIN information_schema.tables t
                ON c.table_schema = t.table_schema AND c.table_name = t.table_name
            WHERE c.table_schema = $1
              AND t.table_type = 'BASE TABLE'
              AND c.data_type::text IN ('timestamp without time zone', 'timestamp with time zone', 'date')
              AND LOWER(c.column_name::text) = ANY($2)
            ORDER BY c.table_name, array_position($2, LOWER(c.column_name::text))
        ),
        ins AS (
            INSERT INTO pgft.time_indexes (schema_name, table_name, time_column)
            SELECT * FROM candidates ORDER BY table_name
            ON CONFLICT (schema_name, table_name) DO NOTHING
            RETURNING 1
        )
        SELECT COUNT(*) FROM ins
        "#,
        &[schema_name.into(), names.into()],
    )?;
    Ok(discovered.unwrap_or(0) as i32)
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
