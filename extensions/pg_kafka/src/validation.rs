//! Validation functions for source tables and columns

use pgrx::prelude::*;

/// Parse a table name into schema and table components
pub fn parse_table_name(source_table: &str) -> Result<(&str, &str), String> {
    let parts: Vec<&str> = source_table.split('.').collect();
    match parts.len() {
        2 => Ok((parts[0], parts[1])),
        1 => Ok(("public", parts[0])),
        _ => Err(format!(
            "Invalid table name '{}': expected 'schema.table' or 'table'",
            source_table
        )),
    }
}

/// Get a list of integer columns from a table (for suggestions in error messages)
pub fn get_integer_columns(schema: &str, table: &str) -> Vec<String> {
    let result = Spi::connect(|client| {
        // Cast column_name to text because information_schema uses sql_identifier type
        let query = "SELECT column_name::text FROM information_schema.columns
                     WHERE table_schema = $1 AND table_name = $2
                     AND data_type IN ('integer', 'bigint', 'smallint')
                     ORDER BY ordinal_position";
        let mut columns = Vec::new();
        let table_result = client.select(query, None, &[schema.into(), table.into()]);
        if let Ok(table) = table_result {
            for row in table {
                if let Ok(Some(col)) = row.get::<String>(1) {
                    columns.push(col);
                }
            }
        }
        Ok::<_, pgrx::spi::SpiError>(columns)
    });
    result.unwrap_or_default()
}

/// Validate a source table for use as a Kafka topic
///
/// Checks:
/// - Table exists
/// - Offset column exists and is integer type
/// - Offset column has UNIQUE or PRIMARY KEY constraint
pub fn validate_source_table(source_table: &str, offset_column: &str) -> Result<(), String> {
    let (schema, table) = parse_table_name(source_table)?;

    // Check table exists
    let table_exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = $1 AND table_name = $2
        )",
        &[schema.into(), table.into()],
    );

    match table_exists {
        Ok(Some(true)) => {}
        Ok(Some(false)) | Ok(None) => {
            // Check if it exists in a different schema
            // Cast table_schema to text because information_schema uses sql_identifier type
            let exists_elsewhere = Spi::get_one_with_args::<String>(
                "SELECT table_schema::text FROM information_schema.tables WHERE table_name = $1 LIMIT 1",
                &[table.into()],
            );

            let hint = if let Ok(Some(other_schema)) = exists_elsewhere {
                format!(
                    "\n\nDid you mean '{}.{}'? The table exists in the '{}' schema.",
                    other_schema, table, other_schema
                )
            } else {
                String::new()
            };

            return Err(format!("Table '{}' does not exist.{}", source_table, hint));
        }
        Err(e) => {
            return Err(format!("Failed to check table existence: {}", e));
        }
    }

    // Check offset column exists and get its type
    // Cast data_type to text because information_schema uses character_data type
    let col_type = Spi::get_one_with_args::<String>(
        "SELECT data_type::text FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
        &[schema.into(), table.into(), offset_column.into()],
    );

    match col_type {
        Ok(Some(dtype)) => {
            // Check column type is orderable integer
            let valid_types = ["integer", "bigint", "smallint"];
            if !valid_types.contains(&dtype.as_str()) {
                return Err(format!(
                    "Offset column '{}' must be integer type (got '{}').\n\n\
                    To use this table as a Kafka topic, add a BIGSERIAL offset column:\n\
                      ALTER TABLE {} ADD COLUMN kafka_offset BIGSERIAL UNIQUE;\n\n\
                    Then use 'kafka_offset' as your offset_column.",
                    offset_column, dtype, source_table
                ));
            }
        }
        Ok(None) => {
            // Column doesn't exist - suggest valid integer columns
            let int_columns = get_integer_columns(schema, table);
            let suggestion = if int_columns.is_empty() {
                format!(
                    "\n\nNo integer columns found. Add a BIGSERIAL offset column:\n\
                      ALTER TABLE {} ADD COLUMN kafka_offset BIGSERIAL UNIQUE;\n\n\
                    Then use 'kafka_offset' as your offset_column.",
                    source_table
                )
            } else {
                format!("\n\nAvailable integer columns: {}", int_columns.join(", "))
            };

            return Err(format!(
                "Column '{}' does not exist in table '{}'.{}",
                offset_column, source_table, suggestion
            ));
        }
        Err(e) => {
            return Err(format!("Failed to check column type: {}", e));
        }
    }

    // Check column has UNIQUE or PRIMARY KEY constraint
    let has_unique = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
            SELECT 1 FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND kcu.column_name = $3
              AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
        )",
        &[schema.into(), table.into(), offset_column.into()],
    );

    match has_unique {
        Ok(Some(true)) => {}
        Ok(Some(false)) | Ok(None) => {
            return Err(format!(
                "Offset column '{}' must have UNIQUE or PRIMARY KEY constraint.\n\n\
                Add a unique constraint:\n\
                  ALTER TABLE {} ADD CONSTRAINT {}_unique UNIQUE ({});\n\n\
                Or use a different column that already has a unique constraint.",
                offset_column, source_table, offset_column, offset_column
            ));
        }
        Err(e) => {
            return Err(format!("Failed to check column constraints: {}", e));
        }
    }

    Ok(())
}

/// Validate a timestamp column for use with a Kafka topic
///
/// Checks:
/// - Column exists in the table
/// - Column type is temporal (timestamp, timestamptz) or bigint (epoch ms)
/// - Warns if no index exists on the column (for ListOffsets performance)
pub fn validate_timestamp_column(source_table: &str, timestamp_column: &str) -> Result<(), String> {
    let (schema, table) = parse_table_name(source_table)?;

    // Check column exists and get its type
    // Cast data_type to text because information_schema uses character_data type
    let col_type = Spi::get_one_with_args::<String>(
        "SELECT data_type::text FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2 AND column_name = $3",
        &[schema.into(), table.into(), timestamp_column.into()],
    );

    match col_type {
        Ok(Some(dtype)) => {
            // Valid types for timestamp column
            let valid_types = [
                "timestamp with time zone",
                "timestamp without time zone",
                "bigint", // epoch milliseconds
            ];
            if !valid_types.contains(&dtype.as_str()) {
                return Err(format!(
                    "Timestamp column '{}' must be a temporal type (got '{}').\n\n\
                    Valid types:\n\
                      - timestamp with time zone (recommended)\n\
                      - timestamp without time zone\n\
                      - bigint (for epoch milliseconds)\n\n\
                    Example:\n\
                      ALTER TABLE {} ADD COLUMN event_time TIMESTAMPTZ DEFAULT now();",
                    timestamp_column, dtype, source_table
                ));
            }
        }
        Ok(None) => {
            return Err(format!(
                "Timestamp column '{}' does not exist in table '{}'.",
                timestamp_column, source_table
            ));
        }
        Err(e) => {
            return Err(format!("Failed to check timestamp column type: {}", e));
        }
    }

    // Check if there's an index on the timestamp column (warning only, not error)
    let has_index = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = $1
              AND tablename = $2
              AND indexdef LIKE '%' || $3 || '%'
        )",
        &[schema.into(), table.into(), timestamp_column.into()],
    );

    match has_index {
        Ok(Some(false)) | Ok(None) => {
            // Issue a warning but don't fail
            pgrx::warning!(
                "Timestamp column '{}' has no index. For better ListOffsets(timestamp) performance, consider:\n\
                  CREATE INDEX ON {} ({});",
                timestamp_column, source_table, timestamp_column
            );
        }
        _ => {}
    }

    Ok(())
}
