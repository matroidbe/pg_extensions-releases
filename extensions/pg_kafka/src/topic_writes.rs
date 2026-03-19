//! Source-backed topic write operations

use pgrx::prelude::*;

use crate::validation::{validate_source_table, validate_timestamp_column};

/// Create a topic from an existing PostgreSQL table
///
/// This exposes an existing table as a Kafka topic, allowing Kafka clients
/// to consume data from the table without data duplication.
///
/// # Arguments
/// * `name` - Topic name (must be unique)
/// * `source_table` - Fully qualified table name (e.g., 'public.orders')
/// * `offset_column` - Column to use as offset (must be integer, unique, indexed)
/// * `value_column` - Column to use as message value (optional, mutually exclusive with value_expr)
/// * `value_expr` - SQL expression for message value (optional, e.g., 'row_to_json(t)')
/// * `key_column` - Column to use as message key (optional)
/// * `key_expr` - SQL expression for message key (optional)
/// * `timestamp_column` - Column to use as timestamp (optional)
///
/// # Returns
/// The topic ID on success
///
/// # Errors
/// Returns an error if:
/// - The source table does not exist
/// - The offset column does not exist or is not an integer type
/// - The offset column does not have a UNIQUE or PRIMARY KEY constraint
/// - Neither value_column nor value_expr is provided
#[pg_extern(sql = "
CREATE FUNCTION create_topic_from_table(
    name TEXT,
    source_table TEXT,
    offset_column TEXT,
    value_column TEXT DEFAULT NULL,
    value_expr TEXT DEFAULT NULL,
    key_column TEXT DEFAULT NULL,
    key_expr TEXT DEFAULT NULL,
    timestamp_column TEXT DEFAULT NULL
) RETURNS INT
VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', 'create_topic_from_table_wrapper';
")]
pub fn create_topic_from_table(
    name: &str,
    source_table: &str,
    offset_column: &str,
    value_column: default!(Option<&str>, "NULL"),
    value_expr: default!(Option<&str>, "NULL"),
    key_column: default!(Option<&str>, "NULL"),
    key_expr: default!(Option<&str>, "NULL"),
    timestamp_column: default!(Option<&str>, "NULL"),
) -> i32 {
    // Validate that at least one of value_column or value_expr is provided
    if value_column.is_none() && value_expr.is_none() {
        pgrx::error!("Either value_column or value_expr must be provided");
    }

    // Validate that both value_column and value_expr are not provided
    if value_column.is_some() && value_expr.is_some() {
        pgrx::error!("Cannot specify both value_column and value_expr");
    }

    // Validate that both key_column and key_expr are not provided
    if key_column.is_some() && key_expr.is_some() {
        pgrx::error!("Cannot specify both key_column and key_expr");
    }

    // Validate the source table
    if let Err(e) = validate_source_table(source_table, offset_column) {
        pgrx::error!("{}", e);
    }

    // Validate timestamp column if provided
    if let Some(ts_col) = timestamp_column {
        if let Err(e) = validate_timestamp_column(source_table, ts_col) {
            pgrx::error!("{}", e);
        }
    }

    // Create the topic
    let result = Spi::get_one_with_args::<i32>(
        "INSERT INTO pgkafka.topics (name, source_table, offset_column, value_column, value_expr, key_column, key_expr, timestamp_column)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         RETURNING id",
        &[
            name.into(),
            source_table.into(),
            offset_column.into(),
            value_column.into(),
            value_expr.into(),
            key_column.into(),
            key_expr.into(),
            timestamp_column.into(),
        ],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => -1,
        Err(e) => {
            pgrx::error!("Failed to create topic from table: {}", e);
        }
    }
}

/// Enable writes for a source-backed topic
///
/// # Arguments
/// * `topic_name` - The topic name
/// * `write_mode` - "stream" (append-only) or "table" (upsert), default: "stream"
/// * `key_column` - For table mode: column to match Kafka message key (required)
/// * `kafka_offset_column` - For table mode: column to track Kafka offset (required)
///
/// # Returns
/// True if the topic was updated successfully
///
/// # Errors
/// - Topic must exist and be source-backed
/// - For "table" mode, key_column and kafka_offset_column are required
#[pg_extern(sql = "
CREATE FUNCTION enable_topic_writes(
    topic_name TEXT,
    write_mode TEXT DEFAULT 'stream',
    key_column TEXT DEFAULT NULL,
    kafka_offset_column TEXT DEFAULT NULL
) RETURNS BOOL
VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', 'enable_topic_writes_wrapper';
")]
pub fn enable_topic_writes(
    topic_name: &str,
    write_mode: default!(&str, "'stream'"),
    key_column: default!(Option<&str>, "NULL"),
    kafka_offset_column: default!(Option<&str>, "NULL"),
) -> bool {
    // Validate write_mode
    if write_mode != "stream" && write_mode != "table" {
        pgrx::error!(
            "Invalid write_mode '{}'. Must be 'stream' or 'table'",
            write_mode
        );
    }

    // Check if topic exists and is source-backed
    let is_source_backed = Spi::get_one_with_args::<bool>(
        "SELECT source_table IS NOT NULL FROM pgkafka.topics WHERE name = $1",
        &[topic_name.into()],
    );

    match is_source_backed {
        Ok(None) => {
            pgrx::error!("Topic '{}' not found", topic_name);
        }
        Ok(Some(false)) => {
            pgrx::error!(
                "Topic '{}' is a native topic, not source-backed. Only source-backed topics can enable writes.",
                topic_name
            );
        }
        Ok(Some(true)) => {}
        Err(e) => {
            pgrx::error!("Failed to check topic: {}", e);
        }
    }

    // For table mode, key_column and kafka_offset_column are required
    if write_mode == "table" {
        if key_column.is_none() {
            pgrx::error!(
                "key_column is required for 'table' mode. This column will store the Kafka message key."
            );
        }
        if kafka_offset_column.is_none() {
            pgrx::error!(
                "kafka_offset_column is required for 'table' mode. This column tracks the Kafka offset for upserts."
            );
        }
    }

    // Update the topic with write settings
    let result = Spi::run_with_args(
        "UPDATE pgkafka.topics SET
            writable = TRUE,
            write_mode = $2,
            write_key_column = $3,
            kafka_offset_column = $4
         WHERE name = $1",
        &[
            topic_name.into(),
            write_mode.into(),
            key_column.into(),
            kafka_offset_column.into(),
        ],
    );

    match result {
        Ok(_) => true,
        Err(e) => {
            pgrx::error!("Failed to enable topic writes: {}", e);
        }
    }
}

/// Disable writes for a source-backed topic
///
/// # Arguments
/// * `topic_name` - The topic name
///
/// # Returns
/// True if the topic was updated successfully
#[pg_extern]
pub fn disable_topic_writes(topic_name: &str) -> bool {
    let result = Spi::run_with_args(
        "UPDATE pgkafka.topics SET
            writable = FALSE,
            write_mode = NULL,
            write_key_column = NULL,
            kafka_offset_column = NULL
         WHERE name = $1",
        &[topic_name.into()],
    );

    match result {
        Ok(_) => true,
        Err(e) => {
            pgrx::error!("Failed to disable topic writes: {}", e);
        }
    }
}
