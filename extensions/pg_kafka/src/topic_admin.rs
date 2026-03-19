//! Topic creation and deletion functions

use pgrx::prelude::*;

/// Create a new topic
#[pg_extern(sql = "
CREATE FUNCTION create_topic(
    name TEXT,
    source_table TEXT DEFAULT NULL,
    offset_column TEXT DEFAULT NULL,
    value_column TEXT DEFAULT NULL,
    value_expr TEXT DEFAULT NULL,
    key_column TEXT DEFAULT NULL,
    key_expr TEXT DEFAULT NULL,
    config jsonb DEFAULT NULL
) RETURNS INT
VOLATILE
LANGUAGE c AS 'MODULE_PATHNAME', 'create_topic_wrapper';
")]
pub fn create_topic(
    name: &str,
    source_table: default!(Option<&str>, "NULL"),
    offset_column: default!(Option<&str>, "NULL"),
    value_column: default!(Option<&str>, "NULL"),
    value_expr: default!(Option<&str>, "NULL"),
    key_column: default!(Option<&str>, "NULL"),
    key_expr: default!(Option<&str>, "NULL"),
    config: default!(Option<pgrx::JsonB>, "NULL"),
) -> i32 {
    let result = Spi::get_one_with_args::<i32>(
        "INSERT INTO pgkafka.topics (name, source_table, offset_column, value_column, value_expr, key_column, key_expr, config)
         VALUES ($1, $2, $3, $4, $5, $6, $7, COALESCE($8, '{}'::jsonb))
         RETURNING id",
        &[
            name.into(),
            source_table.into(),
            offset_column.into(),
            value_column.into(),
            value_expr.into(),
            key_column.into(),
            key_expr.into(),
            config.into(),
        ],
    );

    match result {
        Ok(Some(id)) => id,
        Ok(None) => -1,
        Err(e) => {
            pgrx::error!("Failed to create topic: {}", e);
        }
    }
}

/// Drop a topic
#[pg_extern(sql = "
CREATE FUNCTION drop_topic(name TEXT, delete_messages bool DEFAULT true) RETURNS bool
VOLATILE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'drop_topic_wrapper';
")]
pub fn drop_topic(name: &str, delete_messages: default!(bool, true)) -> bool {
    let query = if delete_messages {
        "DELETE FROM pgkafka.topics WHERE name = $1"
    } else {
        "UPDATE pgkafka.topics SET name = name || '_deleted_' || id WHERE name = $1"
    };

    match Spi::run_with_args(query, &[name.into()]) {
        Ok(_) => true,
        Err(e) => {
            pgrx::error!("Failed to drop topic: {}", e);
        }
    }
}
