//! Message production functions

use pgrx::prelude::*;

/// Parse bytes into text and JSON representations for SQL produce function
pub fn parse_bytes_for_sql(bytes: &[u8]) -> (Option<String>, Option<pgrx::JsonB>) {
    let text = std::str::from_utf8(bytes).ok().map(|s| s.to_string());
    let json = text
        .as_ref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .map(pgrx::JsonB);
    (text, json)
}

/// Produce a message to a topic (SQL interface)
///
/// Note: Source-backed topics are read-only and cannot accept produces.
#[pg_extern(sql = "
CREATE FUNCTION produce(topic TEXT, value bytea, key bytea DEFAULT NULL, headers jsonb DEFAULT NULL) RETURNS bigint
VOLATILE LANGUAGE c AS 'MODULE_PATHNAME', 'produce_wrapper';
")]
pub fn produce(
    topic: &str,
    value: Vec<u8>,
    key: default!(Option<Vec<u8>>, "NULL"),
    headers: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    // Check if topic is source-backed (read-only)
    let is_source_backed = Spi::get_one_with_args::<bool>(
        "SELECT source_table IS NOT NULL FROM pgkafka.topics WHERE name = $1",
        &[topic.into()],
    );

    match is_source_backed {
        Ok(Some(true)) => {
            pgrx::error!(
                "Topic '{}' is backed by a source table and is read-only",
                topic
            );
        }
        Ok(None) => {
            pgrx::error!("Topic '{}' not found", topic);
        }
        Ok(Some(false)) => {}
        Err(e) => {
            pgrx::error!("Failed to check topic: {}", e);
        }
    }

    // Parse key if present
    let (key_text, key_json) = key
        .as_ref()
        .map(|k| parse_bytes_for_sql(k))
        .unwrap_or((None, None));

    // Parse value
    let (value_text, value_json) = parse_bytes_for_sql(&value);

    let result = Spi::get_one_with_args::<i64>(
        "INSERT INTO pgkafka.messages (topic_id, key, key_text, key_json, value, value_text, value_json, headers)
         SELECT t.id, $2, $3, $4, $5, $6, $7, COALESCE($8, '{}'::jsonb)
         FROM pgkafka.topics t WHERE t.name = $1
         RETURNING offset_id",
        &[
            topic.into(),
            key.into(),
            key_text.into(),
            key_json.into(),
            value.into(),
            value_text.into(),
            value_json.into(),
            headers.into(),
        ],
    );

    match result {
        Ok(Some(offset)) => offset,
        Ok(None) => -1,
        Err(e) => {
            pgrx::error!("Failed to produce message: {}", e);
        }
    }
}
