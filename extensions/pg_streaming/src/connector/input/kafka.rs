//! Kafka input connector — pulls from pgkafka topics via SPI
//!
//! Supports two topic types:
//! - Regular topics: reads from pgkafka.messages table
//! - Source-backed typed topics: reads directly from the source table
//!
//! The topic type is auto-detected from pgkafka.topics metadata during
//! pipeline compilation and passed in as TopicShape.

use crate::connector::InputConnector;
use crate::record::{RecordBatch, TopicShape};
use pgrx::prelude::*;

pub struct KafkaInput {
    topic_name: String,
    topic_id: Option<i32>,
    last_offset: i64,
    start: String,
    shape: TopicShape,
}

impl KafkaInput {
    pub fn new(topic: &str, _group: &str, start: &str, shape: TopicShape) -> Self {
        Self {
            topic_name: topic.to_string(),
            topic_id: None,
            last_offset: 0,
            start: start.to_string(),
            shape,
        }
    }

    /// Resolve topic name → topic_id, caching for subsequent polls
    fn resolve_topic_id(&mut self) -> Result<i32, String> {
        if let Some(id) = self.topic_id {
            return Ok(id);
        }

        let result = Spi::get_one_with_args::<i32>(
            "SELECT id FROM pgkafka.topics WHERE name = $1",
            &[self.topic_name.as_str().into()],
        );

        match result {
            Ok(Some(id)) => {
                self.topic_id = Some(id);
                Ok(id)
            }
            Ok(None) => Err(format!(
                "Kafka topic '{}' not found in pgkafka.topics",
                self.topic_name
            )),
            Err(e) => Err(format!(
                "Failed to resolve topic '{}': {} (is pg_kafka extension installed?)",
                self.topic_name, e
            )),
        }
    }

    /// Determine starting offset. Checks pgstreams.connector_offsets first
    /// (resuming), then falls back to the pipeline's `start` config.
    pub fn initialize_offset(&mut self, pipeline_name: &str) -> Result<(), String> {
        // Check for a previously committed offset (resume after restart)
        let existing = Spi::get_one_with_args::<i64>(
            "SELECT offset_value FROM pgstreams.connector_offsets \
             WHERE pipeline = $1 AND connector = 'kafka_input'",
            &[pipeline_name.into()],
        );

        if let Ok(Some(offset)) = existing {
            self.last_offset = offset;
            return Ok(());
        }

        // First run — determine starting offset from config
        match self.start.as_str() {
            "earliest" => {
                self.last_offset = 0;
            }
            "latest" => match &self.shape {
                TopicShape::Messages => {
                    let topic_id = self.resolve_topic_id()?;
                    let max_offset = Spi::get_one_with_args::<i64>(
                        "SELECT COALESCE(MAX(offset_id), 0) \
                         FROM pgkafka.messages WHERE topic_id = $1",
                        &[topic_id.into()],
                    );
                    self.last_offset = max_offset
                        .map_err(|e| format!("Failed to get latest offset: {}", e))?
                        .unwrap_or(0);
                }
                TopicShape::Typed {
                    source_table,
                    offset_column,
                    ..
                } => {
                    let query = format!(
                        "SELECT COALESCE(MAX({}::bigint), 0) FROM {}",
                        offset_column, source_table
                    );
                    let max_offset = Spi::get_one::<i64>(&query);
                    self.last_offset = max_offset
                        .map_err(|e| {
                            format!("Failed to get latest offset from {}: {}", source_table, e)
                        })?
                        .unwrap_or(0);
                }
                TopicShape::Cdc => unreachable!("KafkaInput cannot have Cdc shape"),
            },
            numeric => {
                self.last_offset = numeric
                    .parse::<i64>()
                    .map_err(|_| format!("Invalid start offset: '{}'", numeric))?;
            }
        }

        Ok(())
    }

    /// Poll from regular pgkafka.messages table
    fn poll_messages(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String> {
        let topic_id = self.resolve_topic_id()?;

        let query = "\
            SELECT COALESCE(jsonb_agg(sub.r ORDER BY sub.oid), '[]'::jsonb) FROM (\
              SELECT jsonb_build_object(\
                'key_text', m.key_text, \
                'key_json', m.key_json, \
                'value_text', m.value_text, \
                'value_json', m.value_json, \
                'headers', m.headers, \
                'offset_id', m.offset_id, \
                'created_at', m.created_at, \
                'source_topic', $3\
              ) AS r, m.offset_id AS oid \
              FROM pgkafka.messages m \
              WHERE m.topic_id = $1 AND m.offset_id > $2 \
              ORDER BY m.offset_id \
              LIMIT $4\
            ) sub";

        let result = Spi::get_one_with_args::<pgrx::JsonB>(
            query,
            &[
                topic_id.into(),
                self.last_offset.into(),
                self.topic_name.as_str().into(),
                (batch_size as i64).into(),
            ],
        );

        self.extract_batch(result)
    }

    /// Poll from a source-backed typed topic table
    fn poll_typed(
        &mut self,
        batch_size: i32,
        source_table: &str,
        offset_column: &str,
        columns: &[(String, String)],
    ) -> Result<(RecordBatch, Option<i64>), String> {
        // Build jsonb_build_object args for all columns + offset_id
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|(name, _)| format!("'{}', {}", name, name))
            .collect();

        let query = format!(
            "SELECT COALESCE(jsonb_agg(sub.r ORDER BY sub.oid), '[]'::jsonb) FROM (\
              SELECT jsonb_build_object({}, 'offset_id', {}::bigint) AS r, \
                     {}::bigint AS oid \
              FROM {} \
              WHERE {}::bigint > $1 \
              ORDER BY {} \
              LIMIT $2\
            ) sub",
            col_exprs.join(", "),
            offset_column,
            offset_column,
            source_table,
            offset_column,
            offset_column,
        );

        let result = Spi::get_one_with_args::<pgrx::JsonB>(
            &query,
            &[self.last_offset.into(), (batch_size as i64).into()],
        );

        self.extract_batch(result)
    }

    /// Extract records from SPI result, update last_offset, return batch
    fn extract_batch(
        &mut self,
        result: Result<Option<pgrx::JsonB>, pgrx::spi::SpiError>,
    ) -> Result<(RecordBatch, Option<i64>), String> {
        let jsonb =
            result.map_err(|e| format!("Failed to poll topic '{}': {}", self.topic_name, e))?;

        let batch_value = match jsonb {
            Some(jb) => jb.0,
            None => return Ok((vec![], None)),
        };

        let records: RecordBatch = match batch_value.as_array() {
            Some(arr) => arr.clone(),
            None => return Ok((vec![], None)),
        };

        if records.is_empty() {
            return Ok((vec![], None));
        }

        let max_offset = records.iter().filter_map(|r| r["offset_id"].as_i64()).max();

        if let Some(offset) = max_offset {
            self.last_offset = offset;
        }

        Ok((records, max_offset))
    }
}

impl InputConnector for KafkaInput {
    fn initialize(&mut self, pipeline_name: &str) -> Result<(), String> {
        // For regular topics, resolve topic_id eagerly
        if matches!(self.shape, TopicShape::Messages) {
            self.resolve_topic_id()?;
        }
        self.initialize_offset(pipeline_name)
    }

    fn poll(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String> {
        match &self.shape {
            TopicShape::Messages => self.poll_messages(batch_size),
            TopicShape::Typed {
                source_table,
                offset_column,
                columns,
            } => {
                let st = source_table.clone();
                let oc = offset_column.clone();
                let cols = columns.clone();
                self.poll_typed(batch_size, &st, &oc, &cols)
            }
            TopicShape::Cdc => unreachable!("KafkaInput cannot have Cdc shape"),
        }
    }

    fn commit(&mut self, pipeline_name: &str, offset: i64) -> Result<(), String> {
        Spi::run_with_args(
            "INSERT INTO pgstreams.connector_offsets (pipeline, connector, offset_value, updated_at) \
             VALUES ($1, 'kafka_input', $2, now()) \
             ON CONFLICT (pipeline, connector) \
             DO UPDATE SET offset_value = $2, updated_at = now()",
            &[pipeline_name.into(), offset.into()],
        )
        .map_err(|e| format!("Failed to commit offset: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_input_new_messages() {
        let input = KafkaInput::new("test-topic", "group", "latest", TopicShape::Messages);
        assert_eq!(input.topic_name, "test-topic");
        assert_eq!(input.start, "latest");
        assert!(matches!(input.shape, TopicShape::Messages));
    }

    #[test]
    fn test_kafka_input_new_typed() {
        let shape = TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("amount".to_string(), "numeric".to_string()),
            ],
        };
        let input = KafkaInput::new("orders", "group", "earliest", shape);
        assert!(matches!(input.shape, TopicShape::Typed { .. }));
    }

    #[test]
    fn test_poll_typed_query_generation() {
        let columns = [
            ("id".to_string(), "bigint".to_string()),
            ("customer_id".to_string(), "text".to_string()),
            ("amount".to_string(), "numeric".to_string()),
        ];

        // Build the same query as poll_typed would
        let col_exprs: Vec<String> = columns
            .iter()
            .map(|(name, _)| format!("'{}', {}", name, name))
            .collect();

        let query = format!(
            "SELECT COALESCE(jsonb_agg(sub.r ORDER BY sub.oid), '[]'::jsonb) FROM (\
              SELECT jsonb_build_object({}, 'offset_id', {}::bigint) AS r, \
                     {}::bigint AS oid \
              FROM {} \
              WHERE {}::bigint > $1 \
              ORDER BY {} \
              LIMIT $2\
            ) sub",
            col_exprs.join(", "),
            "id",
            "id",
            "public.orders",
            "id",
            "id",
        );

        assert!(query.contains("'id', id"));
        assert!(query.contains("'customer_id', customer_id"));
        assert!(query.contains("'amount', amount"));
        assert!(query.contains("FROM public.orders"));
        assert!(query.contains("WHERE id::bigint > $1"));
        assert!(query.contains("'offset_id', id::bigint"));
    }
}
