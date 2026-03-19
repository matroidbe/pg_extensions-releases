//! Record types for stream processing
//!
//! Records flow through the pipeline as JSONB values. For regular Kafka topics,
//! the standard fields are: key_text, key_json, value_text, value_json,
//! headers, offset_id, created_at, source_topic. For source-backed typed topics,
//! the fields are the actual table columns (e.g., id, customer_id, amount).

/// A single record flowing through the pipeline (JSON object)
pub type Record = serde_json::Value;

/// A batch of records
pub type RecordBatch = Vec<Record>;

/// Describes the column layout of records from a topic.
/// Used to generate the correct CTE for processor SQL expressions.
#[derive(Debug, Clone)]
pub enum TopicShape {
    /// Regular pgkafka.messages topic — standard Kafka message columns
    Messages,
    /// Source-backed typed topic — columns come from the source table
    Typed {
        source_table: String,
        offset_column: String,
        /// (column_name, pg_type) pairs from information_schema
        columns: Vec<(String, String)>,
    },
    /// CDC (Change Data Capture) — logical replication change records
    /// Fields: operation, table_name, commit_timestamp, lsn, offset_id, new, old
    Cdc,
}

impl TopicShape {
    /// Build the CTE preamble that unpacks a JSONB array of records into
    /// typed columns. The column set depends on the topic shape.
    pub fn batch_cte(&self) -> String {
        match self {
            TopicShape::Messages => MESSAGES_CTE.to_string(),
            TopicShape::Cdc => CDC_CTE.to_string(),
            TopicShape::Typed { columns, .. } => {
                let col_exprs: Vec<String> = columns
                    .iter()
                    .map(|(name, pg_type)| {
                        let cast = pg_type_to_cast(pg_type);
                        if cast.is_empty() {
                            format!("r->>'{}' AS {}", name, name)
                        } else {
                            format!("(r->>'{}')::{} AS {}", name, cast, name)
                        }
                    })
                    .collect();

                format!(
                    "WITH _batch AS (\
                       SELECT r AS _original, {} \
                       FROM jsonb_array_elements($1) AS r\
                     ) ",
                    col_exprs.join(", ")
                )
            }
        }
    }
}

/// Map PostgreSQL type names to SQL cast expressions.
/// Text-like types need no cast (extracted via ->>). Numeric/temporal types
/// need explicit casts to preserve their type in SQL expressions.
pub fn pg_type_to_cast(pg_type: &str) -> &str {
    match pg_type {
        "integer" | "int4" => "integer",
        "smallint" | "int2" => "smallint",
        "bigint" | "int8" => "bigint",
        "numeric" | "decimal" => "numeric",
        "real" | "float4" => "real",
        "double precision" | "float8" => "double precision",
        "boolean" | "bool" => "boolean",
        "timestamp with time zone" | "timestamptz" => "timestamptz",
        "timestamp without time zone" | "timestamp" => "timestamp",
        "date" => "date",
        "time" | "time without time zone" => "time",
        "interval" => "interval",
        "jsonb" => "jsonb",
        "json" => "json",
        "uuid" => "uuid",
        "bytea" => "bytea",
        _ => "", // text, varchar, char, etc. — no cast needed
    }
}

/// CTE for regular pgkafka.messages topics (static)
const MESSAGES_CTE: &str = "WITH _batch AS (\
       SELECT \
         r AS _original, \
         null::bytea AS key, \
         r->>'key_text' AS key_text, \
         (r->'key_json') AS key_json, \
         null::bytea AS value, \
         r->>'value_text' AS value_text, \
         (r->'value_json') AS value_json, \
         (r->'headers') AS headers, \
         (r->>'offset_id')::bigint AS offset_id, \
         (r->>'created_at')::timestamptz AS created_at, \
         r->>'source_topic' AS source_topic \
       FROM jsonb_array_elements($1) AS r\
     ) ";

/// CTE for CDC change records — unpacks operation, table, timestamp, lsn, new/old
const CDC_CTE: &str = "WITH _batch AS (\
       SELECT \
         r AS _original, \
         r->>'operation' AS operation, \
         r->>'table' AS table_name, \
         r->>'timestamp' AS commit_timestamp, \
         r->>'lsn' AS lsn, \
         (r->>'offset_id')::bigint AS offset_id, \
         r->'new' AS new, \
         r->'old' AS old \
       FROM jsonb_array_elements($1) AS r\
     ) ";

/// Build the CTE for regular messages topics (convenience function).
/// Used in tests to construct processors with the default messages shape.
#[cfg(test)]
pub fn batch_cte() -> &'static str {
    MESSAGES_CTE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_is_json_value() {
        let record: Record = serde_json::json!({
            "value_json": {"name": "test"},
            "offset_id": 42
        });
        assert_eq!(record["offset_id"], 42);
        assert_eq!(record["value_json"]["name"], "test");
    }

    #[test]
    fn test_record_batch() {
        let batch: RecordBatch = vec![
            serde_json::json!({"offset_id": 1}),
            serde_json::json!({"offset_id": 2}),
        ];
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn test_batch_cte_has_expected_columns() {
        let cte = batch_cte();
        assert!(cte.contains("key_text"));
        assert!(cte.contains("key_json"));
        assert!(cte.contains("value_text"));
        assert!(cte.contains("value_json"));
        assert!(cte.contains("headers"));
        assert!(cte.contains("offset_id"));
        assert!(cte.contains("created_at"));
        assert!(cte.contains("source_topic"));
        assert!(cte.contains("_original"));
        assert!(cte.contains("jsonb_array_elements"));
    }

    #[test]
    fn test_messages_shape_matches_static_cte() {
        let shape_cte = TopicShape::Messages.batch_cte();
        let static_cte = batch_cte();
        assert_eq!(shape_cte, static_cte);
    }

    #[test]
    fn test_typed_shape_cte_has_columns() {
        let shape = TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("customer_id".to_string(), "text".to_string()),
                ("amount".to_string(), "numeric".to_string()),
                ("status".to_string(), "text".to_string()),
            ],
        };
        let cte = shape.batch_cte();
        assert!(cte.contains("WITH _batch AS"));
        assert!(cte.contains("_original"));
        assert!(cte.contains("jsonb_array_elements"));
        // Typed columns present
        assert!(cte.contains("AS id"));
        assert!(cte.contains("AS customer_id"));
        assert!(cte.contains("AS amount"));
        assert!(cte.contains("AS status"));
    }

    #[test]
    fn test_typed_shape_cte_casts_numeric_types() {
        let shape = TopicShape::Typed {
            source_table: "t".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("score".to_string(), "double precision".to_string()),
                ("active".to_string(), "boolean".to_string()),
            ],
        };
        let cte = shape.batch_cte();
        assert!(cte.contains("(r->>'id')::bigint AS id"));
        assert!(cte.contains("(r->>'score')::double precision AS score"));
        assert!(cte.contains("(r->>'active')::boolean AS active"));
    }

    #[test]
    fn test_typed_shape_cte_no_cast_for_text() {
        let shape = TopicShape::Typed {
            source_table: "t".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("name".to_string(), "text".to_string()),
                ("code".to_string(), "character varying".to_string()),
            ],
        };
        let cte = shape.batch_cte();
        assert!(cte.contains("r->>'name' AS name"));
        assert!(cte.contains("r->>'code' AS code"));
    }

    #[test]
    fn test_cdc_shape_cte_has_expected_columns() {
        let cte = TopicShape::Cdc.batch_cte();
        assert!(cte.contains("WITH _batch AS"));
        assert!(cte.contains("_original"));
        assert!(cte.contains("operation"));
        assert!(cte.contains("table_name"));
        assert!(cte.contains("commit_timestamp"));
        assert!(cte.contains("lsn"));
        assert!(cte.contains("offset_id"));
        assert!(cte.contains("new"));
        assert!(cte.contains("old"));
        assert!(cte.contains("jsonb_array_elements"));
    }

    #[test]
    fn test_pg_type_to_cast() {
        assert_eq!(pg_type_to_cast("integer"), "integer");
        assert_eq!(pg_type_to_cast("bigint"), "bigint");
        assert_eq!(pg_type_to_cast("numeric"), "numeric");
        assert_eq!(pg_type_to_cast("boolean"), "boolean");
        assert_eq!(pg_type_to_cast("timestamptz"), "timestamptz");
        assert_eq!(pg_type_to_cast("jsonb"), "jsonb");
        assert_eq!(pg_type_to_cast("uuid"), "uuid");
        assert_eq!(pg_type_to_cast("text"), "");
        assert_eq!(pg_type_to_cast("character varying"), "");
    }
}
