//! DSL type definitions for pipeline definitions
//!
//! These types map directly to the JSON DSL described in design/pg_streaming/dsl.md

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level pipeline definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub input: InputConfig,
    pub pipeline: PipelineConfig,
    pub output: OutputConfig,
}

/// Pipeline processing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub processors: Vec<ProcessorConfig>,
    #[serde(default)]
    pub error_handling: ErrorHandling,
    pub dead_letter: Option<OutputConfig>,
}

/// Error handling strategy
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorHandling {
    #[default]
    Skip,
    DeadLetter,
    Stop,
}

// =============================================================================
// Input Connectors
// =============================================================================

/// Input connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InputConfig {
    Kafka(KafkaInputConfig),
    Table(TableInputConfig),
    Cdc(CdcInputConfig),
    // Future phases: Mqtt, Http, Broker, Generate
}

/// Kafka input connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaInputConfig {
    pub topic: String,
    #[serde(default = "default_group")]
    pub group: String,
    #[serde(default = "default_start")]
    pub start: String,
    /// Optional schema subject for type safety (e.g., "orders-value").
    /// If omitted, the topic shape is auto-detected from pgkafka.topics.
    #[serde(default)]
    pub schema: Option<String>,
}

fn default_group() -> String {
    String::new() // Will be set to pipeline name if empty
}

fn default_start() -> String {
    "latest".to_string()
}

/// Table input connector (polling)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInputConfig {
    pub name: String,
    pub offset_column: String,
    #[serde(default = "default_poll")]
    pub poll: String,
    pub filter: Option<String>,
}

fn default_poll() -> String {
    "5s".to_string()
}

/// CDC input connector (logical replication via wal2json)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcInputConfig {
    /// Table to capture changes from (schema.table format)
    pub table: String,
    /// Publication name (must pre-exist)
    pub publication: String,
    /// Replication slot name (defaults to pgstreams_{pipeline_name})
    #[serde(default)]
    pub slot: Option<String>,
    /// Operations to capture: subset of INSERT, UPDATE, DELETE
    #[serde(default = "default_cdc_operations")]
    pub operations: Vec<String>,
    /// Poll interval (defaults to "1s")
    #[serde(default)]
    pub poll: Option<String>,
}

fn default_cdc_operations() -> Vec<String> {
    vec![
        "INSERT".to_string(),
        "UPDATE".to_string(),
        "DELETE".to_string(),
    ]
}

// =============================================================================
// Output Connectors
// =============================================================================

/// Output connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutputConfig {
    Kafka(KafkaOutputConfig),
    Table(TableOutputConfig),
    Drop(DropConfig),
    Branch(BranchOutputConfig),
    // Future phases: Http, Mqtt, Broker
}

/// Kafka output connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOutputConfig {
    pub topic: String,
    pub key: Option<String>,
}

/// Table output connector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableOutputConfig {
    pub name: String,
    #[serde(default = "default_write_mode")]
    pub mode: String,
    pub key: Option<String>,
    pub partition_by: Option<String>,
    /// Columns to track for changes (SCD Type 2 mode only)
    #[serde(default)]
    pub tracked: Vec<String>,
}

fn default_write_mode() -> String {
    "append".to_string()
}

/// Drop output (discard all records)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DropConfig {}

/// Branch output — routes records to different outputs based on SQL conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchOutputConfig {
    pub routes: Vec<BranchRoute>,
}

/// A single route in a branch output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchRoute {
    /// SQL boolean condition (omit for default/catch-all route)
    #[serde(rename = "when")]
    pub condition: Option<String>,
    /// Destination: kafka topic, table, or drop
    pub output: RouteOutput,
}

/// Destination for a branch route (non-recursive — no nested branches)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteOutput {
    Kafka(KafkaOutputConfig),
    Table(TableOutputConfig),
    Drop(DropConfig),
}

// =============================================================================
// Processors
// =============================================================================

/// Processor configuration — each variant maps to a processor type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessorConfig {
    /// SQL boolean expression to filter records
    Filter(String),
    /// Map of output_field -> SQL expression
    Mapping(HashMap<String, String>),
    /// SQL enrichment query
    Sql(SqlProcessorConfig),
    /// Stream-to-table join
    TableJoin(TableJoinConfig),
    /// Deduplicate by key within a time window
    Dedupe(DedupeConfig),
    /// Debug logging
    Log(LogConfig),
    /// Incremental aggregation (unbounded, Kappa-style)
    Aggregate(AggregateConfig),
    /// Time-bounded windowed aggregation
    Window(WindowConfig),
    /// Stream-to-stream join with time-bounded buffer
    Join(JoinConfig),
    /// Complex Event Processing — ordered sequence pattern matching
    Cep(CepConfig),
    /// Flatten a JSONB array into individual records (flatMap)
    Unnest(UnnestConfig),
    /// Reference a reusable resource
    Resource(String),
    // Future phases: Branch, Switch, RateLimit
}

/// SQL enrichment processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    pub query: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub result_map: HashMap<String, String>,
    #[serde(default = "default_on_empty")]
    pub on_empty: String,
}

fn default_on_empty() -> String {
    "null".to_string()
}

/// Stream-to-table join processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableJoinConfig {
    pub table: String,
    pub on: String,
    pub columns: HashMap<String, String>,
    #[serde(default = "default_join_type")]
    #[serde(rename = "type")]
    pub join_type: String,
}

fn default_join_type() -> String {
    "left".to_string()
}

/// Deduplicate processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupeConfig {
    pub key: String,
    pub window: String,
}

/// Log processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    pub message: String,
}

fn default_log_level() -> String {
    "info".to_string()
}

/// Incremental aggregation processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateConfig {
    /// SQL expression to group by (becomes group_key)
    pub group_by: String,
    /// Map of output_column_name -> aggregate_expression (e.g., "revenue": "sum(total)")
    pub columns: HashMap<String, String>,
    /// State table name (without pgstreams. prefix). Defaults to <pipeline>_agg.
    #[serde(default)]
    pub state_table: Option<String>,
    /// Rollup intervals (e.g., ["5 minutes", "1 hour", "1 day"])
    #[serde(default)]
    pub rollups: Vec<String>,
    /// What to emit downstream: "updated_rows" (default), "input", or "none"
    #[serde(default = "default_emit")]
    pub emit: String,
}

fn default_emit() -> String {
    "updated_rows".to_string()
}

/// Time-bounded window processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window type: "tumbling", "sliding", "session"
    #[serde(rename = "type")]
    pub window_type: String,
    /// Window size (e.g., "5 minutes", "1 hour") — required for tumbling/sliding
    #[serde(default)]
    pub size: Option<String>,
    /// Slide interval for sliding windows (e.g., "1 minute")
    #[serde(default)]
    pub slide: Option<String>,
    /// Gap for session windows (e.g., "30 minutes")
    #[serde(default)]
    pub gap: Option<String>,
    /// SQL expression to group by
    pub group_by: String,
    /// Map of output_column_name -> aggregate_expression
    pub columns: HashMap<String, String>,
    /// Time configuration (event time vs processing time)
    #[serde(default)]
    pub time: Option<WindowTimeConfig>,
    /// When to emit: "on_close" (default), "on_update", "periodic"
    #[serde(default = "default_window_emit")]
    pub emit: String,
}

/// Time configuration for event-time windows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowTimeConfig {
    /// "event_time" or "processing_time"
    #[serde(rename = "type", default = "default_time_type")]
    pub time_type: String,
    /// SQL expression for event time field
    #[serde(default)]
    pub field: Option<String>,
    /// Watermark delay (e.g., "30 seconds")
    #[serde(default)]
    pub watermark_delay: Option<String>,
    /// Allowed lateness after window close
    #[serde(default)]
    pub allowed_lateness: Option<String>,
}

fn default_window_emit() -> String {
    "on_close".to_string()
}

fn default_time_type() -> String {
    "processing_time".to_string()
}

/// Stream-to-stream join processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinConfig {
    /// Topic to join with
    pub topic: String,
    /// Join type: "inner", "left"
    #[serde(rename = "type", default = "default_join_type")]
    pub join_type: String,
    /// SQL boolean expression for join condition (references other.* for the other side)
    pub on: String,
    /// Time window for matching (e.g., "24 hours")
    pub window: String,
    /// Output columns from the joined side: output_name -> "other.field_expression"
    #[serde(default)]
    pub columns: HashMap<String, String>,
}

/// Complex Event Processing (CEP) pattern-matching processor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CepConfig {
    /// SQL expression to partition events by key (e.g., "value_json->>'user_id'")
    pub key: String,
    /// Ordered sequence of pattern steps to match
    pub pattern: Vec<CepPatternStep>,
    /// Maximum duration from first match to pattern completion (e.g., "5 minutes")
    pub within: String,
    /// Map of output_field_name -> SQL expression for the emitted record
    pub emit: HashMap<String, String>,
}

/// A single step in a CEP pattern sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CepPatternStep {
    /// Symbolic name for this step (used for readability, logging)
    pub name: String,
    /// SQL boolean condition that an event must satisfy to match this step
    #[serde(rename = "when")]
    pub condition: String,
    /// Quantifier: "1" (exactly once, default), "3+" (N or more), "1?" (optional)
    #[serde(default = "default_cep_quantifier")]
    pub times: String,
}

fn default_cep_quantifier() -> String {
    "1".to_string()
}

/// Unnest (flatMap) processor — explodes a JSONB array into individual records
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnnestConfig {
    /// SQL expression returning a JSONB array (e.g., "value_json->'items'")
    pub array: String,
    /// Field name for each array element in the output record
    #[serde(rename = "as")]
    pub as_field: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_pipeline() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "processed"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Kafka(k) => assert_eq!(k.topic, "orders"),
            _ => panic!("expected kafka input"),
        }
        match &def.output {
            OutputConfig::Kafka(k) => assert_eq!(k.topic, "processed"),
            _ => panic!("expected kafka output"),
        }
        assert!(def.pipeline.processors.is_empty());
    }

    #[test]
    fn test_parse_filter_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"filter": "value_json->>'region' = 'US'"}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(def.pipeline.processors.len(), 1);
        match &def.pipeline.processors[0] {
            ProcessorConfig::Filter(expr) => {
                assert_eq!(expr, "value_json->>'region' = 'US'")
            }
            _ => panic!("expected filter processor"),
        }
    }

    #[test]
    fn test_parse_mapping_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"mapping": {"order_id": "value_json->>'id'", "total": "(value_json->>'amount')::numeric"}}
            ]},
            "output": {"kafka": {"topic": "mapped", "key": "order_id"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Mapping(m) => {
                assert_eq!(m.len(), 2);
                assert_eq!(m["order_id"], "value_json->>'id'");
            }
            _ => panic!("expected mapping processor"),
        }
    }

    #[test]
    fn test_parse_error_handling() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {
                "processors": [],
                "error_handling": "dead_letter",
                "dead_letter": {"kafka": {"topic": "errors"}}
            },
            "output": {"kafka": {"topic": "out"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(def.pipeline.error_handling, ErrorHandling::DeadLetter);
        assert!(def.pipeline.dead_letter.is_some());
    }

    #[test]
    fn test_parse_table_input() {
        let json = r#"{
            "input": {"table": {"name": "sensor_readings", "offset_column": "id"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "sensors"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Table(t) => {
                assert_eq!(t.name, "sensor_readings");
                assert_eq!(t.offset_column, "id");
                assert_eq!(t.poll, "5s");
            }
            _ => panic!("expected table input"),
        }
    }

    #[test]
    fn test_parse_cdc_input() {
        let json = r#"{
            "input": {"cdc": {
                "table": "public.orders",
                "publication": "orders_pub",
                "slot": "pgstreams_orders",
                "operations": ["INSERT", "UPDATE"]
            }},
            "pipeline": {"processors": []},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Cdc(c) => {
                assert_eq!(c.table, "public.orders");
                assert_eq!(c.publication, "orders_pub");
                assert_eq!(c.slot.as_deref(), Some("pgstreams_orders"));
                assert_eq!(c.operations, vec!["INSERT", "UPDATE"]);
            }
            _ => panic!("expected cdc input"),
        }
    }

    #[test]
    fn test_parse_cdc_input_defaults() {
        let json = r#"{
            "input": {"cdc": {"table": "public.orders", "publication": "orders_pub"}},
            "pipeline": {"processors": []},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Cdc(c) => {
                assert!(c.slot.is_none());
                assert_eq!(c.operations, vec!["INSERT", "UPDATE", "DELETE"]);
                assert!(c.poll.is_none());
            }
            _ => panic!("expected cdc input"),
        }
    }

    #[test]
    fn test_parse_table_output() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": []},
            "output": {"table": {"name": "analytics.orders", "mode": "upsert", "key": "order_id"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.output {
            OutputConfig::Table(t) => {
                assert_eq!(t.name, "analytics.orders");
                assert_eq!(t.mode, "upsert");
                assert_eq!(t.key.as_deref(), Some("order_id"));
            }
            _ => panic!("expected table output"),
        }
    }

    #[test]
    fn test_parse_scd2_output() {
        let json = r#"{
            "input": {"kafka": {"topic": "customers"}},
            "pipeline": {"processors": []},
            "output": {"table": {
                "name": "dim.customers",
                "mode": "scd2",
                "key": "customer_id",
                "tracked": ["name", "email", "address"]
            }}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.output {
            OutputConfig::Table(t) => {
                assert_eq!(t.name, "dim.customers");
                assert_eq!(t.mode, "scd2");
                assert_eq!(t.key.as_deref(), Some("customer_id"));
                assert_eq!(t.tracked, vec!["name", "email", "address"]);
            }
            _ => panic!("expected table output"),
        }
    }

    #[test]
    fn test_parse_multiple_processors() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"filter": "value_json->>'active' = 'true'"},
                {"mapping": {"id": "value_json->>'id'"}},
                {"log": {"message": "'processed record'"}}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(def.pipeline.processors.len(), 3);
    }

    #[test]
    fn test_parse_invalid_json_fails() {
        let json = r#"{"input": "not_valid"}"#;
        let result: Result<PipelineDefinition, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_defaults() {
        let json = r#"{
            "input": {"kafka": {"topic": "t"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "o"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        assert_eq!(def.pipeline.error_handling, ErrorHandling::Skip);
        match &def.input {
            InputConfig::Kafka(k) => {
                assert_eq!(k.start, "latest");
            }
            _ => panic!("expected kafka"),
        }
    }

    #[test]
    fn test_parse_kafka_input_with_schema() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders", "schema": "orders-value"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "out"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Kafka(k) => {
                assert_eq!(k.schema.as_deref(), Some("orders-value"));
            }
            _ => panic!("expected kafka"),
        }
    }

    #[test]
    fn test_parse_kafka_input_without_schema() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": []},
            "output": {"kafka": {"topic": "out"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.input {
            InputConfig::Kafka(k) => {
                assert!(k.schema.is_none());
            }
            _ => panic!("expected kafka"),
        }
    }

    #[test]
    fn test_parse_aggregate_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"aggregate": {
                    "group_by": "region",
                    "columns": {
                        "revenue": "sum(total)",
                        "order_count": "count(*)",
                        "avg_order": "avg(total)"
                    },
                    "state_table": "revenue_by_region",
                    "rollups": ["5 minutes", "1 hour"],
                    "emit": "updated_rows"
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Aggregate(a) => {
                assert_eq!(a.group_by, "region");
                assert_eq!(a.columns.len(), 3);
                assert_eq!(a.columns["revenue"], "sum(total)");
                assert_eq!(a.state_table.as_deref(), Some("revenue_by_region"));
                assert_eq!(a.rollups.len(), 2);
                assert_eq!(a.emit, "updated_rows");
            }
            _ => panic!("expected aggregate processor"),
        }
    }

    #[test]
    fn test_parse_aggregate_defaults() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"aggregate": {
                    "group_by": "region",
                    "columns": {"total": "sum(amount)"}
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Aggregate(a) => {
                assert!(a.state_table.is_none());
                assert!(a.rollups.is_empty());
                assert_eq!(a.emit, "updated_rows");
            }
            _ => panic!("expected aggregate processor"),
        }
    }

    #[test]
    fn test_parse_window_tumbling() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"window": {
                    "type": "tumbling",
                    "size": "5 minutes",
                    "group_by": "region",
                    "columns": {
                        "revenue": "sum(total)",
                        "orders": "count(*)"
                    },
                    "time": {
                        "type": "event_time",
                        "field": "(value_json->>'event_ts')::timestamptz",
                        "watermark_delay": "30 seconds"
                    },
                    "emit": "on_close"
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Window(w) => {
                assert_eq!(w.window_type, "tumbling");
                assert_eq!(w.size.as_deref(), Some("5 minutes"));
                assert_eq!(w.group_by, "region");
                assert_eq!(w.columns.len(), 2);
                let time = w.time.as_ref().unwrap();
                assert_eq!(time.time_type, "event_time");
                assert_eq!(time.watermark_delay.as_deref(), Some("30 seconds"));
            }
            _ => panic!("expected window processor"),
        }
    }

    #[test]
    fn test_parse_window_session() {
        let json = r#"{
            "input": {"kafka": {"topic": "events"}},
            "pipeline": {"processors": [
                {"window": {
                    "type": "session",
                    "gap": "30 minutes",
                    "group_by": "user_id",
                    "columns": {"page_views": "count(*)"}
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Window(w) => {
                assert_eq!(w.window_type, "session");
                assert_eq!(w.gap.as_deref(), Some("30 minutes"));
                assert!(w.time.is_none());
                assert_eq!(w.emit, "on_close");
            }
            _ => panic!("expected window processor"),
        }
    }

    #[test]
    fn test_parse_join_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"join": {
                    "topic": "shipments",
                    "type": "left",
                    "on": "order_id = other.value_json->>'order_id'",
                    "window": "24 hours",
                    "columns": {
                        "shipped_at": "other.value_json->>'shipped_at'",
                        "carrier": "other.value_json->>'carrier'"
                    }
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Join(j) => {
                assert_eq!(j.topic, "shipments");
                assert_eq!(j.join_type, "left");
                assert_eq!(j.window, "24 hours");
                assert_eq!(j.columns.len(), 2);
            }
            _ => panic!("expected join processor"),
        }
    }

    #[test]
    fn test_parse_branch_output() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [{"filter": "true"}]},
            "output": {"branch": {"routes": [
                {"when": "r->>'region' = 'US'", "output": {"kafka": {"topic": "us_orders"}}},
                {"when": "r->>'region' = 'EU'", "output": {"table": {"name": "eu_orders"}}},
                {"output": {"drop": {}}}
            ]}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.output {
            OutputConfig::Branch(b) => {
                assert_eq!(b.routes.len(), 3);
                assert_eq!(
                    b.routes[0].condition.as_deref(),
                    Some("r->>'region' = 'US'")
                );
                assert!(b.routes[2].condition.is_none()); // default
            }
            _ => panic!("expected branch output"),
        }
    }

    #[test]
    fn test_roundtrip_serialization() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders", "group": "g1", "start": "earliest"}},
            "pipeline": {"processors": [{"filter": "true"}]},
            "output": {"kafka": {"topic": "out"}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&def).unwrap();
        let roundtrip: PipelineDefinition = serde_json::from_str(&serialized).unwrap();
        match &roundtrip.input {
            InputConfig::Kafka(k) => assert_eq!(k.topic, "orders"),
            _ => panic!("expected kafka"),
        }
    }

    #[test]
    fn test_parse_cep_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "events"}},
            "pipeline": {"processors": [
                {"cep": {
                    "key": "value_json->>'user_id'",
                    "pattern": [
                        {"name": "fail", "when": "value_json->>'event' = 'login_failed'", "times": "3+"},
                        {"name": "lock", "when": "value_json->>'event' = 'account_locked'"}
                    ],
                    "within": "5 minutes",
                    "emit": {
                        "user_id": "value_json->>'user_id'",
                        "fail_count": "_matched_count"
                    }
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Cep(c) => {
                assert_eq!(c.key, "value_json->>'user_id'");
                assert_eq!(c.pattern.len(), 2);
                assert_eq!(c.pattern[0].name, "fail");
                assert_eq!(c.pattern[0].times, "3+");
                assert_eq!(c.pattern[1].times, "1"); // default
                assert_eq!(c.within, "5 minutes");
                assert_eq!(c.emit.len(), 2);
            }
            _ => panic!("expected cep processor"),
        }
    }

    #[test]
    fn test_parse_unnest_processor() {
        let json = r#"{
            "input": {"kafka": {"topic": "orders"}},
            "pipeline": {"processors": [
                {"unnest": {"array": "value_json->'items'", "as": "item"}}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Unnest(u) => {
                assert_eq!(u.array, "value_json->'items'");
                assert_eq!(u.as_field, "item");
            }
            _ => panic!("expected unnest processor"),
        }
    }

    #[test]
    fn test_parse_cep_optional_step() {
        let json = r#"{
            "input": {"kafka": {"topic": "events"}},
            "pipeline": {"processors": [
                {"cep": {
                    "key": "value_json->>'session_id'",
                    "pattern": [
                        {"name": "browse", "when": "value_json->>'event' = 'page_view'"},
                        {"name": "cart", "when": "value_json->>'event' = 'add_to_cart'", "times": "1?"},
                        {"name": "purchase", "when": "value_json->>'event' = 'checkout'"}
                    ],
                    "within": "30 minutes",
                    "emit": {"session": "value_json->>'session_id'"}
                }}
            ]},
            "output": {"drop": {}}
        }"#;
        let def: PipelineDefinition = serde_json::from_str(json).unwrap();
        match &def.pipeline.processors[0] {
            ProcessorConfig::Cep(c) => {
                assert_eq!(c.pattern[1].times, "1?");
                assert_eq!(c.pattern.len(), 3);
            }
            _ => panic!("expected cep"),
        }
    }
}
