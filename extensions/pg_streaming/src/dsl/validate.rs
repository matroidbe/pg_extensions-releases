//! Validation for pipeline definitions
//!
//! Structural validation is pure Rust. Semantic validation (SQL expression checking)
//! uses SPI with EXPLAIN to validate expressions without executing them.

use crate::dsl::types::*;

/// Validate a pipeline definition structurally (no SPI needed)
pub fn validate_definition(def: &PipelineDefinition) -> Result<(), String> {
    validate_input(&def.input)?;
    validate_processors(&def.pipeline.processors)?;
    validate_output(&def.output)?;

    if def.pipeline.error_handling == ErrorHandling::DeadLetter
        && def.pipeline.dead_letter.is_none()
    {
        return Err(
            "error_handling is 'dead_letter' but no dead_letter output is configured".to_string(),
        );
    }

    Ok(())
}

fn validate_input(input: &InputConfig) -> Result<(), String> {
    match input {
        InputConfig::Kafka(k) => {
            if k.topic.is_empty() {
                return Err("input.kafka.topic is required".to_string());
            }
            if !["latest", "earliest"].contains(&k.start.as_str())
                && k.start.parse::<i64>().is_err()
            {
                return Err(format!(
                    "input.kafka.start must be 'latest', 'earliest', or an offset number, got '{}'",
                    k.start
                ));
            }
        }
        InputConfig::Table(t) => {
            if t.name.is_empty() {
                return Err("input.table.name is required".to_string());
            }
            if t.offset_column.is_empty() {
                return Err("input.table.offset_column is required".to_string());
            }
            if crate::connector::input::table::parse_poll_interval_ms(&t.poll).is_err() {
                return Err(format!(
                    "input.table.poll '{}' is invalid: expected format like '5s', '500ms', or '1m'",
                    t.poll
                ));
            }
        }
        InputConfig::Cdc(c) => {
            if c.table.is_empty() {
                return Err("input.cdc.table is required".to_string());
            }
            if c.publication.is_empty() {
                return Err("input.cdc.publication is required".to_string());
            }
            if c.operations.is_empty() {
                return Err("input.cdc.operations must have at least one operation".to_string());
            }
            let valid_ops = ["INSERT", "UPDATE", "DELETE"];
            for op in &c.operations {
                if !valid_ops.contains(&op.as_str()) {
                    return Err(format!(
                        "input.cdc.operations: '{}' is not valid. Must be INSERT, UPDATE, or DELETE",
                        op
                    ));
                }
            }
            if let Some(ref poll) = c.poll {
                if crate::connector::input::table::parse_poll_interval_ms(poll).is_err() {
                    return Err(format!(
                        "input.cdc.poll '{}' is invalid: expected format like '1s', '500ms', or '1m'",
                        poll
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_processors(processors: &[ProcessorConfig]) -> Result<(), String> {
    for (i, proc) in processors.iter().enumerate() {
        validate_processor(i, proc)?;
    }
    Ok(())
}

fn validate_processor(index: usize, proc: &ProcessorConfig) -> Result<(), String> {
    match proc {
        ProcessorConfig::Filter(expr) => {
            if expr.is_empty() {
                return Err(format!("processor[{}].filter: expression is empty", index));
            }
        }
        ProcessorConfig::Mapping(fields) => {
            if fields.is_empty() {
                return Err(format!(
                    "processor[{}].mapping: at least one field mapping is required",
                    index
                ));
            }
            for (name, expr) in fields {
                if name.is_empty() {
                    return Err(format!(
                        "processor[{}].mapping: field name cannot be empty",
                        index
                    ));
                }
                if expr.is_empty() {
                    return Err(format!(
                        "processor[{}].mapping: expression for field '{}' is empty",
                        index, name
                    ));
                }
            }
        }
        ProcessorConfig::Sql(config) => {
            if config.query.is_empty() {
                return Err(format!("processor[{}].sql: query is required", index));
            }
        }
        ProcessorConfig::TableJoin(config) => {
            if config.table.is_empty() {
                return Err(format!(
                    "processor[{}].table_join: table is required",
                    index
                ));
            }
            if config.on.is_empty() {
                return Err(format!(
                    "processor[{}].table_join: on condition is required",
                    index
                ));
            }
            if config.columns.is_empty() {
                return Err(format!(
                    "processor[{}].table_join: at least one column mapping is required",
                    index
                ));
            }
        }
        ProcessorConfig::Dedupe(config) => {
            if config.key.is_empty() {
                return Err(format!(
                    "processor[{}].dedupe: key expression is required",
                    index
                ));
            }
            if config.window.is_empty() {
                return Err(format!("processor[{}].dedupe: window is required", index));
            }
        }
        ProcessorConfig::Log(config) => {
            if config.message.is_empty() {
                return Err(format!("processor[{}].log: message is required", index));
            }
        }
        ProcessorConfig::Aggregate(config) => {
            if config.group_by.is_empty() {
                return Err(format!(
                    "processor[{}].aggregate: group_by expression is required",
                    index
                ));
            }
            if config.columns.is_empty() {
                return Err(format!(
                    "processor[{}].aggregate: at least one aggregate column is required",
                    index
                ));
            }
        }
        ProcessorConfig::Window(config) => {
            if config.group_by.is_empty() {
                return Err(format!(
                    "processor[{}].window: group_by expression is required",
                    index
                ));
            }
            if config.columns.is_empty() {
                return Err(format!(
                    "processor[{}].window: at least one aggregate column is required",
                    index
                ));
            }
        }
        ProcessorConfig::Join(config) => {
            if config.topic.is_empty() {
                return Err(format!("processor[{}].join: topic is required", index));
            }
            if config.on.is_empty() {
                return Err(format!(
                    "processor[{}].join: on condition is required",
                    index
                ));
            }
            if config.window.is_empty() {
                return Err(format!("processor[{}].join: window is required", index));
            }
        }
        ProcessorConfig::Cep(config) => {
            if config.key.is_empty() {
                return Err(format!(
                    "processor[{}].cep: key expression is required",
                    index
                ));
            }
            if config.pattern.is_empty() {
                return Err(format!(
                    "processor[{}].cep: pattern must have at least one step",
                    index
                ));
            }
            if config.within.is_empty() {
                return Err(format!(
                    "processor[{}].cep: within (time window) is required",
                    index
                ));
            }
            if config.emit.is_empty() {
                return Err(format!(
                    "processor[{}].cep: emit must have at least one field",
                    index
                ));
            }
            for (j, step) in config.pattern.iter().enumerate() {
                if step.name.is_empty() {
                    return Err(format!(
                        "processor[{}].cep: pattern[{}].name is required",
                        index, j
                    ));
                }
                if step.condition.is_empty() {
                    return Err(format!(
                        "processor[{}].cep: pattern[{}].when is required",
                        index, j
                    ));
                }
                let q = step.times.trim();
                if q.ends_with('?') {
                    if q != "1?" {
                        return Err(format!(
                            "processor[{}].cep: pattern[{}].times '{}' invalid, optional must be '1?'",
                            index, j, q
                        ));
                    }
                } else if let Some(n) = q.strip_suffix('+') {
                    if n.parse::<i32>().map_or(true, |v| v < 1) {
                        return Err(format!(
                            "processor[{}].cep: pattern[{}].times '{}' invalid, expected N+ where N >= 1",
                            index, j, q
                        ));
                    }
                } else if q.parse::<i32>().map_or(true, |v| v < 1) {
                    return Err(format!(
                        "processor[{}].cep: pattern[{}].times '{}' invalid, expected positive integer, N+, or 1?",
                        index, j, q
                    ));
                }
            }
        }
        ProcessorConfig::Unnest(config) => {
            if config.array.is_empty() {
                return Err(format!(
                    "processor[{}].unnest: array expression is required",
                    index
                ));
            }
            if config.as_field.is_empty() {
                return Err(format!(
                    "processor[{}].unnest: 'as' field name is required",
                    index
                ));
            }
        }
        ProcessorConfig::Resource(name) => {
            if name.is_empty() {
                return Err(format!(
                    "processor[{}].resource: resource name is required",
                    index
                ));
            }
        }
    }
    Ok(())
}

fn validate_output(output: &OutputConfig) -> Result<(), String> {
    match output {
        OutputConfig::Kafka(k) => {
            if k.topic.is_empty() {
                return Err("output.kafka.topic is required".to_string());
            }
        }
        OutputConfig::Table(t) => {
            if t.name.is_empty() {
                return Err("output.table.name is required".to_string());
            }
            if !["append", "upsert", "replace", "scd2"].contains(&t.mode.as_str()) {
                return Err(format!(
                    "output.table.mode must be 'append', 'upsert', 'replace', or 'scd2', got '{}'",
                    t.mode
                ));
            }
            if t.mode == "upsert" && t.key.is_none() {
                return Err("output.table.key is required when mode is 'upsert'".to_string());
            }
            if t.mode == "scd2" {
                if t.key.is_none() {
                    return Err(
                        "output.table.key is required when mode is 'scd2' (business key)"
                            .to_string(),
                    );
                }
                if t.tracked.is_empty() {
                    return Err(
                        "output.table.tracked is required when mode is 'scd2' (columns to track for changes)"
                            .to_string(),
                    );
                }
            }
        }
        OutputConfig::Drop(_) => {}
        OutputConfig::Branch(b) => {
            if b.routes.is_empty() {
                return Err("output.branch.routes must have at least one route".to_string());
            }
            let default_count = b.routes.iter().filter(|r| r.condition.is_none()).count();
            if default_count > 1 {
                return Err(
                    "output.branch.routes can have at most one default (no 'when') route"
                        .to_string(),
                );
            }
            for (i, route) in b.routes.iter().enumerate() {
                if let Some(ref cond) = route.condition {
                    if cond.trim().is_empty() {
                        return Err(format!(
                            "output.branch.routes[{}].when must not be empty",
                            i
                        ));
                    }
                }
                validate_route_output(&route.output, i)?;
            }
        }
    }
    Ok(())
}

fn validate_route_output(output: &RouteOutput, route_idx: usize) -> Result<(), String> {
    match output {
        RouteOutput::Kafka(k) => {
            if k.topic.is_empty() {
                return Err(format!(
                    "output.branch.routes[{}].kafka.topic is required",
                    route_idx
                ));
            }
        }
        RouteOutput::Table(t) => {
            if t.name.is_empty() {
                return Err(format!(
                    "output.branch.routes[{}].table.name is required",
                    route_idx
                ));
            }
        }
        RouteOutput::Drop(_) => {}
    }
    Ok(())
}

/// Build a SQL expression to validate a filter/mapping expression via EXPLAIN.
/// This wraps the expression in a SELECT from a dummy record CTE so PostgreSQL
/// can parse and type-check it without executing.
///
/// For regular topics, uses the standard pgkafka.messages columns.
/// For typed topics, uses the actual source table columns.
#[allow(dead_code)]
pub fn build_validation_sql(expr: &str, shape: &crate::record::TopicShape) -> String {
    match shape {
        crate::record::TopicShape::Messages => {
            format!(
                "EXPLAIN SELECT {} FROM (SELECT \
                 null::bytea AS key, \
                 null::text AS key_text, \
                 null::jsonb AS key_json, \
                 null::bytea AS value, \
                 null::text AS value_text, \
                 null::jsonb AS value_json, \
                 null::jsonb AS headers, \
                 null::bigint AS offset_id, \
                 null::timestamptz AS created_at, \
                 null::text AS source_topic\
                 ) AS _r",
                expr
            )
        }
        crate::record::TopicShape::Cdc => {
            format!(
                "EXPLAIN SELECT {} FROM (SELECT \
                 null::text AS operation, \
                 null::text AS table_name, \
                 null::text AS commit_timestamp, \
                 null::text AS lsn, \
                 null::bigint AS offset_id, \
                 null::jsonb AS new, \
                 null::jsonb AS old\
                 ) AS _r",
                expr
            )
        }
        crate::record::TopicShape::Typed { columns, .. } => {
            let col_defs: Vec<String> = columns
                .iter()
                .map(|(name, pg_type)| {
                    let cast = crate::record::pg_type_to_cast(pg_type);
                    if cast.is_empty() {
                        format!("null::text AS {}", name)
                    } else {
                        format!("null::{} AS {}", cast, name)
                    }
                })
                .collect();

            format!(
                "EXPLAIN SELECT {} FROM (SELECT {}) AS _r",
                expr,
                col_defs.join(", ")
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn minimal_def() -> PipelineDefinition {
        PipelineDefinition {
            input: InputConfig::Kafka(KafkaInputConfig {
                topic: "test".to_string(),
                group: String::new(),
                start: "latest".to_string(),
                schema: None,
            }),
            pipeline: PipelineConfig {
                processors: vec![],
                error_handling: ErrorHandling::Skip,
                dead_letter: None,
            },
            output: OutputConfig::Kafka(KafkaOutputConfig {
                topic: "out".to_string(),
                key: None,
            }),
        }
    }

    #[test]
    fn test_validate_minimal_pipeline() {
        assert!(validate_definition(&minimal_def()).is_ok());
    }

    #[test]
    fn test_validate_empty_topic() {
        let mut def = minimal_def();
        def.input = InputConfig::Kafka(KafkaInputConfig {
            topic: String::new(),
            group: String::new(),
            start: "latest".to_string(),
            schema: None,
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("topic is required"));
    }

    #[test]
    fn test_validate_invalid_start() {
        let mut def = minimal_def();
        def.input = InputConfig::Kafka(KafkaInputConfig {
            topic: "t".to_string(),
            group: String::new(),
            start: "invalid".to_string(),
            schema: None,
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("'latest', 'earliest'"));
    }

    #[test]
    fn test_validate_numeric_start() {
        let mut def = minimal_def();
        def.input = InputConfig::Kafka(KafkaInputConfig {
            topic: "t".to_string(),
            group: String::new(),
            start: "42".to_string(),
            schema: None,
        });
        assert!(validate_definition(&def).is_ok());
    }

    #[test]
    fn test_validate_empty_filter() {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Filter(String::new())];
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expression is empty"));
    }

    #[test]
    fn test_validate_empty_mapping() {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Mapping(HashMap::new())];
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one field"));
    }

    #[test]
    fn test_validate_dead_letter_without_output() {
        let mut def = minimal_def();
        def.pipeline.error_handling = ErrorHandling::DeadLetter;
        def.pipeline.dead_letter = None;
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("dead_letter"));
    }

    #[test]
    fn test_validate_upsert_without_key() {
        let mut def = minimal_def();
        def.output = OutputConfig::Table(TableOutputConfig {
            name: "out".to_string(),
            mode: "upsert".to_string(),
            key: None,
            partition_by: None,
            tracked: vec![],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key is required"));
    }

    #[test]
    fn test_validate_scd2_without_key() {
        let mut def = minimal_def();
        def.output = OutputConfig::Table(TableOutputConfig {
            name: "dim.customers".to_string(),
            mode: "scd2".to_string(),
            key: None,
            partition_by: None,
            tracked: vec!["name".to_string()],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("key is required"));
    }

    #[test]
    fn test_validate_scd2_without_tracked() {
        let mut def = minimal_def();
        def.output = OutputConfig::Table(TableOutputConfig {
            name: "dim.customers".to_string(),
            mode: "scd2".to_string(),
            key: Some("customer_id".to_string()),
            partition_by: None,
            tracked: vec![],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("tracked is required"));
    }

    #[test]
    fn test_validate_scd2_valid() {
        let mut def = minimal_def();
        def.output = OutputConfig::Table(TableOutputConfig {
            name: "dim.customers".to_string(),
            mode: "scd2".to_string(),
            key: Some("customer_id".to_string()),
            partition_by: None,
            tracked: vec!["name".to_string(), "email".to_string()],
        });
        let result = validate_definition(&def);
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_validation_sql_messages() {
        let sql = build_validation_sql(
            "value_json->>'name' IS NOT NULL",
            &crate::record::TopicShape::Messages,
        );
        assert!(sql.starts_with("EXPLAIN SELECT"));
        assert!(sql.contains("value_json->>'name' IS NOT NULL"));
        assert!(sql.contains("null::jsonb AS value_json"));
    }

    #[test]
    fn test_build_validation_sql_typed() {
        let shape = crate::record::TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("amount".to_string(), "numeric".to_string()),
                ("region".to_string(), "text".to_string()),
            ],
        };
        let sql = build_validation_sql("amount > 100 AND region = 'US'", &shape);
        assert!(sql.starts_with("EXPLAIN SELECT"));
        assert!(sql.contains("amount > 100 AND region = 'US'"));
        assert!(sql.contains("null::bigint AS id"));
        assert!(sql.contains("null::numeric AS amount"));
        assert!(sql.contains("null::text AS region"));
    }

    #[test]
    fn test_validate_branch_empty_routes() {
        let mut def = minimal_def();
        def.output = OutputConfig::Branch(BranchOutputConfig { routes: vec![] });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one route"));
    }

    #[test]
    fn test_validate_branch_multiple_defaults() {
        let mut def = minimal_def();
        def.output = OutputConfig::Branch(BranchOutputConfig {
            routes: vec![
                BranchRoute {
                    condition: None,
                    output: RouteOutput::Drop(DropConfig {}),
                },
                BranchRoute {
                    condition: None,
                    output: RouteOutput::Drop(DropConfig {}),
                },
            ],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at most one default"));
    }

    #[test]
    fn test_validate_branch_empty_condition() {
        let mut def = minimal_def();
        def.output = OutputConfig::Branch(BranchOutputConfig {
            routes: vec![BranchRoute {
                condition: Some("  ".to_string()),
                output: RouteOutput::Kafka(KafkaOutputConfig {
                    topic: "us_orders".to_string(),
                    key: None,
                }),
            }],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("must not be empty"));
    }

    #[test]
    fn test_validate_branch_empty_topic() {
        let mut def = minimal_def();
        def.output = OutputConfig::Branch(BranchOutputConfig {
            routes: vec![BranchRoute {
                condition: Some("r->>'region' = 'US'".to_string()),
                output: RouteOutput::Kafka(KafkaOutputConfig {
                    topic: String::new(),
                    key: None,
                }),
            }],
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("topic is required"));
    }

    #[test]
    fn test_validate_branch_valid() {
        let mut def = minimal_def();
        def.output = OutputConfig::Branch(BranchOutputConfig {
            routes: vec![
                BranchRoute {
                    condition: Some("r->>'region' = 'US'".to_string()),
                    output: RouteOutput::Kafka(KafkaOutputConfig {
                        topic: "us_orders".to_string(),
                        key: None,
                    }),
                },
                BranchRoute {
                    condition: None,
                    output: RouteOutput::Table(TableOutputConfig {
                        name: "other_orders".to_string(),
                        mode: "append".to_string(),
                        key: None,
                        partition_by: None,
                        tracked: vec![],
                    }),
                },
            ],
        });
        assert!(validate_definition(&def).is_ok());
    }

    fn cep_def(config: CepConfig) -> PipelineDefinition {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Cep(config)];
        def
    }

    fn valid_cep_config() -> CepConfig {
        CepConfig {
            key: "value_json->>'user_id'".to_string(),
            pattern: vec![CepPatternStep {
                name: "fail".to_string(),
                condition: "value_json->>'event' = 'login_failed'".to_string(),
                times: "3+".to_string(),
            }],
            within: "5 minutes".to_string(),
            emit: HashMap::from([("user_id".to_string(), "value_json->>'user_id'".to_string())]),
        }
    }

    #[test]
    fn test_validate_cep_empty_key() {
        let mut c = valid_cep_config();
        c.key = String::new();
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("key expression is required"));
    }

    #[test]
    fn test_validate_cep_empty_pattern() {
        let mut c = valid_cep_config();
        c.pattern = vec![];
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("at least one step"));
    }

    #[test]
    fn test_validate_cep_empty_within() {
        let mut c = valid_cep_config();
        c.within = String::new();
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("within"));
    }

    #[test]
    fn test_validate_cep_empty_emit() {
        let mut c = valid_cep_config();
        c.emit = HashMap::new();
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("at least one field"));
    }

    #[test]
    fn test_validate_cep_invalid_quantifier() {
        let mut c = valid_cep_config();
        c.pattern[0].times = "0".to_string();
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("invalid"));

        let mut c = valid_cep_config();
        c.pattern[0].times = "2?".to_string();
        assert!(validate_definition(&cep_def(c))
            .unwrap_err()
            .contains("optional must be '1?'"));
    }

    #[test]
    fn test_validate_cep_valid() {
        assert!(validate_definition(&cep_def(valid_cep_config())).is_ok());
    }

    #[test]
    fn test_validate_table_poll_invalid() {
        let mut def = minimal_def();
        def.input = InputConfig::Table(TableInputConfig {
            name: "events".to_string(),
            offset_column: "id".to_string(),
            poll: "abc".to_string(),
            filter: None,
        });
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("poll"));
    }

    #[test]
    fn test_validate_table_poll_valid() {
        let mut def = minimal_def();
        def.input = InputConfig::Table(TableInputConfig {
            name: "events".to_string(),
            offset_column: "id".to_string(),
            poll: "5s".to_string(),
            filter: None,
        });
        assert!(validate_definition(&def).is_ok());
    }

    // =========================================================================
    // CDC input validation
    // =========================================================================

    fn cdc_input(table: &str, publication: &str) -> CdcInputConfig {
        CdcInputConfig {
            table: table.to_string(),
            publication: publication.to_string(),
            slot: None,
            operations: vec![
                "INSERT".to_string(),
                "UPDATE".to_string(),
                "DELETE".to_string(),
            ],
            poll: None,
        }
    }

    #[test]
    fn test_validate_cdc_valid() {
        let mut def = minimal_def();
        def.input = InputConfig::Cdc(cdc_input("public.orders", "orders_pub"));
        assert!(validate_definition(&def).is_ok());
    }

    #[test]
    fn test_validate_cdc_empty_table() {
        let mut def = minimal_def();
        def.input = InputConfig::Cdc(cdc_input("", "orders_pub"));
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("table is required"));
    }

    #[test]
    fn test_validate_cdc_empty_publication() {
        let mut def = minimal_def();
        def.input = InputConfig::Cdc(cdc_input("public.orders", ""));
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("publication is required"));
    }

    #[test]
    fn test_validate_cdc_empty_operations() {
        let mut def = minimal_def();
        let mut c = cdc_input("public.orders", "orders_pub");
        c.operations = vec![];
        def.input = InputConfig::Cdc(c);
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one operation"));
    }

    #[test]
    fn test_validate_cdc_invalid_operation() {
        let mut def = minimal_def();
        let mut c = cdc_input("public.orders", "orders_pub");
        c.operations = vec!["INSERT".to_string(), "TRUNCATE".to_string()];
        def.input = InputConfig::Cdc(c);
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("TRUNCATE"));
    }

    #[test]
    fn test_validate_cdc_invalid_poll() {
        let mut def = minimal_def();
        let mut c = cdc_input("public.orders", "orders_pub");
        c.poll = Some("abc".to_string());
        def.input = InputConfig::Cdc(c);
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("poll"));
    }

    // =========================================================================
    // Unnest processor validation
    // =========================================================================

    #[test]
    fn test_validate_unnest_valid() {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Unnest(UnnestConfig {
            array: "value_json->'items'".to_string(),
            as_field: "item".to_string(),
        })];
        assert!(validate_definition(&def).is_ok());
    }

    #[test]
    fn test_validate_unnest_empty_array() {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Unnest(UnnestConfig {
            array: String::new(),
            as_field: "item".to_string(),
        })];
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("array expression is required"));
    }

    #[test]
    fn test_validate_unnest_empty_as() {
        let mut def = minimal_def();
        def.pipeline.processors = vec![ProcessorConfig::Unnest(UnnestConfig {
            array: "value_json->'items'".to_string(),
            as_field: String::new(),
        })];
        let result = validate_definition(&def);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("'as' field name is required"));
    }
}
