//! CompiledPipeline — a ready-to-run pipeline with concrete connectors and processors

use crate::connector::input::cdc::CdcInput;
use crate::connector::input::kafka::KafkaInput;
use crate::connector::input::table::TableInput;
use crate::connector::output::branch::{BranchOutput, Route};
use crate::connector::output::kafka::{KafkaOutput, TypedTopicOutput};
use crate::connector::output::table::TableOutput;
use crate::connector::{DropOutput, InputConnector, OutputConnector};
use crate::dsl::types::*;
use crate::processor::aggregate::AggregateProcessor;
use crate::processor::cep::CepProcessor;
use crate::processor::chain::ProcessorChain;
use crate::processor::dedupe::DedupeProcessor;
use crate::processor::filter::FilterProcessor;
use crate::processor::join::JoinProcessor;
use crate::processor::log::LogProcessor;
use crate::processor::mapping::MappingProcessor;
use crate::processor::sql_enrichment::SqlEnrichmentProcessor;
use crate::processor::unnest::UnnestProcessor;
use crate::processor::window::WindowProcessor;
use crate::processor::Processor;
use crate::record::{RecordBatch, TopicShape};
use pgrx::prelude::*;

/// A compiled pipeline ready for execution
pub struct CompiledPipeline {
    pub name: String,
    #[allow(dead_code)]
    pub pipeline_id: i32,
    pub input: Box<dyn InputConnector>,
    pub chain: ProcessorChain,
    pub output: Box<dyn OutputConnector>,
    pub error_handling: ErrorHandling,
    pub dead_letter_output: Option<Box<dyn OutputConnector>>,
}

impl CompiledPipeline {
    /// Compile a PipelineDefinition into a runnable pipeline.
    /// Resolves topic shape from pgkafka.topics metadata for typed topics.
    pub fn compile(name: &str, pipeline_id: i32, def: &PipelineDefinition) -> Result<Self, String> {
        // Build input connector and resolve topic shape
        let (input, input_shape): (Box<dyn InputConnector>, TopicShape) = match &def.input {
            InputConfig::Kafka(k) => {
                let shape = resolve_topic_shape(&k.topic)?;
                let group = if k.group.is_empty() {
                    name // default consumer group = pipeline name
                } else {
                    &k.group
                };
                (
                    Box::new(KafkaInput::new(&k.topic, group, &k.start, shape.clone())),
                    shape,
                )
            }
            InputConfig::Table(t) => {
                let columns = resolve_table_columns(&t.name)?;
                let shape = TopicShape::Typed {
                    source_table: t.name.clone(),
                    offset_column: t.offset_column.clone(),
                    columns: columns.clone(),
                };
                (Box::new(TableInput::new(t, columns)?), shape)
            }
            InputConfig::Cdc(c) => {
                let slot = c
                    .slot
                    .clone()
                    .unwrap_or_else(|| format!("pgstreams_{}", name.replace('-', "_")));
                let poll_ms = c
                    .poll
                    .as_deref()
                    .map(crate::connector::input::table::parse_poll_interval_ms)
                    .transpose()?
                    .unwrap_or(1000);
                let shape = TopicShape::Cdc;
                (
                    Box::new(CdcInput::new(
                        &c.table,
                        &c.publication,
                        &slot,
                        &c.operations,
                        poll_ms,
                    )),
                    shape,
                )
            }
        };
        let input_cte = input_shape.batch_cte();

        // Build processor chain — first processor uses input CTE, subsequent ones
        // use a generic CTE since mappings reshape the record
        let mut processors: Vec<Box<dyn Processor>> = Vec::new();
        for (i, proc_config) in def.pipeline.processors.iter().enumerate() {
            let cte = if i == 0 {
                &input_cte
            } else {
                // After the first processor, records may have been reshaped.
                // For filter, the shape is preserved (pass-through _original).
                // For mapping, fields are entirely new. Use input CTE for filters
                // and a generic passthrough for post-mapping processors.
                // In practice, the _original contains the full record, so the
                // input CTE works for chained filters. For chained mappings,
                // the new fields are in _original and need a fresh CTE.
                // TODO: Track shape through chain for optimal CTE generation.
                &input_cte
            };
            let proc: Box<dyn Processor> = match proc_config {
                ProcessorConfig::Filter(expr) => Box::new(FilterProcessor::new(expr, cte)),
                ProcessorConfig::Mapping(fields) => Box::new(MappingProcessor::new(fields, cte)),
                ProcessorConfig::Sql(config) => Box::new(SqlEnrichmentProcessor::new(
                    &config.query,
                    &config.args,
                    &config.result_map,
                    &config.on_empty,
                    cte,
                )),
                ProcessorConfig::Log(config) => {
                    Box::new(LogProcessor::new(&config.level, &config.message, cte))
                }
                ProcessorConfig::Dedupe(config) => {
                    let dedupe = DedupeProcessor::new(&config.key, &config.window, name, cte);
                    dedupe.ensure_state_table()?;
                    Box::new(dedupe)
                }
                ProcessorConfig::Aggregate(config) => {
                    let default_table = format!("{}_agg", name);
                    let table_name = config.state_table.as_deref().unwrap_or(&default_table);
                    let state_table = format!("pgstreams.{}", table_name.replace(['-', '.'], "_"));
                    let agg = AggregateProcessor::new(
                        &config.group_by,
                        &config.columns,
                        &state_table,
                        &config.rollups,
                        &config.emit,
                        cte,
                    )?;
                    agg.ensure_state_tables()?;
                    Box::new(agg)
                }
                ProcessorConfig::Window(config) => {
                    let window = WindowProcessor::new(config, name, cte)?;
                    window.ensure_state_table()?;
                    Box::new(window)
                }
                ProcessorConfig::Join(config) => {
                    let join = JoinProcessor::new(config, name, cte)?;
                    join.ensure_state_table()?;
                    Box::new(join)
                }
                ProcessorConfig::Cep(config) => {
                    let cep = CepProcessor::new(config, name, cte)?;
                    cep.ensure_state_table()?;
                    Box::new(cep)
                }
                ProcessorConfig::Unnest(config) => {
                    Box::new(UnnestProcessor::new(&config.array, &config.as_field, cte))
                }
                other => {
                    return Err(format!(
                        "Processor[{}] type {:?} not yet implemented",
                        i,
                        std::mem::discriminant(other)
                    ));
                }
            };
            processors.push(proc);
        }

        // Build output connector — detect writable source-backed topics
        let output: Box<dyn OutputConnector> = compile_output(&def.output)?;

        // Build dead letter output if configured
        let dead_letter_output: Option<Box<dyn OutputConnector>> = match &def.pipeline.dead_letter {
            Some(out_config) => Some(compile_output(out_config)?),
            None => None,
        };

        Ok(Self {
            name: name.to_string(),
            pipeline_id,
            input,
            chain: ProcessorChain::new(processors),
            output,
            error_handling: def.pipeline.error_handling.clone(),
            dead_letter_output,
        })
    }

    /// Initialize the input connector (resolve offsets, cache topic IDs, etc.)
    /// Must be called once before the first process_batch.
    pub fn initialize(&mut self) -> Result<(), String> {
        self.input.initialize(&self.name)
    }

    /// Process one batch: poll → filter/map → write → commit
    pub fn process_batch(&mut self, batch_size: i32) -> Result<ProcessResult, String> {
        // Pull records from input (backpressure via batch_size)
        let (batch, max_offset) = self.input.poll(batch_size)?;

        if batch.is_empty() {
            return Ok(ProcessResult {
                records_in: 0,
                records_out: 0,
            });
        }

        let records_in = batch.len();

        // Keep original batch for error logging/dead letter if needed
        let keep_batch = !matches!(self.error_handling, ErrorHandling::Stop);
        let original_batch = if keep_batch {
            Some(batch.clone())
        } else {
            None
        };

        // Run through processor chain
        let processed = match self.chain.process(batch) {
            Ok(p) => p,
            Err(e) => {
                return match self.error_handling {
                    ErrorHandling::Skip => {
                        log!(
                            "pg_streaming pipeline '{}': processor error (skipping): {}",
                            self.name,
                            e
                        );
                        if let Some(ref orig) = original_batch {
                            Self::log_error_batch(&self.name, &e, orig);
                        }
                        // Commit offset to skip past bad records
                        if let Some(offset) = max_offset {
                            self.input.commit(&self.name, offset)?;
                        }
                        Ok(ProcessResult {
                            records_in,
                            records_out: 0,
                        })
                    }
                    ErrorHandling::Stop => Err(e),
                    ErrorHandling::DeadLetter => {
                        log!(
                            "pg_streaming pipeline '{}': processor error (dead letter): {}",
                            self.name,
                            e
                        );
                        if let Some(ref orig) = original_batch {
                            Self::log_error_batch(&self.name, &e, orig);
                            // Write failed batch to dead letter output
                            if let Some(ref mut dl) = self.dead_letter_output {
                                if let Err(dl_err) = dl.write(orig) {
                                    pgrx::warning!(
                                        "pg_streaming pipeline '{}': dead letter write failed: {}",
                                        self.name,
                                        dl_err
                                    );
                                }
                            }
                        }
                        if let Some(offset) = max_offset {
                            self.input.commit(&self.name, offset)?;
                        }
                        Ok(ProcessResult {
                            records_in,
                            records_out: 0,
                        })
                    }
                };
            }
        };

        let records_out = processed.len();

        // Write to output
        if !processed.is_empty() {
            self.output.write(&processed)?;
        }

        // Commit offset after successful write
        if let Some(offset) = max_offset {
            self.input.commit(&self.name, offset)?;
        }

        Ok(ProcessResult {
            records_in,
            records_out,
        })
    }

    /// Log a batch of failed records to pgstreams.error_log (best-effort).
    fn log_error_batch(pipeline: &str, error: &str, batch: &RecordBatch) {
        let batch_json = serde_json::Value::Array(batch.clone());
        let result = Spi::run_with_args(
            "INSERT INTO pgstreams.error_log (pipeline, error, record) VALUES ($1, $2, $3)",
            &[
                pipeline.into(),
                error.into(),
                pgrx::JsonB(batch_json).into(),
            ],
        );
        if let Err(e) = result {
            pgrx::warning!(
                "pg_streaming pipeline '{}': failed to log error: {}",
                pipeline,
                e
            );
        }
    }
}

/// Result of processing one batch
#[allow(dead_code)]
pub struct ProcessResult {
    pub records_in: usize,
    pub records_out: usize,
}

// =============================================================================
// Output compilation — converts OutputConfig / RouteOutput into connectors
// =============================================================================

/// Compile an OutputConfig into a concrete OutputConnector.
fn compile_output(config: &OutputConfig) -> Result<Box<dyn OutputConnector>, String> {
    match config {
        OutputConfig::Kafka(k) => match resolve_output_topic_info(&k.topic)? {
            Some(typed_info) => Ok(Box::new(typed_info)),
            None => Ok(Box::new(KafkaOutput::new(&k.topic, k.key.as_deref()))),
        },
        OutputConfig::Drop(_) => Ok(Box::new(DropOutput)),
        OutputConfig::Table(t) => Ok(Box::new(TableOutput::new(
            &t.name,
            &t.mode,
            t.key.as_deref(),
            &t.tracked,
        ))),
        OutputConfig::Branch(b) => {
            let mut routes = Vec::new();
            for route_config in &b.routes {
                let output = compile_route_output(&route_config.output)?;
                routes.push(Route {
                    condition: route_config.condition.clone(),
                    output,
                });
            }
            Ok(Box::new(BranchOutput::new(routes)))
        }
    }
}

/// Compile a RouteOutput (non-recursive) into a concrete OutputConnector.
fn compile_route_output(config: &RouteOutput) -> Result<Box<dyn OutputConnector>, String> {
    match config {
        RouteOutput::Kafka(k) => match resolve_output_topic_info(&k.topic)? {
            Some(typed_info) => Ok(Box::new(typed_info)),
            None => Ok(Box::new(KafkaOutput::new(&k.topic, k.key.as_deref()))),
        },
        RouteOutput::Drop(_) => Ok(Box::new(DropOutput)),
        RouteOutput::Table(t) => Ok(Box::new(TableOutput::new(
            &t.name,
            &t.mode,
            t.key.as_deref(),
            &t.tracked,
        ))),
    }
}

// =============================================================================
// Topic shape resolution — queries pgkafka.topics to detect typed topics
// =============================================================================

/// Resolve the topic shape (regular messages vs source-backed typed topic).
/// Queries pgkafka.topics for source_table metadata. If the topic is
/// source-backed, fetches column info from information_schema.
fn resolve_topic_shape(topic_name: &str) -> Result<TopicShape, String> {
    // Query pgkafka.topics for source-backed topic metadata
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_build_object(\
           'source_table', source_table, \
           'offset_column', offset_column\
         ) FROM pgkafka.topics WHERE name = $1 AND source_table IS NOT NULL",
        &[topic_name.into()],
    );

    let meta = match result {
        Ok(Some(jb)) => jb.0,
        Ok(None) => return Ok(TopicShape::Messages), // regular topic or not found
        Err(_) => return Ok(TopicShape::Messages),   // pgkafka not installed — fall back
    };

    let source_table = meta["source_table"]
        .as_str()
        .ok_or("source_table is null")?
        .to_string();
    let offset_column = meta["offset_column"]
        .as_str()
        .ok_or("offset_column is null for source-backed topic")?
        .to_string();

    // Fetch column metadata from information_schema
    let columns = resolve_table_columns(&source_table)?;

    Ok(TopicShape::Typed {
        source_table,
        offset_column,
        columns,
    })
}

/// Resolve column names and types for a table from information_schema.
fn resolve_table_columns(table_ref: &str) -> Result<Vec<(String, String)>, String> {
    // Parse schema.table or just table (default to public)
    let (schema, table) = if let Some(dot_pos) = table_ref.find('.') {
        (&table_ref[..dot_pos], &table_ref[dot_pos + 1..])
    } else {
        ("public", table_ref)
    };

    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object(\
           'name', column_name::text, \
           'type', data_type::text\
         ) ORDER BY ordinal_position::integer), '[]'::jsonb) \
         FROM information_schema.columns \
         WHERE table_schema::text = $1 AND table_name::text = $2",
        &[schema.into(), table.into()],
    );

    let jsonb = result.map_err(|e| format!("Failed to query columns for {}: {}", table_ref, e))?;

    let arr = match jsonb {
        Some(jb) => match jb.0.as_array() {
            Some(a) => a.clone(),
            None => return Err(format!("No columns found for table {}", table_ref)),
        },
        None => return Err(format!("No columns found for table {}", table_ref)),
    };

    if arr.is_empty() {
        return Err(format!("Table {} not found or has no columns", table_ref));
    }

    let columns: Vec<(String, String)> = arr
        .iter()
        .map(|col| {
            let name = col["name"].as_str().unwrap_or("").to_string();
            let pg_type = col["type"].as_str().unwrap_or("text").to_string();
            (name, pg_type)
        })
        .collect();

    Ok(columns)
}

/// Check if an output topic is a writable source-backed topic.
/// Returns Some(TypedTopicOutput) if so, None for regular topics.
fn resolve_output_topic_info(topic_name: &str) -> Result<Option<TypedTopicOutput>, String> {
    let result = Spi::get_one_with_args::<pgrx::JsonB>(
        "SELECT jsonb_build_object(\
           'source_table', source_table, \
           'writable', writable, \
           'write_mode', write_mode, \
           'write_key_column', write_key_column\
         ) FROM pgkafka.topics \
         WHERE name = $1 AND source_table IS NOT NULL AND writable = true",
        &[topic_name.into()],
    );

    let meta = match result {
        Ok(Some(jb)) => jb.0,
        Ok(None) => return Ok(None), // regular topic or not writable
        Err(_) => return Ok(None),   // pgkafka not installed
    };

    let source_table = meta["source_table"]
        .as_str()
        .ok_or("source_table is null")?
        .to_string();
    let write_mode = meta["write_mode"].as_str().unwrap_or("stream").to_string();
    let write_key_column = meta["write_key_column"].as_str().map(|s| s.to_string());

    let columns = resolve_table_columns(&source_table)?;

    Ok(Some(TypedTopicOutput::new(
        &source_table,
        &write_mode,
        write_key_column.as_deref(),
        columns,
    )))
}
