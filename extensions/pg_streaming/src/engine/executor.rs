//! Executor — runs assigned pipelines in the poll-process-commit loop
//!
//! Each executor processes only pipelines assigned to it by the coordinator
//! (worker_id matches). The coordinator distributes pipelines across executors
//! using a least-loaded strategy.

use crate::config::{PG_STREAMING_BATCH_SIZE, PG_STREAMING_ENABLED};
use crate::dsl::parse::parse_pipeline_value;
use crate::engine::pipeline_runtime::CompiledPipeline;
use pgrx::prelude::*;
use std::collections::HashMap;

/// Main executor loop — called from worker.rs
/// Discovers running pipelines, compiles them, and processes batches.
/// Returns whether any pipeline took in records (the loop polls fast while
/// it does, and backs off while idle).
pub fn run_executor_tick(worker_id: i32, compiled: &mut HashMap<i32, CompiledPipeline>) -> bool {
    if !PG_STREAMING_ENABLED.get() {
        return false;
    }

    let batch_size = PG_STREAMING_BATCH_SIZE.get();

    // Discover pipelines assigned to this executor by the coordinator
    let running = match discover_assigned_pipelines(worker_id) {
        Ok(p) => p,
        Err(e) => {
            log!(
                "pg_streaming executor {}: failed to discover pipelines: {}",
                worker_id,
                e
            );
            return false;
        }
    };

    // Remove pipelines that are no longer running
    compiled.retain(|id, _| running.iter().any(|r| r.0 == *id));

    // Compile new pipelines and initialize them
    for (pipeline_id, name, definition_json) in &running {
        if !compiled.contains_key(pipeline_id) {
            match compile_and_init(*pipeline_id, name, definition_json) {
                Ok(cp) => {
                    log!(
                        "pg_streaming executor {}: compiled pipeline '{}' (id={})",
                        worker_id,
                        name,
                        pipeline_id
                    );
                    compiled.insert(*pipeline_id, cp);
                }
                Err(e) => {
                    log!(
                        "pg_streaming executor {}: failed to compile '{}': {}",
                        worker_id,
                        name,
                        e
                    );
                    mark_pipeline_failed(*pipeline_id, &e);
                }
            }
        }
    }

    // Process one batch for each compiled pipeline
    let mut busy = false;
    let pipeline_ids: Vec<i32> = compiled.keys().copied().collect();
    for pipeline_id in pipeline_ids {
        if let Some(pipeline) = compiled.get_mut(&pipeline_id) {
            match pipeline.process_batch(batch_size) {
                Ok(result) => busy |= result.records_in > 0,
                Err(e) => {
                    log!(
                        "pg_streaming executor {}: pipeline '{}' failed: {}",
                        worker_id,
                        pipeline.name,
                        e
                    );
                    mark_pipeline_failed(pipeline_id, &e);
                    compiled.remove(&pipeline_id);
                }
            }
        }
    }
    busy
}

/// Discover pipelines assigned to this executor (state='running' AND worker_id matches)
///
/// A read-only `select`: `Spi::get_one*` would assign an XID, and this runs
/// every poll on every executor — hundreds of empty commits a second idle.
fn discover_assigned_pipelines(
    worker_id: i32,
) -> Result<Vec<(i32, String, serde_json::Value)>, String> {
    let result = Spi::connect(|client| {
        client
            .select(
                "SELECT COALESCE(jsonb_agg(jsonb_build_object(\
           'id', id, 'name', name, 'definition', definition\
         )), '[]'::jsonb) \
         FROM pgstreams.pipelines WHERE state = 'running' AND worker_id = $1",
                Some(1),
                &[worker_id.into()],
            )?
            .first()
            .get_one::<pgrx::JsonB>()
    });

    let jsonb = result.map_err(|e| format!("Failed to query pipelines: {}", e))?;

    let arr = match jsonb {
        Some(jb) => match jb.0.as_array() {
            Some(a) => a.clone(),
            None => return Ok(vec![]),
        },
        None => return Ok(vec![]),
    };

    let mut pipelines = Vec::new();
    for item in arr {
        let id = item["id"].as_i64().ok_or("Missing pipeline id")? as i32;
        let name = item["name"]
            .as_str()
            .ok_or("Missing pipeline name")?
            .to_string();
        let definition = item["definition"].clone();
        pipelines.push((id, name, definition));
    }

    Ok(pipelines)
}

/// Compile a pipeline definition and initialize its input connector
fn compile_and_init(
    pipeline_id: i32,
    name: &str,
    definition_json: &serde_json::Value,
) -> Result<CompiledPipeline, String> {
    let def = parse_pipeline_value(definition_json)?;
    let mut compiled = CompiledPipeline::compile(name, pipeline_id, &def)?;
    compiled.initialize()?;
    Ok(compiled)
}

/// Mark a pipeline as failed in the database
fn mark_pipeline_failed(pipeline_id: i32, error: &str) {
    let _ = Spi::run_with_args(
        "UPDATE pgstreams.pipelines SET state = 'failed', \
         error = $2, stopped_at = now(), worker_id = NULL, \
         updated_at = now() WHERE id = $1",
        &[pipeline_id.into(), error.into()],
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::run_executor_tick;
    use pg_bgworker::spi::{self, xid_assigned};
    use pgrx::prelude::*;
    use std::collections::HashMap;

    // Each executor ticks every poll_interval_ms and the coordinator every
    // second. An idle tick that takes an XID commits an empty transaction:
    // ~400 WAL commit records a second from an idle server.

    #[pg_test]
    fn test_an_idle_executor_tick_takes_no_xid() {
        assert!(!xid_assigned());
        assert!(!run_executor_tick(0, &mut HashMap::new()), "nothing to do");
        assert!(!xid_assigned(), "an idle executor tick took an XID");
    }

    #[pg_test]
    fn test_an_idle_coordinator_tick_takes_no_xid() {
        assert!(!xid_assigned());
        crate::engine::coordinator::run_coordinator_tick();
        assert!(!xid_assigned(), "an idle coordinator tick took an XID");
    }

    #[pg_test]
    fn test_lazy_spi_takes_an_xid_only_when_it_writes() {
        // A claim that matches nothing
        let claimed = spi::get_one::<i32>(
            "UPDATE pgstreams.pipelines SET worker_id = 0 WHERE false RETURNING id",
        );
        assert!(matches!(claimed, Err(pgrx::spi::SpiError::InvalidPosition)));
        assert!(!xid_assigned(), "a write that touched no row took an XID");

        // Several statements, DDL and DML, still run (not read-only)
        spi::run("CREATE TEMP TABLE lazy_t (n int); INSERT INTO lazy_t VALUES (1), (2)").unwrap();
        assert!(xid_assigned());
        let n = spi::get_one_with_args::<i64>(
            "SELECT count(*) FROM lazy_t WHERE n >= $1",
            &[1i32.into()],
        )
        .unwrap();
        assert_eq!(n, Some(2));
    }
}
