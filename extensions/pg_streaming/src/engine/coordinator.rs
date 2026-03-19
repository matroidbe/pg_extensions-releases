//! Coordinator — assigns pipelines to executor workers
//!
//! The coordinator runs as a single background worker. On each tick it:
//! 1. Finds running pipelines with no assigned executor (worker_id IS NULL)
//! 2. Assigns each to the least-loaded executor (fewest current assignments)
//! 3. Detects stale assignments where an executor may have crashed

use crate::config::{PG_STREAMING_ENABLED, PG_STREAMING_WORKER_COUNT};
use pgrx::prelude::*;

/// Run one coordinator tick — assign unassigned pipelines, check health
pub fn run_coordinator_tick() {
    if !PG_STREAMING_ENABLED.get() {
        return;
    }

    assign_unassigned_pipelines();
    reclaim_stale_assignments();
}

/// Find running pipelines with worker_id IS NULL and assign them
/// to the executor with the fewest current assignments.
fn assign_unassigned_pipelines() {
    let worker_count = PG_STREAMING_WORKER_COUNT.get();

    // Get unassigned running pipelines
    let unassigned = Spi::get_one::<pgrx::JsonB>(
        "SELECT COALESCE(jsonb_agg(id ORDER BY id), '[]'::jsonb) \
         FROM pgstreams.pipelines WHERE state = 'running' AND worker_id IS NULL",
    );

    let pipeline_ids: Vec<i64> = match unassigned {
        Ok(Some(jb)) => match jb.0.as_array() {
            Some(arr) => arr.iter().filter_map(|v| v.as_i64()).collect(),
            None => return,
        },
        _ => return,
    };

    if pipeline_ids.is_empty() {
        return;
    }

    // Get current assignment counts per executor
    let counts = Spi::get_one::<pgrx::JsonB>(
        "SELECT COALESCE(jsonb_object_agg(worker_id::text, cnt), '{}'::jsonb) \
         FROM (SELECT worker_id, count(*) AS cnt FROM pgstreams.pipelines \
         WHERE state = 'running' AND worker_id IS NOT NULL \
         GROUP BY worker_id) sub",
    );

    let mut load: Vec<(i32, i64)> = (0..worker_count)
        .map(|i| {
            let count = match &counts {
                Ok(Some(jb)) => jb.0[i.to_string()].as_i64().unwrap_or(0),
                _ => 0,
            };
            (i, count)
        })
        .collect();

    // Assign each unassigned pipeline to the least-loaded executor
    for pipeline_id in pipeline_ids {
        // Sort by load (ascending), then by worker_id for determinism
        load.sort_by_key(|&(wid, cnt)| (cnt, wid));
        let (target_worker, current_count) = load[0];

        let result = Spi::run_with_args(
            "UPDATE pgstreams.pipelines SET worker_id = $1, updated_at = now() \
             WHERE id = $2 AND state = 'running' AND worker_id IS NULL",
            &[target_worker.into(), (pipeline_id as i32).into()],
        );

        if result.is_ok() {
            log!(
                "pg_streaming coordinator: assigned pipeline {} to executor {}",
                pipeline_id,
                target_worker
            );
            // Update our local count
            load[0] = (target_worker, current_count + 1);
        }
    }
}

/// Detect pipelines assigned to executors that may have crashed.
/// A pipeline is considered stale if it's assigned to a worker but hasn't
/// had its updated_at changed in a while. For now, we simply check that
/// worker_id is within the valid range of executor IDs.
fn reclaim_stale_assignments() {
    let worker_count = PG_STREAMING_WORKER_COUNT.get();

    // Reclaim pipelines assigned to executor IDs that no longer exist
    // (e.g., worker_count was reduced)
    let result = Spi::get_one_with_args::<i64>(
        "WITH reclaimed AS (\
           UPDATE pgstreams.pipelines SET worker_id = NULL, updated_at = now() \
           WHERE state = 'running' AND worker_id IS NOT NULL AND worker_id >= $1 \
           RETURNING id\
         ) SELECT count(*) FROM reclaimed",
        &[worker_count.into()],
    );

    if let Ok(Some(count)) = result {
        if count > 0 {
            log!(
                "pg_streaming coordinator: reclaimed {} pipelines from invalid executor IDs",
                count
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_coordinator_module_compiles() {
        // The coordinator relies on SPI for all operations.
        // Real testing happens via pg_test integration tests.
    }
}
