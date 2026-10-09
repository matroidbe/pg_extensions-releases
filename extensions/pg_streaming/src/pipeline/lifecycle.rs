//! Pipeline lifecycle management (start/stop/restart)

use pgrx::prelude::*;

/// Start a pipeline (transition from created/stopped/failed → running)
pub fn start_pipeline_impl(name: &str) {
    let result = Spi::get_one_with_args::<String>(
        "SELECT state FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    );

    let state = match result {
        Ok(Some(s)) => s,
        _ => pgrx::error!("Pipeline '{}' not found", name),
    };

    match state.as_str() {
        "running" => pgrx::error!("Pipeline '{}' is already running", name),
        "created" | "stopped" | "failed" => {}
        other => pgrx::error!("Pipeline '{}' is in unexpected state '{}'", name, other),
    }

    let _ = Spi::run_with_args(
        "UPDATE pgstreams.pipelines SET state = 'running', \
         started_at = now(), stopped_at = NULL, error = NULL, \
         worker_id = NULL, updated_at = now() WHERE name = $1",
        &[name.into()],
    );
}

/// Stop a running pipeline
pub fn stop_pipeline_impl(name: &str) {
    let result = Spi::get_one_with_args::<String>(
        "SELECT state FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    );

    let state = match result {
        Ok(Some(s)) => s,
        _ => pgrx::error!("Pipeline '{}' not found", name),
    };

    if state != "running" {
        pgrx::error!("Pipeline '{}' is not running (state: {})", name, state);
    }

    let _ = Spi::run_with_args(
        "UPDATE pgstreams.pipelines SET state = 'stopped', \
         stopped_at = now(), worker_id = NULL, updated_at = now() WHERE name = $1",
        &[name.into()],
    );
}

/// Restart a pipeline (stop + start)
pub fn restart_pipeline_impl(name: &str) {
    let result = Spi::get_one_with_args::<String>(
        "SELECT state FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    );

    let state = match result {
        Ok(Some(s)) => s,
        _ => pgrx::error!("Pipeline '{}' not found", name),
    };

    // If running, stop first
    if state == "running" {
        let _ = Spi::run_with_args(
            "UPDATE pgstreams.pipelines SET state = 'stopped', \
             stopped_at = now(), worker_id = NULL, updated_at = now() WHERE name = $1",
            &[name.into()],
        );
    }

    // Start
    let _ = Spi::run_with_args(
        "UPDATE pgstreams.pipelines SET state = 'running', \
         started_at = now(), stopped_at = NULL, error = NULL, \
         worker_id = NULL, updated_at = now() WHERE name = $1",
        &[name.into()],
    );
}

/// Rewind a stopped pipeline's async-source cursor (`pgstreams.connector_state`).
///
/// `after = NULL` forgets the cursor, so the source re-reads from the start.
/// `after = '<path>'` resumes strictly after that path — for an
/// `order: lexicographic` file source, a partition prefix such as
/// `'hic/arrival_date=2026-10-01'` replays that partition and everything after.
///
/// Refuses while the pipeline is running: an executor that is mid-batch
/// persists its cursor on commit and would overwrite the rewind.
pub fn replay_pipeline_impl(name: &str, after: Option<&str>) {
    let state = match Spi::get_one_with_args::<String>(
        "SELECT state FROM pgstreams.pipelines WHERE name = $1",
        &[name.into()],
    ) {
        Ok(Some(s)) => s,
        _ => pgrx::error!("Pipeline '{}' not found", name),
    };
    if state == "running" {
        pgrx::error!(
            "Pipeline '{}' is running; stop it first: SELECT pgstreams.stop('{}')",
            name,
            name
        );
    }

    let result = match after {
        None => Spi::run_with_args(
            "DELETE FROM pgstreams.connector_state \
             WHERE pipeline = $1 AND connector_role = 'input'",
            &[name.into()],
        ),
        Some(path) => Spi::run_with_args(
            "INSERT INTO pgstreams.connector_state (pipeline, connector_role, cursor) \
             VALUES ($1, 'input', jsonb_build_object('after', $2::text)) \
             ON CONFLICT (pipeline, connector_role) \
             DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()",
            &[name.into(), path.into()],
        ),
    };
    if let Err(e) = result {
        pgrx::error!("Failed to rewind pipeline '{}': {}", name, e);
    }
}
