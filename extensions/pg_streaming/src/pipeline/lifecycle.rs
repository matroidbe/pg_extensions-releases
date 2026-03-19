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
