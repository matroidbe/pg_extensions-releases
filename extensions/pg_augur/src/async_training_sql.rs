//! SQL-exposed async-training functions.
#![allow(clippy::too_many_arguments, clippy::type_complexity)]

use crate::async_training::{
    cancel_job, get_job_status, queue_training_job, TrainingJobConfig, TrainingJobStatus,
};
use crate::error::AugurPgError;
use crate::task::PgTask;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

fn err<T>(res: Result<T, AugurPgError>) -> T {
    res.unwrap_or_else(|e| pgrx::error!("pg_augur: {e}"))
}

fn build_config(
    exclude_columns: Option<Vec<String>>,
    train_size: f64,
    budget_time: Option<i32>,
    conformal: bool,
    conformal_method: String,
    metric: Option<String>,
    hyperparams: Option<pgrx::JsonB>,
    setup_options: Option<pgrx::JsonB>,
) -> TrainingJobConfig {
    TrainingJobConfig {
        exclude_columns,
        train_size,
        budget_time,
        conformal,
        conformal_method,
        metric,
        hyperparams: hyperparams.map(|j| j.0),
        setup_options: setup_options.map(|j| j.0),
        index_column: None,
        forecast_horizon: None,
        fold_strategy: None,
        include: None,
        exclude_algs: None,
        column_options_json: None,
        chain_actions_json: None,
        feature_columns: None,
    }
}

#[pg_extern]
#[allow(clippy::too_many_arguments)]
fn start_training(
    project_name: &str,
    source_table: &str,
    target_column: &str,
    algorithm: default!(Option<String>, "NULL"),
    automl: default!(bool, false),
    task: default!(Option<String>, "NULL"),
    exclude_columns: default!(Option<Vec<String>>, "NULL"),
    train_size: default!(f64, 0.8),
    budget_time: default!(Option<i32>, "NULL"),
    conformal: default!(bool, false),
    conformal_method: default!(String, "'plus'"),
    metric: default!(Option<String>, "NULL"),
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    setup_options: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    // Resolve task: explicit → parse; otherwise punt ("classification" default
    // matches pg_ml behavior — the worker will actually re-infer if needed).
    let task_str = match task.as_deref() {
        Some(t) => err(PgTask::parse(t)).as_str().to_string(),
        None => "classification".to_string(),
    };

    let mode = if automl || algorithm.is_none() {
        "automl"
    } else {
        "single"
    };

    let config = build_config(
        exclude_columns,
        train_size,
        budget_time,
        conformal,
        conformal_method,
        metric,
        hyperparams,
        setup_options,
    );

    err(queue_training_job(
        project_name,
        source_table,
        target_column,
        &task_str,
        mode,
        algorithm.as_deref(),
        &config,
    ))
}

#[pg_extern]
fn cancel_training(job_id: i64) -> bool {
    err(cancel_job(job_id))
}

#[pg_extern]
fn training_status(
    job_id: i64,
) -> TableIterator<
    'static,
    (
        name!(job_id, i64),
        name!(project_name, String),
        name!(state, String),
        name!(mode, String),
        name!(progress, f64),
        name!(current_step, Option<String>),
        name!(algorithms_tested, Option<i32>),
        name!(algorithms_total, Option<i32>),
        name!(current_algorithm, Option<String>),
        name!(best_so_far, Option<pgrx::JsonB>),
        name!(model_id, Option<i64>),
        name!(error_message, Option<String>),
        name!(started_at, Option<TimestampWithTimeZone>),
        name!(completed_at, Option<TimestampWithTimeZone>),
        name!(elapsed_seconds, Option<f64>),
    ),
> {
    let s: TrainingJobStatus = err(get_job_status(job_id))
        .unwrap_or_else(|| pgrx::error!("pg_augur: job {job_id} not found"));

    let row = (
        s.job_id,
        s.project_name,
        s.state,
        s.mode,
        s.progress,
        s.current_step,
        s.algorithms_tested,
        s.algorithms_total,
        s.current_algorithm,
        s.best_result.map(pgrx::JsonB),
        s.model_id,
        s.error_message,
        s.started_at,
        s.completed_at,
        s.elapsed_seconds,
    );
    TableIterator::new(std::iter::once(row))
}
