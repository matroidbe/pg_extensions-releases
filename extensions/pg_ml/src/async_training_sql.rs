//! SQL-exposed functions for async training
//!
//! These are the #[pg_extern] functions for asynchronous model training.

use pgrx::prelude::*;

use crate::async_training;
use crate::datasets;
use crate::models;
use crate::pycaret;

/// Helper to infer task from column data
fn infer_task_from_column(
    schema: &str,
    table: &str,
    column: &str,
) -> Result<String, crate::error::PgMlError> {
    // Check column type and cardinality to infer classification vs regression
    let sql = format!(
        "SELECT data_type::text, (SELECT COUNT(DISTINCT {col})::int FROM {schema}.{table}) as cardinality
         FROM information_schema.columns
         WHERE table_schema = {schema_lit} AND table_name = {table_lit} AND column_name = {col_lit}",
        col = datasets::sanitize_identifier(column),
        schema = datasets::sanitize_identifier(schema),
        table = datasets::sanitize_identifier(table),
        schema_lit = datasets::quote_literal(schema),
        table_lit = datasets::quote_literal(table),
        col_lit = datasets::quote_literal(column),
    );

    let result: Option<(Option<String>, Option<i32>)> = Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        if let Some(row) = result.next() {
            let data_type: Option<String> = row.get(1)?;
            let cardinality: Option<i32> = row.get(2)?;
            return Ok(Some((data_type, cardinality)));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        crate::error::PgMlError::SpiError(format!("Failed to infer task: {}", e))
    })?;

    match result {
        Some((Some(dtype), Some(cardinality))) => {
            // Text types or low cardinality => classification
            if dtype.contains("char") || dtype.contains("text") || cardinality < 10 {
                Ok("classification".to_string())
            } else {
                Ok("regression".to_string())
            }
        }
        _ => Ok("regression".to_string()), // Default to regression
    }
}

/// Start async training job
///
/// Returns immediately with a job ID. Training runs in background worker.
/// Use `training_status()` to check progress and `cancel_training()` to cancel.
#[pg_extern]
#[allow(clippy::too_many_arguments)]
pub fn start_training(
    project_name: &str,
    source_table: &str,
    target_column: &str,
    algorithm: default!(Option<&str>, "NULL"),
    automl: default!(bool, "false"),
    task: default!(Option<&str>, "NULL"),
    exclude_columns: default!(Option<Vec<String>>, "NULL"),
    train_size: default!(f64, "0.8"),
    budget_time: default!(Option<i32>, "NULL"),
    conformal: default!(bool, "false"),
    conformal_method: default!(&str, "'plus'"),
    metric: default!(Option<&str>, "NULL"),
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    setup_options: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Validate inputs
    if !automl && algorithm.is_none() {
        pgrx::error!("Must specify either 'algorithm' for single model or 'automl => true'");
    }
    if automl && algorithm.is_some() {
        pgrx::error!("Cannot specify both 'algorithm' and 'automl => true'");
    }

    // Determine task (infer if not provided)
    let inferred_task = match task {
        Some(t) => t.to_string(),
        None => {
            // Auto-detect from target column by checking unique values
            let (schema, table) = pycaret::parse_relation_name(source_table);
            match infer_task_from_column(&schema, &table, target_column) {
                Ok(t) => t,
                Err(e) => pgrx::error!("Failed to infer task: {}", e),
            }
        }
    };

    let mode = if automl { "automl" } else { "single" };

    let config = async_training::TrainingJobConfig {
        exclude_columns,
        train_size,
        budget_time,
        conformal,
        conformal_method: conformal_method.to_string(),
        metric: metric.map(|s| s.to_string()),
        hyperparams: hyperparams.map(|j| j.0),
        setup_options: setup_options.map(|j| j.0),
        index_column: None,
        forecast_horizon: None,
        fold_strategy: None,
    };

    // Queue the job
    match async_training::queue_training_job(
        project_name,
        source_table,
        target_column,
        &inferred_task,
        mode,
        algorithm,
        &config,
    ) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("Failed to queue training job: {}", e),
    }
}

/// Get training job status
#[pg_extern]
#[allow(clippy::type_complexity)]
pub fn training_status(
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
        name!(started_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(completed_at, Option<pgrx::datum::TimestampWithTimeZone>),
        name!(elapsed_seconds, Option<f64>),
    ),
> {
    match async_training::get_job_status(job_id) {
        Ok(Some(status)) => TableIterator::once((
            status.job_id,
            status.project_name,
            status.state,
            status.mode,
            status.progress,
            status.current_step,
            status.algorithms_tested,
            status.algorithms_total,
            status.current_algorithm,
            status.best_result.map(pgrx::JsonB),
            status.model_id,
            status.error_message,
            status.started_at,
            status.completed_at,
            status.elapsed_seconds,
        )),
        Ok(None) => pgrx::error!("Training job {} not found", job_id),
        Err(e) => pgrx::error!("Failed to get training status: {}", e),
    }
}

/// Start async time series training job
///
/// Returns immediately with a job ID. Training runs in background worker.
/// Use `training_status()` to check progress and `cancel_training()` to cancel.
#[pg_extern]
#[allow(clippy::too_many_arguments)]
pub fn start_ts_training(
    project_name: &str,
    source_table: &str,
    target_column: &str,
    index_column: &str,
    algorithm: default!(Option<&str>, "NULL"),
    automl: default!(bool, "false"),
    fh: default!(i32, "12"),
    fold_strategy: default!(&str, "'expanding'"),
    budget_time: default!(Option<i32>, "NULL"),
    metric: default!(Option<&str>, "NULL"),
    hyperparams: default!(Option<pgrx::JsonB>, "NULL"),
    setup_options: default!(Option<pgrx::JsonB>, "NULL"),
) -> i64 {
    // Ensure schema exists
    models::ensure_schema().unwrap_or_else(|e| pgrx::error!("Schema setup failed: {}", e));

    // Validate inputs
    if !automl && algorithm.is_none() {
        pgrx::error!("Must specify either 'algorithm' for single model or 'automl => true'");
    }
    if automl && algorithm.is_some() {
        pgrx::error!("Cannot specify both 'algorithm' and 'automl => true'");
    }

    let mode = if automl { "automl" } else { "single" };

    let config = async_training::TrainingJobConfig {
        exclude_columns: None,
        train_size: 0.8,
        budget_time,
        conformal: false,
        conformal_method: "plus".to_string(),
        metric: metric.map(|s| s.to_string()),
        hyperparams: hyperparams.map(|j| j.0),
        setup_options: setup_options.map(|j| j.0),
        index_column: Some(index_column.to_string()),
        forecast_horizon: Some(fh),
        fold_strategy: Some(fold_strategy.to_string()),
    };

    // Queue the job with task = "time_series"
    match async_training::queue_training_job(
        project_name,
        source_table,
        target_column,
        "time_series",
        mode,
        algorithm,
        &config,
    ) {
        Ok(job_id) => job_id,
        Err(e) => pgrx::error!("Failed to queue training job: {}", e),
    }
}

/// Cancel a training job
#[pg_extern]
pub fn cancel_training(job_id: i64) -> bool {
    match async_training::cancel_job(job_id) {
        Ok(cancelled) => cancelled,
        Err(e) => pgrx::error!("Failed to cancel training job: {}", e),
    }
}
