//! Async training for pg_ml
//!
//! Provides background worker-based async training that returns immediately
//! with a job ID while training runs in a separate process.
//!
//! ## Architecture
//!
//! ```text
//! Client                   Background Worker
//!   │                           │
//!   │ start_training()          │
//!   │──────► INSERT job ────────│
//!   │◄────── return job_id      │
//!   │                           │ poll for jobs
//!   │                           │──────► claim job
//!   │                           │──────► run_setup()
//!   │                           │──────► run_create_model()
//!   │                           │──────► store model
//!   │                           │──────► NOTIFY
//!   │ training_status()         │
//!   │──────► SELECT status ─────│
//! ```

use crate::datasets::quote_literal;
use crate::PgMlError;
use pgrx::bgworkers::*;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;
use std::ffi::CString;
use std::time::Duration;

// =============================================================================
// GUC Settings
// =============================================================================

/// Enable training background worker (requires PostgreSQL restart)
static TRAINING_WORKER_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

/// Job polling interval in milliseconds
static TRAINING_POLL_INTERVAL: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1000);

/// Database name for worker to connect to
static TRAINING_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

const DEFAULT_DATABASE: &str = "postgres";

/// Register async training GUC settings
pub fn register_gucs() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_ml.training_worker_enabled",
        c"Enable async training background worker",
        c"When true, pg_ml starts a background worker to process async training jobs",
        &TRAINING_WORKER_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_ml.training_poll_interval",
        c"Job polling interval in milliseconds",
        c"How often the training worker checks for new jobs",
        &TRAINING_POLL_INTERVAL,
        100,
        60000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ml.training_database",
        c"Database for training worker to connect to",
        c"Database name the training worker uses for SPI connections",
        &TRAINING_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );
}

/// Check if training worker is enabled
pub fn is_worker_enabled() -> bool {
    TRAINING_WORKER_ENABLED.get()
}

/// Get polling interval as Duration
pub fn get_poll_interval() -> Duration {
    Duration::from_millis(TRAINING_POLL_INTERVAL.get() as u64)
}

/// Get database name for worker connection
pub fn get_database() -> String {
    TRAINING_DATABASE
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| DEFAULT_DATABASE.to_string())
}

// =============================================================================
// Data Structures
// =============================================================================

/// Configuration for a training job (serialized to JSONB)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TrainingJobConfig {
    pub exclude_columns: Option<Vec<String>>,
    pub train_size: f64,
    pub budget_time: Option<i32>,
    pub conformal: bool,
    pub conformal_method: String,
    pub metric: Option<String>,
    pub hyperparams: Option<serde_json::Value>,
    pub setup_options: Option<serde_json::Value>,
    #[serde(default)]
    pub index_column: Option<String>,
    #[serde(default)]
    pub forecast_horizon: Option<i32>,
    #[serde(default)]
    pub fold_strategy: Option<String>,
}

impl Default for TrainingJobConfig {
    fn default() -> Self {
        Self {
            exclude_columns: None,
            train_size: 0.8,
            budget_time: None,
            conformal: false,
            conformal_method: "plus".to_string(),
            metric: None,
            hyperparams: None,
            setup_options: None,
            index_column: None,
            forecast_horizon: None,
            fold_strategy: None,
        }
    }
}

/// A claimed training job ready for processing
#[derive(Debug, Clone)]
pub struct TrainingJob {
    pub id: i64,
    pub project_name: String,
    pub source_table: String,
    pub target_column: String,
    pub task: String,
    pub mode: String,
    pub algorithm: Option<String>,
    pub config: TrainingJobConfig,
}

/// Status information for a training job
#[derive(Debug, Clone)]
pub struct TrainingJobStatus {
    pub job_id: i64,
    pub project_name: String,
    pub state: String,
    pub mode: String,
    pub progress: f64,
    pub current_step: Option<String>,
    pub algorithms_tested: Option<i32>,
    pub algorithms_total: Option<i32>,
    pub current_algorithm: Option<String>,
    pub best_result: Option<serde_json::Value>,
    pub model_id: Option<i64>,
    pub error_message: Option<String>,
    pub started_at: Option<TimestampWithTimeZone>,
    pub completed_at: Option<TimestampWithTimeZone>,
    pub elapsed_seconds: Option<f64>,
}

// =============================================================================
// Job Queue Functions
// =============================================================================

/// Queue a new training job
#[allow(clippy::too_many_arguments)]
pub fn queue_training_job(
    project_name: &str,
    source_table: &str,
    target_column: &str,
    task: &str,
    mode: &str,
    algorithm: Option<&str>,
    config: &TrainingJobConfig,
) -> Result<i64, PgMlError> {
    let config_json = serde_json::to_string(config)
        .map_err(|e| PgMlError::JsonError(format!("Failed to serialize config: {}", e)))?;

    let algorithm_sql = match algorithm {
        Some(a) => quote_literal(a),
        None => "NULL".to_string(),
    };

    let sql = format!(
        "INSERT INTO pgml.training_jobs
         (project_name, source_table, target_column, task, mode, algorithm, config)
         VALUES ({}, {}, {}, {}, {}, {}, '{}'::jsonb)
         RETURNING id",
        quote_literal(project_name),
        quote_literal(source_table),
        quote_literal(target_column),
        quote_literal(task),
        quote_literal(mode),
        algorithm_sql,
        config_json.replace('\'', "''")
    );

    Spi::get_one::<i64>(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to queue job: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError("No job ID returned".to_string()))
}

/// Claim the next queued job (atomic operation)
pub fn claim_next_job() -> Result<Option<TrainingJob>, PgMlError> {
    let pid = std::process::id();

    // Step 1: Atomically claim the oldest queued job and get its ID
    let claim_sql = format!(
        "UPDATE pgml.training_jobs
         SET state = 'setup', started_at = NOW(), worker_pid = {}
         WHERE id = (
             SELECT id FROM pgml.training_jobs
             WHERE state = 'queued'
             ORDER BY created_at ASC
             LIMIT 1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id",
        pid
    );

    // Use Spi::get_one which returns Ok(None) when no rows match
    // This can happen when there are no queued jobs, which is normal
    let job_id: Option<i64> = match Spi::get_one(&claim_sql) {
        Ok(id) => id,
        Err(pgrx::spi::SpiError::InvalidPosition) => {
            // No rows returned - no jobs to claim
            return Ok(None);
        }
        Err(e) => {
            return Err(PgMlError::SpiError(format!("Failed to claim job: {}", e)));
        }
    };

    let job_id = match job_id {
        Some(id) => id,
        None => return Ok(None), // No jobs to claim
    };

    // Step 2: Fetch the full job details
    let select_sql = format!(
        "SELECT project_name, source_table, target_column, task, mode, algorithm, config::text
         FROM pgml.training_jobs WHERE id = {}",
        job_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&select_sql, None, &[])?;

        if let Some(row) = result.next() {
            let project_name: String = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let source_table: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let target_column: String = row.get(3)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let task: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let mode: String = row.get(5)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let algorithm: Option<String> = row.get(6)?;
            let config_json: String = row.get(7)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;

            let config: TrainingJobConfig = match serde_json::from_str(&config_json) {
                Ok(c) => c,
                Err(_) => {
                    // Config JSON parse error - treat as invalid data
                    return Err(pgrx::spi::SpiError::InvalidPosition);
                }
            };

            return Ok(Some(TrainingJob {
                id: job_id,
                project_name,
                source_table,
                target_column,
                task,
                mode,
                algorithm,
                config,
            }));
        }
        // Job was claimed but not found - shouldn't happen
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| PgMlError::SpiError(format!("Failed to fetch job: {}", e)))
}

/// Update job progress
pub fn update_job_progress(
    job_id: i64,
    state: &str,
    progress: f64,
    current_step: Option<&str>,
) -> Result<(), PgMlError> {
    let current_step_sql = match current_step {
        Some(s) => quote_literal(s),
        None => "NULL".to_string(),
    };

    let sql = format!(
        "UPDATE pgml.training_jobs
         SET state = {}, progress = {}, current_step = {}
         WHERE id = {}",
        quote_literal(state),
        progress,
        current_step_sql,
        job_id
    );

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to update progress: {}", e)))
}

/// Update automl progress
#[allow(dead_code)] // Will be used when automl progress tracking is implemented
pub fn update_automl_progress(
    job_id: i64,
    algorithms_tested: i32,
    algorithms_total: i32,
    current_algorithm: &str,
    best_result: Option<&serde_json::Value>,
) -> Result<(), PgMlError> {
    let best_result_sql = match best_result {
        Some(v) => {
            let json = serde_json::to_string(v).map_err(|e| {
                PgMlError::JsonError(format!("Failed to serialize best_result: {}", e))
            })?;
            format!("'{}'::jsonb", json.replace('\'', "''"))
        }
        None => "NULL".to_string(),
    };

    let sql = format!(
        "UPDATE pgml.training_jobs
         SET algorithms_tested = {}, algorithms_total = {},
             current_algorithm = {}, best_result = {}
         WHERE id = {}",
        algorithms_tested,
        algorithms_total,
        quote_literal(current_algorithm),
        best_result_sql,
        job_id
    );

    Spi::run(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to update automl progress: {}", e)))
}

/// Complete a job successfully
pub fn complete_job(job_id: i64, model_id: i64) -> Result<(), PgMlError> {
    let sql = format!(
        "UPDATE pgml.training_jobs
         SET state = 'completed', progress = 1.0, model_id = {}, completed_at = NOW()
         WHERE id = {}",
        model_id, job_id
    );

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to complete job: {}", e)))
}

/// Mark a job as failed
pub fn fail_job(job_id: i64, error: &str) -> Result<(), PgMlError> {
    let sql = format!(
        "UPDATE pgml.training_jobs
         SET state = 'failed', error_message = {}, completed_at = NOW()
         WHERE id = {}",
        quote_literal(error),
        job_id
    );

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to mark job failed: {}", e)))
}

/// Cancel a job
pub fn cancel_job(job_id: i64) -> Result<bool, PgMlError> {
    let sql = format!(
        "UPDATE pgml.training_jobs
         SET state = 'cancelled', completed_at = NOW()
         WHERE id = {} AND state IN ('queued', 'setup', 'training')
         RETURNING id",
        job_id
    );

    let cancelled: Option<i64> = Spi::get_one(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to cancel job: {}", e)))?;

    Ok(cancelled.is_some())
}

/// Check if a job was cancelled
pub fn is_job_cancelled(job_id: i64) -> Result<bool, PgMlError> {
    let sql = format!(
        "SELECT state = 'cancelled' FROM pgml.training_jobs WHERE id = {}",
        job_id
    );

    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgMlError::SpiError(format!("Failed to check cancellation: {}", e)))?
        .ok_or_else(|| PgMlError::SpiError(format!("Job {} not found", job_id)))
}

/// Get job status
pub fn get_job_status(job_id: i64) -> Result<Option<TrainingJobStatus>, PgMlError> {
    // Cast progress and elapsed_seconds to DOUBLE PRECISION for Rust f64 compatibility
    let sql = format!(
        "SELECT id, project_name, state, mode, progress::double precision, current_step,
                algorithms_tested, algorithms_total, current_algorithm, best_result::text,
                model_id, error_message, started_at, completed_at,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))::double precision as elapsed_seconds
         FROM pgml.training_jobs WHERE id = {}",
        job_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let job_id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let project_name: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let state: String = row.get(3)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let mode: String = row.get(4)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let progress: f64 = row.get(5)?.unwrap_or(0.0);
            let current_step: Option<String> = row.get(6)?;
            let algorithms_tested: Option<i32> = row.get(7)?;
            let algorithms_total: Option<i32> = row.get(8)?;
            let current_algorithm: Option<String> = row.get(9)?;
            let best_result_json: Option<String> = row.get(10)?;
            let model_id: Option<i64> = row.get(11)?;
            let error_message: Option<String> = row.get(12)?;
            let started_at: Option<TimestampWithTimeZone> = row.get(13)?;
            let completed_at: Option<TimestampWithTimeZone> = row.get(14)?;
            let elapsed_seconds: Option<f64> = row.get(15)?;

            let best_result = best_result_json.and_then(|j| serde_json::from_str(&j).ok());

            return Ok(Some(TrainingJobStatus {
                job_id,
                project_name,
                state,
                mode,
                progress,
                current_step,
                algorithms_tested,
                algorithms_total,
                current_algorithm,
                best_result,
                model_id,
                error_message,
                started_at,
                completed_at,
                elapsed_seconds,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgMlError::SpiError(format!("Failed to get job status: {}", e))
    })
}

/// Send NOTIFY on job completion
pub fn notify_completion(
    job_id: i64,
    state: &str,
    model_id: Option<i64>,
    project: &str,
    error: Option<&str>,
) -> Result<(), PgMlError> {
    let payload = serde_json::json!({
        "job_id": job_id,
        "state": state,
        "model_id": model_id,
        "project": project,
        "error": error,
    });

    let sql = format!(
        "SELECT pg_notify('pgml_training', {})",
        quote_literal(&payload.to_string())
    );

    Spi::run(&sql).map_err(|e| PgMlError::SpiError(format!("Failed to send notification: {}", e)))
}

// =============================================================================
// Background Worker Registration
// =============================================================================

/// Register async training background worker (called from _PG_init if enabled)
pub fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_ml_training")
        .set_function("pg_ml_training_worker_main")
        .set_library("pg_ml")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

// =============================================================================
// Background Worker Main
// =============================================================================

/// Parse a relation name into (schema, table)
fn parse_table_ref(source_table: &str) -> Result<(String, String), PgMlError> {
    let parts: Vec<&str> = source_table.split('.').collect();
    match parts.len() {
        1 => Ok(("public".to_string(), parts[0].to_string())),
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => Err(PgMlError::InvalidParameter(format!(
            "Invalid table reference: {}",
            source_table
        ))),
    }
}

/// Process a single training job
fn process_training_job(job: TrainingJob) -> Result<(), PgMlError> {
    let start_time = std::time::Instant::now();

    pgrx::log!(
        "pg_ml_training: processing job {} for project '{}'",
        job.id,
        job.project_name
    );

    // Parse source table
    let (schema, table) = parse_table_ref(&job.source_table)?;

    // Update state to 'setup'
    update_job_progress(job.id, "setup", 0.0, Some("Initializing experiment"))?;

    // Check for cancellation
    if is_job_cancelled(job.id)? {
        pgrx::log!("pg_ml_training: job {} cancelled during setup", job.id);
        return Ok(());
    }

    // Run setup and training based on task type
    let train_result = if job.task == "time_series" {
        // Time series setup
        let index_column = job.config.index_column.as_deref().ok_or_else(|| {
            PgMlError::InvalidParameter("index_column required for time_series".into())
        })?;
        let fh = job.config.forecast_horizon.unwrap_or(12);

        let setup_opts = job.config.setup_options.clone();

        crate::pycaret::run_setup_timeseries(
            &schema,
            &table,
            &job.target_column,
            index_column,
            fh,
            Some(3),
            job.config.fold_strategy.as_deref(),
            setup_opts.as_ref(),
        )?;

        // Update state to 'training'
        update_job_progress(job.id, "training", 0.1, Some("Starting TS training"))?;

        if is_job_cancelled(job.id)? {
            pgrx::log!("pg_ml_training: job {} cancelled before training", job.id);
            return Ok(());
        }

        match job.mode.as_str() {
            "single" => {
                let algorithm = job.algorithm.as_deref().ok_or_else(|| {
                    PgMlError::InvalidParameter("algorithm required for single mode".into())
                })?;
                update_job_progress(
                    job.id,
                    "training",
                    0.2,
                    Some(&format!("Training {}", algorithm)),
                )?;
                crate::pycaret::run_create_ts_model(algorithm, job.config.hyperparams.as_ref())?
            }
            "automl" => {
                update_job_progress(
                    job.id,
                    "training",
                    0.2,
                    Some("Running TS AutoML comparison"),
                )?;
                let sort = job.config.metric.as_deref();
                crate::pycaret::run_compare_ts_models(
                    Some(1),
                    sort,
                    None,
                    None,
                    job.config.budget_time,
                )?
            }
            _ => {
                return Err(PgMlError::InvalidParameter(format!(
                    "Unknown mode: {}",
                    job.mode
                )))
            }
        }
    } else {
        // Classification / Regression setup
        // Merge train_size into setup_options if provided
        // Default to empty object {} instead of Null when setup_options is None
        let mut setup_opts = job
            .config
            .setup_options
            .clone()
            .unwrap_or_else(|| serde_json::json!({}));
        if let serde_json::Value::Object(ref mut map) = setup_opts {
            map.insert(
                "train_size".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(job.config.train_size).unwrap_or_else(|| {
                        serde_json::Number::from_f64(0.8).expect("0.8 is a valid f64")
                    }),
                ),
            );
        }

        // Run setup
        let _setup_result = crate::pycaret::run_setup(
            &schema,
            &table,
            &job.target_column,
            Some(&job.task),
            job.config.exclude_columns.as_deref(),
            Some(&setup_opts),
            None, // id_column - not used for async training yet
        )?;

        // Update state to 'training'
        update_job_progress(job.id, "training", 0.1, Some("Starting training"))?;

        // Check for cancellation
        if is_job_cancelled(job.id)? {
            pgrx::log!("pg_ml_training: job {} cancelled before training", job.id);
            return Ok(());
        }

        // Run training based on mode
        match job.mode.as_str() {
            "single" => {
                let algorithm = job.algorithm.as_deref().ok_or_else(|| {
                    PgMlError::InvalidParameter("algorithm required for single mode".into())
                })?;

                update_job_progress(
                    job.id,
                    "training",
                    0.2,
                    Some(&format!("Training {}", algorithm)),
                )?;

                if job.config.conformal {
                    crate::pycaret::run_create_model_conformal(
                        algorithm,
                        job.config.hyperparams.as_ref(),
                        &job.config.conformal_method,
                        5, // conformal_cv
                    )?
                } else {
                    crate::pycaret::run_create_model(algorithm, job.config.hyperparams.as_ref())?
                }
            }
            "automl" => {
                update_job_progress(job.id, "training", 0.2, Some("Running AutoML comparison"))?;

                // Convert metric to sort parameter
                let sort = job.config.metric.as_deref();

                crate::pycaret::run_compare_models(
                    Some(1), // Select best model
                    sort,
                    None, // include
                    None, // exclude
                    job.config.budget_time,
                )?
            }
            _ => {
                return Err(PgMlError::InvalidParameter(format!(
                    "Unknown mode: {}",
                    job.mode
                )))
            }
        }
    };

    let training_time = start_time.elapsed().as_secs_f64();

    // Check for cancellation before storing
    if is_job_cancelled(job.id)? {
        pgrx::log!(
            "pg_ml_training: job {} cancelled after training, not storing model",
            job.id
        );
        return Ok(());
    }

    update_job_progress(job.id, "training", 0.9, Some("Storing model"))?;

    // Ensure project exists and store model
    let project_id = crate::models::ensure_project(
        &job.project_name,
        &train_result.task,
        &train_result.target_column,
        &train_result.feature_columns,
    )?;

    let model_id = crate::models::store_model(
        project_id,
        &train_result.algorithm,
        job.config.hyperparams.as_ref(),
        train_result.metrics.clone(),
        &train_result.model_bytes,
        train_result.label_classes.as_deref(),
        training_time,
        true, // deploy
        job.config.conformal,
        if job.config.conformal {
            Some(&job.config.conformal_method)
        } else {
            None
        },
    )?;

    // Complete job
    complete_job(job.id, model_id)?;

    // Send notification
    notify_completion(job.id, "completed", Some(model_id), &job.project_name, None)?;

    pgrx::log!(
        "pg_ml_training: job {} completed, model_id={}, training_time={:.2}s",
        job.id,
        model_id,
        training_time
    );

    Ok(())
}

/// Background worker main function - processes async training jobs
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_ml_training_worker_main(_arg: pg_sys::Datum) {
    // Set up signal handlers
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    // Connect to database for SPI access
    let database = get_database();
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);

    pgrx::log!(
        "pg_ml_training: worker started, pid={}, database={}",
        std::process::id(),
        database
    );

    // Check if enabled
    if !is_worker_enabled() {
        pgrx::log!("pg_ml_training: disabled via pg_ml.training_worker_enabled=false");
        return;
    }

    // Initialize Python once for worker lifetime
    if let Err(e) = crate::ensure_python() {
        pgrx::log!("pg_ml_training: failed to initialize Python: {}", e);
        return;
    }

    pgrx::log!("pg_ml_training: Python initialized successfully");

    let poll_interval = get_poll_interval();

    // Main loop
    while BackgroundWorker::wait_latch(Some(poll_interval)) {
        // Check for SIGTERM
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_ml_training: received SIGTERM, shutting down");
            break;
        }

        // Handle SIGHUP for config reload
        if BackgroundWorker::sighup_received() {
            pgrx::log!("pg_ml_training: received SIGHUP");
        }

        // Try to claim and process one job within a transaction
        let result: Result<(), PgMlError> = BackgroundWorker::transaction(|| {
            match claim_next_job() {
                Ok(Some(job)) => {
                    let job_id = job.id;
                    let project = job.project_name.clone();

                    if let Err(e) = process_training_job(job) {
                        pgrx::log!("pg_ml_training: job {} failed: {}", job_id, e);

                        // Mark job as failed
                        if let Err(fail_err) = fail_job(job_id, &e.to_string()) {
                            pgrx::log!(
                                "pg_ml_training: failed to mark job {} as failed: {}",
                                job_id,
                                fail_err
                            );
                        }

                        // Send failure notification
                        if let Err(notify_err) = notify_completion(
                            job_id,
                            "failed",
                            None,
                            &project,
                            Some(&e.to_string()),
                        ) {
                            pgrx::log!(
                                "pg_ml_training: failed to send failure notification: {}",
                                notify_err
                            );
                        }
                    }
                }
                Ok(None) => {
                    // No jobs to process
                }
                Err(e) => {
                    pgrx::log!("pg_ml_training: error claiming job: {}", e);
                }
            }
            Ok(())
        });

        if let Err(e) = result {
            pgrx::log!("pg_ml_training: transaction error: {}", e);
        }
    }

    pgrx::log!("pg_ml_training: worker stopped");
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_training_job_config_default() {
        let config = TrainingJobConfig::default();
        assert_eq!(config.train_size, 0.8);
        assert!(!config.conformal);
        assert_eq!(config.conformal_method, "plus");
        assert!(config.exclude_columns.is_none());
    }

    #[test]
    fn test_training_job_config_serialization() {
        let config = TrainingJobConfig {
            exclude_columns: Some(vec!["id".to_string(), "created_at".to_string()]),
            train_size: 0.7,
            budget_time: Some(30),
            conformal: true,
            conformal_method: "cv".to_string(),
            metric: Some("accuracy".to_string()),
            hyperparams: Some(serde_json::json!({"n_estimators": 100})),
            setup_options: Some(serde_json::json!({"normalize": true})),
            index_column: None,
            forecast_horizon: None,
            fold_strategy: None,
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: TrainingJobConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.train_size, 0.7);
        assert_eq!(parsed.budget_time, Some(30));
        assert!(parsed.conformal);
        assert_eq!(parsed.conformal_method, "cv");
        assert_eq!(parsed.exclude_columns.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_parse_table_ref_with_schema() {
        let (schema, table) = parse_table_ref("public.users").unwrap();
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_table_ref_without_schema() {
        let (schema, table) = parse_table_ref("users").unwrap();
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn test_parse_table_ref_analytics_schema() {
        let (schema, table) = parse_table_ref("analytics.events").unwrap();
        assert_eq!(schema, "analytics");
        assert_eq!(table, "events");
    }

    #[test]
    fn test_parse_table_ref_invalid() {
        let result = parse_table_ref("a.b.c");
        assert!(result.is_err());
    }

    #[test]
    fn test_training_job_config_ts_fields() {
        let config = TrainingJobConfig {
            index_column: Some("ds".to_string()),
            forecast_horizon: Some(12),
            fold_strategy: Some("expanding".to_string()),
            ..Default::default()
        };
        assert_eq!(config.index_column.as_deref(), Some("ds"));
        assert_eq!(config.forecast_horizon, Some(12));
        assert_eq!(config.fold_strategy.as_deref(), Some("expanding"));
    }

    #[test]
    fn test_training_job_config_ts_serialization_roundtrip() {
        let config = TrainingJobConfig {
            index_column: Some("timestamp".to_string()),
            forecast_horizon: Some(6),
            fold_strategy: Some("sliding".to_string()),
            metric: Some("MASE".to_string()),
            budget_time: Some(60),
            ..Default::default()
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: TrainingJobConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.index_column, Some("timestamp".to_string()));
        assert_eq!(parsed.forecast_horizon, Some(6));
        assert_eq!(parsed.fold_strategy, Some("sliding".to_string()));
        assert_eq!(parsed.metric, Some("MASE".to_string()));
    }

    #[test]
    fn test_training_job_config_backwards_compatible_deserialization() {
        // Old config JSON without TS fields should deserialize with None defaults
        let old_json = r#"{
            "exclude_columns": null,
            "train_size": 0.8,
            "budget_time": null,
            "conformal": false,
            "conformal_method": "plus",
            "metric": null,
            "hyperparams": null,
            "setup_options": null
        }"#;

        let config: TrainingJobConfig = serde_json::from_str(old_json).unwrap();
        assert!(config.index_column.is_none());
        assert!(config.forecast_horizon.is_none());
        assert!(config.fold_strategy.is_none());
        assert_eq!(config.train_size, 0.8);
    }

    #[test]
    fn test_training_job_status_construction() {
        let status = TrainingJobStatus {
            job_id: 1,
            project_name: "test".to_string(),
            state: "training".to_string(),
            mode: "single".to_string(),
            progress: 0.5,
            current_step: Some("Training xgboost".to_string()),
            algorithms_tested: None,
            algorithms_total: None,
            current_algorithm: Some("xgboost".to_string()),
            best_result: None,
            model_id: None,
            error_message: None,
            started_at: None,
            completed_at: None,
            elapsed_seconds: Some(120.5),
        };

        assert_eq!(status.state, "training");
        assert_eq!(status.progress, 0.5);
        assert_eq!(status.elapsed_seconds, Some(120.5));
    }
}
