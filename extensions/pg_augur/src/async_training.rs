//! Async training for pg_augur.
//!
//! A single background worker polls `pgaugur.training_jobs` for queued jobs,
//! runs them through `crate::train`, and updates job state.
//!
//! ```text
//! Client                  Background Worker
//!   │ start_training()        │
//!   │──► INSERT job ──────────│
//!   │◄── return job_id        │
//!   │                         │ poll (every N ms)
//!   │                         │──► claim queued job
//!   │                         │──► train::train_* (sync, may block)
//!   │                         │──► store model + state=completed
//!   │                         │──► NOTIFY pgaugur_training
//!   │ training_status()       │
//!   │──► SELECT status ───────│
//! ```

use crate::config;
use crate::data;
use crate::error::AugurPgError;
use crate::models::quote_literal;
use crate::task::PgTask;
use crate::train;
use pgrx::bgworkers::*;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::time::Duration;

// ─────────────────────────────────────────────
// Serialized job config
// ─────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingJobConfig {
    #[serde(default)]
    pub exclude_columns: Option<Vec<String>>,
    #[serde(default = "default_train_size")]
    pub train_size: f64,
    #[serde(default)]
    pub budget_time: Option<i32>,
    #[serde(default)]
    pub conformal: bool,
    #[serde(default = "default_conformal_method")]
    pub conformal_method: String,
    #[serde(default)]
    pub metric: Option<String>,
    #[serde(default)]
    pub hyperparams: Option<serde_json::Value>,
    #[serde(default)]
    pub setup_options: Option<serde_json::Value>,
    #[serde(default)]
    pub index_column: Option<String>,
    #[serde(default)]
    pub forecast_horizon: Option<i32>,
    #[serde(default)]
    pub fold_strategy: Option<String>,
    #[serde(default)]
    pub include: Option<Vec<String>>,
    #[serde(default)]
    pub exclude_algs: Option<Vec<String>>,
    // ── FDW-specific fields (mode='fdw') ──
    /// Serialized column OPTIONS from the feature view (JSON array of {name, opts}).
    /// Used by the background worker which can't access GetForeignColumnOptions.
    #[serde(default)]
    pub column_options_json: Option<String>,
    /// Serialized chain actions (JSON array of ChainAction).
    #[serde(default)]
    pub chain_actions_json: Option<String>,
    /// Feature columns (excluding target/ignore).
    #[serde(default)]
    pub feature_columns: Option<Vec<String>>,
}

fn default_train_size() -> f64 {
    0.8
}
fn default_conformal_method() -> String {
    "plus".to_string()
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
            include: None,
            exclude_algs: None,
            column_options_json: None,
            chain_actions_json: None,
            feature_columns: None,
        }
    }
}

// ─────────────────────────────────────────────
// Queue / status / cancel API (used by sql_functions.rs)
// ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub fn queue_training_job(
    project_name: &str,
    source_table: &str,
    target_column: &str,
    task: &str,
    mode: &str,
    algorithm: Option<&str>,
    config: &TrainingJobConfig,
) -> Result<i64, AugurPgError> {
    let cfg_json = serde_json::to_string(config)?;
    let algo_sql = match algorithm {
        Some(a) => quote_literal(a),
        None => "NULL".to_string(),
    };
    let sql = format!(
        "INSERT INTO pgaugur.training_jobs
           (project_name, source_table, target_column, task, mode, algorithm, config)
         VALUES ({}, {}, {}, {}, {}, {}, {}::jsonb)
         RETURNING id",
        quote_literal(project_name),
        quote_literal(source_table),
        quote_literal(target_column),
        quote_literal(task),
        quote_literal(mode),
        algo_sql,
        quote_literal(&cfg_json),
    );
    Spi::get_one::<i64>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("queue job: {e}")))?
        .ok_or_else(|| AugurPgError::Spi("queue job returned no id".into()))
}

pub fn cancel_job(job_id: i64) -> Result<bool, AugurPgError> {
    let sql = format!(
        "UPDATE pgaugur.training_jobs
         SET state = 'cancelled', completed_at = NOW()
         WHERE id = {} AND state IN ('queued', 'setup', 'training')
         RETURNING id",
        job_id
    );
    let cancelled: Option<i64> =
        Spi::get_one(&sql).map_err(|e| AugurPgError::Spi(format!("cancel job: {e}")))?;
    Ok(cancelled.is_some())
}

pub fn is_job_cancelled(job_id: i64) -> Result<bool, AugurPgError> {
    let sql = format!(
        "SELECT state = 'cancelled' FROM pgaugur.training_jobs WHERE id = {}",
        job_id
    );
    Spi::get_one::<bool>(&sql)
        .map_err(|e| AugurPgError::Spi(format!("check cancel: {e}")))?
        .ok_or_else(|| AugurPgError::Other(format!("job {job_id} not found")))
}

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

pub fn get_job_status(job_id: i64) -> Result<Option<TrainingJobStatus>, AugurPgError> {
    let sql = format!(
        "SELECT id, project_name, state, mode, progress::double precision, current_step,
                algorithms_tested, algorithms_total, current_algorithm, best_result::text,
                model_id, error_message, started_at, completed_at,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))::double precision
         FROM pgaugur.training_jobs WHERE id = {}",
        job_id
    );
    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;
        if let Some(row) = result.next() {
            let job_id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let project_name: String = row.get(2)?.unwrap_or_default();
            let state: String = row.get(3)?.unwrap_or_default();
            let mode: String = row.get(4)?.unwrap_or_default();
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
    .map_err(|e: pgrx::spi::SpiError| AugurPgError::Spi(format!("job status: {e}")))
}

// ─────────────────────────────────────────────
// Worker internals
// ─────────────────────────────────────────────

#[derive(Debug, Clone)]
struct ClaimedJob {
    id: i64,
    project_name: String,
    source_table: String,
    target_column: String,
    task: String,
    mode: String,
    algorithm: Option<String>,
    config: TrainingJobConfig,
}

fn claim_next_job() -> Result<Option<ClaimedJob>, AugurPgError> {
    // Step 1: atomically claim a job ID. Must use Spi::get_one for the UPDATE —
    // Spi::connect + client.select runs in read-only mode and rejects DML.
    let claim_sql = "UPDATE pgaugur.training_jobs
                     SET state = 'setup',
                         started_at = NOW(),
                         worker_pid = pg_backend_pid()
                     WHERE id = (
                         SELECT id FROM pgaugur.training_jobs
                         WHERE state = 'queued'
                         ORDER BY id
                         FOR UPDATE SKIP LOCKED
                         LIMIT 1
                     )
                     RETURNING id";

    let job_id: Option<i64> = match Spi::get_one(claim_sql) {
        Ok(id) => id,
        Err(pgrx::spi::SpiError::InvalidPosition) => return Ok(None),
        Err(e) => return Err(AugurPgError::Spi(format!("claim job: {e}"))),
    };
    let Some(job_id) = job_id else {
        return Ok(None);
    };

    // Step 2: fetch the claimed job's details via a normal SELECT.
    let select_sql = format!(
        "SELECT project_name, source_table, target_column, task, mode,
                algorithm, config::text
         FROM pgaugur.training_jobs WHERE id = {}",
        job_id
    );
    Spi::connect(|client| {
        let mut result = client.select(&select_sql, None, &[])?;
        if let Some(row) = result.next() {
            let project_name: String = row.get(1)?.unwrap_or_default();
            let source_table: String = row.get(2)?.unwrap_or_default();
            let target_column: String = row.get(3)?.unwrap_or_default();
            let task: String = row.get(4)?.unwrap_or_default();
            let mode: String = row.get(5)?.unwrap_or_default();
            let algorithm: Option<String> = row.get(6)?;
            let cfg_json: String = row.get(7)?.unwrap_or_else(|| "{}".to_string());
            let config: TrainingJobConfig = serde_json::from_str(&cfg_json).unwrap_or_default();
            return Ok(Some(ClaimedJob {
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
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| AugurPgError::Spi(format!("claim job details: {e}")))
}

fn update_progress(
    job_id: i64,
    state: &str,
    progress: f64,
    step: Option<&str>,
) -> Result<(), AugurPgError> {
    let step_sql = match step {
        Some(s) => quote_literal(s),
        None => "NULL".to_string(),
    };
    let sql = format!(
        "UPDATE pgaugur.training_jobs
         SET state = {}, progress = {}, current_step = {}
         WHERE id = {}",
        quote_literal(state),
        progress,
        step_sql,
        job_id
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("update progress: {e}")))
}

fn finalize_job(
    job_id: i64,
    state: &str,
    model_id: Option<i64>,
    error: Option<&str>,
) -> Result<(), AugurPgError> {
    let model_sql = match model_id {
        Some(id) => id.to_string(),
        None => "NULL".to_string(),
    };
    let error_sql = match error {
        Some(e) => quote_literal(e),
        None => "NULL".to_string(),
    };
    let sql = format!(
        "UPDATE pgaugur.training_jobs
         SET state = {}, model_id = {}, error_message = {},
             completed_at = NOW(), progress = 1.0
         WHERE id = {}",
        quote_literal(state),
        model_sql,
        error_sql,
        job_id
    );
    Spi::run(&sql).map_err(|e| AugurPgError::Spi(format!("finalize: {e}")))
}

fn notify_completion(
    job_id: i64,
    state: &str,
    model_id: Option<i64>,
    project: &str,
    error: Option<&str>,
) {
    let payload = serde_json::json!({
        "job_id": job_id,
        "state": state,
        "model_id": model_id,
        "project": project,
        "error": error,
    });
    let _ = Spi::run(&format!(
        "SELECT pg_notify('pgaugur_training', {})",
        quote_literal(&payload.to_string())
    ));
}

fn process_one(job: ClaimedJob) {
    pgrx::log!(
        "pg_augur_training: processing job {} for project '{}'",
        job.id,
        job.project_name
    );

    // The train module already covers setup+fit+store, and maps augur errors.
    // We drive state updates around it for observability.

    let pg_task = match PgTask::parse(&job.task) {
        Ok(t) => t,
        Err(e) => {
            let _ = finalize_job(job.id, "failed", None, Some(&e.to_string()));
            notify_completion(
                job.id,
                "failed",
                None,
                &job.project_name,
                Some(&e.to_string()),
            );
            return;
        }
    };

    let (schema, table) = match data::parse_relation(&job.source_table) {
        Ok(v) => v,
        Err(e) => {
            let _ = finalize_job(job.id, "failed", None, Some(&e.to_string()));
            notify_completion(
                job.id,
                "failed",
                None,
                &job.project_name,
                Some(&e.to_string()),
            );
            return;
        }
    };
    let exclude = job.config.exclude_columns.clone().unwrap_or_default();
    let test_fraction = 1.0 - job.config.train_size;

    let _ = update_progress(job.id, "training", 0.2, Some("Fitting model"));

    let outcome = match job.mode.as_str() {
        "single" => {
            let algo = job.algorithm.as_deref().unwrap_or(match pg_task {
                PgTask::Classification => "xgboost",
                PgTask::Regression => "xgboost_reg",
                PgTask::TimeSeries => "ets",
            });
            train::train_single(
                &job.project_name,
                schema.as_deref(),
                &table,
                &job.target_column,
                algo,
                Some(pg_task),
                &exclude,
                job.config.hyperparams.as_ref(),
                test_fraction,
                42,
                true, // deploy
                job.config.conformal,
                Some(&job.config.conformal_method),
            )
        }
        "automl" => train::train_automl(
            &job.project_name,
            schema.as_deref(),
            &table,
            &job.target_column,
            Some(pg_task),
            &exclude,
            job.config.include.as_deref(),
            job.config.exclude_algs.as_deref(),
            test_fraction,
            42,
            true, // deploy
        ),
        "fdw" => process_fdw_job(&job),
        other => Err(AugurPgError::Other(format!("unknown mode '{other}'"))),
    };

    match outcome {
        Ok(o) => {
            let _ = finalize_job(job.id, "completed", Some(o.model_id), None);
            notify_completion(
                job.id,
                "completed",
                Some(o.model_id),
                &job.project_name,
                None,
            );
        }
        Err(e) => {
            let msg = e.to_string();
            let _ = finalize_job(job.id, "failed", None, Some(&msg));
            notify_completion(job.id, "failed", None, &job.project_name, Some(&msg));
        }
    }
}

/// Process an FDW-based training job.
///
/// Deserializes the column OPTIONS and chain actions from the job config
/// (serialized at queue time by `start_train()`), builds the augur pipeline,
/// and executes training.
fn process_fdw_job(job: &ClaimedJob) -> Result<train::TrainOutcome, AugurPgError> {
    use crate::fdw::{ChainAction, TrainChain};
    use crate::fdw_options::ColumnOptions;

    let col_opts_json = job
        .config
        .column_options_json
        .as_ref()
        .ok_or_else(|| AugurPgError::Other("fdw job missing column_options_json".into()))?;
    let chain_actions_json = job
        .config
        .chain_actions_json
        .as_ref()
        .ok_or_else(|| AugurPgError::Other("fdw job missing chain_actions_json".into()))?;

    let column_options: Vec<(String, ColumnOptions)> =
        serde_json::from_str(col_opts_json).map_err(AugurPgError::from)?;
    let actions: Vec<ChainAction> =
        serde_json::from_str(chain_actions_json).map_err(AugurPgError::from)?;

    let (source_schema, source_table) = data::parse_relation(&job.source_table)?;
    let feature_columns = job.config.feature_columns.clone().unwrap_or_default();

    let chain = TrainChain {
        project_name: job.project_name.clone(),
        source_schema,
        source_table: source_table.clone(),
        target_column: job.target_column.clone(),
        task: job.task.clone(),
        feature_view_relid: pgrx::pg_sys::InvalidOid,
        feature_columns: feature_columns.clone(),
        test_fraction: 1.0 - job.config.train_size,
        seed: 42,
        actions,
        output_table: None,
        missing_indicators: false,
        extract_datetime_global: false,
        winsorize_global: None,
        interactions: None,
        discretize_global: None,
        mutual_info_k: None,
        conformal: None,
        column_options,
    };

    crate::fdw::execute_train_chain(&chain)?;

    // Look up the stored model to return a TrainOutcome
    let deployed = crate::models::get_deployed_model(&job.project_name)?;
    Ok(train::TrainOutcome {
        model_id: deployed.id,
        algorithm: deployed.algorithm.clone(),
        metrics: deployed.metrics.clone(),
        deployed: true,
        training_time_seconds: 0.0,
    })
}

// ─────────────────────────────────────────────
// Worker registration + entry point
// ─────────────────────────────────────────────

pub fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_augur_training")
        .set_function("pg_augur_training_worker_main")
        .set_library("pg_augur")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_augur_training_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db = config::get_database();
    let db_c = CString::new(db.as_str()).expect("database name contains NUL");
    BackgroundWorker::connect_worker_to_spi(Some(&db_c.to_string_lossy()), None);

    pgrx::log!("pg_augur_training: worker started (db={})", db);

    while BackgroundWorker::wait_latch(Some(config::get_poll_interval())) {
        if BackgroundWorker::sighup_received() {
            // Config reloaded; nothing else to do.
        }
        if BackgroundWorker::sigterm_received() {
            break;
        }
        if !config::is_worker_enabled() {
            continue;
        }

        // Check if the extension's tables exist (created by CREATE EXTENSION).
        // Do NOT create them here — that conflicts with extension ownership.
        let tables_exist = BackgroundWorker::transaction(|| -> Result<bool, AugurPgError> {
            let exists = Spi::get_one::<bool>(
                "SELECT EXISTS(SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'pgaugur' AND table_name = 'training_jobs')",
            )
            .map_err(|e| AugurPgError::Spi(e.to_string()))?
            .unwrap_or(false);
            Ok(exists)
        });
        match tables_exist {
            Ok(true) => {} // Good, extension is installed
            Ok(false) => {
                // Extension not yet installed in this database — wait
                continue;
            }
            Err(e) => {
                pgrx::warning!("pg_augur_training: schema check failed: {e}");
                continue;
            }
        }

        // Claim and process one job per tick.
        let claimed = BackgroundWorker::transaction(claim_next_job);
        match claimed {
            Ok(Some(job)) => {
                // Training runs in its own transaction; the train_* functions open their
                // own SPI calls. process_one performs multiple small updates so we use a
                // single wrapping transaction for atomicity.
                BackgroundWorker::transaction(|| -> Result<(), AugurPgError> {
                    process_one(job);
                    Ok(())
                })
                .ok();
            }
            Ok(None) => {
                // No queued job; continue polling.
            }
            Err(e) => {
                pgrx::warning!("pg_augur_training: claim_next_job failed: {e}");
            }
        }
    }

    pgrx::log!("pg_augur_training: worker exiting");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn training_job_config_defaults() {
        let cfg = TrainingJobConfig::default();
        assert_eq!(cfg.train_size, 0.8);
        assert_eq!(cfg.conformal_method, "plus");
        assert!(!cfg.conformal);
        assert!(cfg.exclude_columns.is_none());
        assert!(cfg.hyperparams.is_none());
        assert!(cfg.metric.is_none());
    }

    #[test]
    fn training_job_config_roundtrip_json() {
        let cfg = TrainingJobConfig {
            exclude_columns: Some(vec!["id".to_string(), "ts".to_string()]),
            train_size: 0.75,
            budget_time: Some(300),
            conformal: true,
            conformal_method: "minmax".to_string(),
            metric: Some("f1".to_string()),
            hyperparams: Some(serde_json::json!({"learning_rate": 0.05})),
            setup_options: None,
            index_column: Some("date".to_string()),
            forecast_horizon: Some(12),
            fold_strategy: Some("expanding".to_string()),
            include: Some(vec!["xgboost".to_string()]),
            exclude_algs: None,
            column_options_json: None,
            chain_actions_json: None,
            feature_columns: None,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let deserialized: TrainingJobConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.train_size, 0.75);
        assert_eq!(deserialized.conformal_method, "minmax");
        assert!(deserialized.conformal);
        assert_eq!(
            deserialized.exclude_columns.as_ref().unwrap(),
            &["id".to_string(), "ts".to_string()]
        );
        assert_eq!(deserialized.budget_time, Some(300));
        assert_eq!(deserialized.metric, Some("f1".to_string()));
        assert_eq!(deserialized.index_column, Some("date".to_string()));
        assert_eq!(deserialized.forecast_horizon, Some(12));
        assert_eq!(deserialized.fold_strategy, Some("expanding".to_string()));
        assert_eq!(
            deserialized.include.as_ref().unwrap(),
            &["xgboost".to_string()]
        );
    }

    #[test]
    fn training_job_config_from_empty_json() {
        let cfg: TrainingJobConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.train_size, 0.8);
        assert_eq!(cfg.conformal_method, "plus");
        assert!(!cfg.conformal);
        assert!(cfg.exclude_columns.is_none());
    }

    #[test]
    fn training_job_config_partial_json_uses_defaults() {
        let cfg: TrainingJobConfig =
            serde_json::from_str(r#"{"train_size": 0.9, "conformal": true}"#).unwrap();
        assert_eq!(cfg.train_size, 0.9);
        assert!(cfg.conformal);
        assert_eq!(cfg.conformal_method, "plus"); // default
        assert!(cfg.metric.is_none()); // default
    }
}
