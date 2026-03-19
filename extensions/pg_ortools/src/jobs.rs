//! Job queue management for async optimization solving
//!
//! Follows the pg_ml async_training pattern: jobs are queued in a PostgreSQL table,
//! claimed atomically by background workers using FOR UPDATE SKIP LOCKED,
//! and notifications are sent on completion via pg_notify.

use crate::error::PgOrtoolsError;
use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

// =============================================================================
// Data Structures
// =============================================================================

/// Configuration for a solve job (serialized to JSONB)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct SolveJobConfig {
    pub time_limit_seconds: Option<i32>,
}

/// A claimed solve job ready for processing
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SolveJob {
    pub id: i64,
    pub problem_name: String,
    pub config: SolveJobConfig,
}

/// Status information for a solve job
#[derive(Debug, Clone)]
pub struct SolveJobStatus {
    pub job_id: i64,
    pub problem_name: String,
    pub state: String,
    pub progress: f64,
    pub current_step: Option<String>,
    pub error_message: Option<String>,
    pub started_at: Option<TimestampWithTimeZone>,
    pub completed_at: Option<TimestampWithTimeZone>,
    pub elapsed_seconds: Option<f64>,
}

// =============================================================================
// Helper
// =============================================================================

fn quote_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

// =============================================================================
// Job Queue Functions
// =============================================================================

/// Queue a new solve job
pub fn queue_solve_job(problem_name: &str, config: &SolveJobConfig) -> Result<i64, PgOrtoolsError> {
    let config_json = serde_json::to_string(config)
        .map_err(|e| PgOrtoolsError::JsonError(format!("Failed to serialize config: {}", e)))?;

    let sql = format!(
        "INSERT INTO pgortools.solve_jobs (problem_name, config)
         VALUES ({}, '{}'::jsonb)
         RETURNING id",
        quote_literal(problem_name),
        config_json.replace('\'', "''")
    );

    Spi::get_one::<i64>(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to queue job: {}", e)))?
        .ok_or_else(|| PgOrtoolsError::SpiError("No job ID returned".to_string()))
}

/// Claim the next queued job (atomic operation using FOR UPDATE SKIP LOCKED)
pub fn claim_next_job() -> Result<Option<SolveJob>, PgOrtoolsError> {
    let pid = std::process::id();

    let claim_sql = format!(
        "UPDATE pgortools.solve_jobs
         SET state = 'solving', started_at = NOW(), worker_pid = {}
         WHERE id = (
             SELECT id FROM pgortools.solve_jobs
             WHERE state = 'queued'
             ORDER BY created_at ASC
             LIMIT 1
             FOR UPDATE SKIP LOCKED
         )
         RETURNING id",
        pid
    );

    let job_id: Option<i64> = match Spi::get_one(&claim_sql) {
        Ok(id) => id,
        Err(pgrx::spi::SpiError::InvalidPosition) => return Ok(None),
        Err(e) => {
            return Err(PgOrtoolsError::SpiError(format!(
                "Failed to claim job: {}",
                e
            )));
        }
    };

    let job_id = match job_id {
        Some(id) => id,
        None => return Ok(None),
    };

    let select_sql = format!(
        "SELECT problem_name, config::text
         FROM pgortools.solve_jobs WHERE id = {}",
        job_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&select_sql, None, &[])?;

        if let Some(row) = result.next() {
            let problem_name: String = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let config_json: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;

            let config: SolveJobConfig = serde_json::from_str(&config_json).unwrap_or_default();

            return Ok(Some(SolveJob {
                id: job_id,
                problem_name,
                config,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgOrtoolsError::SpiError(format!("Failed to fetch job: {}", e))
    })
}

/// Update job progress
pub fn update_job_progress(
    job_id: i64,
    state: &str,
    progress: f64,
    current_step: Option<&str>,
) -> Result<(), PgOrtoolsError> {
    let current_step_sql = match current_step {
        Some(s) => quote_literal(s),
        None => "NULL".to_string(),
    };

    let sql = format!(
        "UPDATE pgortools.solve_jobs
         SET state = {}, progress = {}, current_step = {}
         WHERE id = {}",
        quote_literal(state),
        progress,
        current_step_sql,
        job_id
    );

    Spi::run(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to update progress: {}", e)))
}

/// Complete a job successfully
pub fn complete_job(job_id: i64) -> Result<(), PgOrtoolsError> {
    let sql = format!(
        "UPDATE pgortools.solve_jobs
         SET state = 'completed', progress = 1.0, completed_at = NOW()
         WHERE id = {}",
        job_id
    );

    Spi::run(&sql).map_err(|e| PgOrtoolsError::SpiError(format!("Failed to complete job: {}", e)))
}

/// Mark a job as failed
pub fn fail_job(job_id: i64, error: &str) -> Result<(), PgOrtoolsError> {
    let sql = format!(
        "UPDATE pgortools.solve_jobs
         SET state = 'failed', error_message = {}, completed_at = NOW()
         WHERE id = {}",
        quote_literal(error),
        job_id
    );

    Spi::run(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to mark job failed: {}", e)))
}

/// Cancel a job
pub fn cancel_job(job_id: i64) -> Result<bool, PgOrtoolsError> {
    let sql = format!(
        "UPDATE pgortools.solve_jobs
         SET state = 'cancelled', completed_at = NOW()
         WHERE id = {} AND state IN ('queued', 'solving')
         RETURNING id",
        job_id
    );

    let cancelled: Option<i64> = Spi::get_one(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to cancel job: {}", e)))?;

    Ok(cancelled.is_some())
}

/// Check if a job was cancelled
pub fn is_job_cancelled(job_id: i64) -> Result<bool, PgOrtoolsError> {
    let sql = format!(
        "SELECT state = 'cancelled' FROM pgortools.solve_jobs WHERE id = {}",
        job_id
    );

    Spi::get_one::<bool>(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to check cancellation: {}", e)))?
        .ok_or(PgOrtoolsError::JobNotFound(job_id))
}

/// Get job status
pub fn get_job_status(job_id: i64) -> Result<Option<SolveJobStatus>, PgOrtoolsError> {
    let sql = format!(
        "SELECT id, problem_name, state, progress::double precision, current_step,
                error_message, started_at, completed_at,
                EXTRACT(EPOCH FROM (COALESCE(completed_at, NOW()) - started_at))::double precision as elapsed_seconds
         FROM pgortools.solve_jobs WHERE id = {}",
        job_id
    );

    Spi::connect(|client| {
        let mut result = client.select(&sql, None, &[])?;

        if let Some(row) = result.next() {
            let job_id: i64 = row.get(1)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let problem_name: String = row.get(2)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let state: String = row.get(3)?.ok_or(pgrx::spi::SpiError::InvalidPosition)?;
            let progress: f64 = row.get(4)?.unwrap_or(0.0);
            let current_step: Option<String> = row.get(5)?;
            let error_message: Option<String> = row.get(6)?;
            let started_at: Option<TimestampWithTimeZone> = row.get(7)?;
            let completed_at: Option<TimestampWithTimeZone> = row.get(8)?;
            let elapsed_seconds: Option<f64> = row.get(9)?;

            return Ok(Some(SolveJobStatus {
                job_id,
                problem_name,
                state,
                progress,
                current_step,
                error_message,
                started_at,
                completed_at,
                elapsed_seconds,
            }));
        }
        Ok(None)
    })
    .map_err(|e: pgrx::spi::SpiError| {
        PgOrtoolsError::SpiError(format!("Failed to get job status: {}", e))
    })
}

/// Send NOTIFY on job completion
pub fn notify_completion(
    job_id: i64,
    state: &str,
    problem: &str,
    error: Option<&str>,
) -> Result<(), PgOrtoolsError> {
    let payload = serde_json::json!({
        "job_id": job_id,
        "state": state,
        "problem": problem,
        "error": error,
    });

    let sql = format!(
        "SELECT pg_notify('pgortools_solve', {})",
        quote_literal(&payload.to_string())
    );

    Spi::run(&sql)
        .map_err(|e| PgOrtoolsError::SpiError(format!("Failed to send notification: {}", e)))
}

// =============================================================================
// Unit Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_solve_job_config_default() {
        let config = SolveJobConfig::default();
        assert!(config.time_limit_seconds.is_none());
    }

    #[test]
    fn test_solve_job_config_serialization() {
        let config = SolveJobConfig {
            time_limit_seconds: Some(60),
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: SolveJobConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.time_limit_seconds, Some(60));
    }

    #[test]
    fn test_quote_literal() {
        assert_eq!(quote_literal("hello"), "'hello'");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_literal(""), "''");
    }
}
