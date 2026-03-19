//! Background worker for async optimization solving
//!
//! Follows the pg_ml async_training worker pattern: polls for queued jobs,
//! claims them atomically, runs the solver, and sends notifications.

use crate::error::PgOrtoolsError;
use crate::jobs;
use pgrx::bgworkers::*;
use pgrx::prelude::*;
use std::ffi::CString;
use std::time::Duration;

// =============================================================================
// GUC Settings
// =============================================================================

/// Enable solver background worker (requires PostgreSQL restart)
static SOLVER_WORKER_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

/// Job polling interval in milliseconds
static SOLVER_POLL_INTERVAL: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1000);

/// Database name for worker to connect to
static SOLVER_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Solver time limit in seconds
static SOLVER_TIME_LIMIT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(300);

const DEFAULT_DATABASE: &str = "postgres";

/// Register solver worker GUC settings
pub fn register_gucs() {
    let in_postmaster = unsafe { pgrx::pg_sys::process_shared_preload_libraries_in_progress };

    if in_postmaster {
        pgrx::GucRegistry::define_bool_guc(
            c"pg_ortools.solver_worker_enabled",
            c"Enable solver background worker",
            c"When true, pg_ortools starts a background worker to process solve jobs",
            &SOLVER_WORKER_ENABLED,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_int_guc(
            c"pg_ortools.solver_poll_interval",
            c"Job polling interval in milliseconds",
            c"How often the solver worker checks for new jobs",
            &SOLVER_POLL_INTERVAL,
            100,
            60000,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_string_guc(
            c"pg_ortools.solver_database",
            c"Database for solver worker to connect to",
            c"Database name the solver worker uses for SPI connections",
            &SOLVER_DATABASE,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_int_guc(
            c"pg_ortools.solver_time_limit",
            c"Solver time limit in seconds",
            c"Maximum time the solver will run for a single problem",
            &SOLVER_TIME_LIMIT,
            1,
            86400,
            pgrx::GucContext::Suset,
            pgrx::GucFlags::default(),
        );
    }
}

/// Check if solver worker is enabled
pub fn is_worker_enabled() -> bool {
    SOLVER_WORKER_ENABLED.get()
}

/// Get polling interval as Duration
pub fn get_poll_interval() -> Duration {
    Duration::from_millis(SOLVER_POLL_INTERVAL.get() as u64)
}

/// Get database name for worker connection
pub fn get_database() -> String {
    SOLVER_DATABASE
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| DEFAULT_DATABASE.to_string())
}

/// Get solver time limit in seconds
#[allow(dead_code)]
pub fn get_solver_time_limit() -> i32 {
    SOLVER_TIME_LIMIT.get()
}

// =============================================================================
// Background Worker Registration
// =============================================================================

/// Register solver background worker (called from _PG_init if enabled)
pub fn register_background_worker() {
    BackgroundWorkerBuilder::new("pg_ortools_solver")
        .set_function("pg_ortools_solver_worker_main")
        .set_library("pg_ortools")
        .enable_shmem_access(None)
        .enable_spi_access()
        .set_start_time(BgWorkerStartTime::RecoveryFinished)
        .set_restart_time(Some(Duration::from_secs(10)))
        .load();
}

// =============================================================================
// Job Processing
// =============================================================================

/// Process a single solve job
fn process_solve_job(job: jobs::SolveJob) -> Result<(), PgOrtoolsError> {
    pgrx::log!(
        "pg_ortools_solver: processing job {} for problem '{}'",
        job.id,
        job.problem_name
    );

    jobs::update_job_progress(job.id, "solving", 0.0, Some("Loading problem"))?;

    if jobs::is_job_cancelled(job.id)? {
        pgrx::log!("pg_ortools_solver: job {} cancelled during setup", job.id);
        return Ok(());
    }

    jobs::update_job_progress(job.id, "solving", 0.1, Some("Building model"))?;

    // Run the solver
    let result = crate::solver::solve_problem(&job.problem_name, false);

    // Check cancellation before completing
    if jobs::is_job_cancelled(job.id)? {
        pgrx::log!("pg_ortools_solver: job {} cancelled after solving", job.id);
        return Ok(());
    }

    match result {
        Ok(_solution) => {
            jobs::update_job_progress(job.id, "solving", 0.9, Some("Storing solution"))?;
            jobs::complete_job(job.id)?;
            jobs::notify_completion(job.id, "completed", &job.problem_name, None)?;

            pgrx::log!("pg_ortools_solver: job {} completed", job.id);
        }
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
}

// =============================================================================
// Background Worker Main
// =============================================================================

/// Background worker main function - processes solve jobs
#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn pg_ortools_solver_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let database = get_database();
    BackgroundWorker::connect_worker_to_spi(Some(&database), None);

    pgrx::log!(
        "pg_ortools_solver: worker started, pid={}, database={}",
        std::process::id(),
        database
    );

    if !is_worker_enabled() {
        pgrx::log!("pg_ortools_solver: disabled via pg_ortools.solver_worker_enabled=false");
        return;
    }

    let poll_interval = get_poll_interval();

    while BackgroundWorker::wait_latch(Some(poll_interval)) {
        if BackgroundWorker::sigterm_received() {
            pgrx::log!("pg_ortools_solver: received SIGTERM, shutting down");
            break;
        }

        if BackgroundWorker::sighup_received() {
            pgrx::log!("pg_ortools_solver: received SIGHUP");
        }

        let result: Result<(), PgOrtoolsError> = BackgroundWorker::transaction(|| {
            match jobs::claim_next_job() {
                Ok(Some(job)) => {
                    let job_id = job.id;
                    let problem = job.problem_name.clone();

                    if let Err(e) = process_solve_job(job) {
                        pgrx::log!("pg_ortools_solver: job {} failed: {}", job_id, e);

                        if let Err(fail_err) = jobs::fail_job(job_id, &e.to_string()) {
                            pgrx::log!(
                                "pg_ortools_solver: failed to mark job {} as failed: {}",
                                job_id,
                                fail_err
                            );
                        }

                        if let Err(notify_err) = jobs::notify_completion(
                            job_id,
                            "failed",
                            &problem,
                            Some(&e.to_string()),
                        ) {
                            pgrx::log!(
                                "pg_ortools_solver: failed to send failure notification: {}",
                                notify_err
                            );
                        }
                    }
                }
                Ok(None) => {
                    // No jobs to process
                }
                Err(e) => {
                    pgrx::log!("pg_ortools_solver: error claiming job: {}", e);
                }
            }
            Ok(())
        });

        if let Err(e) = result {
            pgrx::log!("pg_ortools_solver: transaction error: {}", e);
        }
    }

    pgrx::log!("pg_ortools_solver: worker stopped");
}
