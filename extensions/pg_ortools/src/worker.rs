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

/// Database the worker connects to (`pg_ortools.database`)
static DATABASE: pgrx::GucSetting<Option<CString>> = pgrx::GucSetting::<Option<CString>>::new(None);

/// Deprecated alias of `pg_ortools.database`, still read when it is unset
static SOLVER_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Solver time limit in seconds
static SOLVER_TIME_LIMIT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(300);

/// Variable count threshold for auto strategy (MIP below, local search above)
static AUTO_THRESHOLD: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(500);

/// Default algorithm for local search
static DEFAULT_ALGORITHM: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

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
            c"pg_ortools.database",
            c"Database for the solver worker to connect to",
            c"The database where pg_ortools is installed. Defaults to 'postgres'.",
            &DATABASE,
            pgrx::GucContext::Postmaster,
            pgrx::GucFlags::default(),
        );

        pgrx::GucRegistry::define_string_guc(
            c"pg_ortools.solver_database",
            c"Deprecated: use pg_ortools.database",
            c"Read only when pg_ortools.database is unset.",
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

    pgrx::GucRegistry::define_int_guc(
        c"pg_ortools.auto_threshold",
        c"Variable count threshold for auto strategy",
        c"Problems with more variables than this use local search instead of MIP",
        &AUTO_THRESHOLD,
        1,
        1000000,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_ortools.default_algorithm",
        c"Default algorithm for local search",
        c"Algorithm used by solve_local when not specified (late_acceptance, tabu_search, simulated_annealing, hill_climbing)",
        &DEFAULT_ALGORITHM,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
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
    pg_bgworker::resolve_database(
        pg_bgworker::guc_str(&DATABASE).as_deref(),
        pg_bgworker::guc_str(&SOLVER_DATABASE).as_deref(),
    )
}

/// Get solver time limit in seconds
pub fn get_solver_time_limit() -> i32 {
    SOLVER_TIME_LIMIT.get()
}

/// CP-SAT solve budget from `pg_ortools.solver_time_limit`, used to bound the
/// synchronous `solve_cp_sync` so it can never run indefinitely proving optimality.
#[cfg(feature = "cpsat")]
pub fn cp_solve_limits() -> ortools_core::cpsat::SolveLimits {
    ortools_core::cpsat::SolveLimits::from_secs(get_solver_time_limit() as i64)
}

/// Get auto threshold (variable count above which local search is used)
pub fn get_auto_threshold() -> i32 {
    AUTO_THRESHOLD.get()
}

/// Get default algorithm name
pub fn get_default_algorithm() -> String {
    DEFAULT_ALGORITHM
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "late_acceptance".to_string())
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

/// How often a running CP solve actually hits the database to check whether its
/// job was cancelled. The engine polls its termination hook far more often than
/// this; between checks the cached answer is returned, so the DB cost of
/// cancellation polling stays negligible.
#[cfg(feature = "cpsat")]
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Run a claimed job to completion, managing its own short transactions.
///
/// The job was already claimed (state `solving`, committed) in a *separate*
/// transaction, so no row lock on `solve_jobs` is held here — a concurrent
/// `cancel_solve` can commit `state = 'cancelled'` and be observed. Each SPI step
/// (load model, solve, store, complete) is its own `BackgroundWorker::transaction`,
/// and the CPU-bound solve itself runs with **no transaction open**. This is what
/// fixes both "status stuck at queued" (each state change commits immediately) and
/// "cancel blocks on the claim row lock" (the lock is long gone).
fn run_job(job: jobs::SolveJob) -> Result<(), PgOrtoolsError> {
    let job_id = job.id;
    let problem = job.problem_name.clone();
    let strategy = job.config.strategy.clone();

    pgrx::log!(
        "pg_ortools_solver: processing job {} for problem '{}'",
        job_id,
        problem
    );

    // The job may have been cancelled while it sat queued.
    if BackgroundWorker::transaction(|| jobs::is_job_cancelled(job_id))? {
        pgrx::log!("pg_ortools_solver: job {} cancelled before solving", job_id);
        return Ok(());
    }

    let step = match strategy.as_deref() {
        Some(s) => format!("Solving (strategy: {})", s),
        None => "Solving (MIP)".to_string(),
    };
    let _ = BackgroundWorker::transaction(|| {
        jobs::update_job_progress(job_id, "solving", 0.1, Some(&step))
    });

    // Time budget: the job's own limit wins; otherwise the GUC default.
    let time_limit_secs = job
        .config
        .time_limit_seconds
        .unwrap_or_else(get_solver_time_limit);
    let time_limit = Duration::from_secs(time_limit_secs.max(1) as u64);

    match strategy.as_deref() {
        Some("cpsat") => {
            #[cfg(feature = "cpsat")]
            {
                run_cpsat_job(job_id, &problem, time_limit_secs)?;
            }
            #[cfg(not(feature = "cpsat"))]
            {
                return Err(PgOrtoolsError::InvalidParameter(
                    "strategy 'cpsat' requires building pg_ortools with --features cpsat"
                        .to_string(),
                ));
            }
        }
        Some("hill_climbing")
        | Some("tabu_search")
        | Some("simulated_annealing")
        | Some("late_acceptance") => {
            let algorithm = crate::metaheuristic::parse_algorithm(strategy.as_deref().unwrap())?;
            let solution = BackgroundWorker::transaction(|| {
                crate::metaheuristic::solve_from_db(&problem, &algorithm, time_limit)
            })?;
            finalize_job(job_id, &problem, || {
                crate::solver::store_solution(&problem, &solution)
            })?;
        }
        Some("auto") => {
            let var_count = BackgroundWorker::transaction(|| count_variables(&problem));
            if var_count < get_auto_threshold() as i64 {
                // solve_problem stores its own solution.
                BackgroundWorker::transaction(|| crate::solver::solve_problem(&problem, false))?;
                finalize_job(job_id, &problem, || Ok(()))?;
            } else {
                let algo = crate::metaheuristic::parse_algorithm(&get_default_algorithm())?;
                let solution = BackgroundWorker::transaction(|| {
                    crate::metaheuristic::solve_from_db(&problem, &algo, time_limit)
                })?;
                finalize_job(job_id, &problem, || {
                    crate::solver::store_solution(&problem, &solution)
                })?;
            }
        }
        Some("mip") | None => {
            // solve_problem stores its own solution.
            BackgroundWorker::transaction(|| crate::solver::solve_problem(&problem, false))?;
            finalize_job(job_id, &problem, || Ok(()))?;
        }
        Some(unknown) => {
            return Err(PgOrtoolsError::InvalidParameter(format!(
                "Unknown strategy: '{}'. Valid: mip, cpsat, hill_climbing, tabu_search, \
                 simulated_annealing, late_acceptance, auto",
                unknown
            )));
        }
    }

    Ok(())
}

/// Count a problem's variables (for the `auto` strategy). Must run in a transaction.
fn count_variables(problem: &str) -> i64 {
    Spi::get_one_with_args::<i64>(
        "SELECT COUNT(*)::bigint FROM pgortools.variables v \
         JOIN pgortools.problems p ON v.problem_id = p.id WHERE p.name = $1",
        &[problem.into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0)
}

/// Finalise a solved job in one short transaction: if it was cancelled mid-solve,
/// leave it `cancelled` and run no side effects; otherwise persist the solution
/// (`store`), mark it `completed`, and notify. Checking cancellation and
/// completing in the same transaction keeps the two consistent.
fn finalize_job(
    job_id: i64,
    problem: &str,
    store: impl FnOnce() -> Result<(), PgOrtoolsError>
        + std::panic::UnwindSafe
        + std::panic::RefUnwindSafe,
) -> Result<(), PgOrtoolsError> {
    BackgroundWorker::transaction(|| {
        if jobs::is_job_cancelled(job_id)? {
            pgrx::log!(
                "pg_ortools_solver: job {} cancelled during solve; leaving state=cancelled",
                job_id
            );
            return Ok(());
        }
        store()?;
        jobs::complete_job(job_id)?;
        jobs::notify_completion(job_id, "completed", problem, None)?;
        pgrx::log!("pg_ortools_solver: job {} completed", job_id);
        Ok(())
    })
}

/// Load → solve → store a CP-SAT job. The solve is bounded by `time_limit_secs`
/// and cancellable: a throttled poll of the job's `cancelled` state feeds the
/// engine's cooperative termination hook, so `cancel_solve` stops it mid-search
/// instead of waiting for the whole (possibly unbounded) proof to finish.
#[cfg(feature = "cpsat")]
fn run_cpsat_job(job_id: i64, problem: &str, time_limit_secs: i32) -> Result<(), PgOrtoolsError> {
    use ortools_core::cpsat::SolveLimits;
    use std::time::Instant;

    // Load inside a transaction; the owned model outlives it, so the solve below
    // holds no transaction (and thus no locks) and the cancel poll can open its own.
    let (model, names) = BackgroundWorker::transaction(|| crate::cpsat::load_cp_model(problem))?;

    let mut last_poll = Instant::now();
    let mut cancelled = || {
        if last_poll.elapsed() < CANCEL_POLL_INTERVAL {
            return false;
        }
        last_poll = Instant::now();
        match BackgroundWorker::transaction(|| jobs::is_job_cancelled(job_id)) {
            Ok(c) => c,
            // A transient read failure must not abort a long solve — the time
            // budget still bounds it. Keep going and retry on the next poll.
            Err(e) => {
                pgrx::log!(
                    "pg_ortools_solver: cancel poll failed for job {}: {}",
                    job_id,
                    e
                );
                false
            }
        }
    };

    let limits = SolveLimits::from_secs(time_limit_secs as i64);
    let outcome = model.solve_with(&limits, &mut cancelled);
    let result = crate::cpsat::outcome_to_json(&outcome, &model, &names);

    finalize_job(job_id, problem, || {
        crate::cpsat::store_cp_solution(problem, &result)
    })
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

    // Do nothing until the extension's schema exists in this database.
    if !pg_bgworker::wait_for_extension("pg_ortools", &database) {
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

        // Claim in its own committed transaction. This makes state='solving'
        // visible at once and releases the claim's row lock, so the long solve
        // that follows holds nothing a concurrent cancel_solve could block on.
        let claimed = BackgroundWorker::transaction(jobs::claim_next_job);

        match claimed {
            Ok(Some(job)) => {
                let job_id = job.id;
                let problem = job.problem_name.clone();

                // Solve OUTSIDE the claim transaction; run_job manages its own.
                if let Err(e) = run_job(job) {
                    let msg = e.to_string();
                    pgrx::log!("pg_ortools_solver: job {} failed: {}", job_id, msg);
                    let _ = BackgroundWorker::transaction(|| jobs::fail_job(job_id, &msg));
                    let _ = BackgroundWorker::transaction(|| {
                        jobs::notify_completion(job_id, "failed", &problem, Some(&msg))
                    });
                }
            }
            Ok(None) => {
                // No jobs to process
            }
            Err(e) => {
                pgrx::log!("pg_ortools_solver: error claiming job: {}", e);
            }
        }
    }

    pgrx::log!("pg_ortools_solver: worker stopped");
}
