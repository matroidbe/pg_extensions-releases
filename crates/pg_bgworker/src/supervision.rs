//! Background worker supervision (design/bgworker-supervision)
//!
//! [`Slot`] is the pure state machine, unit-tested here; the shared-memory
//! [`Supervisor`] that workers and SQL use is built on it.

use std::time::Duration;

/// Workers per extension that can be supervised
pub const MAX_WORKERS: usize = 64;
/// A run this long marks the worker healthy and resets its failure count
pub const HEALTHY_AFTER_SECS: i64 = 60;
const BACKOFF_BASE_SECS: u64 = 5;
const BACKOFF_CAP_SECS: u64 = 60;
pub const NAME_LEN: usize = 64;
pub const REASON_LEN: usize = 200;
/// Default for `<ext>.max_worker_failures`
pub const DEFAULT_MAX_FAILURES: i32 = 10;

const UNREPORTED: &str = "exited without reporting a reason (see the server log)";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum State {
    /// Never started
    Idle,
    Starting,
    BackingOff,
    Running,
    /// Gave up after too many consecutive failures; waits for a reset
    Failed,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Idle => "idle",
            State::Starting => "starting",
            State::BackingOff => "backing_off",
            State::Running => "running",
            State::Failed => "failed",
        }
    }
}

/// What a starting worker should do
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Wait `backoff` (zero after a healthy or clean run), then run
    Run { backoff: Duration },
    /// Do no work until reset
    Failed,
}

/// One worker's supervision state. Plain data (no pointers), so it can live
/// in shared memory.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct Slot {
    pub state: State,
    /// Consecutive failures
    pub failures: u32,
    /// Starts after the first
    pub restarts: u32,
    pub pid: i32,
    /// When the current state was entered (Unix seconds)
    pub since: i64,
    running_since: i64,
    /// The last run ended deliberately (SIGTERM, disabled)
    clean_exit: bool,
    /// The last run reported its failure reason
    reported: bool,
    /// The current run has been healthy
    healthy: bool,
    pub reset_requested: bool,
    name: [u8; NAME_LEN],
    name_len: u8,
    reason: [u8; REASON_LEN],
    reason_len: u8,
}

/// Copy `text` into `buf`, truncated on a char boundary; returns the length
fn store(buf: &mut [u8], text: &str) -> u8 {
    let mut end = text.len().min(buf.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    buf[..end].copy_from_slice(&text.as_bytes()[..end]);
    end as u8
}

fn backoff_for(failures: u32) -> Duration {
    if failures == 0 {
        return Duration::ZERO;
    }
    let secs = BACKOFF_BASE_SECS.saturating_mul(1 << (failures - 1).min(16));
    Duration::from_secs(secs.min(BACKOFF_CAP_SECS))
}

impl Slot {
    pub const EMPTY: Slot = Slot {
        state: State::Idle,
        failures: 0,
        restarts: 0,
        pid: 0,
        since: 0,
        running_since: 0,
        clean_exit: false,
        reported: false,
        healthy: false,
        reset_requested: false,
        name: [0; NAME_LEN],
        name_len: 0,
        reason: [0; REASON_LEN],
        reason_len: 0,
    };

    pub fn name(&self) -> &str {
        std::str::from_utf8(&self.name[..self.name_len as usize]).unwrap_or("")
    }

    pub fn last_failure(&self) -> Option<&str> {
        match self.reason_len {
            0 => None,
            n => std::str::from_utf8(&self.reason[..n as usize]).ok(),
        }
    }

    fn set_reason(&mut self, reason: &str) {
        self.reason_len = store(&mut self.reason, reason);
    }

    /// A worker (re)starts. Counts the previous run, and decides whether to
    /// run (after a backoff) or stay failed. `max_failures` 0 = unlimited.
    pub fn begin(&mut self, name: &str, pid: i32, now: i64, max_failures: i32) -> Decision {
        match self.state {
            State::Failed if !self.reset_requested => return Decision::Failed,
            State::Failed => {
                self.reset_requested = false;
                self.failures = 0;
                self.reason_len = 0;
            }
            State::Idle => {}
            _ => {
                self.restarts += 1;
                if !self.clean_exit && !self.healthy {
                    self.failures += 1;
                    if !self.reported {
                        self.set_reason(UNREPORTED);
                    }
                }
            }
        }
        self.clean_exit = false;
        self.reported = false;
        self.healthy = false;
        self.pid = pid;
        self.since = now;
        self.name_len = store(&mut self.name, name);

        if max_failures > 0 && self.failures >= max_failures as u32 {
            self.state = State::Failed;
            return Decision::Failed;
        }
        let backoff = backoff_for(self.failures);
        self.state = if backoff.is_zero() {
            State::Starting
        } else {
            State::BackingOff
        };
        Decision::Run { backoff }
    }

    /// The worker starts serving (after any backoff)
    pub fn running(&mut self, now: i64) {
        self.state = State::Running;
        self.since = now;
        self.running_since = now;
    }

    /// Called from the worker's main loop. Returns `true` when the run has
    /// just become healthy, which resets the failure count.
    pub fn tick(&mut self, now: i64) -> bool {
        if self.state != State::Running
            || self.healthy
            || now - self.running_since < HEALTHY_AFTER_SECS
        {
            return false;
        }
        self.healthy = true;
        self.failures = 0;
        self.reason_len = 0;
        true
    }

    /// The run ends deliberately: not a failure
    pub fn exit_clean(&mut self) {
        self.clean_exit = true;
    }

    /// The run ends because of `reason`: counted at the next start
    pub fn exit_failed(&mut self, reason: &str) {
        self.reported = true;
        self.set_reason(reason);
    }

    /// Ask a failed worker to start again. Returns whether it was failed.
    pub fn request_reset(&mut self) -> bool {
        if self.state == State::Failed {
            self.reset_requested = true;
        }
        self.reset_requested
    }
}

/// One row of `<schema>.worker_status()`
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerStatus {
    pub worker: i32,
    pub name: String,
    pub state: &'static str,
    pub failures: i32,
    pub restarts: i32,
    pub pid: i32,
    pub since_unix: i64,
    pub last_failure: Option<String>,
}

/// Status rows for every slot that has started
pub fn status_rows(slots: &[Slot]) -> Vec<WorkerStatus> {
    slots
        .iter()
        .enumerate()
        .filter(|(_, s)| s.state != State::Idle)
        .map(|(i, s)| WorkerStatus {
            worker: i as i32,
            name: s.name().to_string(),
            state: s.state.as_str(),
            failures: s.failures as i32,
            restarts: s.restarts as i32,
            pid: s.pid,
            since_unix: s.since,
            last_failure: s.last_failure().map(str::to_string),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Shared memory and the worker/SQL API
// ---------------------------------------------------------------------------

use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::OnceLock;

use pgrx::bgworkers::BackgroundWorker;
use pgrx::lwlock::PgLwLock;
use pgrx::prelude::*;
use pgrx::shmem::{PGRXSharedMemory, PgSharedMemoryInitialization};
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

/// Every slot of one extension, in shared memory
#[derive(Clone, Copy)]
#[repr(C)]
pub struct Slots(pub [Slot; MAX_WORKERS]);

impl Default for Slots {
    fn default() -> Self {
        Slots([Slot::EMPTY; MAX_WORKERS])
    }
}

// Plain data: no pointers, valid in any process
unsafe impl PGRXSharedMemory for Slots {}

/// One extension's supervisor. Declare it as a static and call
/// [`Supervisor::init`] from `_PG_init`.
pub struct Supervisor {
    slots: PgLwLock<Slots>,
    max_failures: GucSetting<i32>,
    guc_name: &'static CStr,
}

// The supervisor of this extension's .so (each extension links its own copy
// of this crate), for the shared-memory hooks
static ACTIVE: OnceLock<&'static Supervisor> = OnceLock::new();
// Set when shared memory is attached (in the postmaster; inherited by fork)
static READY: AtomicBool = AtomicBool::new(false);
// Last tick (Unix seconds) of this worker process, to lock at most once a second
static LAST_TICK: AtomicI64 = AtomicI64::new(0);

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

impl Supervisor {
    /// # Safety
    /// `lock_name` must be unique among all shared memory of the server (use
    /// `<ext>_supervision`).
    pub const unsafe fn new(lock_name: &'static CStr, guc_name: &'static CStr) -> Self {
        Supervisor {
            slots: unsafe { PgLwLock::new(lock_name) },
            max_failures: GucSetting::<i32>::new(DEFAULT_MAX_FAILURES),
            guc_name,
        }
    }

    /// Register `<ext>.max_worker_failures` and, when loading through
    /// `shared_preload_libraries`, the shared memory. Call from `_PG_init`.
    pub fn init(&'static self) {
        GucRegistry::define_int_guc(
            self.guc_name,
            c"Consecutive background worker failures before giving up",
            c"After this many failures in a row a worker stops retrying until <schema>.reset_workers() or a server restart. 0 means never give up.",
            &self.max_failures,
            0,
            10_000,
            GucContext::Sighup,
            GucFlags::default(),
        );
        if !unsafe { pg_sys::process_shared_preload_libraries_in_progress } {
            return;
        }
        if ACTIVE.set(self).is_err() {
            return;
        }
        install_shmem_hooks();
    }

    /// Run `f` on the slots under an exclusive lock; `None` without shared
    /// memory (library not preloaded)
    fn with<R>(&self, f: impl FnOnce(&mut Slots) -> R) -> Option<R> {
        if !READY.load(Ordering::Acquire) {
            return None;
        }
        Some(f(&mut self.slots.exclusive()))
    }

    /// Called first thing when worker `slot` starts. Waits out the backoff
    /// after failures, or, once the worker has failed too often, waits for
    /// `reset_workers()`. Returns `false` when the worker should exit
    /// instead (SIGTERM or postmaster death): call [`Supervisor::exit_clean`].
    pub fn start(&self, slot: usize, name: &str) -> bool {
        let slot = slot.min(MAX_WORKERS - 1);
        let mut warned = false;
        loop {
            let decision = self
                .with(|s| {
                    s.0[slot].begin(
                        name,
                        std::process::id() as i32,
                        now_unix(),
                        self.max_failures.get(),
                    )
                })
                .unwrap_or(Decision::Run {
                    backoff: Duration::ZERO,
                });
            match decision {
                Decision::Run { backoff } => {
                    if !backoff.is_zero() {
                        let (failures, reason) = self
                            .with(|s| {
                                (
                                    s.0[slot].failures,
                                    s.0[slot].last_failure().unwrap_or("").to_string(),
                                )
                            })
                            .unwrap_or_default();
                        pgrx::log!(
                            "{name}: failed {failures} time(s) in a row (last: {reason}); retrying in {}s",
                            backoff.as_secs()
                        );
                        if !wait(backoff) {
                            return false;
                        }
                    }
                    self.with(|s| s.0[slot].running(now_unix()));
                    return true;
                }
                Decision::Failed => {
                    if !warned {
                        let (failures, reason) = self
                            .with(|s| {
                                (
                                    s.0[slot].failures,
                                    s.0[slot].last_failure().unwrap_or("").to_string(),
                                )
                            })
                            .unwrap_or_default();
                        pgrx::warning!(
                            "{name}: failed {failures} times in a row and will not be restarted \
                             (last failure: {reason}). Fix the cause, then run reset_workers() \
                             in the extension's schema, or restart the server."
                        );
                        warned = true;
                    }
                    // Idle (not exit: exit 0 would unregister the worker)
                    loop {
                        if !wait(Duration::from_secs(1)) {
                            return false;
                        }
                        if self.with(|s| s.0[slot].reset_requested).unwrap_or(true) {
                            pgrx::log!("{name}: reset requested, starting");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Call from the worker's main loop: marks the run healthy after
    /// [`HEALTHY_AFTER_SECS`] (cheap: locks at most once a second)
    pub fn tick(&self, slot: usize) {
        let now = now_unix();
        if LAST_TICK.swap(now, Ordering::Relaxed) == now {
            return;
        }
        self.with(|s| s.0[slot.min(MAX_WORKERS - 1)].tick(now));
    }

    /// End the worker deliberately (SIGTERM, disabled): not a failure. The
    /// postmaster restarts it.
    pub fn exit_clean(&self, slot: usize) -> ! {
        self.with(|s| s.0[slot.min(MAX_WORKERS - 1)].exit_clean());
        crate::exit_for_restart()
    }

    /// End the worker because of `reason`: counted as a failure
    pub fn exit_failed(&self, slot: usize, reason: &str) -> ! {
        pgrx::log!("worker failed: {reason}");
        self.with(|s| s.0[slot.min(MAX_WORKERS - 1)].exit_failed(reason));
        crate::exit_for_restart()
    }

    /// Rows for `<schema>.worker_status()`
    pub fn status(&self) -> Vec<WorkerStatus> {
        self.with(|s| status_rows(&s.0)).unwrap_or_default()
    }

    /// `<schema>.reset_workers()`: restart every failed worker; returns how
    /// many were failed
    pub fn reset(&self) -> i32 {
        self.with(|s| {
            s.0.iter_mut()
                .filter(|slot| slot.state == State::Failed)
                .map(|slot| slot.request_reset())
                .count() as i32
        })
        .unwrap_or(0)
    }
}

/// Wait up to `total` on the latch, applying configuration reloads. `false`
/// on SIGTERM or postmaster death.
fn wait(total: Duration) -> bool {
    let deadline = std::time::Instant::now() + total;
    loop {
        let left = deadline.saturating_duration_since(std::time::Instant::now());
        if left.is_zero() {
            return true;
        }
        // pgrx's wait_latch never runs CHECK_FOR_INTERRUPTS: without this the
        // ProcSignalBarrier of DROP DATABASE is never acknowledged
        pgrx::check_for_interrupts!();
        if !BackgroundWorker::wait_latch(Some(left.min(Duration::from_secs(1))))
            || BackgroundWorker::sigterm_received()
        {
            return false;
        }
        crate::reload_config_if_signalled();
    }
}

/// `since` as a timestamptz
pub fn to_timestamptz(unix: i64) -> Option<pgrx::datum::TimestampWithTimeZone> {
    let tz = unsafe { pg_sys::time_t_to_timestamptz(unix as pg_sys::pg_time_t) };
    pgrx::datum::TimestampWithTimeZone::try_from(tz).ok()
}

/// Raise an error unless the caller is a superuser
pub fn require_superuser(function: &str) {
    if !unsafe { pg_sys::superuser() } {
        pgrx::error!("{function}() requires superuser");
    }
}

// The same hooks pgrx's pg_shmem_init! installs, for a supervisor reached
// through ACTIVE rather than a named static
#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;

fn install_shmem_hooks() {
    unsafe {
        #[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
        {
            PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
            pg_sys::shmem_request_hook = Some(on_shmem_request);
        }
        #[cfg(feature = "pg14")]
        if let Some(sup) = ACTIVE.get() {
            PgSharedMemoryInitialization::on_shmem_request(&sup.slots);
        }
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(on_shmem_startup);
    }
}

#[cfg(any(feature = "pg15", feature = "pg16", feature = "pg17", feature = "pg18"))]
#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        if let Some(sup) = ACTIVE.get() {
            PgSharedMemoryInitialization::on_shmem_request(&sup.slots);
        }
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn on_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }
        if let Some(sup) = ACTIVE.get() {
            PgSharedMemoryInitialization::on_shmem_startup(&sup.slots, Slots::default());
            READY.store(true, Ordering::Release);
        }
    }
}

/// Defines `worker_status()` and `reset_workers()` in the calling extension,
/// for its supervisor (an absolute path such as `crate::SUPERVISOR`)
#[macro_export]
macro_rules! supervision_sql {
    ($supervisor:path) => {
        /// Supervision state of this extension's background workers
        /// (design/bgworker-supervision)
        #[::pgrx::pg_extern]
        fn worker_status() -> ::pgrx::iter::TableIterator<
            'static,
            (
                ::pgrx::name!(worker, i32),
                ::pgrx::name!(name, String),
                ::pgrx::name!(state, String),
                ::pgrx::name!(failures, i32),
                ::pgrx::name!(restarts, i32),
                ::pgrx::name!(pid, i32),
                ::pgrx::name!(since, Option<::pgrx::datum::TimestampWithTimeZone>),
                ::pgrx::name!(last_failure, Option<String>),
            ),
        > {
            let rows = $supervisor.status().into_iter().map(|r| {
                (
                    r.worker,
                    r.name,
                    r.state.to_string(),
                    r.failures,
                    r.restarts,
                    r.pid,
                    $crate::supervision::to_timestamptz(r.since_unix),
                    r.last_failure,
                )
            });
            ::pgrx::iter::TableIterator::new(rows.collect::<Vec<_>>())
        }

        /// Restart background workers that gave up after too many failures;
        /// returns how many there were. Superuser only.
        #[::pgrx::pg_extern]
        fn reset_workers() -> i32 {
            $crate::supervision::require_superuser("reset_workers");
            $supervisor.reset()
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: i32 = 10;

    fn started(slot: &mut Slot, now: i64) -> Decision {
        slot.begin("w", 100, now, MAX)
    }

    fn backoff_secs(d: Decision) -> u64 {
        match d {
            Decision::Run { backoff } => backoff.as_secs(),
            Decision::Failed => panic!("expected to run"),
        }
    }

    /// Start, then die without reporting (as an ERROR does)
    fn crash(slot: &mut Slot, now: i64) -> Decision {
        let d = started(slot, now);
        if d != Decision::Failed {
            slot.running(now);
        }
        d
    }

    #[test]
    fn first_start_runs_at_once() {
        let mut slot = Slot::EMPTY;
        assert_eq!(backoff_secs(started(&mut slot, 0)), 0);
        assert_eq!(slot.failures, 0);
        assert_eq!(slot.restarts, 0);
        assert_eq!(slot.state, State::Starting);
        assert_eq!(slot.name(), "w");
        slot.running(1);
        assert_eq!(slot.state, State::Running);
    }

    #[test]
    fn unreported_death_counts_with_a_default_reason() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        assert_eq!(backoff_secs(started(&mut slot, 10)), 5);
        assert_eq!(slot.failures, 1);
        assert_eq!(slot.restarts, 1);
        assert_eq!(slot.state, State::BackingOff);
        assert!(slot.last_failure().unwrap().contains("server log"));
    }

    #[test]
    fn clean_exit_is_not_a_failure() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        slot.exit_clean();
        assert_eq!(backoff_secs(started(&mut slot, 10)), 0);
        assert_eq!(slot.failures, 0);
        assert_eq!(slot.restarts, 1);
    }

    #[test]
    fn reported_failure_keeps_its_reason() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        slot.exit_failed("could not bind 0.0.0.0:9092: Address in use");
        started(&mut slot, 10);
        assert_eq!(slot.failures, 1);
        assert_eq!(
            slot.last_failure(),
            Some("could not bind 0.0.0.0:9092: Address in use")
        );
    }

    #[test]
    fn backoff_doubles_up_to_a_cap() {
        let mut slot = Slot::EMPTY;
        let mut waits = Vec::new();
        crash(&mut slot, 0);
        for i in 1..9 {
            waits.push(backoff_secs(crash(&mut slot, i * 100)));
        }
        assert_eq!(waits, [5, 10, 20, 40, 60, 60, 60, 60]);
    }

    #[test]
    fn reaching_the_limit_fails_and_stays_failed() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        for i in 1..MAX as i64 {
            assert!(matches!(crash(&mut slot, i * 100), Decision::Run { .. }));
        }
        assert_eq!(started(&mut slot, 5000), Decision::Failed);
        assert_eq!(slot.state, State::Failed);
        assert_eq!(slot.failures, MAX as u32);
        assert_eq!(slot.since, 5000);

        // A clean exit (e.g. DROP DATABASE ... FORCE) does not clear it
        slot.exit_clean();
        assert_eq!(started(&mut slot, 6000), Decision::Failed);
        assert_eq!(slot.since, 5000, "failed since the first time");
    }

    #[test]
    fn reset_runs_again_from_zero() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        for i in 1..=MAX as i64 {
            crash(&mut slot, i * 100);
        }
        assert_eq!(slot.state, State::Failed);
        assert!(slot.request_reset());
        assert_eq!(backoff_secs(started(&mut slot, 9000)), 0);
        assert_eq!(slot.failures, 0);
        assert_eq!(slot.last_failure(), None);
        assert_eq!(slot.state, State::Starting);
    }

    #[test]
    fn reset_of_a_working_slot_does_nothing() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        assert!(!slot.request_reset());
        assert!(!slot.reset_requested);
    }

    #[test]
    fn a_healthy_run_resets_the_count() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        crash(&mut slot, 100);
        assert_eq!(slot.failures, 1);
        assert!(!slot.tick(100 + HEALTHY_AFTER_SECS - 1));
        assert!(slot.tick(100 + HEALTHY_AFTER_SECS));
        assert_eq!(slot.failures, 0);
        assert_eq!(slot.last_failure(), None);
        // ... and its later death is not counted either
        assert_eq!(backoff_secs(started(&mut slot, 1000)), 0);
        assert_eq!(slot.failures, 0);
    }

    #[test]
    fn backing_off_does_not_count_as_healthy_time() {
        let mut slot = Slot::EMPTY;
        crash(&mut slot, 0);
        started(&mut slot, 100); // backs off from 100
        assert!(!slot.tick(100 + HEALTHY_AFTER_SECS), "not running yet");
        slot.running(200);
        assert!(!slot.tick(200 + HEALTHY_AFTER_SECS - 1));
        assert!(slot.tick(200 + HEALTHY_AFTER_SECS));
    }

    #[test]
    fn zero_limit_never_gives_up() {
        let mut slot = Slot::EMPTY;
        slot.begin("w", 1, 0, 0);
        for i in 1..50 {
            slot.running(i * 100);
            assert!(matches!(
                slot.begin("w", 1, i * 100 + 1, 0),
                Decision::Run { .. }
            ));
        }
        assert_eq!(slot.failures, 49);
    }

    #[test]
    fn long_text_is_truncated_on_a_char_boundary() {
        let mut slot = Slot::EMPTY;
        slot.begin(&"n".repeat(500), 1, 0, MAX);
        assert_eq!(slot.name().len(), NAME_LEN);
        slot.exit_failed(&"é".repeat(500));
        let reason = slot.last_failure().unwrap();
        assert!(reason.len() <= REASON_LEN && reason.chars().all(|c| c == 'é'));
    }

    #[test]
    fn status_rows_describe_the_slots() {
        let mut slots = [Slot::EMPTY; MAX_WORKERS];
        slots[1].begin("pg_kafka worker 1", 42, 7, MAX);
        let rows = status_rows(&slots);
        assert_eq!(rows.len(), 1, "never-started slots are omitted");
        assert_eq!(rows[0].worker, 1);
        assert_eq!(rows[0].name, "pg_kafka worker 1");
        assert_eq!(rows[0].state, "starting");
        assert_eq!(rows[0].pid, 42);
        assert_eq!(rows[0].since_unix, 7);
    }
}
