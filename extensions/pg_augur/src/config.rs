use std::ffi::CString;
use std::time::Duration;

pub static TRAINING_WORKER_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

pub static TRAINING_POLL_INTERVAL: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(1000);

pub static TRAINING_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

pub static MAX_TRAINING_SECONDS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(3600);

const DEFAULT_DATABASE: &str = "postgres";

pub fn register_gucs() {
    pgrx::GucRegistry::define_bool_guc(
        c"pg_augur.training_worker_enabled",
        c"Enable async training background worker",
        c"When true, pg_augur starts a background worker to process async training jobs",
        &TRAINING_WORKER_ENABLED,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_augur.training_poll_interval",
        c"Job polling interval in milliseconds",
        c"How often the training worker checks for new jobs (100-60000 ms)",
        &TRAINING_POLL_INTERVAL,
        100,
        60000,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_string_guc(
        c"pg_augur.training_database",
        c"Database for training worker to connect to",
        c"Database name the training worker uses for SPI connections",
        &TRAINING_DATABASE,
        pgrx::GucContext::Sighup,
        pgrx::GucFlags::default(),
    );

    pgrx::GucRegistry::define_int_guc(
        c"pg_augur.max_training_seconds",
        c"Maximum wall-clock seconds any single training job may run",
        c"Jobs exceeding this limit are marked failed by the worker",
        &MAX_TRAINING_SECONDS,
        1,
        86400,
        pgrx::GucContext::Userset,
        pgrx::GucFlags::default(),
    );
}

pub fn is_worker_enabled() -> bool {
    TRAINING_WORKER_ENABLED.get()
}

pub fn get_poll_interval() -> Duration {
    Duration::from_millis(TRAINING_POLL_INTERVAL.get() as u64)
}

pub fn get_database() -> String {
    TRAINING_DATABASE
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| DEFAULT_DATABASE.to_string())
}

#[allow(dead_code)]
pub fn max_training_seconds() -> i64 {
    MAX_TRAINING_SECONDS.get() as i64
}
