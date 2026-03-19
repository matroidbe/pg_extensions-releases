//! Configuration and GUC settings for pg_streaming

use std::ffi::CString;

/// GUC setting to enable/disable the stream processing engine
pub static PG_STREAMING_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

/// GUC setting for the number of executor workers
pub static PG_STREAMING_WORKER_COUNT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(4);

/// GUC setting for the batch size (records per poll)
pub static PG_STREAMING_BATCH_SIZE: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(100);

/// GUC setting for the poll interval in milliseconds
pub static PG_STREAMING_POLL_INTERVAL_MS: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(10);

/// GUC setting for the checkpoint interval in milliseconds
pub static PG_STREAMING_CHECKPOINT_INTERVAL_MS: pgrx::GucSetting<i32> =
    pgrx::GucSetting::<i32>::new(1000);

/// GUC setting to enable/disable Prometheus metrics
pub static PG_STREAMING_METRICS_ENABLED: pgrx::GucSetting<bool> =
    pgrx::GucSetting::<bool>::new(false);

/// GUC setting for the database to connect to
pub static PG_STREAMING_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// Default database name
pub const DEFAULT_DATABASE: &str = "postgres";
