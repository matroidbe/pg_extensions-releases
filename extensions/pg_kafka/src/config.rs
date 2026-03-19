//! Configuration and GUC settings for pg_kafka

use std::ffi::CString;

/// GUC setting for the Kafka protocol server port
pub static PG_KAFKA_PORT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(9092);

/// GUC setting for the host address to bind to
pub static PG_KAFKA_HOST: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// GUC setting for the advertised host address for clients
pub static PG_KAFKA_ADVERTISED_HOST: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// GUC setting to enable/disable the Kafka server
pub static PG_KAFKA_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(true);

/// GUC setting for the number of worker threads
pub static PG_KAFKA_WORKER_COUNT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(4);

/// GUC setting for the database to connect to
pub static PG_KAFKA_DATABASE: pgrx::GucSetting<Option<CString>> =
    pgrx::GucSetting::<Option<CString>>::new(None);

/// GUC setting for the Prometheus metrics port (default 9187)
pub static PG_KAFKA_METRICS_PORT: pgrx::GucSetting<i32> = pgrx::GucSetting::<i32>::new(9187);

/// GUC setting to enable/disable Prometheus metrics endpoint (default false)
pub static PG_KAFKA_METRICS_ENABLED: pgrx::GucSetting<bool> = pgrx::GucSetting::<bool>::new(false);

/// Default database name
pub const DEFAULT_DATABASE: &str = "postgres";

/// Default host address
pub const DEFAULT_HOST: &str = "0.0.0.0";
