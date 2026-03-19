//! Observability SQL function implementations
//!
//! Provides queryable functions for pipeline status, errors, late events,
//! metrics, consumer lag, and trace/debug views.

use pgrx::prelude::*;

/// Format a duration in seconds into a human-readable string like "2h 15m 30s"
fn format_uptime(seconds: f64) -> String {
    if seconds < 0.0 {
        return "0s".to_string();
    }
    let total = seconds as u64;
    let days = total / 86400;
    let hours = (total % 86400) / 3600;
    let mins = (total % 3600) / 60;
    let secs = total % 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if mins > 0 {
        parts.push(format!("{}m", mins));
    }
    if secs > 0 || parts.is_empty() {
        parts.push(format!("{}s", secs));
    }
    parts.join(" ")
}

/// Pipeline status overview — one row per pipeline
#[allow(clippy::type_complexity)]
pub fn status_impl() -> Vec<(
    String,
    String,
    Option<i32>,
    Option<String>,
    Option<pgrx::datum::TimestampWithTimeZone>,
    Option<pgrx::datum::TimestampWithTimeZone>,
    Option<String>,
)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT name, state, worker_id, error, started_at, stopped_at, \
             EXTRACT(EPOCH FROM (now() - started_at))::double precision AS uptime_secs \
             FROM pgstreams.pipelines ORDER BY name",
            None,
            &[],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let name: String = row.get(1)?.unwrap_or_default();
            let state: String = row.get(2)?.unwrap_or_default();
            let worker_id: Option<i32> = row.get(3)?;
            let error: Option<String> = row.get(4)?;
            let started_at: Option<pgrx::datum::TimestampWithTimeZone> = row.get(5)?;
            let stopped_at: Option<pgrx::datum::TimestampWithTimeZone> = row.get(6)?;
            let uptime_secs: Option<f64> = row.get(7)?;

            let uptime = if state == "running" {
                uptime_secs.map(format_uptime)
            } else {
                None
            };

            rows.push((
                name, state, worker_id, error, started_at, stopped_at, uptime,
            ));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Recent errors for a pipeline
#[allow(clippy::type_complexity)]
pub fn errors_impl(
    name: &str,
    limit: i32,
) -> Vec<(
    i64,
    String,
    Option<String>,
    String,
    Option<pgrx::JsonB>,
    pgrx::datum::TimestampWithTimeZone,
)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, pipeline, processor, error, record, created_at \
             FROM pgstreams.error_log \
             WHERE pipeline = $1 \
             ORDER BY created_at DESC \
             LIMIT $2",
            None,
            &[name.into(), limit.into()],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let pipeline: String = row.get(2)?.unwrap_or_default();
            let processor: Option<String> = row.get(3)?;
            let error: String = row.get(4)?.unwrap_or_default();
            let record: Option<pgrx::JsonB> = row.get(5)?;
            let created_at: pgrx::datum::TimestampWithTimeZone = row
                .get(6)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());

            rows.push((id, pipeline, processor, error, record, created_at));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Recent late events for a pipeline
#[allow(clippy::type_complexity)]
pub fn late_events_impl(
    name: &str,
    limit: i32,
) -> Vec<(
    i64,
    String,
    String,
    Option<pgrx::datum::TimestampWithTimeZone>,
    pgrx::datum::TimestampWithTimeZone,
    pgrx::datum::TimestampWithTimeZone,
    pgrx::datum::TimestampWithTimeZone,
    pgrx::datum::TimestampWithTimeZone,
)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT id, pipeline, processor, event_time, \
             window_start, window_end, watermark, created_at \
             FROM pgstreams.late_events \
             WHERE pipeline = $1 \
             ORDER BY created_at DESC \
             LIMIT $2",
            None,
            &[name.into(), limit.into()],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let id: i64 = row.get(1)?.unwrap_or(0);
            let pipeline: String = row.get(2)?.unwrap_or_default();
            let processor: String = row.get(3)?.unwrap_or_default();
            let event_time: Option<pgrx::datum::TimestampWithTimeZone> = row.get(4)?;
            let window_start: pgrx::datum::TimestampWithTimeZone = row
                .get(5)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());
            let window_end: pgrx::datum::TimestampWithTimeZone = row
                .get(6)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());
            let watermark: pgrx::datum::TimestampWithTimeZone = row
                .get(7)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());
            let created_at: pgrx::datum::TimestampWithTimeZone = row
                .get(8)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());

            rows.push((
                id,
                pipeline,
                processor,
                event_time,
                window_start,
                window_end,
                watermark,
                created_at,
            ));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Recent metrics for a pipeline
pub fn metrics_impl(
    name: &str,
    limit: i32,
) -> Vec<(String, String, f64, pgrx::datum::TimestampWithTimeZone)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT pipeline, metric, value, measured_at \
             FROM pgstreams.metrics \
             WHERE pipeline = $1 \
             ORDER BY measured_at DESC \
             LIMIT $2",
            None,
            &[name.into(), limit.into()],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let pipeline: String = row.get(1)?.unwrap_or_default();
            let metric: String = row.get(2)?.unwrap_or_default();
            let value: f64 = row.get(3)?.unwrap_or(0.0);
            let measured_at: pgrx::datum::TimestampWithTimeZone = row
                .get(4)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());

            rows.push((pipeline, metric, value, measured_at));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Connector offsets / lag overview
pub fn lag_impl() -> Vec<(String, String, i64, pgrx::datum::TimestampWithTimeZone)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT pipeline, connector, offset_value, updated_at \
             FROM pgstreams.connector_offsets \
             ORDER BY pipeline, connector",
            None,
            &[],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let pipeline: String = row.get(1)?.unwrap_or_default();
            let connector: String = row.get(2)?.unwrap_or_default();
            let offset_value: i64 = row.get(3)?.unwrap_or(0);
            let updated_at: pgrx::datum::TimestampWithTimeZone = row
                .get(4)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());

            rows.push((pipeline, connector, offset_value, updated_at));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

/// Quick trace of recent error records for debugging
pub fn trace_impl(
    name: &str,
    limit: i32,
) -> Vec<(
    Option<pgrx::JsonB>,
    String,
    pgrx::datum::TimestampWithTimeZone,
)> {
    Spi::connect(|client| {
        let table = client.select(
            "SELECT record, error, created_at \
             FROM pgstreams.error_log \
             WHERE pipeline = $1 \
             ORDER BY created_at DESC \
             LIMIT $2",
            None,
            &[name.into(), limit.into()],
        )?;

        let mut rows = Vec::new();
        for row in table {
            let record: Option<pgrx::JsonB> = row.get(1)?;
            let error: String = row.get(2)?.unwrap_or_default();
            let created_at: pgrx::datum::TimestampWithTimeZone = row
                .get(3)?
                .unwrap_or_else(|| pgrx::datum::TimestampWithTimeZone::try_from(0i64).unwrap());

            rows.push((record, error, created_at));
        }

        Ok::<_, spi::Error>(rows)
    })
    .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_uptime_seconds() {
        assert_eq!(format_uptime(45.0), "45s");
    }

    #[test]
    fn test_format_uptime_minutes() {
        assert_eq!(format_uptime(125.0), "2m 5s");
    }

    #[test]
    fn test_format_uptime_hours() {
        assert_eq!(format_uptime(7830.0), "2h 10m 30s");
    }

    #[test]
    fn test_format_uptime_days() {
        assert_eq!(format_uptime(90061.0), "1d 1h 1m 1s");
    }

    #[test]
    fn test_format_uptime_zero() {
        assert_eq!(format_uptime(0.0), "0s");
    }

    #[test]
    fn test_format_uptime_negative() {
        assert_eq!(format_uptime(-5.0), "0s");
    }

    #[test]
    fn test_format_uptime_exact_hour() {
        assert_eq!(format_uptime(3600.0), "1h");
    }

    #[test]
    fn test_format_uptime_exact_day() {
        assert_eq!(format_uptime(86400.0), "1d");
    }
}
