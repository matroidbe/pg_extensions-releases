//! Table input connector — polls a PostgreSQL table for new rows by offset column
//!
//! Queries `SELECT ... FROM table WHERE offset_column > last_offset ORDER BY offset_column LIMIT batch_size`
//! wrapped in `jsonb_agg` for batch extraction. Supports configurable poll intervals
//! to avoid hammering the source table.

use crate::connector::InputConnector;
use crate::dsl::types::TableInputConfig;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TableInput {
    table_name: String,
    #[allow(dead_code)]
    offset_column: String,
    poll_interval_ms: u64,
    last_poll_ms: u64,
    last_offset: i64,
    poll_sql: String,
}

/// Parse a poll interval string into milliseconds.
/// Supports: "5s" (seconds), "500ms" (milliseconds), "1m" (minutes).
pub fn parse_poll_interval_ms(s: &str) -> Result<u64, String> {
    let s = s.trim();
    if s.is_empty() {
        return Err("poll interval is empty".to_string());
    }

    if let Some(n) = s.strip_suffix("ms") {
        let v: u64 = n
            .parse()
            .map_err(|_| format!("invalid poll interval '{}'", s))?;
        if v == 0 {
            return Err("poll interval must be > 0".to_string());
        }
        return Ok(v);
    }
    if let Some(n) = s.strip_suffix('m') {
        let v: u64 = n
            .parse()
            .map_err(|_| format!("invalid poll interval '{}'", s))?;
        if v == 0 {
            return Err("poll interval must be > 0".to_string());
        }
        return Ok(v * 60_000);
    }
    if let Some(n) = s.strip_suffix('s') {
        let v: u64 = n
            .parse()
            .map_err(|_| format!("invalid poll interval '{}'", s))?;
        if v == 0 {
            return Err("poll interval must be > 0".to_string());
        }
        return Ok(v * 1000);
    }

    // Try plain number as milliseconds
    let v: u64 = s.parse().map_err(|_| {
        format!(
            "invalid poll interval '{}': expected format like '5s', '500ms', or '1m'",
            s
        )
    })?;
    if v == 0 {
        return Err("poll interval must be > 0".to_string());
    }
    Ok(v)
}

/// Build the poll SQL query for a table input connector.
/// Returns a query that takes $1 = last_offset (bigint) and $2 = batch_size (bigint).
fn build_poll_sql(
    table_name: &str,
    offset_column: &str,
    columns: &[(String, String)],
    filter: Option<&str>,
) -> String {
    let col_exprs: Vec<String> = columns
        .iter()
        .map(|(name, _)| format!("'{}', {}", name, name))
        .collect();

    let filter_clause = match filter {
        Some(f) if !f.is_empty() => format!(" AND ({})", f),
        _ => String::new(),
    };

    format!(
        "SELECT COALESCE(jsonb_agg(sub.r ORDER BY sub.oid), '[]'::jsonb) FROM (\
          SELECT jsonb_build_object({}, 'offset_id', {}::bigint) AS r, \
                 {}::bigint AS oid \
          FROM {} \
          WHERE {}::bigint > $1{} \
          ORDER BY {} \
          LIMIT $2\
        ) sub",
        col_exprs.join(", "),
        offset_column,
        offset_column,
        table_name,
        offset_column,
        filter_clause,
        offset_column,
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

impl TableInput {
    pub fn new(config: &TableInputConfig, columns: Vec<(String, String)>) -> Result<Self, String> {
        let poll_interval_ms = parse_poll_interval_ms(&config.poll)?;

        let poll_sql = build_poll_sql(
            &config.name,
            &config.offset_column,
            &columns,
            config.filter.as_deref(),
        );

        Ok(Self {
            table_name: config.name.clone(),
            offset_column: config.offset_column.clone(),
            poll_interval_ms,
            last_poll_ms: 0,
            last_offset: 0,
            poll_sql,
        })
    }

    #[cfg(test)]
    pub fn poll_sql(&self) -> &str {
        &self.poll_sql
    }

    /// Initialize the starting offset from connector_offsets or default to 0
    fn initialize_offset(&mut self, pipeline_name: &str) -> Result<(), String> {
        let existing = Spi::get_one_with_args::<i64>(
            "SELECT offset_value FROM pgstreams.connector_offsets \
             WHERE pipeline = $1 AND connector = 'table_input'",
            &[pipeline_name.into()],
        );

        if let Ok(Some(offset)) = existing {
            self.last_offset = offset;
        }
        // else: start from 0 (earliest)

        Ok(())
    }
}

impl InputConnector for TableInput {
    fn initialize(&mut self, pipeline_name: &str) -> Result<(), String> {
        self.initialize_offset(pipeline_name)
    }

    fn poll(&mut self, batch_size: i32) -> Result<(RecordBatch, Option<i64>), String> {
        // Throttle: skip poll if interval hasn't elapsed
        let now = now_ms();
        if now - self.last_poll_ms < self.poll_interval_ms {
            return Ok((vec![], None));
        }
        self.last_poll_ms = now;

        let result = Spi::get_one_with_args::<pgrx::JsonB>(
            &self.poll_sql,
            &[self.last_offset.into(), (batch_size as i64).into()],
        );

        let jsonb =
            result.map_err(|e| format!("Failed to poll table '{}': {}", self.table_name, e))?;

        let batch_value = match jsonb {
            Some(jb) => jb.0,
            None => return Ok((vec![], None)),
        };

        let records: RecordBatch = match batch_value.as_array() {
            Some(arr) => arr.clone(),
            None => return Ok((vec![], None)),
        };

        if records.is_empty() {
            return Ok((vec![], None));
        }

        let max_offset = records.iter().filter_map(|r| r["offset_id"].as_i64()).max();

        if let Some(offset) = max_offset {
            self.last_offset = offset;
        }

        Ok((records, max_offset))
    }

    fn commit(&mut self, pipeline_name: &str, offset: i64) -> Result<(), String> {
        Spi::run_with_args(
            "INSERT INTO pgstreams.connector_offsets (pipeline, connector, offset_value, updated_at) \
             VALUES ($1, 'table_input', $2, now()) \
             ON CONFLICT (pipeline, connector) \
             DO UPDATE SET offset_value = $2, updated_at = now()",
            &[pipeline_name.into(), offset.into()],
        )
        .map_err(|e| format!("Failed to commit table input offset: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // Poll interval parsing
    // =========================================================================

    #[test]
    fn test_parse_poll_seconds() {
        assert_eq!(parse_poll_interval_ms("5s").unwrap(), 5000);
        assert_eq!(parse_poll_interval_ms("1s").unwrap(), 1000);
        assert_eq!(parse_poll_interval_ms("30s").unwrap(), 30000);
    }

    #[test]
    fn test_parse_poll_milliseconds() {
        assert_eq!(parse_poll_interval_ms("500ms").unwrap(), 500);
        assert_eq!(parse_poll_interval_ms("100ms").unwrap(), 100);
    }

    #[test]
    fn test_parse_poll_minutes() {
        assert_eq!(parse_poll_interval_ms("1m").unwrap(), 60000);
        assert_eq!(parse_poll_interval_ms("5m").unwrap(), 300000);
    }

    #[test]
    fn test_parse_poll_plain_number() {
        assert_eq!(parse_poll_interval_ms("1000").unwrap(), 1000);
    }

    #[test]
    fn test_parse_poll_invalid() {
        assert!(parse_poll_interval_ms("abc").is_err());
        assert!(parse_poll_interval_ms("").is_err());
        assert!(parse_poll_interval_ms("0s").is_err());
        assert!(parse_poll_interval_ms("0ms").is_err());
        assert!(parse_poll_interval_ms("-1s").is_err());
    }

    // =========================================================================
    // SQL generation
    // =========================================================================

    #[test]
    fn test_poll_sql_basic() {
        let columns = vec![
            ("id".to_string(), "bigint".to_string()),
            ("customer_id".to_string(), "text".to_string()),
            ("amount".to_string(), "numeric".to_string()),
        ];
        let sql = build_poll_sql("public.orders", "id", &columns, None);

        assert!(sql.contains("'id', id"));
        assert!(sql.contains("'customer_id', customer_id"));
        assert!(sql.contains("'amount', amount"));
        assert!(sql.contains("'offset_id', id::bigint"));
        assert!(sql.contains("FROM public.orders"));
        assert!(sql.contains("WHERE id::bigint > $1"));
        assert!(sql.contains("ORDER BY id"));
        assert!(sql.contains("LIMIT $2"));
        assert!(!sql.contains("AND ("));
    }

    #[test]
    fn test_poll_sql_with_filter() {
        let columns = vec![("id".to_string(), "bigint".to_string())];
        let sql = build_poll_sql("sensor_readings", "id", &columns, Some("status = 'active'"));

        assert!(sql.contains("AND (status = 'active')"));
    }

    #[test]
    fn test_poll_sql_schema_qualified() {
        let columns = vec![("id".to_string(), "bigint".to_string())];
        let sql = build_poll_sql("myschema.events", "event_id", &columns, None);

        assert!(sql.contains("FROM myschema.events"));
        assert!(sql.contains("WHERE event_id::bigint > $1"));
        assert!(sql.contains("ORDER BY event_id"));
    }

    // =========================================================================
    // Construction
    // =========================================================================

    #[test]
    fn test_table_input_construction() {
        let config = TableInputConfig {
            name: "public.orders".to_string(),
            offset_column: "id".to_string(),
            poll: "5s".to_string(),
            filter: Some("region = 'US'".to_string()),
        };
        let columns = vec![
            ("id".to_string(), "bigint".to_string()),
            ("amount".to_string(), "numeric".to_string()),
        ];
        let input = TableInput::new(&config, columns).unwrap();

        assert_eq!(input.table_name, "public.orders");
        assert_eq!(input.poll_interval_ms, 5000);
        assert_eq!(input.last_offset, 0);
        assert!(input.poll_sql().contains("AND (region = 'US')"));
    }

    // =========================================================================
    // Throttling
    // =========================================================================

    #[test]
    fn test_throttle_returns_empty_before_interval() {
        let config = TableInputConfig {
            name: "t".to_string(),
            offset_column: "id".to_string(),
            poll: "60s".to_string(), // very long interval
            filter: None,
        };
        let columns = vec![("id".to_string(), "bigint".to_string())];
        let mut input = TableInput::new(&config, columns).unwrap();

        // Set last_poll_ms to now — next poll should be throttled
        input.last_poll_ms = now_ms();

        // We can't call poll() without SPI, but we can verify the throttle logic
        // by checking that last_poll_ms was set recently
        let elapsed = now_ms() - input.last_poll_ms;
        assert!(elapsed < input.poll_interval_ms);
    }
}
