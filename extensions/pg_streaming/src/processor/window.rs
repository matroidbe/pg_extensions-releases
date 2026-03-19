//! Window processor — time-bounded aggregation with tumbling, sliding, and session windows
//!
//! Uses a state table to track open windows:
//!   pgstreams.<pipeline>_window_state (group_key, window_start, window_end, accumulators...)
//!
//! Processing flow:
//! 1. Determine which window(s) each record belongs to
//! 2. Upsert accumulators in the window state table
//! 3. Check for windows that should close (watermark has advanced past window_end)
//! 4. Emit closed windows as aggregate rows
//! 5. Delete closed window rows from state
//!
//! For processing_time windows, now() determines the window.
//! For event_time windows, the record's timestamp field determines the window.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

#[cfg_attr(test, derive(Debug))]
pub struct WindowProcessor {
    state_table: String,
    #[allow(dead_code)]
    pipeline_name: String,
    #[allow(dead_code)]
    window_type: WindowType,
    #[allow(dead_code)]
    group_by: String,
    columns: Vec<ParsedWindowColumn>,
    #[allow(dead_code)]
    time_config: TimeConfig,
    #[allow(dead_code)]
    emit: WindowEmit,
    /// SQL to upsert window state for a batch (excludes late records)
    upsert_sql: String,
    /// SQL to find and emit closed windows
    emit_sql: String,
    /// SQL to delete emitted (closed) windows
    delete_sql: String,
    /// SQL to detect and log late records to pgstreams.late_events
    late_log_sql: String,
}

#[derive(Debug, Clone)]
enum WindowType {
    Tumbling { size: String },
    Sliding { size: String, slide: String },
    Session { gap: String },
}

#[derive(Debug, Clone)]
struct TimeConfig {
    /// SQL expression for record timestamp (None = processing time / now())
    time_field: Option<String>,
    /// Watermark delay interval (e.g., "30 seconds")
    watermark_delay: String,
}

#[derive(Debug, Clone, PartialEq)]
enum WindowEmit {
    OnClose,
    OnUpdate,
}

/// Reuse aggregate parsing from the aggregate processor
#[derive(Debug, Clone)]
struct ParsedWindowColumn {
    name: String,
    agg_expr: String, // raw aggregate expression for SQL generation
}

// =============================================================================
// Window assignment SQL
// =============================================================================

/// Generate the SQL expression that assigns each record to its window(s).
/// Returns a subquery that produces (group_key, window_start, window_end, agg_columns...)
/// for each record in the batch.
fn window_assign_sql(
    window_type: &WindowType,
    group_by: &str,
    time_expr: &str,
    columns: &[ParsedWindowColumn],
) -> String {
    let agg_exprs: Vec<String> = columns
        .iter()
        .map(|c| format!("{} AS {}", c.agg_expr, c.name))
        .collect();
    let agg_list = agg_exprs.join(", ");

    match window_type {
        WindowType::Tumbling { size } => {
            // Each record belongs to exactly one window
            // window_start = epoch-bucket of timestamp
            // window_end = window_start + size
            format!(
                "SELECT ({group})::text AS group_key, \
                 _ws AS window_start, \
                 _ws + interval '{size}' AS window_end, \
                 {aggs} \
                 FROM _batch, \
                 LATERAL (SELECT to_timestamp(\
                   floor(extract(epoch from {time}) / extract(epoch from interval '{size}')) \
                   * extract(epoch from interval '{size}')) AS _ws\
                 ) _w \
                 GROUP BY ({group})::text, _ws",
                group = group_by,
                size = size,
                time = time_expr,
                aggs = agg_list,
            )
        }
        WindowType::Sliding { size, slide } => {
            // Each record belongs to multiple windows.
            // Generate all overlapping window starts using generate_series.
            // window_start = each slide boundary that contains the record
            format!(
                "SELECT ({group})::text AS group_key, \
                 _ws AS window_start, \
                 _ws + interval '{size}' AS window_end, \
                 {aggs} \
                 FROM _batch, \
                 LATERAL (\
                   SELECT gs AS _ws FROM generate_series(\
                     to_timestamp(\
                       floor(extract(epoch from {time}) / extract(epoch from interval '{slide}')) \
                       * extract(epoch from interval '{slide}')) \
                     - interval '{size}' + interval '{slide}', \
                     to_timestamp(\
                       floor(extract(epoch from {time}) / extract(epoch from interval '{slide}')) \
                       * extract(epoch from interval '{slide}')), \
                     interval '{slide}'\
                   ) AS gs\
                 ) _w \
                 GROUP BY ({group})::text, _ws",
                group = group_by,
                size = size,
                slide = slide,
                time = time_expr,
                aggs = agg_list,
            )
        }
        WindowType::Session { gap } => {
            // Session windows: each record extends or creates a session.
            // Insert with window_start = event_time, window_end = event_time + gap.
            // Session merging happens separately.
            format!(
                "SELECT ({group})::text AS group_key, \
                 {time} AS window_start, \
                 {time} + interval '{gap}' AS window_end, \
                 {aggs} \
                 FROM _batch \
                 GROUP BY ({group})::text, {time}",
                group = group_by,
                time = time_expr,
                gap = gap,
                aggs = agg_list,
            )
        }
    }
}

// =============================================================================
// State table DDL
// =============================================================================

fn window_state_ddl(table: &str, columns: &[ParsedWindowColumn]) -> String {
    let mut col_defs = vec![
        "group_key TEXT NOT NULL".to_string(),
        "window_start TIMESTAMPTZ NOT NULL".to_string(),
        "window_end TIMESTAMPTZ NOT NULL".to_string(),
    ];
    for col in columns {
        // Use NUMERIC for all aggregate accumulators (safe default)
        col_defs.push(format!("{} NUMERIC NOT NULL DEFAULT 0", col.name));
    }
    col_defs.push("updated_at TIMESTAMPTZ NOT NULL DEFAULT now()".to_string());
    col_defs.push("PRIMARY KEY (group_key, window_start)".to_string());

    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        table,
        col_defs.join(", ")
    )
}

// =============================================================================
// SQL generation
// =============================================================================

fn build_window_upsert_sql(
    cte: &str,
    window_type: &WindowType,
    group_by: &str,
    columns: &[ParsedWindowColumn],
    state_table: &str,
    time_expr: &str,
    watermark: &str,
) -> String {
    let assign = window_assign_sql(window_type, group_by, time_expr, columns);

    let mut insert_cols = vec![
        "group_key".to_string(),
        "window_start".to_string(),
        "window_end".to_string(),
    ];
    let mut insert_vals = vec![
        "group_key".to_string(),
        "window_start".to_string(),
        "window_end".to_string(),
    ];
    for col in columns {
        insert_cols.push(col.name.clone());
        insert_vals.push(col.name.clone());
    }
    insert_cols.push("updated_at".to_string());
    insert_vals.push("now()".to_string());

    // ON CONFLICT: accumulate values (sum-merge for all types)
    let mut conflict_sets: Vec<String> = columns
        .iter()
        .map(|c| format!("{n} = {t}.{n} + EXCLUDED.{n}", n = c.name, t = state_table))
        .collect();

    // For session windows, extend window_end to the latest
    if matches!(window_type, WindowType::Session { .. }) {
        conflict_sets.push(format!(
            "window_end = GREATEST({}.window_end, EXCLUDED.window_end)",
            state_table
        ));
    }
    conflict_sets.push("updated_at = now()".to_string());

    format!(
        "{cte}, _window_agg AS ({assign}) \
         INSERT INTO {table} ({cols}) \
         SELECT {vals} FROM _window_agg \
         WHERE window_end > {watermark} \
         ON CONFLICT (group_key, window_start) DO UPDATE SET {sets}",
        cte = cte.trim(),
        assign = assign,
        table = state_table,
        cols = insert_cols.join(", "),
        vals = insert_vals.join(", "),
        watermark = watermark,
        sets = conflict_sets.join(", "),
    )
}

/// Generate SQL that detects late records and logs them to pgstreams.late_events.
/// A record is "late" if its computed window_end <= watermark (the window has already closed).
/// Unlike the upsert SQL, this logs individual records (no GROUP BY aggregation).
fn build_late_log_sql(
    cte: &str,
    window_type: &WindowType,
    time_expr: &str,
    watermark: &str,
    pipeline_name: &str,
) -> String {
    let late_filter = match window_type {
        WindowType::Tumbling { size } => {
            // window_end = window_start + size, late if window_end <= watermark
            format!(
                "SELECT _original AS record, \
                 ({time})::timestamptz AS event_time, \
                 _ws AS window_start, \
                 _ws + interval '{size}' AS window_end \
                 FROM _batch, \
                 LATERAL (SELECT to_timestamp(\
                   floor(extract(epoch from {time}) / extract(epoch from interval '{size}')) \
                   * extract(epoch from interval '{size}')) AS _ws\
                 ) _w \
                 WHERE _ws + interval '{size}' <= {watermark}",
                time = time_expr,
                size = size,
                watermark = watermark,
            )
        }
        WindowType::Sliding { size, slide } => {
            // A record is late if its latest window (the one starting at the slide-aligned
            // timestamp) has closed: slide_aligned_start + size <= watermark
            format!(
                "SELECT _original AS record, \
                 ({time})::timestamptz AS event_time, \
                 _ws AS window_start, \
                 _ws + interval '{size}' AS window_end \
                 FROM _batch, \
                 LATERAL (SELECT to_timestamp(\
                   floor(extract(epoch from {time}) / extract(epoch from interval '{slide}')) \
                   * extract(epoch from interval '{slide}')) AS _ws\
                 ) _w \
                 WHERE _ws + interval '{size}' <= {watermark}",
                time = time_expr,
                size = size,
                slide = slide,
                watermark = watermark,
            )
        }
        WindowType::Session { gap } => {
            // Session: record is late if event_time + gap <= watermark
            format!(
                "SELECT _original AS record, \
                 ({time})::timestamptz AS event_time, \
                 ({time})::timestamptz AS window_start, \
                 ({time} + interval '{gap}')::timestamptz AS window_end \
                 FROM _batch \
                 WHERE {time} + interval '{gap}' <= {watermark}",
                time = time_expr,
                gap = gap,
                watermark = watermark,
            )
        }
    };

    format!(
        "{cte}, _late AS ({late_filter}) \
         INSERT INTO pgstreams.late_events \
         (pipeline, processor, record, event_time, window_start, window_end, watermark) \
         SELECT '{pipeline}', 'window', record, event_time, window_start, window_end, {watermark} \
         FROM _late",
        cte = cte.trim(),
        late_filter = late_filter,
        pipeline = pipeline_name,
        watermark = watermark,
    )
}

fn build_emit_sql(state_table: &str, columns: &[ParsedWindowColumn], watermark: &str) -> String {
    let col_exprs: Vec<String> = std::iter::once("'group_key', group_key".to_string())
        .chain(std::iter::once("'window_start', window_start".to_string()))
        .chain(std::iter::once("'window_end', window_end".to_string()))
        .chain(columns.iter().map(|c| format!("'{}', {}", c.name, c.name)))
        .collect();

    format!(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object({})), '[]'::jsonb) \
         FROM {} WHERE window_end <= {}",
        col_exprs.join(", "),
        state_table,
        watermark,
    )
}

fn build_delete_sql(state_table: &str, watermark: &str) -> String {
    format!(
        "DELETE FROM {} WHERE window_end <= {}",
        state_table, watermark
    )
}

// =============================================================================
// WindowProcessor implementation
// =============================================================================

impl WindowProcessor {
    pub fn new(
        config: &crate::dsl::types::WindowConfig,
        pipeline_name: &str,
        cte: &str,
    ) -> Result<Self, String> {
        // Parse window type
        let window_type = match config.window_type.as_str() {
            "tumbling" => {
                let size = config
                    .size
                    .as_deref()
                    .ok_or("tumbling window requires 'size'")?;
                WindowType::Tumbling {
                    size: size.to_string(),
                }
            }
            "sliding" => {
                let size = config
                    .size
                    .as_deref()
                    .ok_or("sliding window requires 'size'")?;
                let slide = config
                    .slide
                    .as_deref()
                    .ok_or("sliding window requires 'slide'")?;
                WindowType::Sliding {
                    size: size.to_string(),
                    slide: slide.to_string(),
                }
            }
            "session" => {
                let gap = config
                    .gap
                    .as_deref()
                    .ok_or("session window requires 'gap'")?;
                WindowType::Session {
                    gap: gap.to_string(),
                }
            }
            other => return Err(format!("Unknown window type: {}", other)),
        };

        // Parse time config
        let time_config = match &config.time {
            Some(tc) if tc.time_type == "event_time" => {
                let field = tc
                    .field
                    .as_deref()
                    .ok_or("event_time windows require 'field'")?;
                TimeConfig {
                    time_field: Some(field.to_string()),
                    watermark_delay: tc.watermark_delay.as_deref().unwrap_or("0").to_string(),
                }
            }
            _ => TimeConfig {
                time_field: None,
                watermark_delay: "0".to_string(),
            },
        };

        let time_expr = time_config
            .time_field
            .as_deref()
            .unwrap_or("now()")
            .to_string();

        // Parse columns
        if config.columns.is_empty() {
            return Err("Window processor requires at least one aggregate column".to_string());
        }
        let mut col_names: Vec<&String> = config.columns.keys().collect();
        col_names.sort();
        let columns: Vec<ParsedWindowColumn> = col_names
            .iter()
            .map(|name| ParsedWindowColumn {
                name: (*name).clone(),
                agg_expr: config.columns[*name].clone(),
            })
            .collect();

        let emit = match config.emit.as_str() {
            "on_update" => WindowEmit::OnUpdate,
            _ => WindowEmit::OnClose,
        };

        // State table
        let state_table = format!(
            "pgstreams.{}_window_state",
            pipeline_name.replace(['-', '.'], "_")
        );

        // Watermark expression
        let watermark = if time_config.watermark_delay == "0" {
            "now()".to_string()
        } else {
            format!("now() - interval '{}'", time_config.watermark_delay)
        };

        // Build SQL
        let upsert_sql = build_window_upsert_sql(
            cte,
            &window_type,
            &config.group_by,
            &columns,
            &state_table,
            &time_expr,
            &watermark,
        );
        let emit_sql = build_emit_sql(&state_table, &columns, &watermark);
        let delete_sql = build_delete_sql(&state_table, &watermark);
        let late_log_sql =
            build_late_log_sql(cte, &window_type, &time_expr, &watermark, pipeline_name);

        Ok(Self {
            state_table,
            pipeline_name: pipeline_name.to_string(),
            window_type,
            group_by: config.group_by.clone(),
            columns,
            time_config,
            emit,
            upsert_sql,
            emit_sql,
            delete_sql,
            late_log_sql,
        })
    }

    /// Create the window state table. Called during pipeline compilation.
    pub fn ensure_state_table(&self) -> Result<(), String> {
        let ddl = window_state_ddl(&self.state_table, &self.columns);
        Spi::run(&ddl).map_err(|e| {
            format!(
                "Failed to create window state table '{}': {}",
                self.state_table, e
            )
        })?;

        // Index on window_end for efficient close detection
        let idx = format!(
            "CREATE INDEX IF NOT EXISTS {}_end_idx ON {} (window_end)",
            self.state_table.replace('.', "_").replace("pgstreams_", ""),
            self.state_table
        );
        let _ = Spi::run(&idx);

        Ok(())
    }

    /// Get the generated upsert SQL (for testing)
    #[cfg(test)]
    pub fn upsert_sql(&self) -> &str {
        &self.upsert_sql
    }

    /// Get the emit SQL (for testing)
    #[cfg(test)]
    pub fn emit_sql(&self) -> &str {
        &self.emit_sql
    }

    /// Get the delete SQL (for testing)
    #[cfg(test)]
    pub fn delete_sql(&self) -> &str {
        &self.delete_sql
    }

    /// Get the late log SQL (for testing)
    #[cfg(test)]
    pub fn late_log_sql(&self) -> &str {
        &self.late_log_sql
    }
}

impl Processor for WindowProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            // Even with empty batch, check for windows to close
            return self.emit_closed_windows();
        }

        let batch_json = serde_json::Value::Array(batch);

        // 1. Log late records to pgstreams.late_events (best-effort)
        let late_jsonb = pgrx::JsonB(batch_json.clone());
        if let Err(e) = Spi::run_with_args(&self.late_log_sql, &[late_jsonb.into()]) {
            pgrx::warning!("Late event logging failed: {}", e);
        }

        // 2. Upsert on-time records only (late records filtered by WHERE window_end > watermark)
        let batch_jsonb = pgrx::JsonB(batch_json);
        Spi::run_with_args(&self.upsert_sql, &[batch_jsonb.into()])
            .map_err(|e| format!("Window upsert failed on '{}': {}", self.state_table, e))?;

        // 3. Emit closed windows
        self.emit_closed_windows()
    }

    fn name(&self) -> &str {
        "window"
    }
}

impl WindowProcessor {
    fn emit_closed_windows(&self) -> Result<RecordBatch, String> {
        // Query for closed windows
        let result = Spi::get_one::<pgrx::JsonB>(&self.emit_sql);

        let jsonb = result
            .map_err(|e| format!("Window emit query failed on '{}': {}", self.state_table, e))?;

        let closed: RecordBatch = match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => arr.clone(),
                None => vec![],
            },
            None => vec![],
        };

        // Delete emitted windows
        if !closed.is_empty() {
            let _ = Spi::run(&self.delete_sql);
        }

        Ok(closed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::types::{WindowConfig, WindowTimeConfig};
    use crate::record::batch_cte;
    use std::collections::HashMap;

    fn make_columns(defs: &[(&str, &str)]) -> HashMap<String, String> {
        defs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn tumbling_config(size: &str, group_by: &str, columns: &[(&str, &str)]) -> WindowConfig {
        WindowConfig {
            window_type: "tumbling".to_string(),
            size: Some(size.to_string()),
            slide: None,
            gap: None,
            group_by: group_by.to_string(),
            columns: make_columns(columns),
            time: None,
            emit: "on_close".to_string(),
        }
    }

    #[test]
    fn test_window_state_ddl() {
        let columns = vec![
            ParsedWindowColumn {
                name: "revenue".to_string(),
                agg_expr: "sum(total)".to_string(),
            },
            ParsedWindowColumn {
                name: "orders".to_string(),
                agg_expr: "count(*)".to_string(),
            },
        ];
        let ddl = window_state_ddl("pgstreams.test_window_state", &columns);
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pgstreams.test_window_state"));
        assert!(ddl.contains("group_key TEXT NOT NULL"));
        assert!(ddl.contains("window_start TIMESTAMPTZ NOT NULL"));
        assert!(ddl.contains("window_end TIMESTAMPTZ NOT NULL"));
        assert!(ddl.contains("revenue NUMERIC NOT NULL DEFAULT 0"));
        assert!(ddl.contains("orders NUMERIC NOT NULL DEFAULT 0"));
        assert!(ddl.contains("PRIMARY KEY (group_key, window_start)"));
    }

    #[test]
    fn test_tumbling_window_upsert_sql() {
        let config = tumbling_config("5 minutes", "region", &[("revenue", "sum(total)")]);
        let proc = WindowProcessor::new(&config, "test_pipeline", batch_cte()).unwrap();

        let sql = proc.upsert_sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("_window_agg AS"));
        assert!(sql.contains("(region)::text AS group_key"));
        assert!(sql.contains("extract(epoch from interval '5 minutes')"));
        assert!(sql.contains("INSERT INTO pgstreams.test_pipeline_window_state"));
        assert!(sql.contains("ON CONFLICT (group_key, window_start) DO UPDATE SET"));
    }

    #[test]
    fn test_sliding_window_sql() {
        let config = WindowConfig {
            window_type: "sliding".to_string(),
            size: Some("10 minutes".to_string()),
            slide: Some("2 minutes".to_string()),
            gap: None,
            group_by: "sensor_id".to_string(),
            columns: make_columns(&[("avg_temp", "avg(temperature)")]),
            time: None,
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "sensors", batch_cte()).unwrap();

        let sql = proc.upsert_sql();
        assert!(sql.contains("generate_series"));
        assert!(sql.contains("10 minutes"));
        assert!(sql.contains("2 minutes"));
    }

    #[test]
    fn test_session_window_sql() {
        let config = WindowConfig {
            window_type: "session".to_string(),
            size: None,
            slide: None,
            gap: Some("30 minutes".to_string()),
            group_by: "user_id".to_string(),
            columns: make_columns(&[("page_views", "count(*)")]),
            time: None,
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "sessions", batch_cte()).unwrap();

        let sql = proc.upsert_sql();
        assert!(sql.contains("30 minutes"));
        assert!(sql.contains("GREATEST")); // session extends window_end
    }

    #[test]
    fn test_event_time_watermark() {
        let config = WindowConfig {
            window_type: "tumbling".to_string(),
            size: Some("1 hour".to_string()),
            slide: None,
            gap: None,
            group_by: "device_id".to_string(),
            columns: make_columns(&[("readings", "count(*)")]),
            time: Some(WindowTimeConfig {
                time_type: "event_time".to_string(),
                field: Some("(value_json->>'event_ts')::timestamptz".to_string()),
                watermark_delay: Some("30 seconds".to_string()),
                allowed_lateness: None,
            }),
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "iot", batch_cte()).unwrap();

        // Upsert should use event time field
        assert!(proc.upsert_sql().contains("event_ts"));

        // Emit should account for watermark delay
        assert!(proc.emit_sql().contains("now() - interval '30 seconds'"));
    }

    #[test]
    fn test_emit_and_delete_sql() {
        let config = tumbling_config("5 minutes", "key", &[("total", "sum(val)")]);
        let proc = WindowProcessor::new(&config, "test", batch_cte()).unwrap();

        let emit = proc.emit_sql();
        assert!(emit.contains("jsonb_agg(jsonb_build_object("));
        assert!(emit.contains("'group_key', group_key"));
        assert!(emit.contains("'window_start', window_start"));
        assert!(emit.contains("'window_end', window_end"));
        assert!(emit.contains("'total', total"));
        assert!(emit.contains("window_end <= now()"));

        let delete = proc.delete_sql();
        assert!(delete.contains("DELETE FROM"));
        assert!(delete.contains("window_end <= now()"));
    }

    #[test]
    fn test_processing_time_default() {
        // When no time config is specified, processing time (now()) is used
        let config = tumbling_config("5 minutes", "key", &[("total", "sum(val)")]);
        let proc = WindowProcessor::new(&config, "test", batch_cte()).unwrap();
        // Upsert should use now() for time
        assert!(proc.upsert_sql().contains("now()"));
        // Emit should use now() as watermark (no delay)
        assert!(proc.emit_sql().contains("window_end <= now()"));
    }

    #[test]
    fn test_missing_size_for_tumbling() {
        let config = WindowConfig {
            window_type: "tumbling".to_string(),
            size: None,
            slide: None,
            gap: None,
            group_by: "key".to_string(),
            columns: make_columns(&[("total", "sum(val)")]),
            time: None,
            emit: "on_close".to_string(),
        };
        let result = WindowProcessor::new(&config, "test", batch_cte());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("size"));
    }

    #[test]
    fn test_unknown_window_type() {
        let config = WindowConfig {
            window_type: "hopping".to_string(),
            size: Some("5 minutes".to_string()),
            slide: None,
            gap: None,
            group_by: "key".to_string(),
            columns: make_columns(&[("total", "sum(val)")]),
            time: None,
            emit: "on_close".to_string(),
        };
        let result = WindowProcessor::new(&config, "test", batch_cte());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown window type"));
    }

    #[test]
    fn test_empty_columns_rejected() {
        let config = WindowConfig {
            window_type: "tumbling".to_string(),
            size: Some("5 minutes".to_string()),
            slide: None,
            gap: None,
            group_by: "key".to_string(),
            columns: HashMap::new(),
            time: None,
            emit: "on_close".to_string(),
        };
        let result = WindowProcessor::new(&config, "test", batch_cte());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("at least one"));
    }

    // =========================================================================
    // Late event detection tests
    // =========================================================================

    #[test]
    fn test_upsert_excludes_late_records() {
        let config = tumbling_config("5 minutes", "region", &[("revenue", "sum(total)")]);
        let proc = WindowProcessor::new(&config, "test_pipeline", batch_cte()).unwrap();
        let sql = proc.upsert_sql();
        // Upsert should filter out late records
        assert!(sql.contains("WHERE window_end > now()"));
    }

    #[test]
    fn test_upsert_excludes_late_with_watermark_delay() {
        let config = WindowConfig {
            window_type: "tumbling".to_string(),
            size: Some("1 hour".to_string()),
            slide: None,
            gap: None,
            group_by: "device_id".to_string(),
            columns: make_columns(&[("readings", "count(*)")]),
            time: Some(WindowTimeConfig {
                time_type: "event_time".to_string(),
                field: Some("(value_json->>'event_ts')::timestamptz".to_string()),
                watermark_delay: Some("30 seconds".to_string()),
                allowed_lateness: None,
            }),
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "iot", batch_cte()).unwrap();
        let sql = proc.upsert_sql();
        assert!(sql.contains("WHERE window_end > now() - interval '30 seconds'"));
    }

    #[test]
    fn test_late_log_sql_tumbling() {
        let config = WindowConfig {
            window_type: "tumbling".to_string(),
            size: Some("1 hour".to_string()),
            slide: None,
            gap: None,
            group_by: "device_id".to_string(),
            columns: make_columns(&[("readings", "count(*)")]),
            time: Some(WindowTimeConfig {
                time_type: "event_time".to_string(),
                field: Some("(value_json->>'event_ts')::timestamptz".to_string()),
                watermark_delay: Some("30 seconds".to_string()),
                allowed_lateness: None,
            }),
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "iot_pipeline", batch_cte()).unwrap();
        let sql = proc.late_log_sql();

        assert!(sql.contains("INSERT INTO pgstreams.late_events"));
        assert!(sql.contains("'iot_pipeline'")); // pipeline name embedded
        assert!(sql.contains("'window'")); // processor type
        assert!(sql.contains("_ws + interval '1 hour'")); // window_end computation
        assert!(sql.contains("<= now() - interval '30 seconds'")); // late detection filter
    }

    #[test]
    fn test_late_log_sql_sliding() {
        let config = WindowConfig {
            window_type: "sliding".to_string(),
            size: Some("10 minutes".to_string()),
            slide: Some("2 minutes".to_string()),
            gap: None,
            group_by: "sensor_id".to_string(),
            columns: make_columns(&[("avg_temp", "avg(temperature)")]),
            time: Some(WindowTimeConfig {
                time_type: "event_time".to_string(),
                field: Some("(value_json->>'ts')::timestamptz".to_string()),
                watermark_delay: Some("10 seconds".to_string()),
                allowed_lateness: None,
            }),
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "sensors", batch_cte()).unwrap();
        let sql = proc.late_log_sql();

        assert!(sql.contains("INSERT INTO pgstreams.late_events"));
        assert!(sql.contains("'sensors'"));
        // Sliding uses the slide-aligned start + size for late detection
        assert!(sql.contains("_ws + interval '10 minutes'"));
        assert!(sql.contains("2 minutes")); // slide interval in window start computation
    }

    #[test]
    fn test_late_log_sql_session() {
        let config = WindowConfig {
            window_type: "session".to_string(),
            size: None,
            slide: None,
            gap: Some("30 minutes".to_string()),
            group_by: "user_id".to_string(),
            columns: make_columns(&[("page_views", "count(*)")]),
            time: Some(WindowTimeConfig {
                time_type: "event_time".to_string(),
                field: Some("(value_json->>'ts')::timestamptz".to_string()),
                watermark_delay: Some("1 minute".to_string()),
                allowed_lateness: None,
            }),
            emit: "on_close".to_string(),
        };
        let proc = WindowProcessor::new(&config, "sessions", batch_cte()).unwrap();
        let sql = proc.late_log_sql();

        assert!(sql.contains("INSERT INTO pgstreams.late_events"));
        assert!(sql.contains("'sessions'"));
        // Session: late if event_time + gap <= watermark
        assert!(sql.contains("+ interval '30 minutes'"));
        assert!(sql.contains("<= now() - interval '1 minute'"));
    }

    #[test]
    fn test_processing_time_late_log() {
        // Processing time: watermark is now(), so records are never truly "late"
        // but the SQL should still be valid
        let config = tumbling_config("5 minutes", "key", &[("total", "sum(val)")]);
        let proc = WindowProcessor::new(&config, "proc_time", batch_cte()).unwrap();
        let sql = proc.late_log_sql();

        assert!(sql.contains("INSERT INTO pgstreams.late_events"));
        assert!(sql.contains("'proc_time'"));
        // Uses now() as both time_expr and watermark
        assert!(sql.contains("<= now()"));
    }
}
