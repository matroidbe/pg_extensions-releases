//! Stream-to-stream join processor — joins records from two topics within a time window
//!
//! Uses a buffer table to store records from the "other" side:
//!   pgstreams.<pipeline>_join_buffer (side, join_key, record, event_time, expires_at)
//!
//! Processing flow:
//! 1. For each incoming record (left side):
//!    a. Insert into buffer as side='left'
//!    b. Probe buffer for matching side='right' records within the time window
//!    c. For each match, emit a merged record
//! 2. The timer worker deletes expired buffer entries
//!
//! For left joins, unmatched left records are emitted with NULL values
//! for the right-side columns when they expire.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

#[cfg_attr(test, derive(Debug))]
pub struct JoinProcessor {
    state_table: String,
    #[allow(dead_code)]
    join_type: JoinType,
    #[allow(dead_code)]
    window: String,
    #[allow(dead_code)]
    other_columns: Vec<(String, String)>, // (output_name, other_expr)
    /// SQL to insert left-side records into the buffer
    insert_sql: String,
    /// SQL to probe the buffer for matches and emit joined records
    probe_sql: String,
}

#[derive(Debug, Clone, PartialEq)]
enum JoinType {
    Inner,
    Left,
}

// =============================================================================
// State table DDL
// =============================================================================

fn join_buffer_ddl(table: &str) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {} (\
         id BIGSERIAL PRIMARY KEY, \
         side TEXT NOT NULL, \
         join_key TEXT NOT NULL, \
         record JSONB NOT NULL, \
         event_time TIMESTAMPTZ NOT NULL DEFAULT now(), \
         expires_at TIMESTAMPTZ NOT NULL\
         )",
        table
    )
}

fn join_buffer_indexes(table: &str) -> Vec<String> {
    let base = table.replace('.', "_").replace("pgstreams_", "");
    vec![
        format!(
            "CREATE INDEX IF NOT EXISTS {}_key_idx ON {} (side, join_key, event_time)",
            base, table
        ),
        format!(
            "CREATE INDEX IF NOT EXISTS {}_expires_idx ON {} (expires_at)",
            base, table
        ),
    ]
}

// =============================================================================
// SQL generation
// =============================================================================

/// Build the SQL that inserts incoming (left-side) records into the buffer
/// and probes for matching right-side records.
fn build_insert_sql(cte: &str, join_key_expr: &str, state_table: &str, window: &str) -> String {
    // Insert each batch record into the buffer as side='left'
    format!(
        "{cte}INSERT INTO {table} (side, join_key, record, event_time, expires_at) \
         SELECT 'left', ({key})::text, _batch._original, now(), now() + interval '{window}' \
         FROM _batch",
        cte = cte.trim(),
        table = state_table,
        key = join_key_expr,
        window = window,
    )
}

/// Build the SQL that probes the buffer for matching records from the other side.
/// Returns joined records as a JSONB array.
fn build_probe_sql(
    cte: &str,
    join_key_expr: &str,
    join_type: &JoinType,
    state_table: &str,
    other_columns: &[(String, String)],
) -> String {
    // Build the merge expression: left record || right columns
    let merge_exprs: Vec<String> = other_columns
        .iter()
        .map(|(out_name, other_expr)| {
            // other_expr references "other.xxx" — replace "other." with "_right.record->"
            let pg_expr = other_expr
                .replace("other.value_json", "_right.record->'value_json'")
                .replace("other.", "_right.record->>");
            format!("'{}', {}", out_name, pg_expr)
        })
        .collect();

    let merge = if merge_exprs.is_empty() {
        "_batch._original || _right.record".to_string()
    } else {
        format!(
            "_batch._original || jsonb_build_object({})",
            merge_exprs.join(", ")
        )
    };

    let join_clause = match join_type {
        JoinType::Inner => "JOIN",
        JoinType::Left => "LEFT JOIN",
    };

    format!(
        "{cte}SELECT COALESCE(jsonb_agg(_merged), '[]'::jsonb) \
         FROM _batch \
         {join} {table} _right \
           ON _right.side = 'right' \
           AND _right.join_key = ({key})::text \
           AND _right.expires_at > now() \
         , LATERAL (SELECT {merge} AS _m) _merged_row \
         WHERE _merged_row._m IS NOT NULL",
        cte = cte.trim(),
        join = join_clause,
        table = state_table,
        key = join_key_expr,
        merge = merge,
    )
}

// =============================================================================
// JoinProcessor implementation
// =============================================================================

/// Parse the join condition to extract the left-side key expression.
/// The "on" condition is expected to be: "left_expr = other.right_expr"
/// We extract the left-side expression to use as the join key.
fn extract_join_key(on_condition: &str) -> Result<String, String> {
    // Simple parsing: split on " = " and take the left side
    if let Some(eq_pos) = on_condition.find(" = ") {
        let left = on_condition[..eq_pos].trim();
        if left.is_empty() {
            return Err("Join condition left side is empty".to_string());
        }
        Ok(left.to_string())
    } else {
        Err(format!(
            "Join condition must contain ' = ': got '{}'",
            on_condition
        ))
    }
}

impl JoinProcessor {
    pub fn new(
        config: &crate::dsl::types::JoinConfig,
        pipeline_name: &str,
        cte: &str,
    ) -> Result<Self, String> {
        let join_type = match config.join_type.as_str() {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            other => {
                return Err(format!(
                    "Unknown join type: '{}'. Use 'inner' or 'left'",
                    other
                ))
            }
        };

        let join_key = extract_join_key(&config.on)?;

        let state_table = format!(
            "pgstreams.{}_join_buffer",
            pipeline_name.replace(['-', '.'], "_")
        );

        let mut other_columns: Vec<(String, String)> = config
            .columns
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        other_columns.sort_by(|a, b| a.0.cmp(&b.0));

        let insert_sql = build_insert_sql(cte, &join_key, &state_table, &config.window);
        let probe_sql = build_probe_sql(cte, &join_key, &join_type, &state_table, &other_columns);

        Ok(Self {
            state_table,
            join_type,
            window: config.window.clone(),
            other_columns,
            insert_sql,
            probe_sql,
        })
    }

    /// Create the join buffer table. Called during pipeline compilation.
    pub fn ensure_state_table(&self) -> Result<(), String> {
        let ddl = join_buffer_ddl(&self.state_table);
        Spi::run(&ddl).map_err(|e| {
            format!(
                "Failed to create join buffer table '{}': {}",
                self.state_table, e
            )
        })?;

        for idx_sql in join_buffer_indexes(&self.state_table) {
            let _ = Spi::run(&idx_sql);
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn insert_sql(&self) -> &str {
        &self.insert_sql
    }

    #[cfg(test)]
    pub fn probe_sql(&self) -> &str {
        &self.probe_sql
    }
}

impl Processor for JoinProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json.clone());

        // 1. Insert left-side records into the buffer
        Spi::run_with_args(&self.insert_sql, &[batch_jsonb.into()])
            .map_err(|e| format!("Join buffer insert failed on '{}': {}", self.state_table, e))?;

        // 2. Probe for matching right-side records
        let probe_jsonb = pgrx::JsonB(batch_json);
        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.probe_sql, &[probe_jsonb.into()]);

        let jsonb =
            result.map_err(|e| format!("Join probe failed on '{}': {}", self.state_table, e))?;

        let joined: RecordBatch = match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => arr.clone(),
                None => vec![],
            },
            None => vec![],
        };

        Ok(joined)
    }

    fn name(&self) -> &str {
        "join"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::types::JoinConfig;
    use crate::record::batch_cte;
    use std::collections::HashMap;

    fn make_columns(defs: &[(&str, &str)]) -> HashMap<String, String> {
        defs.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn basic_join_config() -> JoinConfig {
        JoinConfig {
            topic: "shipments".to_string(),
            join_type: "left".to_string(),
            on: "order_id = other.value_json->>'order_id'".to_string(),
            window: "24 hours".to_string(),
            columns: make_columns(&[
                ("shipped_at", "other.value_json->>'shipped_at'"),
                ("carrier", "other.value_json->>'carrier'"),
            ]),
        }
    }

    #[test]
    fn test_extract_join_key() {
        let key = extract_join_key("order_id = other.value_json->>'order_id'").unwrap();
        assert_eq!(key, "order_id");
    }

    #[test]
    fn test_extract_join_key_complex() {
        let key =
            extract_join_key("(value_json->>'id')::text = other.value_json->>'ref_id'").unwrap();
        assert_eq!(key, "(value_json->>'id')::text");
    }

    #[test]
    fn test_extract_join_key_invalid() {
        let err = extract_join_key("no_equals_here").unwrap_err();
        assert!(err.contains("must contain"));
    }

    #[test]
    fn test_join_buffer_ddl() {
        let ddl = join_buffer_ddl("pgstreams.test_join_buffer");
        assert!(ddl.contains("CREATE TABLE IF NOT EXISTS pgstreams.test_join_buffer"));
        assert!(ddl.contains("side TEXT NOT NULL"));
        assert!(ddl.contains("join_key TEXT NOT NULL"));
        assert!(ddl.contains("record JSONB NOT NULL"));
        assert!(ddl.contains("expires_at TIMESTAMPTZ NOT NULL"));
    }

    #[test]
    fn test_insert_sql_generation() {
        let config = basic_join_config();
        let proc = JoinProcessor::new(&config, "order_pipeline", batch_cte()).unwrap();

        let sql = proc.insert_sql();
        assert!(sql.contains("INSERT INTO pgstreams.order_pipeline_join_buffer"));
        assert!(sql.contains("'left'"));
        assert!(sql.contains("(order_id)::text"));
        assert!(sql.contains("24 hours"));
    }

    #[test]
    fn test_probe_sql_generation() {
        let config = basic_join_config();
        let proc = JoinProcessor::new(&config, "order_pipeline", batch_cte()).unwrap();

        let sql = proc.probe_sql();
        assert!(sql.contains("LEFT JOIN pgstreams.order_pipeline_join_buffer _right"));
        assert!(sql.contains("_right.side = 'right'"));
        assert!(sql.contains("_right.join_key = (order_id)::text"));
        assert!(sql.contains("jsonb_build_object("));
        assert!(sql.contains("'carrier'"));
        assert!(sql.contains("'shipped_at'"));
    }

    #[test]
    fn test_inner_join_sql() {
        let config = JoinConfig {
            topic: "payments".to_string(),
            join_type: "inner".to_string(),
            on: "id = other.order_id".to_string(),
            window: "1 hour".to_string(),
            columns: make_columns(&[("amount", "other.value_json->>'amount'")]),
        };
        let proc = JoinProcessor::new(&config, "test", batch_cte()).unwrap();

        let sql = proc.probe_sql();
        assert!(sql.contains("JOIN pgstreams.test_join_buffer _right"));
        assert!(!sql.contains("LEFT JOIN"));
    }

    #[test]
    fn test_empty_batch_passthrough() {
        let config = basic_join_config();
        let proc = JoinProcessor::new(&config, "test", batch_cte()).unwrap();
        let result = proc.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_unknown_join_type() {
        let config = JoinConfig {
            topic: "t".to_string(),
            join_type: "full".to_string(),
            on: "a = other.b".to_string(),
            window: "1 hour".to_string(),
            columns: HashMap::new(),
        };
        let result = JoinProcessor::new(&config, "test", batch_cte());
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown join type"));
    }
}
