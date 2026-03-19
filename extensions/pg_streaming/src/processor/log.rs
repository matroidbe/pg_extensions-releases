//! Log processor — evaluates a SQL expression and logs it for debugging
//!
//! Records pass through unchanged. The message expression is evaluated
//! per-record and logged at the configured level.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

pub struct LogProcessor {
    level: String,
    message_expr: String,
    /// Pre-built SQL that evaluates the message expression for each batch row
    sql: String,
}

impl LogProcessor {
    pub fn new(level: &str, message_expr: &str, cte: &str) -> Self {
        // Build SQL that evaluates the message expression for each row
        // and returns both the message and the original record
        let sql = format!(
            "{}SELECT COALESCE(jsonb_agg(jsonb_build_object(\
             '_msg', ({})::text, '_rec', _original\
             )), '[]'::jsonb) FROM _batch",
            cte, message_expr
        );

        Self {
            level: level.to_string(),
            message_expr: message_expr.to_string(),
            sql,
        }
    }

    /// Get the generated SQL for testing
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl Processor for LogProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.sql, &[batch_jsonb.into()]);

        let jsonb = result.map_err(|e| {
            format!(
                "Log processor failed (message: '{}'): {}",
                self.message_expr, e
            )
        })?;

        let items = match &jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => arr.clone(),
                None => return Ok(vec![]),
            },
            None => return Ok(vec![]),
        };

        // Log each message and collect original records
        let mut output = Vec::with_capacity(items.len());
        for item in &items {
            if let Some(msg) = item["_msg"].as_str() {
                match self.level.as_str() {
                    "warning" | "warn" => {
                        warning!("pg_streaming log: {}", msg);
                    }
                    _ => {
                        log!("pg_streaming log: {}", msg);
                    }
                }
            }
            if let Some(rec) = item.get("_rec") {
                output.push(rec.clone());
            }
        }

        Ok(output)
    }

    fn name(&self) -> &str {
        "log"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch_cte;

    #[test]
    fn test_log_sql_generation() {
        let proc = LogProcessor::new(
            "info",
            "'Processing order: ' || value_json->>'id'",
            batch_cte(),
        );
        let sql = proc.sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("'_msg'"));
        assert!(sql.contains("'Processing order: ' || value_json->>'id'"));
        assert!(sql.contains("'_rec', _original"));
    }

    #[test]
    fn test_log_empty_batch_passthrough() {
        let proc = LogProcessor::new("info", "'test'", batch_cte());
        let result = proc.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
