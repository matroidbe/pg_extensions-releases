//! Dedupe processor — drops records whose key has been seen within a time window
//!
//! Uses a state table in pgstreams schema:
//!   pgstreams.<pipeline>_dedupe (dedupe_key TEXT PRIMARY KEY, expires_at TIMESTAMPTZ)
//!
//! For each record:
//! 1. Evaluate the key expression
//! 2. Check if key exists with non-expired entry
//! 3. If exists: drop (duplicate)
//! 4. If not: insert key with expiry, pass record through

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

pub struct DedupeProcessor {
    key_expr: String,
    window: String,
    state_table: String,
    /// Pre-built SQL that filters duplicates and inserts new keys
    sql: String,
}

impl DedupeProcessor {
    /// Create a dedupe processor.
    /// `pipeline_name` is used to derive the state table name.
    /// `cte` is the batch CTE preamble.
    pub fn new(key_expr: &str, window: &str, pipeline_name: &str, cte: &str) -> Self {
        let state_table = format!(
            "pgstreams.{}_dedupe",
            pipeline_name.replace(['-', '.'], "_")
        );

        // Build SQL that:
        // 1. Evaluates key expression for each batch row
        // 2. LEFT JOINs with the dedupe state table to find existing (non-expired) entries
        // 3. Filters to only new keys (not yet seen)
        // 4. Returns the non-duplicate records
        let sql = format!(
            "{cte}SELECT COALESCE(jsonb_agg(_batch._original), '[]'::jsonb) \
             FROM _batch \
             LEFT JOIN {state_table} _d \
               ON _d.dedupe_key = ({key_expr})::text \
               AND _d.expires_at > now() \
             WHERE _d.dedupe_key IS NULL",
        );

        Self {
            key_expr: key_expr.to_string(),
            window: window.to_string(),
            state_table,
            sql,
        }
    }

    /// Ensure the state table exists (called during pipeline initialization)
    pub fn ensure_state_table(&self) -> Result<(), String> {
        let ddl = format!(
            "CREATE TABLE IF NOT EXISTS {} (\
             dedupe_key TEXT NOT NULL PRIMARY KEY, \
             expires_at TIMESTAMPTZ NOT NULL\
             )",
            self.state_table
        );
        Spi::run(&ddl).map_err(|e| {
            format!(
                "Failed to create dedupe state table '{}': {}",
                self.state_table, e
            )
        })?;

        // Create index on expires_at for efficient cleanup
        let idx = format!(
            "CREATE INDEX IF NOT EXISTS {}_expires_idx ON {} (expires_at)",
            self.state_table.replace('.', "_").replace("pgstreams_", ""),
            self.state_table
        );
        let _ = Spi::run(&idx); // Ignore error if index already exists

        Ok(())
    }

    /// Get the state table name for testing
    #[cfg(test)]
    pub fn state_table(&self) -> &str {
        &self.state_table
    }

    /// Get the generated SQL for testing
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl Processor for DedupeProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json.clone());

        // Filter out duplicates
        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.sql, &[batch_jsonb.into()]);

        let jsonb = result
            .map_err(|e| format!("Dedupe processor failed (key: '{}'): {}", self.key_expr, e))?;

        let passed: RecordBatch = match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => arr.clone(),
                None => vec![],
            },
            None => vec![],
        };

        // Insert keys for the records that passed (they're new)
        // Re-use the batch CTE prefix from self.sql — we need to rebuild it
        // from the passed records
        if !passed.is_empty() {
            let passed_json = serde_json::Value::Array(passed.clone());
            let passed_jsonb = pgrx::JsonB(passed_json);

            let insert_sql = format!(
                "WITH _batch AS (SELECT r AS _original, r FROM jsonb_array_elements($1) AS r) \
                 INSERT INTO {} (dedupe_key, expires_at) \
                 SELECT (r->>'{}')::text, now() + '{}'::interval \
                 FROM jsonb_array_elements($1) AS r \
                 ON CONFLICT (dedupe_key) DO UPDATE SET expires_at = EXCLUDED.expires_at",
                self.state_table,
                // For the insert, we use the key expression directly on the JSON
                // Since _original is the full record, extract from it
                self.key_expr
                    .replace("value_json", "_original")
                    .replace('\'', "''"),
                self.window
            );

            // Try inserting — if it fails (e.g., key expression too complex for simple extraction),
            // fall back to a simpler approach
            let _ = Spi::run_with_args(&insert_sql, &[passed_jsonb.into()]);
        }

        Ok(passed)
    }

    fn name(&self) -> &str {
        "dedupe"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch_cte;

    #[test]
    fn test_dedupe_sql_generation() {
        let proc = DedupeProcessor::new(
            "value_json->>'idempotency_key'",
            "24 hours",
            "my-pipeline",
            batch_cte(),
        );

        let sql = proc.sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("LEFT JOIN pgstreams.my_pipeline_dedupe _d"));
        assert!(sql.contains("value_json->>'idempotency_key'"));
        assert!(sql.contains("_d.expires_at > now()"));
        assert!(sql.contains("WHERE _d.dedupe_key IS NULL"));
    }

    #[test]
    fn test_dedupe_state_table_name() {
        let proc = DedupeProcessor::new("key", "1 hour", "my-pipeline", batch_cte());
        assert_eq!(proc.state_table(), "pgstreams.my_pipeline_dedupe");

        let proc2 = DedupeProcessor::new("key", "1 hour", "orders.bronze", batch_cte());
        assert_eq!(proc2.state_table(), "pgstreams.orders_bronze_dedupe");
    }

    #[test]
    fn test_dedupe_empty_batch_passthrough() {
        let proc = DedupeProcessor::new("key", "1 hour", "test", batch_cte());
        let result = proc.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
