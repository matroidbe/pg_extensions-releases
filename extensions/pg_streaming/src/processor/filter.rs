//! Filter processor — evaluates a SQL boolean expression to keep/drop records

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

pub struct FilterProcessor {
    expression: String,
    sql: String,
}

impl FilterProcessor {
    /// Create a filter processor with a specific CTE preamble.
    /// The CTE defines what columns are available to the filter expression.
    pub fn new(expression: &str, cte: &str) -> Self {
        let sql = format!(
            "{}SELECT COALESCE(jsonb_agg(_original), '[]'::jsonb) FROM _batch WHERE {}",
            cte, expression
        );
        Self {
            expression: expression.to_string(),
            sql,
        }
    }

    /// Get the generated SQL for testing
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl Processor for FilterProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.sql, &[batch_jsonb.into()]);

        let jsonb =
            result.map_err(|e| format!("Filter expression '{}' failed: {}", self.expression, e))?;

        match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => Ok(arr.clone()),
                None => Ok(vec![]),
            },
            None => Ok(vec![]),
        }
    }

    fn name(&self) -> &str {
        "filter"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{batch_cte, TopicShape};

    #[test]
    fn test_filter_sql_generation() {
        let f = FilterProcessor::new("value_json->>'region' = 'US'", batch_cte());
        let sql = f.sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("value_json->>'region' = 'US'"));
        assert!(sql.contains("jsonb_agg(_original)"));
        assert!(sql.contains("FROM _batch WHERE"));
    }

    #[test]
    fn test_filter_empty_batch_passthrough() {
        let f = FilterProcessor::new("true", batch_cte());
        let result = f.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_filter_sql_with_typed_topic() {
        let shape = TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("amount".to_string(), "numeric".to_string()),
                ("region".to_string(), "text".to_string()),
            ],
        };
        let f = FilterProcessor::new("region = 'US' AND amount > 100", &shape.batch_cte());
        let sql = f.sql();
        assert!(sql.contains("AS id"));
        assert!(sql.contains("AS amount"));
        assert!(sql.contains("AS region"));
        assert!(sql.contains("region = 'US' AND amount > 100"));
        assert!(sql.contains("FROM _batch WHERE"));
    }
}
