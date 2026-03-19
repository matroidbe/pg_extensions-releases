//! Unnest (flatMap) processor — explodes a JSONB array into individual records
//!
//! Each record's array field is expanded via LATERAL jsonb_array_elements(),
//! producing one output record per array element. All existing fields are
//! preserved and the array element is added under the configured field name.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;

pub struct UnnestProcessor {
    array_expr: String,
    as_field: String,
    sql: String,
}

impl UnnestProcessor {
    pub fn new(array_expr: &str, as_field: &str, cte: &str) -> Self {
        let sql = format!(
            "{}SELECT COALESCE(\
             jsonb_agg(_original || jsonb_build_object('{}', elem)), \
             '[]'::jsonb) \
             FROM _batch, \
             LATERAL jsonb_array_elements({}) AS elem",
            cte, as_field, array_expr
        );
        Self {
            array_expr: array_expr.to_string(),
            as_field: as_field.to_string(),
            sql,
        }
    }

    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl Processor for UnnestProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.sql, &[batch_jsonb.into()]);

        let jsonb = result.map_err(|e| {
            format!(
                "Unnest processor failed (array: '{}', as: '{}'): {}",
                self.array_expr, self.as_field, e
            )
        })?;

        match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => Ok(arr.clone()),
                None => Ok(vec![]),
            },
            None => Ok(vec![]),
        }
    }

    fn name(&self) -> &str {
        "unnest"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{batch_cte, TopicShape};

    #[test]
    fn test_unnest_sql_generation() {
        let u = UnnestProcessor::new("value_json->'items'", "item", batch_cte());
        let sql = u.sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("jsonb_build_object('item', elem)"));
        assert!(sql.contains("jsonb_array_elements(value_json->'items')"));
        assert!(sql.contains("LATERAL"));
    }

    #[test]
    fn test_unnest_empty_batch_passthrough() {
        let u = UnnestProcessor::new("value_json->'items'", "item", batch_cte());
        let result = u.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_unnest_sql_with_typed_topic() {
        let shape = TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("items".to_string(), "jsonb".to_string()),
            ],
        };
        let u = UnnestProcessor::new("items", "item", &shape.batch_cte());
        let sql = u.sql();
        assert!(sql.contains("AS id"));
        assert!(sql.contains("AS items"));
        assert!(sql.contains("jsonb_array_elements(items)"));
    }
}
