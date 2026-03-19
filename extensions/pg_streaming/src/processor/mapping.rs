//! Mapping processor — transforms records by evaluating SQL expressions for each field

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::collections::HashMap;

pub struct MappingProcessor {
    fields: HashMap<String, String>,
    sql: String,
}

impl MappingProcessor {
    /// Create a mapping processor with a specific CTE preamble.
    /// The CTE defines what columns are available to the mapping expressions.
    pub fn new(fields: &HashMap<String, String>, cte: &str) -> Self {
        // Build jsonb_build_object arguments: 'field_name', expression, ...
        let field_exprs: Vec<String> = fields
            .iter()
            .map(|(name, expr)| format!("'{}', {}", name, expr))
            .collect();

        let sql = format!(
            "{}SELECT COALESCE(jsonb_agg(jsonb_build_object({})), '[]'::jsonb) FROM _batch",
            cte,
            field_exprs.join(", ")
        );

        Self {
            fields: fields.clone(),
            sql,
        }
    }

    /// Get the generated SQL for testing
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.sql
    }
}

impl Processor for MappingProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.sql, &[batch_jsonb.into()]);

        let jsonb = result.map_err(|e| {
            format!(
                "Mapping processor failed (fields: {:?}): {}",
                self.fields.keys().collect::<Vec<_>>(),
                e
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
        "mapping"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{batch_cte, TopicShape};

    #[test]
    fn test_mapping_sql_generation() {
        let mut fields = HashMap::new();
        fields.insert("order_id".to_string(), "value_json->>'id'".to_string());
        fields.insert(
            "total".to_string(),
            "(value_json->>'amount')::numeric".to_string(),
        );

        let m = MappingProcessor::new(&fields, batch_cte());
        let sql = m.sql();

        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("jsonb_build_object("));
        assert!(sql.contains("jsonb_agg("));
        assert!(sql.contains("'order_id'"));
        assert!(sql.contains("value_json->>'id'"));
        assert!(sql.contains("'total'"));
        assert!(sql.contains("(value_json->>'amount')::numeric"));
    }

    #[test]
    fn test_mapping_empty_batch_passthrough() {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), "offset_id".to_string());
        let m = MappingProcessor::new(&fields, batch_cte());
        let result = m.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_mapping_sql_with_typed_topic() {
        let shape = TopicShape::Typed {
            source_table: "public.orders".to_string(),
            offset_column: "id".to_string(),
            columns: vec![
                ("id".to_string(), "bigint".to_string()),
                ("customer_id".to_string(), "text".to_string()),
                ("amount".to_string(), "numeric".to_string()),
            ],
        };
        let mut fields = HashMap::new();
        fields.insert("order_id".to_string(), "id".to_string());
        fields.insert("total".to_string(), "amount".to_string());

        let m = MappingProcessor::new(&fields, &shape.batch_cte());
        let sql = m.sql();

        // Should use typed topic CTE columns
        assert!(sql.contains("AS id"));
        assert!(sql.contains("AS amount"));
        // Mapping should reference columns directly
        assert!(sql.contains("'order_id'"));
        assert!(sql.contains("'total'"));
    }
}
