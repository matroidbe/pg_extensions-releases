//! SQL enrichment processor — executes a SQL query per record to enrich from any table
//!
//! For each record in the batch, evaluates the query with args extracted from the
//! record, then merges the result columns back into the record.

use crate::processor::Processor;
use crate::record::RecordBatch;
use pgrx::prelude::*;
use std::collections::HashMap;

pub struct SqlEnrichmentProcessor {
    query: String,
    /// Pre-built SQL that runs the enrichment for the entire batch at once
    batch_sql: String,
}

impl SqlEnrichmentProcessor {
    /// Create a SQL enrichment processor.
    /// `cte` is the batch CTE preamble (defines available columns).
    pub fn new(
        query: &str,
        args: &[String],
        result_map: &HashMap<String, String>,
        on_empty: &str,
        cte: &str,
    ) -> Self {
        // Build a batch SQL that:
        // 1. Unpacks the batch via the CTE
        // 2. For each record, runs the enrichment query via LATERAL join
        // 3. Merges enrichment results into the original record
        // 4. Returns the enriched batch as JSONB array
        let batch_sql = build_batch_enrichment_sql(query, args, result_map, on_empty, cte);

        Self {
            query: query.to_string(),
            batch_sql,
        }
    }

    /// Get the generated SQL for testing
    #[cfg(test)]
    pub fn sql(&self) -> &str {
        &self.batch_sql
    }
}

impl Processor for SqlEnrichmentProcessor {
    fn process(&self, batch: RecordBatch) -> Result<RecordBatch, String> {
        if batch.is_empty() {
            return Ok(batch);
        }

        let batch_json = serde_json::Value::Array(batch);
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.batch_sql, &[batch_jsonb.into()]);

        let jsonb = result
            .map_err(|e| format!("SQL enrichment failed (query: '{}'): {}", self.query, e))?;

        match jsonb {
            Some(jb) => match jb.0.as_array() {
                Some(arr) => Ok(arr.clone()),
                None => Ok(vec![]),
            },
            None => Ok(vec![]),
        }
    }

    fn name(&self) -> &str {
        "sql"
    }
}

/// Build the batch SQL for enrichment.
///
/// Strategy: Use a LATERAL subquery to run the enrichment query for each batch row,
/// then merge the enrichment columns into the original record via jsonb concatenation.
fn build_batch_enrichment_sql(
    query: &str,
    args: &[String],
    result_map: &HashMap<String, String>,
    on_empty: &str,
    cte: &str,
) -> String {
    // Build the LATERAL subquery with parameter substitution.
    // Replace $1, $2, etc. in the query with the arg expressions.
    let mut lateral_query = query.to_string();
    for (i, arg_expr) in args.iter().enumerate().rev() {
        // Replace $N with the expression referencing the batch row
        lateral_query = lateral_query.replace(&format!("${}", i + 1), &format!("({})", arg_expr));
    }

    // Build the jsonb_build_object for enrichment columns
    let enrich_exprs: Vec<String> = result_map
        .iter()
        .map(|(output_field, result_col)| format!("'{}', _enrich.{}", output_field, result_col))
        .collect();

    let enrich_obj = if enrich_exprs.is_empty() {
        "'{}'::jsonb".to_string()
    } else {
        format!("jsonb_build_object({})", enrich_exprs.join(", "))
    };

    // Handle on_empty: null (add NULLs), skip (filter out), error (fail)
    let join_type = match on_empty {
        "skip" => "JOIN", // inner join — drops records with no enrichment match
        _ => "LEFT JOIN", // null/error — keep all records
    };

    format!(
        "{cte}SELECT COALESCE(jsonb_agg(\
         _batch._original || {enrich_obj}\
         ), '[]'::jsonb) \
         FROM _batch \
         {join_type} LATERAL ({lateral_query} LIMIT 1) AS _enrich ON true",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::batch_cte;

    #[test]
    fn test_sql_enrichment_sql_generation() {
        let mut result_map = HashMap::new();
        result_map.insert("customer_name".to_string(), "name".to_string());
        result_map.insert("customer_tier".to_string(), "tier".to_string());

        let proc = SqlEnrichmentProcessor::new(
            "SELECT name, tier FROM customers WHERE id = $1",
            &["value_json->>'customer_id'".to_string()],
            &result_map,
            "null",
            batch_cte(),
        );

        let sql = proc.sql();
        assert!(sql.contains("WITH _batch AS"));
        assert!(sql.contains("LEFT JOIN LATERAL"));
        assert!(sql.contains("FROM customers WHERE id = (value_json->>'customer_id')"));
        assert!(sql.contains("jsonb_build_object("));
        assert!(sql.contains("_enrich.name"));
        assert!(sql.contains("_enrich.tier"));
        assert!(sql.contains("_batch._original ||"));
    }

    #[test]
    fn test_sql_enrichment_skip_mode() {
        let mut result_map = HashMap::new();
        result_map.insert("name".to_string(), "name".to_string());

        let proc = SqlEnrichmentProcessor::new(
            "SELECT name FROM users WHERE id = $1",
            &["value_json->>'user_id'".to_string()],
            &result_map,
            "skip",
            batch_cte(),
        );

        let sql = proc.sql();
        // skip mode uses inner JOIN (not LEFT JOIN)
        assert!(sql.contains("JOIN LATERAL"));
        assert!(!sql.contains("LEFT JOIN LATERAL"));
    }

    #[test]
    fn test_sql_enrichment_multiple_args() {
        let mut result_map = HashMap::new();
        result_map.insert("price".to_string(), "price".to_string());

        let proc = SqlEnrichmentProcessor::new(
            "SELECT price FROM products WHERE id = $1 AND region = $2",
            &[
                "value_json->>'product_id'".to_string(),
                "value_json->>'region'".to_string(),
            ],
            &result_map,
            "null",
            batch_cte(),
        );

        let sql = proc.sql();
        assert!(sql.contains("WHERE id = (value_json->>'product_id')"));
        assert!(sql.contains("AND region = (value_json->>'region')"));
    }

    #[test]
    fn test_sql_enrichment_empty_batch_passthrough() {
        let proc =
            SqlEnrichmentProcessor::new("SELECT 1", &[], &HashMap::new(), "null", batch_cte());
        let result = proc.process(vec![]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
