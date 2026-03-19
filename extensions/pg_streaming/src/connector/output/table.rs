//! Table output connector — writes records directly to PostgreSQL tables
//!
//! Three write modes:
//! - append: INSERT only (for fact tables, event logs)
//! - upsert: INSERT ... ON CONFLICT DO UPDATE (for dimension tables)
//! - replace: DELETE matching rows then INSERT (for full recomputation)

use crate::connector::OutputConnector;
use crate::record::RecordBatch;
use pgrx::prelude::*;

pub struct TableOutput {
    table_name: String,
    mode: String,
    key_columns: Vec<String>,
    tracked_columns: Vec<String>,
}

impl TableOutput {
    pub fn new(table_name: &str, mode: &str, key: Option<&str>, tracked: &[String]) -> Self {
        let key_columns: Vec<String> = key
            .map(|k| k.split(',').map(|s| s.trim().to_string()).collect())
            .unwrap_or_default();

        Self {
            table_name: table_name.to_string(),
            mode: mode.to_string(),
            key_columns,
            tracked_columns: tracked.to_vec(),
        }
    }

    /// Extract ordered column names from the first record
    fn resolve_columns(&self, record: &serde_json::Value) -> Vec<String> {
        match record.as_object() {
            Some(obj) => obj.keys().cloned().collect(),
            None => vec![],
        }
    }

    /// Build an INSERT statement for a single record
    fn build_insert(&self, record: &serde_json::Value, columns: &[String]) -> Option<String> {
        let mut values = Vec::new();
        for col in columns {
            if let Some(val) = record.get(col) {
                values.push(json_value_to_sql_literal(val));
            } else {
                values.push("NULL".to_string());
            }
        }

        if values.is_empty() {
            return None;
        }

        Some(format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.table_name,
            columns.join(", "),
            values.join(", ")
        ))
    }

    /// Build an UPSERT (INSERT ... ON CONFLICT DO UPDATE) for a single record
    fn build_upsert(&self, record: &serde_json::Value, columns: &[String]) -> Option<String> {
        if self.key_columns.is_empty() {
            return None;
        }

        let mut values = Vec::new();
        for col in columns {
            if let Some(val) = record.get(col) {
                values.push(json_value_to_sql_literal(val));
            } else {
                values.push("NULL".to_string());
            }
        }

        if values.is_empty() {
            return None;
        }

        let update_clauses: Vec<String> = columns
            .iter()
            .filter(|c| !self.key_columns.contains(c))
            .map(|c| format!("{} = EXCLUDED.{}", c, c))
            .collect();

        let conflict_cols = self.key_columns.join(", ");

        Some(format!(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}",
            self.table_name,
            columns.join(", "),
            values.join(", "),
            conflict_cols,
            if update_clauses.is_empty() {
                // All columns are key columns — just do nothing
                format!("{} = EXCLUDED.{}", self.key_columns[0], self.key_columns[0])
            } else {
                update_clauses.join(", ")
            }
        ))
    }

    /// Build a batch SCD Type 2 writable CTE for a set of records.
    /// Detects changes against tracked columns, closes old current rows,
    /// and inserts new current rows — all in one atomic SQL statement.
    fn build_scd2_batch(&self, records: &RecordBatch, columns: &[String]) -> Option<String> {
        if self.key_columns.is_empty() || self.tracked_columns.is_empty() {
            return None;
        }

        // Build the batch values list: (col1, col2, ...)
        let mut values_rows: Vec<String> = Vec::new();
        for record in records {
            let row_vals: Vec<String> = columns
                .iter()
                .map(|col| {
                    record
                        .get(col)
                        .map(json_value_to_sql_literal)
                        .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            values_rows.push(format!("({})", row_vals.join(", ")));
        }

        if values_rows.is_empty() {
            return None;
        }

        // Column definitions for the VALUES clause with type casts on first row
        let col_list = columns.join(", ");

        // Business key join condition: dim.key = _incoming.key
        let key_join: Vec<String> = self
            .key_columns
            .iter()
            .map(|k| format!("dim.{k} = _incoming.{k}"))
            .collect();
        let key_join_clause = key_join.join(" AND ");

        // Change detection: any tracked column differs
        let change_conditions: Vec<String> = self
            .tracked_columns
            .iter()
            .map(|c| format!("dim.{c} IS DISTINCT FROM _incoming.{c}"))
            .collect();
        let change_filter = change_conditions.join(" OR ");

        // All data columns except SCD metadata (valid_from, valid_to, is_current)
        let scd_meta = ["valid_from", "valid_to", "is_current"];
        let data_columns: Vec<&String> = columns
            .iter()
            .filter(|c| !scd_meta.contains(&c.as_str()))
            .collect();
        let data_col_list = data_columns
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        Some(format!(
            "WITH _incoming({col_list}) AS (VALUES {values}), \
             _changes AS (\
               SELECT _incoming.* \
               FROM _incoming \
               LEFT JOIN {table} dim ON {key_join} AND dim.is_current = true \
               WHERE dim.{first_key} IS NULL OR ({change_filter})\
             ), \
             _close AS (\
               UPDATE {table} SET valid_to = now(), is_current = false \
               FROM _changes c \
               WHERE {close_join} AND {table}.is_current = true \
               RETURNING {table}.{first_key}\
             ) \
             INSERT INTO {table} ({data_cols}, valid_from, valid_to, is_current) \
             SELECT {data_select}, now(), '9999-12-31'::timestamptz, true \
             FROM _changes",
            col_list = col_list,
            values = values_rows.join(", "),
            table = self.table_name,
            key_join = key_join_clause,
            first_key = self.key_columns[0],
            change_filter = change_filter,
            close_join = self
                .key_columns
                .iter()
                .map(|k| format!("{table}.{k} = c.{k}", table = self.table_name))
                .collect::<Vec<_>>()
                .join(" AND "),
            data_cols = data_col_list,
            data_select = data_columns
                .iter()
                .map(|c| c.as_str())
                .collect::<Vec<_>>()
                .join(", "),
        ))
    }

    /// Build a DELETE + INSERT for replace mode
    fn build_replace(
        &self,
        record: &serde_json::Value,
        columns: &[String],
    ) -> Option<(String, String)> {
        if self.key_columns.is_empty() {
            // No key — just insert (same as append)
            return self
                .build_insert(record, columns)
                .map(|ins| (String::new(), ins));
        }

        let where_clauses: Vec<String> = self
            .key_columns
            .iter()
            .filter_map(|k| {
                record
                    .get(k)
                    .map(|v| format!("{} = {}", k, json_value_to_sql_literal(v)))
            })
            .collect();

        let delete = format!(
            "DELETE FROM {} WHERE {}",
            self.table_name,
            where_clauses.join(" AND ")
        );

        let insert = self.build_insert(record, columns)?;

        Some((delete, insert))
    }
}

impl OutputConnector for TableOutput {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        if records.is_empty() {
            return Ok(());
        }

        // Resolve columns from first record
        let columns = self.resolve_columns(&records[0]);
        if columns.is_empty() {
            return Err("Record has no fields".to_string());
        }

        // SCD Type 2 processes the entire batch at once (writable CTE)
        if self.mode == "scd2" {
            let query = self
                .build_scd2_batch(records, &columns)
                .ok_or("Failed to build SCD Type 2 query (missing key or tracked columns)")?;
            return Spi::run(&query)
                .map_err(|e| format!("SCD Type 2 write failed on '{}': {}", self.table_name, e));
        }

        for record in records {
            match self.mode.as_str() {
                "upsert" => {
                    let query = self
                        .build_upsert(record, &columns)
                        .ok_or("Failed to build upsert query")?;
                    Spi::run(&query).map_err(|e| {
                        format!("Failed to upsert into '{}': {}", self.table_name, e)
                    })?;
                }
                "replace" => {
                    let (delete, insert) = self
                        .build_replace(record, &columns)
                        .ok_or("Failed to build replace query")?;
                    if !delete.is_empty() {
                        Spi::run(&delete).map_err(|e| {
                            format!("Failed to delete from '{}': {}", self.table_name, e)
                        })?;
                    }
                    Spi::run(&insert).map_err(|e| {
                        format!("Failed to insert into '{}': {}", self.table_name, e)
                    })?;
                }
                _ => {
                    // "append" (default)
                    let query = self
                        .build_insert(record, &columns)
                        .ok_or("Failed to build insert query")?;
                    Spi::run(&query).map_err(|e| {
                        format!("Failed to insert into '{}': {}", self.table_name, e)
                    })?;
                }
            }
        }

        Ok(())
    }
}

/// Convert a JSON value to a safe SQL literal string
fn json_value_to_sql_literal(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            let json_str = serde_json::to_string(val).unwrap_or_default();
            format!("'{}'::jsonb", json_str.replace('\'', "''"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_output_append_insert() {
        let output = TableOutput::new("analytics.orders", "append", None, &[]);
        let record = serde_json::json!({
            "order_id": "ord-1",
            "amount": 99.99,
            "region": "US"
        });
        let columns = output.resolve_columns(&record);
        let query = output.build_insert(&record, &columns).unwrap();
        assert!(query.starts_with("INSERT INTO analytics.orders"));
        assert!(query.contains("'ord-1'"));
        assert!(query.contains("99.99"));
    }

    #[test]
    fn test_table_output_upsert() {
        let output = TableOutput::new("dim.customers", "upsert", Some("customer_id"), &[]);
        let record = serde_json::json!({
            "customer_id": "cust-1",
            "name": "Alice",
            "email": "alice@example.com"
        });
        let columns = output.resolve_columns(&record);
        let query = output.build_upsert(&record, &columns).unwrap();
        assert!(query.contains("INSERT INTO dim.customers"));
        assert!(query.contains("ON CONFLICT (customer_id)"));
        assert!(query.contains("DO UPDATE SET"));
        assert!(query.contains("name = EXCLUDED.name"));
        assert!(query.contains("email = EXCLUDED.email"));
    }

    #[test]
    fn test_table_output_upsert_composite_key() {
        let output = TableOutput::new("facts.metrics", "upsert", Some("region, bucket"), &[]);
        let record = serde_json::json!({
            "region": "US",
            "bucket": "2024-01-01",
            "revenue": 1000
        });
        let columns = output.resolve_columns(&record);
        let query = output.build_upsert(&record, &columns).unwrap();
        assert!(query.contains("ON CONFLICT (region, bucket)"));
        assert!(query.contains("revenue = EXCLUDED.revenue"));
    }

    #[test]
    fn test_table_output_replace() {
        let output = TableOutput::new("staging.data", "replace", Some("id"), &[]);
        let record = serde_json::json!({
            "id": 42,
            "value": "updated"
        });
        let columns = output.resolve_columns(&record);
        let (delete, insert) = output.build_replace(&record, &columns).unwrap();
        assert!(delete.contains("DELETE FROM staging.data WHERE id = 42"));
        assert!(insert.contains("INSERT INTO staging.data"));
    }

    #[test]
    fn test_table_output_replace_no_key() {
        let output = TableOutput::new("log.events", "replace", None, &[]);
        let record = serde_json::json!({"msg": "hello"});
        let columns = output.resolve_columns(&record);
        let (delete, insert) = output.build_replace(&record, &columns).unwrap();
        assert!(delete.is_empty());
        assert!(insert.contains("INSERT INTO log.events"));
    }

    #[test]
    fn test_json_literal_escaping() {
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(null)), "NULL");
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(true)), "true");
        assert_eq!(json_value_to_sql_literal(&serde_json::json!(42)), "42");
        assert_eq!(
            json_value_to_sql_literal(&serde_json::json!("O'Brien")),
            "'O''Brien'"
        );
    }

    // =========================================================================
    // SCD Type 2 tests
    // =========================================================================

    #[test]
    fn test_scd2_batch_sql_structure() {
        let tracked = vec!["name".to_string(), "email".to_string()];
        let output = TableOutput::new("dim.customers", "scd2", Some("customer_id"), &tracked);
        let records = vec![serde_json::json!({
            "customer_id": "cust-1",
            "name": "Alice",
            "email": "alice@example.com"
        })];
        let columns = output.resolve_columns(&records[0]);
        let sql = output.build_scd2_batch(&records, &columns).unwrap();

        // Change detection CTE
        assert!(sql.contains("_incoming"));
        assert!(sql.contains("_changes"));
        // Close old current rows
        assert!(sql.contains("_close"));
        assert!(sql.contains("UPDATE dim.customers SET valid_to = now(), is_current = false"));
        // Insert new current rows
        assert!(sql.contains("INSERT INTO dim.customers"));
        assert!(sql.contains("valid_from, valid_to, is_current"));
        assert!(sql.contains("'9999-12-31'::timestamptz, true"));
    }

    #[test]
    fn test_scd2_change_detection() {
        let tracked = vec!["name".to_string(), "email".to_string()];
        let output = TableOutput::new("dim.customers", "scd2", Some("customer_id"), &tracked);
        let records = vec![serde_json::json!({
            "customer_id": "cust-1",
            "name": "Alice",
            "email": "alice@example.com"
        })];
        let columns = output.resolve_columns(&records[0]);
        let sql = output.build_scd2_batch(&records, &columns).unwrap();

        // Should detect changes on tracked columns
        assert!(sql.contains("dim.name IS DISTINCT FROM _incoming.name"));
        assert!(sql.contains("dim.email IS DISTINCT FROM _incoming.email"));
        // Should join on business key
        assert!(sql.contains("dim.customer_id = _incoming.customer_id"));
        assert!(sql.contains("dim.is_current = true"));
    }

    #[test]
    fn test_scd2_multiple_records() {
        let tracked = vec!["status".to_string()];
        let output = TableOutput::new("dim.products", "scd2", Some("product_id"), &tracked);
        let records = vec![
            serde_json::json!({"product_id": "p1", "status": "active"}),
            serde_json::json!({"product_id": "p2", "status": "discontinued"}),
        ];
        let columns = output.resolve_columns(&records[0]);
        let sql = output.build_scd2_batch(&records, &columns).unwrap();

        // Both records in VALUES clause
        assert!(sql.contains("'p1'"));
        assert!(sql.contains("'p2'"));
        assert!(sql.contains("'active'"));
        assert!(sql.contains("'discontinued'"));
    }

    #[test]
    fn test_scd2_excludes_metadata_from_data_insert() {
        let tracked = vec!["name".to_string()];
        let output = TableOutput::new("dim.customers", "scd2", Some("customer_id"), &tracked);
        let records = vec![serde_json::json!({
            "customer_id": "cust-1",
            "name": "Alice"
        })];
        let columns = output.resolve_columns(&records[0]);
        let sql = output.build_scd2_batch(&records, &columns).unwrap();

        // The INSERT should add valid_from, valid_to, is_current as separate columns
        // not include them in the data column select
        assert!(sql.contains("now(), '9999-12-31'::timestamptz, true"));
    }

    #[test]
    fn test_scd2_missing_key_returns_none() {
        let tracked = vec!["name".to_string()];
        let output = TableOutput::new("dim.customers", "scd2", None, &tracked);
        let records = vec![serde_json::json!({"name": "Alice"})];
        let columns = output.resolve_columns(&records[0]);
        assert!(output.build_scd2_batch(&records, &columns).is_none());
    }

    #[test]
    fn test_scd2_missing_tracked_returns_none() {
        let output = TableOutput::new("dim.customers", "scd2", Some("customer_id"), &[]);
        let records = vec![serde_json::json!({"customer_id": "c1", "name": "Alice"})];
        let columns = output.resolve_columns(&records[0]);
        assert!(output.build_scd2_batch(&records, &columns).is_none());
    }

    #[test]
    fn test_scd2_composite_key() {
        let tracked = vec!["name".to_string()];
        let output = TableOutput::new("dim.locations", "scd2", Some("country, city"), &tracked);
        let records = vec![serde_json::json!({
            "country": "BE",
            "city": "Brussels",
            "name": "Brussels Capital"
        })];
        let columns = output.resolve_columns(&records[0]);
        let sql = output.build_scd2_batch(&records, &columns).unwrap();

        // Composite key join
        assert!(sql.contains("dim.country = _incoming.country"));
        assert!(sql.contains("dim.city = _incoming.city"));
        // Close join should also use composite key
        assert!(sql.contains("dim.locations.country = c.country"));
        assert!(sql.contains("dim.locations.city = c.city"));
    }
}
