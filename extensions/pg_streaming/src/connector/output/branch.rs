//! Branch output connector — routes records to different outputs based on SQL conditions

use crate::connector::OutputConnector;
use crate::record::RecordBatch;
use pgrx::prelude::*;

/// A single route: a SQL boolean condition and its output connector
pub struct Route {
    /// SQL boolean expression evaluated against each record's fields.
    /// None means this is the default/catch-all route.
    pub condition: Option<String>,
    pub output: Box<dyn OutputConnector>,
}

/// Branch output — evaluates conditions and routes records to matching outputs.
/// Records are evaluated against routes in order. A record is sent to every
/// route whose condition matches (fan-out). If a default route (condition=None)
/// is present, it receives records that matched no conditional route.
pub struct BranchOutput {
    routes: Vec<Route>,
    /// Pre-built SQL for routing. Takes a JSONB array parameter ($1) and returns
    /// rows with (route_index, record). Built once at compile time.
    route_sql: String,
}

impl BranchOutput {
    pub fn new(routes: Vec<Route>) -> Self {
        let route_sql = build_route_sql(&routes);
        Self { routes, route_sql }
    }

    #[cfg(test)]
    pub fn route_sql(&self) -> &str {
        &self.route_sql
    }
}

impl OutputConnector for BranchOutput {
    fn write(&self, records: &RecordBatch) -> Result<(), String> {
        if records.is_empty() || self.routes.is_empty() {
            return Ok(());
        }

        // Use SQL to evaluate conditions and get route assignments as a single JSONB array
        let batch_json = serde_json::Value::Array(records.clone());
        let batch_jsonb = pgrx::JsonB(batch_json);

        let result = Spi::get_one_with_args::<pgrx::JsonB>(&self.route_sql, &[batch_jsonb.into()]);

        let jsonb = result
            .map_err(|e| format!("Branch routing query failed: {}", e))?
            .unwrap_or(pgrx::JsonB(serde_json::json!([])));

        let rows = jsonb
            .0
            .as_array()
            .ok_or("Branch routing returned non-array")?;

        // Collect records per route
        let mut route_batches: Vec<RecordBatch> = self.routes.iter().map(|_| Vec::new()).collect();

        for row in rows {
            let idx = row["i"].as_i64().unwrap_or(0) as usize;
            let record = &row["r"];
            if let Some(batch) = route_batches.get_mut(idx) {
                batch.push(record.clone());
            }
        }

        // Write each route's batch to its output
        for (i, batch) in route_batches.iter().enumerate() {
            if !batch.is_empty() {
                self.routes[i]
                    .output
                    .write(batch)
                    .map_err(|e| format!("Branch route {} write failed: {}", i, e))?;
            }
        }

        Ok(())
    }
}

/// Build a SQL query that takes a JSONB array ($1) and returns a JSONB array
/// of `{"i": route_index, "r": record}` objects via `jsonb_agg`.
/// Uses UNION ALL for each conditional route plus an EXCEPT-based default route.
fn build_route_sql(routes: &[Route]) -> String {
    if routes.is_empty() {
        return "SELECT '[]'::jsonb".to_string();
    }

    let mut parts: Vec<String> = Vec::new();
    let mut default_index: Option<usize> = None;

    for (i, route) in routes.iter().enumerate() {
        if let Some(ref cond) = route.condition {
            parts.push(format!(
                "SELECT {}::int AS i, r AS r \
                 FROM jsonb_array_elements($1) AS r \
                 WHERE ({})",
                i, cond
            ));
        } else {
            default_index = Some(i);
        }
    }

    // If there's a default route, it gets records not matched by any conditional route
    if let Some(def_idx) = default_index {
        let conditional_conditions: Vec<String> = routes
            .iter()
            .filter_map(|r| r.condition.as_ref())
            .map(|c| format!("({})", c))
            .collect();

        if conditional_conditions.is_empty() {
            // Default route only — gets all records
            parts.push(format!(
                "SELECT {}::int AS i, r AS r \
                 FROM jsonb_array_elements($1) AS r",
                def_idx
            ));
        } else {
            let any_match = conditional_conditions.join(" OR ");
            parts.push(format!(
                "SELECT {}::int AS i, r AS r \
                 FROM jsonb_array_elements($1) AS r \
                 WHERE NOT ({})",
                def_idx, any_match
            ));
        }
    }

    let inner = parts.join(" UNION ALL ");
    format!(
        "SELECT COALESCE(jsonb_agg(jsonb_build_object('i', _routed.i, 'r', _routed.r)), \
         '[]'::jsonb) FROM ({}) AS _routed",
        inner
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::DropOutput;

    #[test]
    fn test_route_sql_single_condition() {
        let routes = vec![Route {
            condition: Some("r->>'region' = 'US'".to_string()),
            output: Box::new(DropOutput),
        }];
        let sql = build_route_sql(&routes);
        assert!(sql.contains("0::int AS i"));
        assert!(sql.contains("r->>'region' = 'US'"));
        assert!(sql.contains("jsonb_agg"));
        assert!(!sql.contains("UNION ALL"));
    }

    #[test]
    fn test_route_sql_multiple_conditions() {
        let routes = vec![
            Route {
                condition: Some("r->>'region' = 'US'".to_string()),
                output: Box::new(DropOutput),
            },
            Route {
                condition: Some("r->>'region' = 'EU'".to_string()),
                output: Box::new(DropOutput),
            },
        ];
        let sql = build_route_sql(&routes);
        assert!(sql.contains("0::int AS i"));
        assert!(sql.contains("1::int AS i"));
        assert!(sql.contains("UNION ALL"));
    }

    #[test]
    fn test_route_sql_with_default() {
        let routes = vec![
            Route {
                condition: Some("r->>'region' = 'US'".to_string()),
                output: Box::new(DropOutput),
            },
            Route {
                condition: None, // default
                output: Box::new(DropOutput),
            },
        ];
        let sql = build_route_sql(&routes);
        // Default route should use NOT (condition)
        assert!(sql.contains("1::int AS i"));
        assert!(sql.contains("NOT ("));
    }

    #[test]
    fn test_route_sql_default_only() {
        let routes = vec![Route {
            condition: None,
            output: Box::new(DropOutput),
        }];
        let sql = build_route_sql(&routes);
        assert!(sql.contains("0::int AS i"));
        // Should select all records (no WHERE filter inside the UNION part)
        assert!(!sql.contains("WHERE NOT"));
        assert!(!sql.contains("WHERE ("));
    }

    #[test]
    fn test_route_sql_empty_routes() {
        let sql = build_route_sql(&[]);
        assert_eq!(sql, "SELECT '[]'::jsonb");
    }

    #[test]
    fn test_route_sql_fan_out() {
        // Records can match multiple routes (fan-out)
        let routes = vec![
            Route {
                condition: Some("(r->>'amount')::numeric > 100".to_string()),
                output: Box::new(DropOutput),
            },
            Route {
                condition: Some("r->>'region' = 'US'".to_string()),
                output: Box::new(DropOutput),
            },
        ];
        let sql = build_route_sql(&routes);
        // Both conditions are independent UNION ALL — a US order > 100 hits both
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("(r->>'amount')::numeric > 100"));
        assert!(sql.contains("r->>'region' = 'US'"));
    }

    #[test]
    fn test_branch_output_construction() {
        let routes = vec![
            Route {
                condition: Some("r->>'type' = 'A'".to_string()),
                output: Box::new(DropOutput),
            },
            Route {
                condition: None,
                output: Box::new(DropOutput),
            },
        ];
        let branch = BranchOutput::new(routes);
        assert!(branch.route_sql().contains("UNION ALL"));
    }
}
