use pgrx::prelude::*;

/// Resolve template variables like {column_name} from a row JSON value.
/// When `sql_quote` is true, string values are wrapped in single quotes for SQL safety.
fn resolve_template_inner(template: &str, row: &serde_json::Value, sql_quote: bool) -> String {
    let mut result = template.to_string();
    while let Some(start) = result.find('{') {
        if let Some(end) = result[start..].find('}') {
            let field = &result[start + 1..start + end];
            let value = row
                .get(field)
                .map(|v| match v {
                    serde_json::Value::String(s) => {
                        if sql_quote {
                            format!("'{}'", s.replace('\'', "''"))
                        } else {
                            s.clone()
                        }
                    }
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "NULL".to_string(),
                    other => other.to_string(),
                })
                .unwrap_or_else(|| {
                    if sql_quote {
                        "NULL".to_string()
                    } else {
                        "unknown".to_string()
                    }
                });
            result = format!(
                "{}{}{}",
                &result[..start],
                value,
                &result[start + end + 1..]
            );
        } else {
            break;
        }
    }
    result
}

/// Resolve template for display purposes (no SQL quoting).
pub fn resolve_template(template: &str, row: &serde_json::Value) -> String {
    resolve_template_inner(template, row, false)
}

/// Evaluate a guard expression against a row's data.
/// Returns true if the guard passes, false otherwise.
pub fn evaluate_guard(guard_expr: &str, row: &serde_json::Value) -> bool {
    let resolved = resolve_template_inner(guard_expr, row, true);
    Spi::get_one::<bool>(&format!("SELECT {}", resolved))
        .ok()
        .flatten()
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_template_simple() {
        let row = serde_json::json!({"customer_id": "42", "region": "EU"});
        assert_eq!(resolve_template("ar:{customer_id}", &row), "ar:42");
    }

    #[test]
    fn test_resolve_template_multiple() {
        let row = serde_json::json!({"region": "EU", "kind": "sales"});
        assert_eq!(resolve_template("{region}:{kind}", &row), "EU:sales");
    }

    #[test]
    fn test_resolve_template_no_vars() {
        let row = serde_json::json!({});
        assert_eq!(resolve_template("static", &row), "static");
    }

    #[test]
    fn test_resolve_template_numeric() {
        let row = serde_json::json!({"id": 123});
        assert_eq!(resolve_template("item:{id}", &row), "item:123");
    }

    #[test]
    fn test_resolve_template_sql_quoting() {
        let row = serde_json::json!({"status": "active"});
        let resolved = resolve_template_inner("{status} = 'active'", &row, true);
        assert_eq!(resolved, "'active' = 'active'");
    }
}
