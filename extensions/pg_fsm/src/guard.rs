use pgrx::prelude::*;

/// Resolve a `{name}` placeholder.
///
/// In SQL-eval context (guard expressions), recognises:
/// - `{current_user}` / `{current_role}` → bare SQL identifier reference.
/// - `{session.<key>}` → `current_setting('pgfsm.session.<key>', true)`.
///
/// In display context (action templates), these names fall through to a row column
/// lookup so existing templates continue to work.
fn substitute_placeholder(field: &str, row: &serde_json::Value, sql_quote: bool) -> String {
    if sql_quote {
        match field {
            "current_user" => return "current_user".to_string(),
            "current_role" => return "current_role".to_string(),
            f if f.starts_with("session.") => {
                let key = &f[8..];
                let escaped = key.replace('\'', "''");
                return format!("current_setting('pgfsm.session.{}', true)", escaped);
            }
            _ => {}
        }
    }
    row.get(field)
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
        })
}

/// Resolve template variables like {column_name} from a row JSON value.
/// When `sql_quote` is true, string values are wrapped in single quotes for SQL safety.
fn resolve_template_inner(template: &str, row: &serde_json::Value, sql_quote: bool) -> String {
    let mut result = template.to_string();
    while let Some(start) = result.find('{') {
        if let Some(end) = result[start..].find('}') {
            let field = &result[start + 1..start + end];
            let value = substitute_placeholder(field, row, sql_quote);
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

/// Rewrite `has_role(<expr>)` into Postgres's `pg_has_role(<expr>, 'MEMBER')`.
///
/// Postgres's two-arg `pg_has_role(role, priv)` defaults the user to `current_user`,
/// so this is the simplest shape that doesn't need a SQL wrapper function.
fn rewrite_has_role(expr: &str) -> String {
    let mut out = String::with_capacity(expr.len());
    let bytes = expr.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Match `has_role(` only when not preceded by an identifier char.
        let preceded_by_ident = i > 0 && {
            let c = bytes[i - 1];
            c.is_ascii_alphanumeric() || c == b'_'
        };
        if !preceded_by_ident && expr[i..].starts_with("has_role(") {
            // Find matching close paren, tracking depth and string literals.
            let start = i + "has_role(".len();
            let mut depth = 1;
            let mut j = start;
            let mut in_string = false;
            while j < bytes.len() {
                let c = bytes[j];
                if in_string {
                    if c == b'\'' {
                        // peek for doubled '' (escaped quote)
                        if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                            j += 2;
                            continue;
                        }
                        in_string = false;
                    }
                } else {
                    match c {
                        b'\'' => in_string = true,
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                j += 1;
            }
            if j < bytes.len() && depth == 0 {
                let inner = &expr[start..j];
                out.push_str("pg_has_role(");
                out.push_str(inner);
                out.push_str(", 'MEMBER')");
                i = j + 1;
                continue;
            }
        }
        // No match — copy one UTF-8 code point.
        let ch_end = (1..=4)
            .map(|n| i + n)
            .find(|&n| n > bytes.len() || expr.is_char_boundary(n))
            .unwrap_or(i + 1);
        out.push_str(&expr[i..ch_end]);
        i = ch_end;
    }
    out
}

/// Evaluate a guard expression against a row's data.
/// Returns true if the guard passes, false otherwise.
pub fn evaluate_guard(guard_expr: &str, row: &serde_json::Value) -> bool {
    let resolved = resolve_template_inner(guard_expr, row, true);
    let rewritten = rewrite_has_role(&resolved);
    Spi::get_one::<bool>(&format!("SELECT {}", rewritten))
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

    #[test]
    fn test_resolve_template_current_user_in_guard() {
        let row = serde_json::json!({});
        let resolved = resolve_template_inner("{current_user} = 'admin'", &row, true);
        assert_eq!(resolved, "current_user = 'admin'");
    }

    #[test]
    fn test_resolve_template_session_key_in_guard() {
        let row = serde_json::json!({});
        let resolved = resolve_template_inner("{session.tenant_id}::int = 1", &row, true);
        assert_eq!(
            resolved,
            "current_setting('pgfsm.session.tenant_id', true)::int = 1"
        );
    }

    #[test]
    fn test_resolve_template_current_user_falls_through_in_action() {
        // In display context (sql_quote=false), `current_user` should look up the row
        // (not the SQL function), so existing action templates keep working.
        let row = serde_json::json!({"current_user": "ops_bot"});
        let resolved = resolve_template_inner("user:{current_user}", &row, false);
        assert_eq!(resolved, "user:ops_bot");
    }

    #[test]
    fn test_rewrite_has_role_simple() {
        let out = rewrite_has_role("has_role('sales_manager')");
        assert_eq!(out, "pg_has_role('sales_manager', 'MEMBER')");
    }

    #[test]
    fn test_rewrite_has_role_with_other_expr() {
        let out = rewrite_has_role("has_role('admin') AND amount > 100");
        assert_eq!(out, "pg_has_role('admin', 'MEMBER') AND amount > 100");
    }

    #[test]
    fn test_rewrite_has_role_skips_pg_has_role() {
        let out = rewrite_has_role("pg_has_role(current_user, 'r', 'MEMBER')");
        assert_eq!(out, "pg_has_role(current_user, 'r', 'MEMBER')");
    }

    #[test]
    fn test_rewrite_has_role_with_paren_in_string() {
        let out = rewrite_has_role("has_role('weird)role')");
        assert_eq!(out, "pg_has_role('weird)role', 'MEMBER')");
    }
}
