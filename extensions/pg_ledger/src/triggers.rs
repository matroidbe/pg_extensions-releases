use pgrx::prelude::*;

/// Ensure AFTER triggers are installed on a table for pg_ledger.
/// Called by create_rule() when a new rule is added.
pub fn ensure_triggers_installed(schema_name: &str, table_name: &str) {
    let qualified = format!("{}.{}", schema_name, table_name);
    let safe_name = format!("{}_{}", schema_name, table_name).replace('.', "_");

    // Check if trigger already exists
    let exists = Spi::get_one_with_args::<bool>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = $1 AND c.relname = $2
            AND t.tgname = $3
        )
        "#,
        &[
            schema_name.into(),
            table_name.into(),
            format!("pgledger_{}_insert", safe_name).into(),
        ],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if exists {
        return;
    }

    // Create the trigger function
    let func_sql = format!(
        r#"
        CREATE OR REPLACE FUNCTION pgledger.trig_{safe_name}() RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM pgledger.process_row('{qualified}', 'INSERT', row_to_json(NEW)::jsonb, NULL::jsonb);
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                PERFORM pgledger.process_row('{qualified}', 'UPDATE', row_to_json(NEW)::jsonb, row_to_json(OLD)::jsonb);
                RETURN NEW;
            ELSIF TG_OP = 'DELETE' THEN
                PERFORM pgledger.process_row('{qualified}', 'DELETE', NULL::jsonb, row_to_json(OLD)::jsonb);
                RETURN OLD;
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql
        "#
    );
    Spi::run(&func_sql).expect("failed to create trigger function");

    // Create triggers for INSERT, UPDATE, DELETE
    for op in &["INSERT", "UPDATE", "DELETE"] {
        let trigger_sql = format!(
            r#"
            CREATE TRIGGER pgledger_{safe_name}_{op_lower}
            AFTER {op} ON {qualified}
            FOR EACH ROW
            EXECUTE FUNCTION pgledger.trig_{safe_name}()
            "#,
            op_lower = op.to_lowercase()
        );
        Spi::run(&trigger_sql).expect("failed to create trigger");
    }
}

/// Process a row mutation and create journal entries based on rules.
/// Called from the PL/pgSQL trigger function.
#[pg_extern]
pub fn process_row(
    table_name: &str,
    operation: &str,
    new_row: Option<pgrx::JsonB>,
    old_row: Option<pgrx::JsonB>,
) {
    // Find applicable rules for this table
    let rules = load_rules_for_table(table_name);

    if rules.is_empty() {
        if crate::PG_LEDGER_ENABLED.get() && crate::PG_LEDGER_STRICT_MODE.get() {
            pgrx::warning!(
                "pg_ledger: no rules defined for table '{}', skipping journaling",
                table_name
            );
        }
        return;
    }

    match operation {
        "INSERT" => {
            if let Some(ref new) = new_row {
                apply_rules(&rules, &new.0, table_name, None);
            }
        }
        "UPDATE" => {
            // Reverse old entries, apply new
            if let Some(ref old) = old_row {
                apply_rules_reversed(&rules, &old.0, table_name, None);
            }
            if let Some(ref new) = new_row {
                apply_rules(&rules, &new.0, table_name, None);
            }
        }
        "DELETE" => {
            if let Some(ref old) = old_row {
                apply_rules_reversed(&rules, &old.0, table_name, None);
            }
        }
        _ => pgrx::error!("pg_ledger: unknown operation '{}'", operation),
    }
}

/// Rule data loaded from the rule table.
struct LoadedRule {
    source_column: String,
    entries: Vec<RuleEntryData>,
    condition: Option<String>,
    #[allow(dead_code)]
    priority: i32,
}

struct RuleEntryData {
    side: String,
    account: String,
    amount_expr: Option<String>,
}

/// Load all active rules for a given table, ordered by priority DESC.
fn load_rules_for_table(table_name: &str) -> Vec<LoadedRule> {
    let mut rules = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT source_column, entries, condition, priority
                FROM pgledger.rule
                WHERE source_table = $1 AND active = true
                ORDER BY priority DESC
                "#,
                None,
                &[table_name.into()],
            )
            .unwrap();

        for row in result {
            let source_column: String = row.get_by_name("source_column").unwrap().unwrap();
            let condition: Option<String> = row.get_by_name("condition").unwrap();
            let priority: i32 = row.get_by_name("priority").unwrap().unwrap_or(0);

            // Parse entries from the composite array — query them as JSON
            let entries_json = Spi::get_one_with_args::<pgrx::JsonB>(
                r#"
                SELECT to_jsonb(entries) FROM pgledger.rule
                WHERE source_table = $1 AND source_column = $2 AND active = true
                ORDER BY priority DESC LIMIT 1
                "#,
                &[table_name.into(), source_column.clone().into()],
            );

            let entry_data = match entries_json {
                Ok(Some(json)) => parse_rule_entries(&json.0),
                _ => Vec::new(),
            };

            rules.push(LoadedRule {
                source_column,
                entries: entry_data,
                condition,
                priority,
            });
        }
    });

    rules
}

/// Parse rule entries from JSON array.
fn parse_rule_entries(json: &serde_json::Value) -> Vec<RuleEntryData> {
    let mut entries = Vec::new();
    if let Some(arr) = json.as_array() {
        for item in arr {
            let side = item
                .get("side")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let account = item
                .get("account")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let amount_expr = item
                .get("amount_expr")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            entries.push(RuleEntryData {
                side,
                account,
                amount_expr,
            });
        }
    }
    entries
}

/// Resolve template variables like {customer_id} from a row JSON.
/// When `sql_quote` is true, string values are wrapped in single quotes for SQL safety.
fn resolve_template_inner(template: &str, row: &serde_json::Value, sql_quote: bool) -> String {
    let mut result = template.to_string();
    // Find all {field_name} patterns and replace with values from row
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

/// Resolve template for account names (no SQL quoting).
fn resolve_template(template: &str, row: &serde_json::Value) -> String {
    resolve_template_inner(template, row, false)
}

/// Resolve template for SQL conditions (string values are single-quoted).
fn resolve_sql_condition(template: &str, row: &serde_json::Value) -> String {
    resolve_template_inner(template, row, true)
}

/// Resolve the amount for a rule entry from the row data.
fn resolve_amount(
    entry: &RuleEntryData,
    row: &serde_json::Value,
    source_column: &str,
) -> Option<f64> {
    match &entry.amount_expr {
        None => {
            // Use the source column value
            row.get(source_column).and_then(|v| match v {
                serde_json::Value::Number(n) => n.as_f64(),
                serde_json::Value::String(s) => s.parse::<f64>().ok(),
                _ => None,
            })
        }
        Some(expr) => {
            // Resolve template variables in expression, then evaluate
            let resolved = resolve_template(expr, row);
            // Simple case: it's just a number
            if let Ok(val) = resolved.parse::<f64>() {
                return Some(val);
            }
            // Try evaluating as SQL expression
            Spi::get_one::<f64>(&format!("SELECT ({})", resolved))
                .ok()
                .flatten()
        }
    }
}

/// Apply rules for a row and create journal entries.
fn apply_rules(
    rules: &[LoadedRule],
    row: &serde_json::Value,
    table_name: &str,
    source_id: Option<&str>,
) {
    for rule in rules {
        // Check condition if present
        if let Some(ref cond) = rule.condition {
            let resolved_cond = resolve_sql_condition(cond, row);
            let passes = Spi::get_one::<bool>(&format!("SELECT {}", resolved_cond))
                .ok()
                .flatten()
                .unwrap_or(false);
            if !passes {
                continue;
            }
        }

        // Get the source column value
        let col_value = row.get(&rule.source_column);
        if col_value.is_none() || col_value == Some(&serde_json::Value::Null) {
            continue;
        }

        // Create journal header
        let row_id = row
            .get("id")
            .map(|v| v.to_string().trim_matches('"').to_string());
        let journal_id = crate::journal::create_journal_header_internal(
            Some(table_name),
            source_id.or(row_id.as_deref()),
            None,
        );

        // Create entries
        for entry in &rule.entries {
            let amount = resolve_amount(entry, row, &rule.source_column);
            if let Some(amt) = amount {
                if amt == 0.0 {
                    continue;
                }
                let account = resolve_template(&entry.account, row);
                let (debit, credit) = match entry.side.as_str() {
                    "debit" => (amt.abs(), 0.0),
                    "credit" => (0.0, amt.abs()),
                    _ => continue,
                };
                crate::journal::create_journal_line_internal(
                    journal_id, &account, debit, credit, "USD",
                );
            }
        }

        crate::balance::mark_ledger_activity();
        return; // First matching rule wins
    }
}

/// Apply rules in reverse (for UPDATE old row / DELETE).
fn apply_rules_reversed(
    rules: &[LoadedRule],
    row: &serde_json::Value,
    table_name: &str,
    source_id: Option<&str>,
) {
    for rule in rules {
        if let Some(ref cond) = rule.condition {
            let resolved_cond = resolve_sql_condition(cond, row);
            let passes = Spi::get_one::<bool>(&format!("SELECT {}", resolved_cond))
                .ok()
                .flatten()
                .unwrap_or(false);
            if !passes {
                continue;
            }
        }

        let col_value = row.get(&rule.source_column);
        if col_value.is_none() || col_value == Some(&serde_json::Value::Null) {
            continue;
        }

        let row_id = row
            .get("id")
            .map(|v| v.to_string().trim_matches('"').to_string());
        let journal_id = crate::journal::create_journal_header_internal(
            Some(table_name),
            source_id.or(row_id.as_deref()),
            Some("Reversal"),
        );

        for entry in &rule.entries {
            let amount = resolve_amount(entry, row, &rule.source_column);
            if let Some(amt) = amount {
                if amt == 0.0 {
                    continue;
                }
                let account = resolve_template(&entry.account, row);
                // Swap sides for reversal
                let (debit, credit) = match entry.side.as_str() {
                    "debit" => (0.0, amt.abs()),
                    "credit" => (amt.abs(), 0.0),
                    _ => continue,
                };
                crate::journal::create_journal_line_internal(
                    journal_id, &account, debit, credit, "USD",
                );
            }
        }

        crate::balance::mark_ledger_activity();
        return;
    }
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
        let row = serde_json::json!({"region": "EU", "type": "sales"});
        assert_eq!(
            resolve_template("revenue:{region}:{type}", &row),
            "revenue:EU:sales"
        );
    }

    #[test]
    fn test_resolve_template_no_vars() {
        let row = serde_json::json!({});
        assert_eq!(resolve_template("cash", &row), "cash");
    }

    #[test]
    fn test_resolve_template_numeric() {
        let row = serde_json::json!({"id": 123});
        assert_eq!(resolve_template("item:{id}", &row), "item:123");
    }
}
