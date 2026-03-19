use pgrx::prelude::*;

/// Create a new account determination rule.
/// Returns the rule ID.
#[pg_extern]
pub fn create_rule(
    source_table: &str,
    source_column: &str,
    entries: pgrx::JsonB,
    condition: default!(Option<&str>, "NULL"),
    priority: default!(Option<i32>, "NULL"),
    description: default!(Option<&str>, "NULL"),
    currency_column: default!(Option<&str>, "NULL"),
) -> i32 {
    let priority = priority.unwrap_or(0);

    // Validate entries JSON array
    let arr = entries
        .0
        .as_array()
        .unwrap_or_else(|| pgrx::error!("entries must be a JSON array"));

    if arr.is_empty() {
        pgrx::error!("entries must not be empty");
    }

    let mut has_debit = false;
    let mut has_credit = false;

    for (i, entry) in arr.iter().enumerate() {
        let side = entry
            .get("side")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| pgrx::error!("entry {} missing 'side'", i));

        match side {
            "debit" => has_debit = true,
            "credit" => has_credit = true,
            _ => pgrx::error!(
                "entry {} has invalid side '{}', must be 'debit' or 'credit'",
                i,
                side
            ),
        }

        if entry.get("account").and_then(|v| v.as_str()).is_none() {
            pgrx::error!("entry {} missing 'account'", i);
        }
    }

    if !has_debit || !has_credit {
        pgrx::error!("entries must contain at least one debit and one credit side");
    }

    // Build the entries as SQL array of rule_entry composite type
    let mut entry_values = Vec::new();
    for entry in arr {
        let side = entry.get("side").unwrap().as_str().unwrap();
        let account = entry.get("account").unwrap().as_str().unwrap();
        let amount_expr = entry
            .get("amount_expr")
            .and_then(|v| v.as_str())
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .unwrap_or_else(|| "NULL".to_string());

        entry_values.push(format!(
            "ROW('{}', '{}', {})::pgledger.rule_entry",
            side.replace('\'', "''"),
            account.replace('\'', "''"),
            amount_expr
        ));
    }

    let entries_sql = format!("ARRAY[{}]", entry_values.join(", "));

    let insert_sql = format!(
        r#"
        INSERT INTO pgledger.rule (source_table, source_column, entries, priority, condition, description, currency_column)
        VALUES ($1, $2, {entries_sql}, $3, $4, $5, $6)
        RETURNING id
        "#
    );

    let rule_id = Spi::get_one_with_args::<i32>(
        &insert_sql,
        &[
            source_table.into(),
            source_column.into(),
            priority.into(),
            condition.into(),
            description.into(),
            currency_column.into(),
        ],
    )
    .expect("failed to create rule")
    .expect("no id returned from rule insert");

    // Parse schema.table from source_table
    let (schema_name, table_name) = parse_qualified_name(source_table);
    crate::triggers::ensure_triggers_installed(&schema_name, &table_name);

    rule_id
}

/// Drop (delete) a rule by ID.
#[pg_extern]
pub fn drop_rule(rule_id: i32) {
    let deleted = Spi::get_one_with_args::<i32>(
        "DELETE FROM pgledger.rule WHERE id = $1 RETURNING id",
        &[rule_id.into()],
    );

    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("rule {} does not exist", rule_id),
    }
}

/// Enable a rule.
#[pg_extern]
pub fn enable_rule(rule_id: i32) {
    Spi::run_with_args(
        "UPDATE pgledger.rule SET active = true WHERE id = $1",
        &[rule_id.into()],
    )
    .expect("failed to enable rule");
}

/// Disable a rule without deleting it.
#[pg_extern]
pub fn disable_rule(rule_id: i32) {
    Spi::run_with_args(
        "UPDATE pgledger.rule SET active = false WHERE id = $1",
        &[rule_id.into()],
    )
    .expect("failed to disable rule");
}

/// Parse a possibly-qualified table name into (schema, table).
fn parse_qualified_name(name: &str) -> (String, String) {
    if let Some(pos) = name.find('.') {
        (name[..pos].to_string(), name[pos + 1..].to_string())
    } else {
        ("public".to_string(), name.to_string())
    }
}
