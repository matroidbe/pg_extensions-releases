use pgrx::prelude::*;

/// Look up a sequence definition id by name. Errors if not found.
pub fn get_sequence_id(name: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgsequence.sequence_def WHERE name = $1",
        &[name.into()],
    )
    .unwrap_or_else(|e| pgrx::error!("pg_sequence: {}", e))
    .unwrap_or_else(|| pgrx::error!("pg_sequence: sequence '{}' does not exist", name))
}

/// Create a new sequence definition.
#[allow(clippy::too_many_arguments)]
#[pg_extern]
pub fn create_sequence(
    name: &str,
    prefix: default!(Option<&str>, "''"),
    suffix: default!(Option<&str>, "''"),
    format: default!(Option<&str>, "'{prefix}{counter}{suffix}'"),
    counter_pad: default!(Option<i32>, "1"),
    start_value: default!(Option<i64>, "1"),
    increment: default!(Option<i32>, "1"),
    reset_policy: default!(Option<&str>, "'never'"),
    gap_free: default!(Option<bool>, "false"),
    description: default!(Option<&str>, "NULL"),
) -> i32 {
    let pfx = prefix.unwrap_or("");
    let sfx = suffix.unwrap_or("");
    let fmt = format.unwrap_or("{prefix}{counter}{suffix}");
    let pad = counter_pad.unwrap_or(1);
    let start = start_value.unwrap_or(1);
    let inc = increment.unwrap_or(1);
    let policy = reset_policy.unwrap_or("never");
    let gf = gap_free.unwrap_or(false);

    if !["never", "yearly", "monthly"].contains(&policy) {
        pgrx::error!(
            "pg_sequence: invalid reset_policy '{}', must be 'never', 'yearly', or 'monthly'",
            policy
        );
    }

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgsequence.sequence_def
            (name, prefix, suffix, format, counter_pad, start_value, increment, reset_policy, gap_free, description)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
        "#,
        &[
            name.into(),
            pfx.into(),
            sfx.into(),
            fmt.into(),
            pad.into(),
            start.into(),
            inc.into(),
            policy.into(),
            gf.into(),
            description.into(),
        ],
    )
    .expect("failed to create sequence")
    .expect("no id returned")
}

/// Modify an existing sequence definition. Does not reset counters.
#[allow(clippy::too_many_arguments)]
#[pg_extern]
pub fn alter_sequence(
    name: &str,
    prefix: default!(Option<&str>, "NULL"),
    suffix: default!(Option<&str>, "NULL"),
    format: default!(Option<&str>, "NULL"),
    counter_pad: default!(Option<i32>, "NULL"),
    increment: default!(Option<i32>, "NULL"),
    reset_policy: default!(Option<&str>, "NULL"),
    gap_free: default!(Option<bool>, "NULL"),
    description: default!(Option<&str>, "NULL"),
) {
    let seq_id = get_sequence_id(name);

    if let Some(policy) = reset_policy {
        if !["never", "yearly", "monthly"].contains(&policy) {
            pgrx::error!(
                "pg_sequence: invalid reset_policy '{}', must be 'never', 'yearly', or 'monthly'",
                policy
            );
        }
    }

    // Build dynamic UPDATE
    let mut sets = Vec::new();
    let mut args: Vec<pgrx::datum::DatumWithOid> = vec![seq_id.into()];
    let mut idx = 2;

    if let Some(v) = prefix {
        sets.push(format!("prefix = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = suffix {
        sets.push(format!("suffix = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = format {
        sets.push(format!("format = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = counter_pad {
        sets.push(format!("counter_pad = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = increment {
        sets.push(format!("increment = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = reset_policy {
        sets.push(format!("reset_policy = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = gap_free {
        sets.push(format!("gap_free = ${idx}"));
        args.push(v.into());
        idx += 1;
    }
    if let Some(v) = description {
        sets.push(format!("description = ${idx}"));
        args.push(v.into());
        let _ = idx; // suppress unused warning
    }

    if sets.is_empty() {
        return;
    }

    let sql = format!(
        "UPDATE pgsequence.sequence_def SET {} WHERE id = $1",
        sets.join(", ")
    );

    Spi::run_with_args(&sql, &args).expect("failed to alter sequence");
}

/// Drop a sequence definition and all its counters.
#[pg_extern]
pub fn drop_sequence(name: &str) {
    let deleted = Spi::get_one_with_args::<i32>(
        "DELETE FROM pgsequence.sequence_def WHERE name = $1 RETURNING id",
        &[name.into()],
    );
    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("pg_sequence: sequence '{}' does not exist", name),
    }
}
