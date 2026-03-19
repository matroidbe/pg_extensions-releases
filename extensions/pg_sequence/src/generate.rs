use pgrx::prelude::*;

use crate::definition::get_sequence_id;
use crate::format::{format_template, FormatContext};

/// Sequence definition fields needed for generation.
struct SeqDef {
    id: i32,
    prefix: String,
    suffix: String,
    format: String,
    counter_pad: i32,
    start_value: i64,
    increment: i32,
    reset_policy: String,
}

fn load_seq_def(name: &str) -> SeqDef {
    let seq_id = get_sequence_id(name);
    let mut def = None;
    Spi::connect(|client| {
        let row = client
            .select(
                r#"
                SELECT prefix, suffix, format, counter_pad, start_value, increment, reset_policy
                FROM pgsequence.sequence_def WHERE id = $1
                "#,
                None,
                &[seq_id.into()],
            )
            .unwrap()
            .first();
        let prefix: String = row.get_by_name("prefix").unwrap().unwrap();
        let suffix: String = row.get_by_name("suffix").unwrap().unwrap();
        let format: String = row.get_by_name("format").unwrap().unwrap();
        let counter_pad: i32 = row.get_by_name("counter_pad").unwrap().unwrap();
        let start_value: i64 = row.get_by_name("start_value").unwrap().unwrap();
        let increment: i32 = row.get_by_name("increment").unwrap().unwrap();
        let reset_policy: String = row.get_by_name("reset_policy").unwrap().unwrap();

        def = Some(SeqDef {
            id: seq_id,
            prefix,
            suffix,
            format,
            counter_pad,
            start_value,
            increment,
            reset_policy,
        });
    });
    def.unwrap()
}

/// Compute period_key from reset_policy using PostgreSQL's now().
fn compute_period_key(reset_policy: &str) -> String {
    match reset_policy {
        "yearly" => Spi::get_one::<String>("SELECT to_char(now(), 'YYYY')")
            .unwrap()
            .unwrap(),
        "monthly" => Spi::get_one::<String>("SELECT to_char(now(), 'YYYY-MM')")
            .unwrap()
            .unwrap(),
        _ => String::new(), // "never"
    }
}

/// Get current date parts for template formatting.
fn date_parts() -> (String, String, String) {
    let year = Spi::get_one::<String>("SELECT to_char(now(), 'YYYY')")
        .unwrap()
        .unwrap();
    let month = Spi::get_one::<String>("SELECT to_char(now(), 'MM')")
        .unwrap()
        .unwrap();
    let day = Spi::get_one::<String>("SELECT to_char(now(), 'DD')")
        .unwrap()
        .unwrap();
    (year, month, day)
}

/// Get the next counter value (raw number). Atomically increments the counter.
#[pg_extern]
pub fn next_val(name: &str, scope_key: default!(Option<&str>, "''")) -> i64 {
    let scope = scope_key.unwrap_or("");
    let def = load_seq_def(name);
    let period_key = compute_period_key(&def.reset_policy);

    Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (sequence_id, scope_key, period_key)
        DO UPDATE SET current_value = pgsequence.counter.current_value + $5
        RETURNING current_value
        "#,
        &[
            def.id.into(),
            scope.into(),
            period_key.as_str().into(),
            def.start_value.into(),
            (def.increment as i64).into(),
        ],
    )
    .expect("failed to generate next value")
    .expect("no value returned")
}

/// Get the next formatted document number. Atomically increments the counter.
#[pg_extern]
pub fn next_formatted(name: &str, scope_key: default!(Option<&str>, "''")) -> String {
    let scope = scope_key.unwrap_or("");
    let def = load_seq_def(name);
    let period_key = compute_period_key(&def.reset_policy);

    let counter_val = Spi::get_one_with_args::<i64>(
        r#"
        INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (sequence_id, scope_key, period_key)
        DO UPDATE SET current_value = pgsequence.counter.current_value + $5
        RETURNING current_value
        "#,
        &[
            def.id.into(),
            scope.into(),
            period_key.as_str().into(),
            def.start_value.into(),
            (def.increment as i64).into(),
        ],
    )
    .expect("failed to generate next value")
    .expect("no value returned");

    let (year, month, day) = date_parts();
    let ctx = FormatContext {
        prefix: def.prefix,
        suffix: def.suffix,
        counter: counter_val,
        counter_pad: def.counter_pad as usize,
        year,
        month,
        day,
        scope: scope.to_string(),
    };

    format_template(&def.format, &ctx)
        .unwrap_or_else(|e| pgrx::error!("pg_sequence: format error: {}", e))
}

/// Read the current counter value without incrementing.
#[pg_extern]
pub fn current_val(name: &str, scope_key: default!(Option<&str>, "''")) -> i64 {
    let scope = scope_key.unwrap_or("");
    let def = load_seq_def(name);
    let period_key = compute_period_key(&def.reset_policy);

    Spi::get_one_with_args::<i64>(
        r#"
        SELECT current_value FROM pgsequence.counter
        WHERE sequence_id = $1 AND scope_key = $2 AND period_key = $3
        "#,
        &[def.id.into(), scope.into(), period_key.as_str().into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0)
}

/// Preview the next formatted number without incrementing.
#[pg_extern]
pub fn preview(name: &str, scope_key: default!(Option<&str>, "''")) -> String {
    let scope = scope_key.unwrap_or("");
    let def = load_seq_def(name);
    let period_key = compute_period_key(&def.reset_policy);

    let current = Spi::get_one_with_args::<i64>(
        r#"
        SELECT current_value FROM pgsequence.counter
        WHERE sequence_id = $1 AND scope_key = $2 AND period_key = $3
        "#,
        &[def.id.into(), scope.into(), period_key.as_str().into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    // Preview: what the next call would produce
    let next_counter = if current == 0 {
        def.start_value
    } else {
        current + def.increment as i64
    };

    let (year, month, day) = date_parts();
    let ctx = FormatContext {
        prefix: def.prefix,
        suffix: def.suffix,
        counter: next_counter,
        counter_pad: def.counter_pad as usize,
        year,
        month,
        day,
        scope: scope.to_string(),
    };

    format_template(&def.format, &ctx)
        .unwrap_or_else(|e| pgrx::error!("pg_sequence: format error: {}", e))
}

/// Manually reset a counter to a specific value (or to start_value - increment if none given).
#[pg_extern]
pub fn reset_counter(
    name: &str,
    scope_key: default!(Option<&str>, "''"),
    new_value: default!(Option<i64>, "NULL"),
) {
    let scope = scope_key.unwrap_or("");
    let def = load_seq_def(name);
    let period_key = compute_period_key(&def.reset_policy);
    let val = new_value.unwrap_or(0);

    Spi::run_with_args(
        r#"
        DELETE FROM pgsequence.counter
        WHERE sequence_id = $1 AND scope_key = $2 AND period_key = $3
        "#,
        &[def.id.into(), scope.into(), period_key.as_str().into()],
    )
    .expect("failed to reset counter");

    if val > 0 {
        Spi::run_with_args(
            r#"
            INSERT INTO pgsequence.counter (sequence_id, scope_key, period_key, current_value)
            VALUES ($1, $2, $3, $4)
            "#,
            &[
                def.id.into(),
                scope.into(),
                period_key.as_str().into(),
                val.into(),
            ],
        )
        .expect("failed to set counter value");
    }
}
