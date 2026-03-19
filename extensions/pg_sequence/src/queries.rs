use pgrx::prelude::*;

use crate::definition::get_sequence_id;

/// List all sequence definitions.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_sequences() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(prefix, String),
        name!(format, String),
        name!(reset_policy, String),
        name!(gap_free, bool),
        name!(description, Option<String>),
    ),
> {
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT name, prefix, format, reset_policy, gap_free, description
                FROM pgsequence.sequence_def
                ORDER BY name
                "#,
                None,
                &[],
            )
            .unwrap();

        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let prefix: String = row.get_by_name("prefix").unwrap().unwrap();
            let format: String = row.get_by_name("format").unwrap().unwrap();
            let reset_policy: String = row.get_by_name("reset_policy").unwrap().unwrap();
            let gap_free: bool = row.get_by_name("gap_free").unwrap().unwrap();
            let description: Option<String> = row.get_by_name("description").unwrap();
            rows.push((name, prefix, format, reset_policy, gap_free, description));
        }
    });

    TableIterator::new(rows)
}

/// Get detailed info for a single sequence.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn sequence_info(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(prefix, String),
        name!(suffix, String),
        name!(format, String),
        name!(counter_pad, i32),
        name!(start_value, i64),
        name!(increment, i32),
        name!(reset_policy, String),
        name!(gap_free, bool),
        name!(counter_count, i64),
    ),
> {
    let seq_id = get_sequence_id(name);
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT s.name, s.prefix, s.suffix, s.format, s.counter_pad,
                       s.start_value, s.increment, s.reset_policy, s.gap_free,
                       (SELECT COUNT(*) FROM pgsequence.counter c WHERE c.sequence_id = s.id) as counter_count
                FROM pgsequence.sequence_def s
                WHERE s.id = $1
                "#,
                None,
                &[seq_id.into()],
            )
            .unwrap();

        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let prefix: String = row.get_by_name("prefix").unwrap().unwrap();
            let suffix: String = row.get_by_name("suffix").unwrap().unwrap();
            let fmt: String = row.get_by_name("format").unwrap().unwrap();
            let counter_pad: i32 = row.get_by_name("counter_pad").unwrap().unwrap();
            let start_value: i64 = row.get_by_name("start_value").unwrap().unwrap();
            let increment: i32 = row.get_by_name("increment").unwrap().unwrap();
            let reset_policy: String = row.get_by_name("reset_policy").unwrap().unwrap();
            let gap_free: bool = row.get_by_name("gap_free").unwrap().unwrap();
            let counter_count: i64 = row.get_by_name("counter_count").unwrap().unwrap();
            rows.push((
                name,
                prefix,
                suffix,
                fmt,
                counter_pad,
                start_value,
                increment,
                reset_policy,
                gap_free,
                counter_count,
            ));
        }
    });

    TableIterator::new(rows)
}

/// List all active counters for a sequence.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_counters(
    name: &str,
) -> TableIterator<
    'static,
    (
        name!(scope_key, String),
        name!(period_key, String),
        name!(current_value, i64),
    ),
> {
    let seq_id = get_sequence_id(name);
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT scope_key, period_key, current_value
                FROM pgsequence.counter
                WHERE sequence_id = $1
                ORDER BY scope_key, period_key
                "#,
                None,
                &[seq_id.into()],
            )
            .unwrap();

        for row in result {
            let scope_key: String = row.get_by_name("scope_key").unwrap().unwrap();
            let period_key: String = row.get_by_name("period_key").unwrap().unwrap();
            let current_value: i64 = row.get_by_name("current_value").unwrap().unwrap();
            rows.push((scope_key, period_key, current_value));
        }
    });

    TableIterator::new(rows)
}
