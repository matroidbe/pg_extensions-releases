use pgrx::datum::TimestampWithTimeZone;
use pgrx::prelude::*;

/// Get the transition history for a table, optionally filtered by row_id.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn history_for(
    table_name: &str,
    row_id: default!(Option<&str>, "NULL"),
) -> TableIterator<
    'static,
    (
        name!(old_state, String),
        name!(new_state, String),
        name!(event, Option<String>),
        name!(transitioned_at, TimestampWithTimeZone),
        name!(transitioned_by, Option<String>),
    ),
> {
    let mut rows = Vec::new();

    Spi::connect(|client| {
        let result = match row_id {
            Some(rid) => client
                .select(
                    r#"
                    SELECT old_state, new_state, event, transitioned_at, transitioned_by
                    FROM pgfsm.history
                    WHERE table_name = $1 AND row_id = $2
                    ORDER BY transitioned_at, id
                    "#,
                    None,
                    &[table_name.into(), rid.into()],
                )
                .unwrap(),
            None => client
                .select(
                    r#"
                    SELECT old_state, new_state, event, transitioned_at, transitioned_by
                    FROM pgfsm.history
                    WHERE table_name = $1
                    ORDER BY transitioned_at, id
                    "#,
                    None,
                    &[table_name.into()],
                )
                .unwrap(),
        };

        for row in result {
            let old_state: String = row.get_by_name("old_state").unwrap().unwrap();
            let new_state: String = row.get_by_name("new_state").unwrap().unwrap();
            let event: Option<String> = row.get_by_name("event").unwrap();
            let transitioned_at: TimestampWithTimeZone =
                row.get_by_name("transitioned_at").unwrap().unwrap();
            let transitioned_by: Option<String> = row.get_by_name("transitioned_by").unwrap();
            rows.push((
                old_state,
                new_state,
                event,
                transitioned_at,
                transitioned_by,
            ));
        }
    });

    TableIterator::new(rows)
}

/// Cursor-based pagination over `history_for`.
///
/// Returns rows older than `before` (or all rows if `before` is NULL), newest first,
/// up to `limit` rows. Stable ordering by (transitioned_at DESC, id DESC).
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn history_for_paginated(
    table_name: &str,
    row_id: &str,
    before: default!(Option<TimestampWithTimeZone>, "NULL"),
    limit: default!(i32, "50"),
) -> TableIterator<
    'static,
    (
        name!(old_state, String),
        name!(new_state, String),
        name!(event, Option<String>),
        name!(transitioned_at, TimestampWithTimeZone),
        name!(transitioned_by, Option<String>),
    ),
> {
    let mut rows = Vec::new();
    let limit = limit.max(1) as i64;

    Spi::connect(|client| {
        let result = match before {
            Some(ts) => client
                .select(
                    r#"
                    SELECT old_state, new_state, event, transitioned_at, transitioned_by
                    FROM pgfsm.history
                    WHERE table_name = $1 AND row_id = $2 AND transitioned_at < $3
                    ORDER BY transitioned_at DESC, id DESC
                    LIMIT $4
                    "#,
                    None,
                    &[table_name.into(), row_id.into(), ts.into(), limit.into()],
                )
                .unwrap(),
            None => client
                .select(
                    r#"
                    SELECT old_state, new_state, event, transitioned_at, transitioned_by
                    FROM pgfsm.history
                    WHERE table_name = $1 AND row_id = $2
                    ORDER BY transitioned_at DESC, id DESC
                    LIMIT $3
                    "#,
                    None,
                    &[table_name.into(), row_id.into(), limit.into()],
                )
                .unwrap(),
        };

        for row in result {
            let old_state: String = row.get_by_name("old_state").unwrap().unwrap();
            let new_state: String = row.get_by_name("new_state").unwrap().unwrap();
            let event: Option<String> = row.get_by_name("event").unwrap();
            let transitioned_at: TimestampWithTimeZone =
                row.get_by_name("transitioned_at").unwrap().unwrap();
            let transitioned_by: Option<String> = row.get_by_name("transitioned_by").unwrap();
            rows.push((
                old_state,
                new_state,
                event,
                transitioned_at,
                transitioned_by,
            ));
        }
    });

    TableIterator::new(rows)
}

/// Get the current state of a row.
#[pg_extern]
pub fn current_state(
    table_name: &str,
    row_id: &str,
    status_column: default!(Option<&str>, "'status'"),
) -> String {
    let status_column = status_column.unwrap_or("status");
    let query = format!(
        "SELECT {}::text FROM {} WHERE id = $1::int",
        status_column, table_name
    );
    Spi::get_one_with_args::<String>(&query, &[row_id.into()])
        .ok()
        .flatten()
        .unwrap_or_else(|| {
            pgrx::error!(
                "pg_fsm: row with id '{}' not found in '{}'",
                row_id,
                table_name
            );
        })
}

/// List all defined state machines.
#[pg_extern]
pub fn list_machines() -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(initial_state, String),
        name!(description, Option<String>),
    ),
> {
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT name, initial, description FROM pgfsm.machine ORDER BY name",
                None,
                &[],
            )
            .unwrap();
        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let initial: String = row.get_by_name("initial").unwrap().unwrap();
            let description: Option<String> = row.get_by_name("description").unwrap();
            rows.push((name, initial, description));
        }
    });
    TableIterator::new(rows)
}

/// List the states defined for a machine.
#[pg_extern]
pub fn list_states(
    machine_name: &str,
) -> TableIterator<
    'static,
    (
        name!(name, String),
        name!(is_final, bool),
        name!(description, Option<String>),
    ),
> {
    let machine_id = lookup_machine_id_or_error(machine_name);
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT name, is_final, description FROM pgfsm.state WHERE machine_id = $1 ORDER BY name",
                None,
                &[machine_id.into()],
            )
            .unwrap();
        for row in result {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let is_final: bool = row.get_by_name("is_final").unwrap().unwrap_or(false);
            let description: Option<String> = row.get_by_name("description").unwrap();
            rows.push((name, is_final, description));
        }
    });
    TableIterator::new(rows)
}

/// Distinct event names defined for a machine.
#[pg_extern]
pub fn list_events(machine_name: &str) -> Vec<String> {
    let machine_id = lookup_machine_id_or_error(machine_name);
    let mut events = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                "SELECT DISTINCT event FROM pgfsm.transition WHERE machine_id = $1 ORDER BY event",
                None,
                &[machine_id.into()],
            )
            .unwrap();
        for row in result {
            let event: String = row.get_by_name("event").unwrap().unwrap();
            events.push(event);
        }
    });
    events
}

/// List the transitions defined for a machine, including UI metadata.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn list_transitions(
    machine_name: &str,
) -> TableIterator<
    'static,
    (
        name!(from_state, String),
        name!(event, String),
        name!(to_state, String),
        name!(guard, Option<String>),
        name!(label, Option<String>),
        name!(variant, Option<String>),
        name!(sort_order, i32),
    ),
> {
    let machine_id = lookup_machine_id_or_error(machine_name);
    let mut rows = Vec::new();
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT from_state, event, to_state, guard, label, variant, sort_order
                FROM pgfsm.transition
                WHERE machine_id = $1
                ORDER BY from_state, sort_order, event
                "#,
                None,
                &[machine_id.into()],
            )
            .unwrap();
        for row in result {
            let from_state: String = row.get_by_name("from_state").unwrap().unwrap();
            let event: String = row.get_by_name("event").unwrap().unwrap();
            let to_state: String = row.get_by_name("to_state").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();
            let label: Option<String> = row.get_by_name("label").unwrap();
            let variant: Option<String> = row.get_by_name("variant").unwrap();
            let sort_order: i32 = row.get_by_name("sort_order").unwrap().unwrap_or(0);
            rows.push((
                from_state, event, to_state, guard, label, variant, sort_order,
            ));
        }
    });
    TableIterator::new(rows)
}

fn lookup_machine_id_or_error(machine_name: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgfsm.machine WHERE name = $1",
        &[machine_name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("machine '{}' does not exist", machine_name);
    })
}

/// Generate a DOT diagram of a state machine.
#[pg_extern]
pub fn machine_diagram(machine_name: &str) -> String {
    let machine_id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgfsm.machine WHERE name = $1",
        &[machine_name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("machine '{}' does not exist", machine_name);
    });

    let initial = Spi::get_one_with_args::<String>(
        "SELECT initial FROM pgfsm.machine WHERE id = $1",
        &[machine_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_default();

    let mut dot = format!("digraph {} {{\n", machine_name);
    dot.push_str("  rankdir=LR;\n");
    dot.push_str("  __start__ [shape=point];\n");
    dot.push_str(&format!("  __start__ -> \"{}\";\n", initial));

    // Mark final states
    Spi::connect(|client| {
        let states = client
            .select(
                "SELECT name, is_final FROM pgfsm.state WHERE machine_id = $1 ORDER BY name",
                None,
                &[machine_id.into()],
            )
            .unwrap();

        for row in states {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            let is_final: bool = row.get_by_name("is_final").unwrap().unwrap_or(false);
            if is_final {
                dot.push_str(&format!("  \"{}\" [shape=doublecircle];\n", name));
            }
        }
    });

    // Add transitions
    Spi::connect(|client| {
        let transitions = client
            .select(
                r#"
                SELECT from_state, to_state, event, guard
                FROM pgfsm.transition
                WHERE machine_id = $1
                ORDER BY from_state, event
                "#,
                None,
                &[machine_id.into()],
            )
            .unwrap();

        for row in transitions {
            let from: String = row.get_by_name("from_state").unwrap().unwrap();
            let to: String = row.get_by_name("to_state").unwrap().unwrap();
            let event: String = row.get_by_name("event").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();

            let label = match guard {
                Some(ref g) if !g.is_empty() => format!("{} [{}]", event, g),
                _ => event,
            };

            dot.push_str(&format!(
                "  \"{}\" -> \"{}\" [label=\"{}\"];\n",
                from, to, label
            ));
        }
    });

    dot.push_str("}\n");
    dot
}

/// Generate a Mermaid `stateDiagram-v2` of a state machine.
#[pg_extern]
pub fn machine_diagram_mermaid(machine_name: &str) -> String {
    let machine_id = lookup_machine_id_or_error(machine_name);
    let initial = Spi::get_one_with_args::<String>(
        "SELECT initial FROM pgfsm.machine WHERE id = $1",
        &[machine_id.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_default();

    let mut out = String::from("stateDiagram-v2\n");
    out.push_str(&format!("    [*] --> {}\n", initial));

    // Final states get an outgoing arrow to [*].
    let mut final_states: Vec<String> = Vec::new();
    Spi::connect(|client| {
        let states = client
            .select(
                "SELECT name FROM pgfsm.state WHERE machine_id = $1 AND is_final = true ORDER BY name",
                None,
                &[machine_id.into()],
            )
            .unwrap();
        for row in states {
            let name: String = row.get_by_name("name").unwrap().unwrap();
            final_states.push(name);
        }
    });

    Spi::connect(|client| {
        let transitions = client
            .select(
                r#"
                SELECT from_state, to_state, event, guard
                FROM pgfsm.transition
                WHERE machine_id = $1
                ORDER BY from_state, event
                "#,
                None,
                &[machine_id.into()],
            )
            .unwrap();

        for row in transitions {
            let from: String = row.get_by_name("from_state").unwrap().unwrap();
            let to: String = row.get_by_name("to_state").unwrap().unwrap();
            let event: String = row.get_by_name("event").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();
            let label = match guard {
                Some(ref g) if !g.is_empty() => format!("{} [{}]", event, g),
                _ => event,
            };
            out.push_str(&format!("    {} --> {}: {}\n", from, to, label));
        }
    });

    for name in final_states {
        out.push_str(&format!("    {} --> [*]\n", name));
    }

    out
}
