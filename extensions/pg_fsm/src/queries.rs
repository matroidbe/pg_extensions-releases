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
