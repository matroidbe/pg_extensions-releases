use pgrx::prelude::*;

/// Transition a row to a new state by firing an event.
/// Looks up the current state, finds the matching transition, and performs an UPDATE.
/// The BEFORE trigger re-validates the transition (defense in depth).
#[pg_extern]
pub fn transition(
    table_name: &str,
    row_id: &str,
    event: &str,
    status_column: default!(Option<&str>, "'status'"),
) -> String {
    let status_column = status_column.unwrap_or("status");

    // Get binding info
    let (machine_id, _initial) = load_binding_or_error(table_name, status_column);

    // Get current state
    let current_state = get_current_state_by_id(table_name, row_id, status_column);

    // Find transition for this event from current state
    let new_state = find_transition_target(
        machine_id,
        &current_state,
        event,
        table_name,
        row_id,
        status_column,
    );

    // Execute the UPDATE — the BEFORE trigger will validate and log
    let update_sql = format!(
        "UPDATE {} SET {} = $1 WHERE id = $2::int",
        table_name, status_column
    );
    Spi::run_with_args(&update_sql, &[new_state.clone().into(), row_id.into()])
        .expect("failed to update state");

    new_state
}

/// Check if a transition is possible for the given event, without executing it.
#[pg_extern]
pub fn can_transition(
    table_name: &str,
    row_id: &str,
    event: &str,
    status_column: default!(Option<&str>, "'status'"),
) -> bool {
    let status_column = status_column.unwrap_or("status");

    let (machine_id, _initial) = match load_binding(table_name, status_column) {
        Some(b) => b,
        None => return false,
    };

    let current_state = get_current_state_by_id(table_name, row_id, status_column);

    // Get the row as JSON for guard evaluation
    let row_json = get_row_json(table_name, row_id);

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT id, guard
                FROM pgfsm.transition
                WHERE machine_id = $1 AND from_state = $2 AND event = $3
                ORDER BY priority DESC
                "#,
                None,
                &[machine_id.into(), current_state.into(), event.into()],
            )
            .unwrap();

        for row in result {
            let guard: Option<String> = row.get_by_name("guard").unwrap();
            match guard {
                Some(ref g) if !g.is_empty() => {
                    if crate::guard::evaluate_guard(g, &row_json) {
                        return true;
                    }
                }
                _ => return true,
            }
        }
        false
    })
}

/// List all available events for a row's current state.
#[pg_extern]
pub fn available_events(
    table_name: &str,
    row_id: &str,
    status_column: default!(Option<&str>, "'status'"),
) -> TableIterator<
    'static,
    (
        name!(event, String),
        name!(to_state, String),
        name!(description, Option<String>),
    ),
> {
    let status_column = status_column.unwrap_or("status");

    let (machine_id, _initial) = match load_binding(table_name, status_column) {
        Some(b) => b,
        None => return TableIterator::new(Vec::new()),
    };

    let current_state = get_current_state_by_id(table_name, row_id, status_column);
    let row_json = get_row_json(table_name, row_id);

    let mut events = Vec::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT event, to_state, guard, description
                FROM pgfsm.transition
                WHERE machine_id = $1 AND from_state = $2
                ORDER BY priority DESC, event
                "#,
                None,
                &[machine_id.into(), current_state.into()],
            )
            .unwrap();

        for row in result {
            let event: String = row.get_by_name("event").unwrap().unwrap();
            let to_state: String = row.get_by_name("to_state").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();
            let description: Option<String> = row.get_by_name("description").unwrap();

            let guard_passes = match guard {
                Some(ref g) if !g.is_empty() => crate::guard::evaluate_guard(g, &row_json),
                _ => true,
            };

            if guard_passes {
                events.push((event, to_state, description));
            }
        }
    });

    TableIterator::new(events)
}

/// Find the target state for an event, considering guards.
fn find_transition_target(
    machine_id: i32,
    current_state: &str,
    event: &str,
    table_name: &str,
    row_id: &str,
    _status_column: &str,
) -> String {
    let row_json = get_row_json(table_name, row_id);

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT to_state, guard
                FROM pgfsm.transition
                WHERE machine_id = $1 AND from_state = $2 AND event = $3
                ORDER BY priority DESC
                "#,
                None,
                &[machine_id.into(), current_state.into(), event.into()],
            )
            .unwrap();

        for row in result {
            let to_state: String = row.get_by_name("to_state").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();

            let guard_passes = match guard {
                Some(ref g) if !g.is_empty() => crate::guard::evaluate_guard(g, &row_json),
                _ => true,
            };

            if guard_passes {
                return to_state;
            }
        }

        pgrx::error!(
            "pg_fsm: no valid transition for event '{}' from state '{}' on '{}'",
            event,
            current_state,
            table_name
        );
    })
}

/// Get the current state of a row by its ID.
fn get_current_state_by_id(table_name: &str, row_id: &str, status_column: &str) -> String {
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

/// Get a row as JSON for guard evaluation.
fn get_row_json(table_name: &str, row_id: &str) -> serde_json::Value {
    let query = format!(
        "SELECT row_to_json(t)::text FROM {} t WHERE id = $1::int",
        table_name
    );
    let json_str = Spi::get_one_with_args::<String>(&query, &[row_id.into()])
        .ok()
        .flatten()
        .unwrap_or_else(|| "{}".to_string());

    serde_json::from_str(&json_str).unwrap_or(serde_json::Value::Null)
}

/// Load binding for a table+column. Returns (machine_id, initial_state).
fn load_binding(table_name: &str, status_column: &str) -> Option<(i32, String)> {
    Spi::connect(|client| {
        let mut result = client
            .select(
                r#"
                SELECT b.machine_id, m.initial
                FROM pgfsm.binding b
                JOIN pgfsm.machine m ON b.machine_id = m.id
                WHERE b.table_name = $1 AND b.status_column = $2 AND b.active = true
                "#,
                None,
                &[table_name.into(), status_column.into()],
            )
            .unwrap();

        if let Some(row) = result.next() {
            let machine_id: i32 = row.get_by_name("machine_id").unwrap().unwrap();
            let initial: String = row.get_by_name("initial").unwrap().unwrap();
            return Some((machine_id, initial));
        }
        None
    })
}

/// Load binding or error if not found.
fn load_binding_or_error(table_name: &str, status_column: &str) -> (i32, String) {
    load_binding(table_name, status_column).unwrap_or_else(|| {
        pgrx::error!(
            "pg_fsm: no binding found for table '{}' column '{}'",
            table_name,
            status_column
        );
    })
}
