use pgrx::prelude::*;

/// Transition a row to a new state by firing an event.
/// Looks up the current state, finds the matching transition, and performs an UPDATE.
/// The BEFORE trigger re-validates the transition (defense in depth).
///
/// When `allow_noop = true`, if the row is already in the target state for this event,
/// returns the current state without raising and without inserting history.
#[pg_extern]
pub fn transition(
    table_name: &str,
    row_id: &str,
    event: &str,
    status_column: default!(Option<&str>, "'status'"),
    allow_noop: default!(Option<bool>, "false"),
) -> String {
    let status_column = status_column.unwrap_or("status");
    let allow_noop = allow_noop.unwrap_or(false);

    // Get binding info
    let (machine_id, _initial) = load_binding_or_error(table_name, status_column);

    // Get current state
    let current_state = get_current_state_by_id(table_name, row_id, status_column);

    // Idempotent check: if we're already in a target state reachable by this event
    // (from any from_state), return early.
    if allow_noop {
        let already_there = Spi::get_one_with_args::<bool>(
            r#"
            SELECT EXISTS(
                SELECT 1 FROM pgfsm.transition
                WHERE machine_id = $1 AND event = $2 AND to_state = $3
            )
            "#,
            &[
                machine_id.into(),
                event.into(),
                current_state.clone().into(),
            ],
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if already_there {
            return current_state;
        }
    }

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

/// Like `available_events`, but returns *every* event the machine defines, with
/// per-event `available` and `reason_unavailable` columns useful for tooltips.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn events_with_targets(
    table_name: &str,
    row_id: &str,
    status_column: default!(Option<&str>, "'status'"),
) -> TableIterator<
    'static,
    (
        name!(event, String),
        name!(to_state, Option<String>),
        name!(label, String),
        name!(variant, Option<String>),
        name!(available, bool),
        name!(reason_unavailable, Option<String>),
    ),
> {
    let status_column = status_column.unwrap_or("status");

    let (machine_id, _initial) = match load_binding(table_name, status_column) {
        Some(b) => b,
        None => return TableIterator::new(Vec::new()),
    };

    let current_state = get_current_state_by_id(table_name, row_id, status_column);
    let row_json = get_row_json(table_name, row_id);

    // Collect all transitions for the machine, group by event.
    type Row = (
        String,         // from_state
        String,         // to_state
        Option<String>, // guard
        Option<String>, // label_db
        Option<String>, // variant
    );
    let mut by_event: std::collections::BTreeMap<String, Vec<Row>> =
        std::collections::BTreeMap::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT from_state, to_state, event, guard, label, variant, sort_order, priority
                FROM pgfsm.transition
                WHERE machine_id = $1
                ORDER BY sort_order, priority DESC, from_state
                "#,
                None,
                &[machine_id.into()],
            )
            .unwrap();

        for row in result {
            let from_state: String = row.get_by_name("from_state").unwrap().unwrap();
            let to_state: String = row.get_by_name("to_state").unwrap().unwrap();
            let event: String = row.get_by_name("event").unwrap().unwrap();
            let guard: Option<String> = row.get_by_name("guard").unwrap();
            let label: Option<String> = row.get_by_name("label").unwrap();
            let variant: Option<String> = row.get_by_name("variant").unwrap();
            by_event
                .entry(event)
                .or_default()
                .push((from_state, to_state, guard, label, variant));
        }
    });

    let mut out = Vec::new();
    for (event, transitions) in by_event {
        // First pass: any transition from the current state for this event?
        let from_current: Vec<&Row> = transitions
            .iter()
            .filter(|(from, _, _, _, _)| from == &current_state)
            .collect();

        if from_current.is_empty() {
            // Pull metadata from any transition for the event so the UI has labels.
            let (_, to_state, _, label_db, variant) = &transitions[0];
            let label = label_db.clone().unwrap_or_else(|| event.clone());
            out.push((
                event,
                Some(to_state.clone()),
                label,
                variant.clone(),
                false,
                Some("wrong_state".to_string()),
            ));
            continue;
        }

        // We have at least one transition from current_state for this event.
        // Pick first whose guard passes; otherwise the first one and report guard_failed.
        let mut chosen_idx: Option<usize> = None;
        for (i, r) in from_current.iter().enumerate() {
            let (_, _, guard, _, _) = *r;
            let passes = match guard {
                Some(g) if !g.is_empty() => crate::guard::evaluate_guard(g, &row_json),
                _ => true,
            };
            if passes {
                chosen_idx = Some(i);
                break;
            }
        }
        let (available, reason, picked) = match chosen_idx {
            Some(i) => (true, None, from_current[i]),
            None => (false, Some("guard_failed".to_string()), from_current[0]),
        };
        let (_, to_state, _, label_db, variant) = picked;
        let label = label_db.clone().unwrap_or_else(|| event.clone());
        out.push((
            event,
            Some(to_state.clone()),
            label,
            variant.clone(),
            available,
            reason,
        ));
    }

    TableIterator::new(out)
}

/// List all available events for a row's current state.
#[allow(clippy::type_complexity)]
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
        name!(label, String),
        name!(variant, Option<String>),
        name!(sort_order, i32),
        name!(confirm_required, bool),
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
                SELECT event, to_state, guard, description, label, variant, sort_order, confirm_required
                FROM pgfsm.transition
                WHERE machine_id = $1 AND from_state = $2
                ORDER BY sort_order, priority DESC, event
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
            let label_db: Option<String> = row.get_by_name("label").unwrap();
            let variant: Option<String> = row.get_by_name("variant").unwrap();
            let sort_order: i32 = row.get_by_name("sort_order").unwrap().unwrap_or(0);
            let confirm_required: bool = row
                .get_by_name("confirm_required")
                .unwrap()
                .unwrap_or(false);

            let guard_passes = match guard {
                Some(ref g) if !g.is_empty() => crate::guard::evaluate_guard(g, &row_json),
                _ => true,
            };

            if guard_passes {
                let label = label_db.unwrap_or_else(|| event.clone());
                events.push((
                    event,
                    to_state,
                    description,
                    label,
                    variant,
                    sort_order,
                    confirm_required,
                ));
            }
        }
    });

    TableIterator::new(events)
}

/// Batch version of `available_events` for a list of row ids.
///
/// Single SPI round-trip to fetch all rows + transitions, then evaluate guards
/// per-row in Rust. Returns one row per (row_id, available event) pair.
#[allow(clippy::type_complexity)]
#[pg_extern]
pub fn available_events_batch(
    table_name: &str,
    row_ids: pgrx::Array<'_, i64>,
    status_column: default!(Option<&str>, "'status'"),
) -> TableIterator<
    'static,
    (
        name!(row_id, i64),
        name!(event, String),
        name!(to_state, String),
        name!(label, String),
        name!(variant, Option<String>),
        name!(sort_order, i32),
        name!(confirm_required, bool),
    ),
> {
    let status_column = status_column.unwrap_or("status");

    let (machine_id, _initial) = match load_binding(table_name, status_column) {
        Some(b) => b,
        None => return TableIterator::new(Vec::new()),
    };

    // Collect ids, drop NULLs.
    let ids: Vec<i64> = row_ids.iter().flatten().collect();
    if ids.is_empty() {
        return TableIterator::new(Vec::new());
    }

    // Fetch rows: id + state + full row JSON for guard evaluation.
    let row_query = format!(
        "SELECT id::bigint AS id, {col}::text AS state, row_to_json(t)::text AS row_json
         FROM {tbl} t WHERE id = ANY($1::bigint[])",
        col = status_column,
        tbl = table_name
    );

    let mut row_data: Vec<(i64, String, serde_json::Value)> = Vec::new();
    let ids_array = ids.clone();
    Spi::connect(|client| {
        let result = client
            .select(&row_query, None, &[ids_array.into()])
            .unwrap();
        for row in result {
            let id: i64 = row.get_by_name("id").unwrap().unwrap();
            let state: String = row.get_by_name("state").unwrap().unwrap();
            let json_str: Option<String> = row.get_by_name("row_json").unwrap();
            let json: serde_json::Value = json_str
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or(serde_json::Value::Null);
            row_data.push((id, state, json));
        }
    });

    // Fetch all transitions for this machine grouped by from_state.
    type TransitionRow = (
        String,         // event
        String,         // to_state
        Option<String>, // guard
        Option<String>, // label
        Option<String>, // variant
        i32,            // sort_order
        bool,           // confirm_required
    );
    let mut by_from: std::collections::HashMap<String, Vec<TransitionRow>> =
        std::collections::HashMap::new();

    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT from_state, event, to_state, guard, label, variant, sort_order, confirm_required
                FROM pgfsm.transition
                WHERE machine_id = $1
                ORDER BY sort_order, priority DESC, event
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
            let confirm_required: bool = row
                .get_by_name("confirm_required")
                .unwrap()
                .unwrap_or(false);

            by_from.entry(from_state).or_default().push((
                event,
                to_state,
                guard,
                label,
                variant,
                sort_order,
                confirm_required,
            ));
        }
    });

    let mut out = Vec::new();
    for (id, state, json) in &row_data {
        if let Some(transitions) = by_from.get(state) {
            for (event, to_state, guard, label, variant, sort_order, confirm_required) in
                transitions
            {
                let guard_passes = match guard {
                    Some(g) if !g.is_empty() => crate::guard::evaluate_guard(g, json),
                    _ => true,
                };
                if guard_passes {
                    let label = label.clone().unwrap_or_else(|| event.clone());
                    out.push((
                        *id,
                        event.clone(),
                        to_state.clone(),
                        label,
                        variant.clone(),
                        *sort_order,
                        *confirm_required,
                    ));
                }
            }
        }
    }

    TableIterator::new(out)
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
///
/// Exact text match first; falls back to regclass canonicalisation so callers using
/// `'orders'` and `'public.orders'` resolve to the same binding.
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

        let mut result = client
            .select(
                r#"
                SELECT b.machine_id, m.initial
                FROM pgfsm.binding b
                JOIN pgfsm.machine m ON b.machine_id = m.id
                WHERE b.status_column = $2 AND b.active = true
                  AND to_regclass(b.table_name) = to_regclass($1)
                  AND to_regclass($1) IS NOT NULL
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

/// Resolve a table reference (regclass) to the binding's stored table_name string.
/// Returns `None` if no active binding exists for `target` + `status_column`.
#[pg_extern]
pub fn resolve_binding(
    target: pgrx::PgRelation,
    status_column: default!(Option<&str>, "'status'"),
) -> Option<String> {
    let status_column = status_column.unwrap_or("status");
    let oid = target.oid();
    Spi::connect(|client| {
        let mut result = client
            .select(
                r#"
                SELECT b.table_name
                FROM pgfsm.binding b
                WHERE b.status_column = $2 AND b.active = true
                  AND to_regclass(b.table_name) = $1
                "#,
                None,
                &[oid.into(), status_column.into()],
            )
            .unwrap();
        result
            .next()
            .and_then(|row| row.get_by_name::<String, _>("table_name").ok().flatten())
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
