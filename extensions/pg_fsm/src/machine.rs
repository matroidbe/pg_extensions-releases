use pgrx::prelude::*;

/// Create a new state machine with an initial state.
/// The initial state is automatically added to the state table.
#[pg_extern]
pub fn create_machine(
    name: &str,
    initial_state: &str,
    description: default!(Option<&str>, "NULL"),
) -> i32 {
    // Insert the machine
    let machine_id = Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgfsm.machine (name, initial, description)
        VALUES ($1, $2, $3)
        RETURNING id
        "#,
        &[name.into(), initial_state.into(), description.into()],
    )
    .expect("failed to create machine")
    .expect("no id returned from machine insert");

    // Auto-create the initial state
    Spi::run_with_args(
        r#"
        INSERT INTO pgfsm.state (machine_id, name)
        VALUES ($1, $2)
        "#,
        &[machine_id.into(), initial_state.into()],
    )
    .expect("failed to create initial state");

    machine_id
}

/// Add a state to an existing machine.
#[pg_extern]
pub fn add_state(
    machine_name: &str,
    state_name: &str,
    is_final: default!(Option<bool>, "false"),
    description: default!(Option<&str>, "NULL"),
) {
    let machine_id = get_machine_id(machine_name);
    let is_final = is_final.unwrap_or(false);

    Spi::run_with_args(
        r#"
        INSERT INTO pgfsm.state (machine_id, name, is_final, description)
        VALUES ($1, $2, $3, $4)
        "#,
        &[
            machine_id.into(),
            state_name.into(),
            is_final.into(),
            description.into(),
        ],
    )
    .expect("failed to add state");
}

/// Add a transition between two states in a machine.
#[pg_extern]
pub fn add_transition(
    machine_name: &str,
    from_state: &str,
    to_state: &str,
    event: &str,
    guard: default!(Option<&str>, "NULL"),
    description: default!(Option<&str>, "NULL"),
) -> i32 {
    let machine_id = get_machine_id(machine_name);

    // Validate from_state exists
    validate_state_exists(machine_id, from_state);
    // Validate to_state exists
    validate_state_exists(machine_id, to_state);

    // Check from_state is not final
    let is_final = Spi::get_one_with_args::<bool>(
        "SELECT is_final FROM pgfsm.state WHERE machine_id = $1 AND name = $2",
        &[machine_id.into(), from_state.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if is_final {
        pgrx::error!("cannot add transition from final state '{}'", from_state);
    }

    // Auto-assign priority: next available for this (machine, from_state, event) group
    let next_priority = Spi::get_one_with_args::<i32>(
        r#"
        SELECT COALESCE(MAX(priority), -1) + 1
        FROM pgfsm.transition
        WHERE machine_id = $1 AND from_state = $2 AND event = $3
        "#,
        &[machine_id.into(), from_state.into(), event.into()],
    )
    .unwrap_or(Some(0))
    .unwrap_or(0);

    Spi::get_one_with_args::<i32>(
        r#"
        INSERT INTO pgfsm.transition (machine_id, from_state, to_state, event, guard, priority, description)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id
        "#,
        &[
            machine_id.into(),
            from_state.into(),
            to_state.into(),
            event.into(),
            guard.into(),
            next_priority.into(),
            description.into(),
        ],
    )
    .expect("failed to add transition")
    .expect("no id returned from transition insert")
}

/// Add an action to a transition.
#[pg_extern]
pub fn add_action(
    transition_id: i32,
    action_type: &str,
    action_value: &str,
    run_order: default!(Option<i32>, "0"),
) {
    // Validate action_type
    match action_type {
        "notify" | "sql" | "function" => {}
        _ => pgrx::error!(
            "invalid action_type '{}': must be 'notify', 'sql', or 'function'",
            action_type
        ),
    }

    // Validate transition exists
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgfsm.transition WHERE id = $1)",
        &[transition_id.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !exists {
        pgrx::error!("transition {} does not exist", transition_id);
    }

    let run_order = run_order.unwrap_or(0);

    Spi::run_with_args(
        r#"
        INSERT INTO pgfsm.action (transition_id, action_type, action_value, run_order)
        VALUES ($1, $2, $3, $4)
        "#,
        &[
            transition_id.into(),
            action_type.into(),
            action_value.into(),
            run_order.into(),
        ],
    )
    .expect("failed to add action");
}

/// Drop a machine and all its states, transitions, actions (via CASCADE).
#[pg_extern]
pub fn drop_machine(machine_name: &str) {
    let deleted = Spi::get_one_with_args::<String>(
        "DELETE FROM pgfsm.machine WHERE name = $1 RETURNING name",
        &[machine_name.into()],
    );

    match deleted {
        Ok(Some(_)) => {}
        _ => pgrx::error!("machine '{}' does not exist", machine_name),
    }
}

/// Look up machine ID by name, error if not found.
fn get_machine_id(name: &str) -> i32 {
    Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgfsm.machine WHERE name = $1",
        &[name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("machine '{}' does not exist", name);
    })
}

/// Validate that a state exists in a machine.
fn validate_state_exists(machine_id: i32, state_name: &str) {
    let exists = Spi::get_one_with_args::<bool>(
        "SELECT EXISTS(SELECT 1 FROM pgfsm.state WHERE machine_id = $1 AND name = $2)",
        &[machine_id.into(), state_name.into()],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !exists {
        pgrx::error!(
            "state '{}' does not exist in machine (id={})",
            state_name,
            machine_id
        );
    }
}
