use pgrx::prelude::*;

/// Bind a table to a state machine. Installs BEFORE INSERT/UPDATE triggers
/// that enforce valid state transitions.
#[pg_extern]
pub fn bind_table(
    machine_name: &str,
    table_name: &str,
    status_column: default!(Option<&str>, "'status'"),
) {
    let status_column = status_column.unwrap_or("status");

    // Validate machine exists
    let machine_id = Spi::get_one_with_args::<i32>(
        "SELECT id FROM pgfsm.machine WHERE name = $1",
        &[machine_name.into()],
    )
    .ok()
    .flatten()
    .unwrap_or_else(|| {
        pgrx::error!("machine '{}' does not exist", machine_name);
    });

    // Parse schema.table
    let (schema_name, tbl_name) = parse_qualified_name(table_name);

    // Validate table exists and has the status column
    let col_exists = Spi::get_one_with_args::<bool>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM information_schema.columns
            WHERE table_schema::text = $1 AND table_name::text = $2 AND column_name::text = $3
        )
        "#,
        &[
            schema_name.clone().into(),
            tbl_name.clone().into(),
            status_column.into(),
        ],
    )
    .unwrap_or(Some(false))
    .unwrap_or(false);

    if !col_exists {
        pgrx::error!(
            "table '{}' does not have column '{}'",
            table_name,
            status_column
        );
    }

    // Insert binding
    Spi::run_with_args(
        r#"
        INSERT INTO pgfsm.binding (machine_id, table_name, status_column)
        VALUES ($1, $2, $3)
        ON CONFLICT (table_name, status_column)
        DO UPDATE SET machine_id = EXCLUDED.machine_id, active = true
        "#,
        &[machine_id.into(), table_name.into(), status_column.into()],
    )
    .expect("failed to create binding");

    // Install triggers
    install_triggers(&schema_name, &tbl_name, table_name, status_column);
}

/// Unbind a table from its state machine. Removes enforcement triggers.
#[pg_extern]
pub fn unbind_table(table_name: &str, status_column: default!(Option<&str>, "'status'")) {
    let status_column = status_column.unwrap_or("status");
    let (schema_name, tbl_name) = parse_qualified_name(table_name);
    let safe_name = format!("{}_{}", schema_name, tbl_name);

    // Remove binding
    Spi::run_with_args(
        "DELETE FROM pgfsm.binding WHERE table_name = $1 AND status_column = $2",
        &[table_name.into(), status_column.into()],
    )
    .expect("failed to remove binding");

    // Drop triggers
    for suffix in &["insert", "update"] {
        let drop_sql = format!(
            "DROP TRIGGER IF EXISTS pgfsm_{}_{} ON {}.{}",
            safe_name, suffix, schema_name, tbl_name
        );
        Spi::run(&drop_sql).ok();
    }

    // Drop trigger function
    let drop_func = format!(
        "DROP FUNCTION IF EXISTS pgfsm.enforce_{}() CASCADE",
        safe_name
    );
    Spi::run(&drop_func).ok();
}

/// Install BEFORE INSERT and BEFORE UPDATE triggers on a table.
fn install_triggers(schema_name: &str, tbl_name: &str, qualified_name: &str, status_column: &str) {
    let safe_name = format!("{}_{}", schema_name, tbl_name);

    // Create the trigger function
    let func_sql = format!(
        r#"
        CREATE OR REPLACE FUNCTION pgfsm.enforce_{safe_name}() RETURNS TRIGGER AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                PERFORM pgfsm.validate_insert('{qualified_name}', '{status_column}', row_to_json(NEW)::jsonb);
                RETURN NEW;
            ELSIF TG_OP = 'UPDATE' THEN
                IF OLD.{status_column} IS DISTINCT FROM NEW.{status_column} THEN
                    PERFORM pgfsm.validate_transition(
                        '{qualified_name}',
                        '{status_column}',
                        OLD.{status_column}::text,
                        NEW.{status_column}::text,
                        row_to_json(NEW)::jsonb
                    );
                END IF;
                RETURN NEW;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        "#
    );
    Spi::run(&func_sql).expect("failed to create trigger function");

    // BEFORE INSERT trigger
    let insert_trigger = format!(
        r#"
        CREATE TRIGGER pgfsm_{safe_name}_insert
        BEFORE INSERT ON {schema_name}.{tbl_name}
        FOR EACH ROW
        EXECUTE FUNCTION pgfsm.enforce_{safe_name}()
        "#
    );
    Spi::run(&insert_trigger).expect("failed to create INSERT trigger");

    // BEFORE UPDATE trigger
    let update_trigger = format!(
        r#"
        CREATE TRIGGER pgfsm_{safe_name}_update
        BEFORE UPDATE ON {schema_name}.{tbl_name}
        FOR EACH ROW
        EXECUTE FUNCTION pgfsm.enforce_{safe_name}()
        "#
    );
    Spi::run(&update_trigger).expect("failed to create UPDATE trigger");
}

/// Validate that an INSERT uses the machine's initial state.
#[pg_extern]
pub fn validate_insert(table_name: &str, status_column: &str, row: pgrx::JsonB) {
    if !crate::PG_FSM_ENABLED.get() {
        return;
    }

    let binding = load_binding(table_name, status_column);
    let (machine_id, initial_state) = match binding {
        Some(b) => b,
        None => return, // No binding, no enforcement
    };

    let new_state = row
        .0
        .get(status_column)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    match new_state {
        Some(ref s) if s == &initial_state => {
            // Valid: inserting with initial state, log it
            log_history(machine_id, table_name, &row.0, "", &initial_state, None);
        }
        Some(ref s) => {
            pgrx::error!(
                "pg_fsm: INSERT must use initial state '{}', got '{}'",
                initial_state,
                s
            );
        }
        None => {
            // NULL status — we allow it but won't track. The user should set a DEFAULT.
        }
    }
}

/// Validate a state transition and execute actions if valid.
#[pg_extern]
pub fn validate_transition(
    table_name: &str,
    status_column: &str,
    old_state: &str,
    new_state: &str,
    row: pgrx::JsonB,
) {
    if !crate::PG_FSM_ENABLED.get() {
        return;
    }

    let binding = load_binding(table_name, status_column);
    let (machine_id, _initial) = match binding {
        Some(b) => b,
        None => return,
    };

    // Find valid transitions from old_state to new_state
    let transition = find_valid_transition(machine_id, old_state, new_state, &row.0);

    match transition {
        Some((transition_id, event)) => {
            // Log history
            log_history(
                machine_id,
                table_name,
                &row.0,
                old_state,
                new_state,
                Some(&event),
            );

            // Execute actions
            execute_actions(transition_id, &row.0);
        }
        None => {
            pgrx::error!(
                "pg_fsm: invalid transition from '{}' to '{}' on table '{}'",
                old_state,
                new_state,
                table_name
            );
        }
    }
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

/// Find a valid transition from old_state to new_state, evaluating guards.
/// Returns (transition_id, event) if found.
fn find_valid_transition(
    machine_id: i32,
    old_state: &str,
    new_state: &str,
    row: &serde_json::Value,
) -> Option<(i32, String)> {
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT id, event, guard
                FROM pgfsm.transition
                WHERE machine_id = $1 AND from_state = $2 AND to_state = $3
                ORDER BY priority DESC
                "#,
                None,
                &[machine_id.into(), old_state.into(), new_state.into()],
            )
            .unwrap();

        for trow in result {
            let tid: i32 = trow.get_by_name("id").unwrap().unwrap();
            let event: String = trow.get_by_name("event").unwrap().unwrap();
            let guard: Option<String> = trow.get_by_name("guard").unwrap();

            match guard {
                Some(ref g) if !g.is_empty() => {
                    if crate::guard::evaluate_guard(g, row) {
                        return Some((tid, event));
                    }
                }
                _ => return Some((tid, event)),
            }
        }
        None
    })
}

/// Log a state transition to the history table.
fn log_history(
    machine_id: i32,
    table_name: &str,
    row: &serde_json::Value,
    old_state: &str,
    new_state: &str,
    event: Option<&str>,
) {
    let row_id = row
        .get("id")
        .map(|v| v.to_string().trim_matches('"').to_string());

    Spi::run_with_args(
        r#"
        INSERT INTO pgfsm.history (machine_id, table_name, row_id, old_state, new_state, event)
        VALUES ($1, $2, $3, $4, $5, $6)
        "#,
        &[
            machine_id.into(),
            table_name.into(),
            row_id.into(),
            old_state.into(),
            new_state.into(),
            event.into(),
        ],
    )
    .expect("failed to log transition history");
}

/// Execute actions associated with a transition.
fn execute_actions(transition_id: i32, row: &serde_json::Value) {
    Spi::connect(|client| {
        let result = client
            .select(
                r#"
                SELECT action_type, action_value
                FROM pgfsm.action
                WHERE transition_id = $1
                ORDER BY run_order
                "#,
                None,
                &[transition_id.into()],
            )
            .unwrap();

        for arow in result {
            let action_type: String = arow.get_by_name("action_type").unwrap().unwrap();
            let action_value: String = arow.get_by_name("action_value").unwrap().unwrap();

            match action_type.as_str() {
                "notify" => {
                    let resolved = crate::guard::resolve_template(&action_value, row);
                    Spi::run(&format!("NOTIFY {}", resolved))
                        .expect("failed to execute notify action");
                }
                "sql" => {
                    let resolved = crate::guard::resolve_template(&action_value, row);
                    Spi::run(&resolved).expect("failed to execute sql action");
                }
                "function" => {
                    let resolved = crate::guard::resolve_template(&action_value, row);
                    Spi::run(&format!("SELECT {}", resolved))
                        .expect("failed to execute function action");
                }
                _ => {
                    pgrx::warning!("pg_fsm: unknown action type '{}'", action_type);
                }
            }
        }
    });
}

/// Parse "schema.table" into (schema, table). Defaults to "public" if no schema.
fn parse_qualified_name(name: &str) -> (String, String) {
    if let Some(dot) = name.find('.') {
        (name[..dot].to_string(), name[dot + 1..].to_string())
    } else {
        ("public".to_string(), name.to_string())
    }
}
