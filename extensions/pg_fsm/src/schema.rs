// Bootstrap SQL for pg_fsm tables, triggers, and indexes

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- Machine definition: a named state machine with an initial state
-- =============================================================================
CREATE TABLE @extschema@.machine (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    initial     TEXT NOT NULL,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Valid states per machine
-- =============================================================================
CREATE TABLE @extschema@.state (
    id          SERIAL PRIMARY KEY,
    machine_id  INT NOT NULL REFERENCES @extschema@.machine(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    is_final    BOOLEAN NOT NULL DEFAULT false,
    description TEXT,
    UNIQUE(machine_id, name)
);

-- =============================================================================
-- Transitions: (machine, from_state, event) -> to_state
-- =============================================================================
CREATE TABLE @extschema@.transition (
    id          SERIAL PRIMARY KEY,
    machine_id  INT NOT NULL REFERENCES @extschema@.machine(id) ON DELETE CASCADE,
    from_state  TEXT NOT NULL,
    to_state    TEXT NOT NULL,
    event       TEXT NOT NULL,
    guard       TEXT,
    priority    INT NOT NULL DEFAULT 0,
    description TEXT,
    UNIQUE(machine_id, from_state, event, priority)
);

CREATE INDEX idx_transition_lookup ON @extschema@.transition(machine_id, from_state, event);

-- =============================================================================
-- Actions: side effects executed on transition
-- =============================================================================
CREATE TABLE @extschema@.action (
    id              SERIAL PRIMARY KEY,
    transition_id   INT NOT NULL REFERENCES @extschema@.transition(id) ON DELETE CASCADE,
    action_type     TEXT NOT NULL CHECK (action_type IN ('notify', 'sql', 'function')),
    action_value    TEXT NOT NULL,
    run_order       INT NOT NULL DEFAULT 0
);

-- =============================================================================
-- Table bindings: which tables are governed by which machine
-- =============================================================================
CREATE TABLE @extschema@.binding (
    id              SERIAL PRIMARY KEY,
    machine_id      INT NOT NULL REFERENCES @extschema@.machine(id) ON DELETE CASCADE,
    table_name      TEXT NOT NULL,
    status_column   TEXT NOT NULL DEFAULT 'status',
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(table_name, status_column)
);

-- =============================================================================
-- Transition history: immutable audit trail
-- =============================================================================
CREATE TABLE @extschema@.history (
    id                  BIGSERIAL PRIMARY KEY,
    machine_id          INT NOT NULL,
    table_name          TEXT NOT NULL,
    row_id              TEXT,
    old_state           TEXT NOT NULL,
    new_state           TEXT NOT NULL,
    event               TEXT,
    transitioned_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    transitioned_by     TEXT DEFAULT current_user
);

CREATE INDEX idx_history_table_row ON @extschema@.history(table_name, row_id);
CREATE INDEX idx_history_machine ON @extschema@.history(machine_id);

-- =============================================================================
-- Immutability: prevent UPDATE/DELETE on history
-- =============================================================================
CREATE FUNCTION @extschema@.prevent_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'pg_fsm: transition history is immutable';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER history_immutable
    BEFORE UPDATE OR DELETE ON @extschema@.history
    FOR EACH ROW EXECUTE FUNCTION @extschema@.prevent_mutation();
"#,
    name = "bootstrap_tables",
    bootstrap
);
