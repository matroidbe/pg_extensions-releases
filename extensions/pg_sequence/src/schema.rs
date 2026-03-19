// Bootstrap SQL for pg_sequence tables and indexes

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- sequence_def: defines how document numbers are generated
-- =============================================================================
CREATE TABLE @extschema@.sequence_def (
    id           SERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    prefix       TEXT NOT NULL DEFAULT '',
    suffix       TEXT NOT NULL DEFAULT '',
    format       TEXT NOT NULL DEFAULT '{prefix}{counter}{suffix}',
    counter_pad  INT NOT NULL DEFAULT 1 CHECK (counter_pad >= 1 AND counter_pad <= 20),
    start_value  BIGINT NOT NULL DEFAULT 1,
    increment    INT NOT NULL DEFAULT 1 CHECK (increment >= 1),
    reset_policy TEXT NOT NULL DEFAULT 'never' CHECK (reset_policy IN ('never','yearly','monthly')),
    gap_free     BOOLEAN NOT NULL DEFAULT false,
    description  TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- counter: active counters per sequence+scope+period combination
-- =============================================================================
CREATE TABLE @extschema@.counter (
    id            SERIAL PRIMARY KEY,
    sequence_id   INT NOT NULL REFERENCES @extschema@.sequence_def(id) ON DELETE CASCADE,
    scope_key     TEXT NOT NULL DEFAULT '',
    period_key    TEXT NOT NULL DEFAULT '',
    current_value BIGINT NOT NULL DEFAULT 0,
    UNIQUE(sequence_id, scope_key, period_key)
);
CREATE INDEX idx_counter_seq ON @extschema@.counter(sequence_id);
"#,
    name = "bootstrap_tables",
    bootstrap
);
