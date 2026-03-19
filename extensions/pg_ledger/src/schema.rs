// Bootstrap SQL for pg_ledger tables, triggers, and types

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- Composite type for rule entries
-- =============================================================================
CREATE TYPE @extschema@.rule_entry AS (
    side        TEXT,
    account     TEXT,
    amount_expr TEXT
);

-- =============================================================================
-- Journal header: groups entries from a single event
-- =============================================================================
CREATE TABLE @extschema@.journal (
    id              BIGSERIAL PRIMARY KEY,
    xact_id         XID8 NOT NULL,
    posted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    fiscal_period   TEXT,
    source_table    TEXT,
    source_id       TEXT,
    description     TEXT,
    created_by      TEXT DEFAULT current_user
);

CREATE INDEX idx_journal_xact_id ON @extschema@.journal(xact_id);
CREATE INDEX idx_journal_posted_at ON @extschema@.journal(posted_at);

-- =============================================================================
-- Journal entry: individual debit/credit lines
-- =============================================================================
CREATE TABLE @extschema@.journal_entry (
    id              BIGSERIAL PRIMARY KEY,
    journal_id      BIGINT NOT NULL REFERENCES @extschema@.journal(id),
    account         TEXT NOT NULL,
    debit           NUMERIC(19,4) NOT NULL DEFAULT 0,
    credit          NUMERIC(19,4) NOT NULL DEFAULT 0,
    currency        TEXT NOT NULL DEFAULT 'USD',
    base_amount     NUMERIC(19,4),
    CONSTRAINT single_side CHECK (
        (debit > 0 AND credit = 0) OR
        (credit > 0 AND debit = 0) OR
        (debit = 0 AND credit = 0)
    )
);

CREATE INDEX idx_je_journal_id ON @extschema@.journal_entry(journal_id);
CREATE INDEX idx_je_account ON @extschema@.journal_entry(account);

-- =============================================================================
-- Immutability: prevent UPDATE/DELETE on journal tables
-- =============================================================================
CREATE FUNCTION @extschema@.prevent_mutation() RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'pg_ledger: journal entries are immutable. Use pgledger.reverse() for corrections.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_immutable
    BEFORE UPDATE OR DELETE ON @extschema@.journal
    FOR EACH ROW EXECUTE FUNCTION @extschema@.prevent_mutation();

CREATE TRIGGER journal_entry_immutable
    BEFORE UPDATE OR DELETE ON @extschema@.journal_entry
    FOR EACH ROW EXECUTE FUNCTION @extschema@.prevent_mutation();

-- =============================================================================
-- Fiscal period: open/close accounting periods
-- =============================================================================
CREATE TABLE @extschema@.fiscal_period (
    period      TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    closed_at   TIMESTAMPTZ,
    closed_by   TEXT
);

-- =============================================================================
-- Exchange rate: currency conversion
-- =============================================================================
CREATE TABLE @extschema@.exchange_rate (
    from_currency   TEXT NOT NULL,
    to_currency     TEXT NOT NULL,
    valid_from      DATE NOT NULL,
    rate            NUMERIC(19,10) NOT NULL,
    valid_to        DATE,
    PRIMARY KEY (from_currency, to_currency, valid_from)
);

-- =============================================================================
-- Rule: account determination rules
-- =============================================================================
CREATE TABLE @extschema@.rule (
    id              SERIAL PRIMARY KEY,
    source_table    TEXT NOT NULL,
    source_column   TEXT NOT NULL,
    entries         @extschema@.rule_entry[] NOT NULL,
    priority        INT NOT NULL DEFAULT 0,
    condition       TEXT,
    description     TEXT,
    currency_column TEXT,
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by      TEXT DEFAULT current_user
);

CREATE INDEX idx_rule_source ON @extschema@.rule(source_table, source_column);
"#,
    name = "bootstrap_tables",
    bootstrap
);
