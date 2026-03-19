// Bootstrap SQL for pg_currency tables and indexes

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- currency: ISO 4217 currency master data
-- =============================================================================
CREATE TABLE @extschema@.currency (
    code            TEXT PRIMARY KEY CHECK (length(code) = 3),
    name            TEXT NOT NULL,
    symbol          TEXT,
    decimal_places  SMALLINT NOT NULL DEFAULT 2 CHECK (decimal_places BETWEEN 0 AND 4),
    active          BOOLEAN NOT NULL DEFAULT true,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- exchange_rate: rates relative to base currency (Odoo-inspired model)
-- rate meaning: 1 unit of base currency = rate units of this currency
-- =============================================================================
CREATE TABLE @extschema@.exchange_rate (
    id              BIGSERIAL PRIMARY KEY,
    currency_code   TEXT NOT NULL REFERENCES @extschema@.currency(code) ON DELETE CASCADE,
    rate            NUMERIC(20, 10) NOT NULL CHECK (rate > 0),
    valid_from      DATE NOT NULL DEFAULT CURRENT_DATE,
    rate_type       TEXT DEFAULT NULL,
    source          TEXT DEFAULT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX idx_rate_unique ON @extschema@.exchange_rate(currency_code, valid_from, COALESCE(rate_type, ''));
CREATE INDEX idx_rate_lookup ON @extschema@.exchange_rate(currency_code, valid_from DESC);
"#,
    name = "bootstrap_tables",
    bootstrap
);
