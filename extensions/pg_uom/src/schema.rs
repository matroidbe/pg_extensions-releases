// Bootstrap SQL for pg_uom tables and indexes

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- Category: human-readable grouping for units (mass, length, volume, etc.)
-- =============================================================================
CREATE TABLE @extschema@.category (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Unit: individual units with SI dimension vectors and conversion factors
-- factor is relative to SI base units (kg, m, s, K, mol, A, cd)
-- offset handles affine conversions (temperature: base = value * factor + offset)
-- =============================================================================
CREATE TABLE @extschema@.unit (
    id              SERIAL PRIMARY KEY,
    category_id     INT REFERENCES @extschema@.category(id) ON DELETE SET NULL,
    name            TEXT NOT NULL UNIQUE,
    symbol          TEXT NOT NULL UNIQUE,
    factor          DOUBLE PRECISION NOT NULL CHECK (factor > 0),
    "offset"        DOUBLE PRECISION NOT NULL DEFAULT 0,
    -- SI base dimension exponents
    dim_mass        SMALLINT NOT NULL DEFAULT 0,   -- kg
    dim_length      SMALLINT NOT NULL DEFAULT 0,   -- m
    dim_time        SMALLINT NOT NULL DEFAULT 0,   -- s
    dim_temp        SMALLINT NOT NULL DEFAULT 0,   -- K
    dim_amount      SMALLINT NOT NULL DEFAULT 0,   -- mol
    dim_current     SMALLINT NOT NULL DEFAULT 0,   -- A
    dim_luminosity  SMALLINT NOT NULL DEFAULT 0,   -- cd
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_unit_category ON @extschema@.unit(category_id);
CREATE INDEX idx_unit_symbol ON @extschema@.unit(symbol);
CREATE INDEX idx_unit_dims ON @extschema@.unit(
    dim_mass, dim_length, dim_time, dim_temp,
    dim_amount, dim_current, dim_luminosity
);
"#,
    name = "bootstrap_tables",
    bootstrap
);
