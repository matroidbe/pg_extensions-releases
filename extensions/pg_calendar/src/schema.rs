// Bootstrap SQL for pg_calendar tables and indexes

pgrx::extension_sql!(
    r#"
-- =============================================================================
-- Calendar: named working calendars
-- =============================================================================
CREATE TABLE @extschema@.calendar (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- =============================================================================
-- Weekly pattern: 7 rows per calendar (one per day of week)
-- day_of_week: 0=Sunday, 1=Monday, ..., 6=Saturday (matches EXTRACT(DOW))
-- =============================================================================
CREATE TABLE @extschema@.weekly_pattern (
    id          SERIAL PRIMARY KEY,
    calendar_id INT NOT NULL REFERENCES @extschema@.calendar(id) ON DELETE CASCADE,
    day_of_week SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    is_working  BOOLEAN NOT NULL DEFAULT false,
    UNIQUE(calendar_id, day_of_week)
);

CREATE INDEX idx_weekly_pattern_calendar ON @extschema@.weekly_pattern(calendar_id);

-- =============================================================================
-- Holiday: named non-working days, optionally recurring (month+day match)
-- =============================================================================
CREATE TABLE @extschema@.holiday (
    id           SERIAL PRIMARY KEY,
    calendar_id  INT NOT NULL REFERENCES @extschema@.calendar(id) ON DELETE CASCADE,
    name         TEXT NOT NULL,
    holiday_date DATE NOT NULL,
    recurring    BOOLEAN NOT NULL DEFAULT false,
    UNIQUE(calendar_id, holiday_date, name)
);

CREATE INDEX idx_holiday_calendar ON @extschema@.holiday(calendar_id);
CREATE INDEX idx_holiday_date ON @extschema@.holiday(holiday_date);

-- =============================================================================
-- Exception: per-date overrides (highest priority)
-- =============================================================================
CREATE TABLE @extschema@.exception (
    id             SERIAL PRIMARY KEY,
    calendar_id    INT NOT NULL REFERENCES @extschema@.calendar(id) ON DELETE CASCADE,
    exception_date DATE NOT NULL,
    is_working     BOOLEAN NOT NULL,
    reason         TEXT,
    UNIQUE(calendar_id, exception_date)
);

CREATE INDEX idx_exception_calendar ON @extschema@.exception(calendar_id);
"#,
    name = "bootstrap_tables",
    bootstrap
);
