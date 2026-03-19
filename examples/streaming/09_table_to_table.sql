-- =============================================================================
-- 09: Table-to-Table Pipelines
-- =============================================================================
-- Poll a PostgreSQL table for new rows, transform, and write to another table.
-- No Kafka required — pure PostgreSQL streaming.

-- =============================================================================
-- SETUP: Source and target tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS raw_events (
    id          BIGSERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS processed_events (
    id           BIGSERIAL PRIMARY KEY,
    event_type   TEXT NOT NULL,
    user_id      TEXT,
    action       TEXT,
    metadata     JSONB,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS device_readings (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    temperature NUMERIC(5,2),
    humidity    NUMERIC(5,2),
    battery     INT,
    read_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS device_latest (
    device_id      TEXT PRIMARY KEY,
    temperature    NUMERIC(5,2),
    humidity       NUMERIC(5,2),
    battery        INT,
    last_reading   TIMESTAMPTZ,
    reading_count  INT DEFAULT 0
);

-- =============================================================================
-- PIPELINE 1: Process raw events (filter heartbeats, extract fields)
-- =============================================================================

SELECT pgstreams.create_pipeline('process_raw_events', $$
{
  "input": {
    "table": {
      "name": "public.raw_events",
      "offset_column": "id",
      "poll": "2s"
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "value_json->>'event_type' != 'heartbeat'"},
      {"mapping": {
        "event_type": "value_json->>'event_type'",
        "user_id":    "value_json->'payload'->>'user_id'",
        "action":     "value_json->'payload'->>'action'",
        "metadata":   "value_json->'payload' - 'user_id' - 'action'"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "public.processed_events",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Device latest state (upsert)
-- =============================================================================

SELECT pgstreams.create_pipeline('device_latest_state', $$
{
  "input": {
    "table": {
      "name": "public.device_readings",
      "offset_column": "id",
      "poll": "1s"
    }
  },
  "pipeline": {
    "processors": [
      {"mapping": {
        "device_id":     "value_json->>'device_id'",
        "temperature":   "(value_json->>'temperature')::numeric",
        "humidity":      "(value_json->>'humidity')::numeric",
        "battery":       "(value_json->>'battery')::int",
        "last_reading":  "value_json->>'read_at'",
        "reading_count": "1"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "public.device_latest",
      "mode": "upsert",
      "key": "device_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START PIPELINES
-- =============================================================================

SELECT pgstreams.start('process_raw_events');
SELECT pgstreams.start('device_latest_state');

-- =============================================================================
-- INSERT TEST DATA (pipelines process in real time)
-- =============================================================================

-- Events: mix of clicks, signups, and heartbeats
INSERT INTO raw_events (event_type, payload) VALUES
    ('click',     '{"user_id": "u-1", "action": "view", "page": "/home"}'),
    ('heartbeat', '{"status": "ok"}'),                                       -- filtered out
    ('signup',    '{"user_id": "u-2", "action": "register", "source": "google"}'),
    ('click',     '{"user_id": "u-1", "action": "click", "page": "/products", "button": "buy"}'),
    ('heartbeat', '{"status": "ok"}');                                       -- filtered out

-- Device readings: multiple readings per device
INSERT INTO device_readings (device_id, temperature, humidity, battery, read_at) VALUES
    ('sensor-001', 22.5, 45.0, 95, '2025-03-15 10:00:00+00'),
    ('sensor-002', 28.3, 60.2, 80, '2025-03-15 10:00:00+00'),
    ('sensor-001', 23.1, 44.5, 94, '2025-03-15 10:05:00+00'),   -- update sensor-001
    ('sensor-003', 18.0, 70.0, 50, '2025-03-15 10:05:00+00'),
    ('sensor-001', 24.0, 43.0, 93, '2025-03-15 10:10:00+00');   -- update sensor-001 again

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Processed events: heartbeats filtered out, fields extracted
SELECT event_type, user_id, action, metadata
FROM processed_events
ORDER BY id;
-- Expected (3 rows, no heartbeats):
-- | event_type | user_id | action   | metadata                          |
-- |------------|---------|----------|-----------------------------------|
-- | click      | u-1     | view     | {"page": "/home"}                 |
-- | signup     | u-2     | register | {"source": "google"}              |
-- | click      | u-1     | click    | {"page": "/products", "button":..}|

-- Heartbeats were filtered
SELECT count(*) AS should_be_zero
FROM processed_events
WHERE event_type = 'heartbeat';

-- Device latest: only most recent reading per device
SELECT device_id, temperature, humidity, battery, last_reading
FROM device_latest
ORDER BY device_id;
-- Expected (3 devices, sensor-001 has latest values):
-- | device_id  | temperature | humidity | battery | last_reading        |
-- |------------|-------------|----------|---------|---------------------|
-- | sensor-001 |       24.00 |    43.00 |      93 | 2025-03-15 10:10:00 |
-- | sensor-002 |       28.30 |    60.20 |      80 | 2025-03-15 10:00:00 |
-- | sensor-003 |       18.00 |    70.00 |      50 | 2025-03-15 10:05:00 |

-- Consumer lag
SELECT * FROM pgstreams.lag();

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('process_raw_events');
SELECT pgstreams.stop('device_latest_state');
SELECT pgstreams.drop_pipeline('process_raw_events');
SELECT pgstreams.drop_pipeline('device_latest_state');
DROP TABLE IF EXISTS raw_events;
DROP TABLE IF EXISTS processed_events;
DROP TABLE IF EXISTS device_readings;
DROP TABLE IF EXISTS device_latest;
