-- =============================================================================
-- 11: Observability
-- =============================================================================
-- Monitor and debug pipelines using built-in observability functions.
--
-- This example creates a real pipeline, feeds it data (including some that
-- will cause errors), then demonstrates all the observability tools.

-- =============================================================================
-- SETUP: Create a pipeline with intentional edge cases
-- =============================================================================

SELECT pgkafka.create_typed_topic('events', $$
{
  "type": "object",
  "properties": {
    "event_id":   {"type": "string"},
    "event_type": {"type": "string"},
    "amount":     {"type": "string"},
    "user_id":    {"type": "string"}
  }
}
$$::jsonb);

CREATE TABLE IF NOT EXISTS processed_events (
    id         BIGSERIAL PRIMARY KEY,
    event_id   TEXT,
    event_type TEXT,
    amount     NUMERIC,
    user_id    TEXT,
    processed_at TIMESTAMPTZ DEFAULT now()
);

SELECT pgstreams.create_pipeline('demo_pipeline', $$
{
  "input": {
    "kafka": {
      "topic": "events",
      "group": "demo-obs",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "value_json->>'event_type' != 'heartbeat'"},
      {"mapping": {
        "event_id":   "value_json->>'event_id'",
        "event_type": "value_json->>'event_type'",
        "amount":     "(value_json->>'amount')::numeric",
        "user_id":    "value_json->>'user_id'"
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

SELECT pgstreams.start('demo_pipeline');

-- =============================================================================
-- INSERT TEST DATA (including records that may cause processing errors)
-- =============================================================================

INSERT INTO events (event_id, event_type, amount, user_id) VALUES
    ('EVT-001', 'purchase', '150.00', 'u-1'),
    ('EVT-002', 'heartbeat', '0', 'system'),       -- filtered out
    ('EVT-003', 'purchase', '299.99', 'u-2'),
    ('EVT-004', 'refund',   '50.00',  'u-1'),
    ('EVT-005', 'purchase', 'not_a_number', 'u-3'); -- may cause cast error

SELECT pg_sleep(3);

-- =============================================================================
-- OBSERVABILITY QUERIES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Pipeline status: see what's running
-- -----------------------------------------------------------------------------

SELECT * FROM pgstreams.status();
-- Returns: name, state, worker_id, error, started_at, stopped_at, uptime

-- -----------------------------------------------------------------------------
-- 2. Metrics: throughput, latency, lag
-- -----------------------------------------------------------------------------

SELECT * FROM pgstreams.metrics('demo_pipeline', 20);
-- Returns: pipeline, metric, value, measured_at

-- Throughput trend over the last hour
SELECT
    date_trunc('minute', measured_at) AS minute,
    avg(value) AS avg_rate
FROM pgstreams.metrics('demo_pipeline', 100)
WHERE metric = 'input_rate'
GROUP BY 1
ORDER BY 1 DESC;

-- All pipelines with errors in the last 10 minutes
SELECT DISTINCT pipeline
FROM pgstreams.metrics(NULL, 100)
WHERE metric = 'errors'
  AND value > 0
  AND measured_at > now() - interval '10 minutes';

-- -----------------------------------------------------------------------------
-- 3. Error log (Dead Letter Queue)
-- -----------------------------------------------------------------------------

SELECT * FROM pgstreams.errors('demo_pipeline', 10);
-- Returns: id, pipeline, processor, error, record, created_at

-- Quick trace: see the last 5 failed records
SELECT * FROM pgstreams.trace('demo_pipeline', 5);

-- Error distribution by processor
SELECT
    processor,
    count(*) AS error_count,
    min(created_at) AS first_error,
    max(created_at) AS last_error
FROM pgstreams.errors('demo_pipeline', 1000)
GROUP BY processor
ORDER BY error_count DESC;

-- -----------------------------------------------------------------------------
-- 4. Consumer lag
-- -----------------------------------------------------------------------------

SELECT * FROM pgstreams.lag();
-- Returns: pipeline, connector, committed_offset, updated_at

-- -----------------------------------------------------------------------------
-- 5. Pipeline lifecycle management
-- -----------------------------------------------------------------------------

-- Stop a pipeline
SELECT pgstreams.stop('demo_pipeline');

-- Check it stopped
SELECT name, state FROM pgstreams.status();

-- Restart it
SELECT pgstreams.start('demo_pipeline');

-- Update definition (auto-restarts if running)
SELECT pgstreams.update_pipeline('demo_pipeline', $$
{
  "input": {"kafka": {"topic": "events", "group": "demo-obs-v2", "start": "earliest"}},
  "pipeline": {
    "processors": [
      {"filter": "value_json->>'event_type' != 'heartbeat'"},
      {"mapping": {
        "event_id":   "value_json->>'event_id'",
        "event_type": "value_json->>'event_type'",
        "amount":     "coalesce((value_json->>'amount')::numeric, 0)",
        "user_id":    "value_json->>'user_id'"
      }}
    ]
  },
  "output": {"table": {"name": "public.processed_events", "mode": "append"}}
}
$$::jsonb);

-- -----------------------------------------------------------------------------
-- 6. Operational health dashboard (single query)
-- -----------------------------------------------------------------------------

SELECT
    s.name,
    s.state,
    s.uptime,
    s.error,
    l.committed_offset AS offset,
    (SELECT count(*) FROM pgstreams.errors(s.name, 1000)
     WHERE created_at > now() - interval '1 hour') AS errors_1h,
    (SELECT value FROM pgstreams.metrics(s.name, 1)
     WHERE metric = 'input_rate' LIMIT 1) AS current_rate
FROM pgstreams.status() s
LEFT JOIN pgstreams.lag() l ON l.pipeline = s.name
ORDER BY s.state, s.name;

-- =============================================================================
-- VERIFY: processed events arrived
-- =============================================================================

SELECT event_id, event_type, amount, user_id
FROM processed_events
ORDER BY event_id;

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('demo_pipeline');
SELECT pgstreams.drop_pipeline('demo_pipeline');
SELECT pgkafka.drop_topic('events');
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS processed_events;
