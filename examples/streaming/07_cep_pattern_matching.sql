-- =============================================================================
-- 07: Complex Event Processing (CEP)
-- =============================================================================
-- Detect ordered sequences of events within time windows.
-- Useful for fraud detection, anomaly detection, and workflow monitoring.

-- =============================================================================
-- SETUP: Typed topics
-- =============================================================================

-- Input: credit card transactions
SELECT pgkafka.create_typed_topic('transactions', $$
{
  "type": "object",
  "properties": {
    "tx_id":      {"type": "string"},
    "card_id":    {"type": "string"},
    "amount":     {"type": "number"},
    "merchant":   {"type": "string"},
    "timestamp":  {"type": "string", "format": "date-time"}
  },
  "required": ["tx_id", "card_id", "amount", "timestamp"]
}
$$::jsonb);

-- Output: fraud alerts
SELECT pgkafka.create_typed_topic('fraud_alerts', $$
{
  "type": "object",
  "properties": {
    "card_id":         {"type": "string"},
    "small_tx_count":  {"type": "integer"},
    "small_tx_total":  {"type": "number"},
    "large_tx_amount": {"type": "number"},
    "first_event":     {"type": "string", "format": "date-time"},
    "last_event":      {"type": "string", "format": "date-time"},
    "alert_type":      {"type": "string"}
  }
}
$$::jsonb);

-- Input: service health events
SELECT pgkafka.create_typed_topic('service_events', $$
{
  "type": "object",
  "properties": {
    "event_id":  {"type": "string"},
    "service":   {"type": "string"},
    "level":     {"type": "string"},
    "message":   {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["service", "level", "message", "timestamp"]
}
$$::jsonb);

-- Output: service incidents table
CREATE TABLE IF NOT EXISTS service_incidents (
    id           BIGSERIAL PRIMARY KEY,
    service      TEXT NOT NULL,
    warn_messages TEXT,
    error_message TEXT,
    pattern      TEXT,
    detected_at  TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

-- Card testing pattern: 4 small transactions followed by a large one
INSERT INTO transactions (tx_id, card_id, amount, merchant, "timestamp") VALUES
    ('TX-001', 'card-abc-123', 1.00,    'Gas Station',   '2025-03-15 10:00:00+00'),
    ('TX-002', 'card-abc-123', 2.50,    'Coffee Shop',   '2025-03-15 10:00:30+00'),
    ('TX-003', 'card-abc-123', 0.99,    'Vending',       '2025-03-15 10:01:00+00'),
    ('TX-004', 'card-abc-123', 5.00,    'Parking',       '2025-03-15 10:01:30+00'),
    ('TX-005', 'card-abc-123', 1299.99, 'Electronics',   '2025-03-15 10:02:00+00');

-- Normal card usage (should NOT trigger fraud alert)
INSERT INTO transactions (tx_id, card_id, amount, merchant, "timestamp") VALUES
    ('TX-010', 'card-xyz-999', 150.00,  'Grocery Store', '2025-03-15 10:00:00+00'),
    ('TX-011', 'card-xyz-999', 45.00,   'Restaurant',    '2025-03-15 12:00:00+00');

-- Service degradation pattern: warning -> warning -> error
INSERT INTO service_events (event_id, service, level, message, "timestamp") VALUES
    ('EVT-01', 'api-gateway', 'warning', 'High latency detected (p99 > 500ms)',  '2025-03-15 10:00:00+00'),
    ('EVT-02', 'api-gateway', 'info',    'Health check OK',                       '2025-03-15 10:00:30+00'),
    ('EVT-03', 'api-gateway', 'warning', 'Connection pool near capacity (90%)',   '2025-03-15 10:01:00+00'),
    ('EVT-04', 'api-gateway', 'error',   'Service unavailable - circuit breaker', '2025-03-15 10:01:30+00');

-- =============================================================================
-- PIPELINE 1: Fraud Detection — rapid small purchases + large one
-- =============================================================================
-- Pattern: 3+ small transactions (< $10) within 5 minutes, then one > $500.

SELECT pgstreams.create_pipeline('fraud_card_testing', $$
{
  "input": {
    "kafka": {
      "topic": "transactions",
      "group": "fraud-cep",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "cep": {
          "pattern": [
            {
              "name": "small_tx",
              "condition": "(value_json->>'amount')::numeric < 10",
              "quantifier": "{3,}"
            },
            {
              "name": "large_tx",
              "condition": "(value_json->>'amount')::numeric > 500"
            }
          ],
          "within": "5 minutes",
          "group_by": "value_json->>'card_id'",
          "match": "strict_contiguity",
          "emit": {
            "card_id":         "value_json->>'card_id'",
            "small_tx_count":  "count(small_tx)",
            "small_tx_total":  "sum(small_tx.amount)",
            "large_tx_amount": "large_tx.amount",
            "first_event":     "min(small_tx.timestamp)",
            "last_event":      "large_tx.timestamp",
            "alert_type":      "'card_testing'"
          }
        }
      }
    ]
  },
  "output": {
    "kafka": {
      "topic": "fraud_alerts",
      "key": "card_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Service Degradation — warning, warning, error
-- =============================================================================

SELECT pgstreams.create_pipeline('service_degradation', $$
{
  "input": {
    "kafka": {
      "topic": "service_events",
      "group": "health-cep",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "cep": {
          "pattern": [
            {
              "name": "warn1",
              "condition": "value_json->>'level' = 'warning'"
            },
            {
              "name": "warn2",
              "condition": "value_json->>'level' = 'warning'"
            },
            {
              "name": "err",
              "condition": "value_json->>'level' = 'error'"
            }
          ],
          "within": "2 minutes",
          "group_by": "value_json->>'service'",
          "match": "relaxed_contiguity",
          "emit": {
            "service":       "value_json->>'service'",
            "warn_messages": "array_agg(warn1.message || warn2.message)",
            "error_message": "err.message",
            "pattern":       "'degradation'",
            "detected_at":   "now()"
          }
        }
      }
    ]
  },
  "output": {
    "table": {
      "name": "public.service_incidents",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START PIPELINES
-- =============================================================================

SELECT pgstreams.start('fraud_card_testing');
SELECT pgstreams.start('service_degradation');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Fraud alerts: should detect card-abc-123 pattern
SELECT card_id, small_tx_count, small_tx_total, large_tx_amount, alert_type
FROM fraud_alerts;
-- Expected:
-- | card_id       | small_tx_count | small_tx_total | large_tx_amount | alert_type   |
-- |---------------|----------------|----------------|-----------------|--------------|
-- | card-abc-123  |              4 |           9.49 |         1299.99 | card_testing |

-- No alert for card-xyz-999 (normal usage, no small->large pattern)
SELECT count(*) AS should_be_zero
FROM fraud_alerts
WHERE card_id = 'card-xyz-999';

-- Service incidents: should detect api-gateway degradation
SELECT service, error_message, pattern
FROM service_incidents;
-- Expected:
-- | service     | error_message                          | pattern     |
-- |-------------|----------------------------------------|-------------|
-- | api-gateway | Service unavailable - circuit breaker  | degradation |

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('fraud_card_testing');
SELECT pgstreams.stop('service_degradation');
SELECT pgstreams.drop_pipeline('fraud_card_testing');
SELECT pgstreams.drop_pipeline('service_degradation');
SELECT pgkafka.drop_topic('fraud_alerts');
SELECT pgkafka.drop_topic('service_events');
SELECT pgkafka.drop_topic('transactions');
DROP TABLE IF EXISTS fraud_alerts;
DROP TABLE IF EXISTS service_events;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS service_incidents;
