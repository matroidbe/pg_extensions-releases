-- =============================================================================
-- 01: Basic Filter and Mapping
-- =============================================================================
-- Filter records from a Kafka topic and reshape them into a new schema.
--
-- Scenario: An "orders" topic contains all orders. We want to extract only
-- high-value orders (> $500) and produce a simplified record to a
-- "high_value_orders_out" topic.
--
-- This example is fully self-contained: it creates typed topics, inserts
-- test data, runs the pipeline, and verifies the output.

-- =============================================================================
-- SETUP: Create typed topics (tables + Kafka topics in one step)
-- =============================================================================

-- Input topic: orders (creates public.orders table automatically)
SELECT pgkafka.create_typed_topic('orders', '{
  "type": "object",
  "properties": {
    "order_id":         {"type": "string"},
    "customer_id":      {"type": "integer"},
    "amount":           {"type": "number"},
    "currency":         {"type": "string", "maxLength": 3},
    "region":           {"type": "string"},
    "created_at":       {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "customer_id", "amount", "currency"]
}'::jsonb);

-- Output topic: high_value_orders_out
SELECT pgkafka.create_typed_topic('high_value_orders_out', '{
  "type": "object",
  "properties": {
    "order_id":     {"type": "string"},
    "customer_id":  {"type": "integer"},
    "amount":       {"type": "number"},
    "currency":     {"type": "string", "maxLength": 3},
    "flagged_at":   {"type": "string", "format": "date-time"}
  }
}'::jsonb);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

INSERT INTO orders (order_id, customer_id, amount, currency, region, created_at) VALUES
    ('ORD-0001', 1,  750.00, 'USD', 'US-East', '2025-03-15 10:00:00+00'),
    ('ORD-0002', 2,  120.00, 'USD', 'EU-West', '2025-03-15 10:01:00+00'),  -- below threshold
    ('ORD-0003', 3, 1200.00, 'EUR', 'EU-West', '2025-03-15 10:02:00+00'),
    ('ORD-0004', 1,   49.99, 'USD', 'US-East', '2025-03-15 10:03:00+00'),  -- below threshold
    ('ORD-0005', 4, 5000.00, 'USD', 'APAC',    '2025-03-15 10:04:00+00'),
    ('ORD-0006', 2,  501.00, 'GBP', 'EU-West', '2025-03-15 10:05:00+00');

-- =============================================================================
-- CREATE AND START PIPELINE
-- =============================================================================

SELECT pgstreams.create_pipeline('high_value_orders', '{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "high-value-filter",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "(value_json->>''amount'')::numeric > 500"},
      {"mapping": {
        "order_id":    "value_json->>''order_id''",
        "customer_id": "(value_json->>''customer_id'')::int",
        "amount":      "(value_json->>''amount'')::numeric",
        "currency":    "value_json->>''currency''",
        "flagged_at":  "now()"
      }}
    ]
  },
  "output": {
    "kafka": {
      "topic": "high_value_orders_out",
      "key": "order_id"
    }
  }
}'::jsonb);

SELECT pgstreams.start('high_value_orders');

-- =============================================================================
-- VERIFY
-- =============================================================================

-- Wait for processing
SELECT pg_sleep(2);

-- Check pipeline status
SELECT * FROM pgstreams.status();

-- Verify output: should contain 4 records (ORD-0001, ORD-0003, ORD-0005, ORD-0006)
SELECT order_id, customer_id, amount, currency
FROM high_value_orders_out
ORDER BY amount;
-- Expected:
-- | order_id | customer_id | amount  | currency |
-- |----------|-------------|---------|----------|
-- | ORD-0006 |           2 |  501.00 | GBP      |
-- | ORD-0001 |           1 |  750.00 | USD      |
-- | ORD-0003 |           3 | 1200.00 | EUR      |
-- | ORD-0005 |           4 | 5000.00 | USD      |

-- Confirm filtered-out records are NOT in the output
SELECT count(*) AS should_be_zero
FROM high_value_orders_out
WHERE amount <= 500;

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('high_value_orders');
SELECT pgstreams.drop_pipeline('high_value_orders');
SELECT pgkafka.drop_topic('high_value_orders_out');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS high_value_orders_out;
DROP TABLE IF EXISTS orders;
