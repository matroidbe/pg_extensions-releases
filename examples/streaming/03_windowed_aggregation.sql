-- =============================================================================
-- 03: Windowed Aggregation
-- =============================================================================
-- Aggregate streaming records over time windows.
--
-- Supports: tumbling, sliding, session, and count-based windows.

-- =============================================================================
-- SETUP: Typed topics
-- =============================================================================

-- Input: orders with event_time for windowing
SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "amount":      {"type": "number"},
    "region":      {"type": "string"},
    "event_time":  {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "amount", "region", "event_time"]
}
$$::jsonb);

-- Input: clickstream for session windows
SELECT pgkafka.create_typed_topic('clickstream', $$
{
  "type": "object",
  "properties": {
    "user_id":   {"type": "string"},
    "page_url":  {"type": "string"},
    "timestamp": {"type": "string", "format": "date-time"}
  },
  "required": ["user_id", "page_url", "timestamp"]
}
$$::jsonb);

-- Output table for windowed results
CREATE TABLE IF NOT EXISTS order_stats_5m_out (
    id              BIGSERIAL PRIMARY KEY,
    region          TEXT,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    order_count     BIGINT,
    total_amount    NUMERIC,
    avg_amount      NUMERIC,
    max_amount      NUMERIC
);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

-- Orders spread across two 5-minute windows
-- Window 1: 10:00 - 10:05
INSERT INTO orders (order_id, customer_id, amount, region, event_time) VALUES
    ('ORD-001', 'C-1', 200.00, 'US-East', '2025-03-15 10:00:30+00'),
    ('ORD-002', 'C-2', 800.00, 'US-East', '2025-03-15 10:01:00+00'),
    ('ORD-003', 'C-3', 150.00, 'EU-West', '2025-03-15 10:02:00+00'),
    ('ORD-004', 'C-1', 500.00, 'US-East', '2025-03-15 10:04:00+00');
-- Window 2: 10:05 - 10:10
INSERT INTO orders (order_id, customer_id, amount, region, event_time) VALUES
    ('ORD-005', 'C-4', 300.00, 'EU-West', '2025-03-15 10:06:00+00'),
    ('ORD-006', 'C-2', 950.00, 'US-East', '2025-03-15 10:07:00+00');
-- Late event (after watermark) to trigger window 2 close
INSERT INTO orders (order_id, customer_id, amount, region, event_time) VALUES
    ('ORD-007', 'C-1', 100.00, 'US-East', '2025-03-15 10:15:30+00');

-- Clickstream data for session windows
INSERT INTO clickstream (user_id, page_url, "timestamp") VALUES
    ('u-123', '/home',              '2025-03-15 10:00:00+00'),
    ('u-123', '/products',          '2025-03-15 10:02:00+00'),
    ('u-123', '/products/widget',   '2025-03-15 10:05:00+00'),
    ('u-123', '/cart',              '2025-03-15 10:08:00+00'),
    ('u-123', '/checkout',          '2025-03-15 10:10:00+00'),
    -- 35-min gap -> session closes
    ('u-123', '/home',              '2025-03-15 10:45:00+00'),
    ('u-456', '/home',              '2025-03-15 10:00:00+00'),
    ('u-456', '/about',             '2025-03-15 10:03:00+00');

-- =============================================================================
-- PIPELINE 1: Tumbling Window — 5-minute order summaries by region
-- =============================================================================
-- Non-overlapping, fixed-size windows. Each record belongs to exactly one window.

SELECT pgstreams.create_pipeline('order_stats_5m', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "order-stats-5m",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "window": {
          "type": "tumbling",
          "size": "5 minutes",
          "group_by": "value_json->>'region'",
          "columns": {
            "order_count":  "count(*)",
            "total_amount": "sum((value_json->>'amount')::numeric)",
            "avg_amount":   "avg((value_json->>'amount')::numeric)",
            "max_amount":   "max((value_json->>'amount')::numeric)"
          },
          "time": {
            "field": "value_json->>'event_time'",
            "type": "event_time",
            "watermark": "10 seconds"
          },
          "emit": "on_close"
        }
      }
    ]
  },
  "output": {
    "table": {
      "name": "public.order_stats_5m_out",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Session Window — user activity sessions
-- =============================================================================
-- Dynamic windows that close when no events arrive within the gap period.

CREATE TABLE IF NOT EXISTS user_sessions (
    id            BIGSERIAL PRIMARY KEY,
    user_id       TEXT,
    window_start  TIMESTAMPTZ,
    window_end    TIMESTAMPTZ,
    page_views    BIGINT,
    unique_pages  BIGINT,
    first_page    TEXT,
    last_page     TEXT
);

SELECT pgstreams.create_pipeline('user_sessions', $$
{
  "input": {
    "kafka": {
      "topic": "clickstream",
      "group": "session-tracker",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "window": {
          "type": "session",
          "gap": "30 minutes",
          "group_by": "value_json->>'user_id'",
          "columns": {
            "page_views":    "count(*)",
            "unique_pages":  "count(DISTINCT value_json->>'page_url')",
            "first_page":    "min(value_json->>'page_url')",
            "last_page":     "max(value_json->>'page_url')"
          },
          "time": {
            "field": "value_json->>'timestamp'",
            "type": "event_time",
            "watermark": "5 minutes"
          },
          "emit": "on_close"
        }
      }
    ]
  },
  "output": {
    "table": {
      "name": "public.user_sessions",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 3: Count Window — emit after every 3 orders per customer
-- =============================================================================

SELECT pgkafka.create_typed_topic('customer_order_batches', $$
{
  "type": "object",
  "properties": {
    "customer_id": {"type": "string"},
    "batch_total": {"type": "number"},
    "batch_count": {"type": "integer"}
  }
}
$$::jsonb);

SELECT pgstreams.create_pipeline('order_batches', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "order-batches",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "window": {
          "type": "count",
          "size": "3",
          "group_by": "value_json->>'customer_id'",
          "columns": {
            "batch_total":  "sum((value_json->>'amount')::numeric)",
            "batch_count":  "count(*)"
          },
          "emit": "on_close"
        }
      }
    ]
  },
  "output": {
    "kafka": {
      "topic": "customer_order_batches",
      "key": "customer_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START ALL PIPELINES
-- =============================================================================

SELECT pgstreams.start('order_stats_5m');
SELECT pgstreams.start('user_sessions');
SELECT pgstreams.start('order_batches');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Tumbling windows: should see completed windows
SELECT region, window_start, window_end, order_count, total_amount, avg_amount
FROM order_stats_5m_out
ORDER BY window_start, region;
-- Expected (window 10:00-10:05):
-- | region  | window_start        | window_end          | order_count | total_amount | avg_amount |
-- |---------|---------------------|---------------------|-------------|--------------|------------|
-- | EU-West | 2025-03-15 10:00:00 | 2025-03-15 10:05:00 |           1 |       150.00 |     150.00 |
-- | US-East | 2025-03-15 10:00:00 | 2025-03-15 10:05:00 |           3 |      1500.00 |     500.00 |

-- Session windows: u-123's first session should be closed (35-min gap)
SELECT user_id, page_views, unique_pages, first_page, last_page
FROM user_sessions
ORDER BY user_id, window_start;

-- Count windows: customer C-1 has 3 orders -> should have 1 batch
SELECT customer_id, batch_total, batch_count
FROM customer_order_batches;

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('order_stats_5m');
SELECT pgstreams.stop('user_sessions');
SELECT pgstreams.stop('order_batches');
SELECT pgstreams.drop_pipeline('order_stats_5m');
SELECT pgstreams.drop_pipeline('user_sessions');
SELECT pgstreams.drop_pipeline('order_batches');
SELECT pgkafka.drop_topic('customer_order_batches');
SELECT pgkafka.drop_topic('clickstream');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS order_stats_5m_out;
DROP TABLE IF EXISTS user_sessions;
DROP TABLE IF EXISTS customer_order_batches;
DROP TABLE IF EXISTS clickstream;
DROP TABLE IF EXISTS orders;
