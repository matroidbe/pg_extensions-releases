-- =============================================================================
-- 04: Unbounded (Kappa) Aggregation with Rollups
-- =============================================================================
-- Incrementally aggregate without windows — every record updates the running
-- totals. Optionally generate time-based rollup tables for fast historical
-- queries at different granularities.

-- =============================================================================
-- SETUP: Typed topic
-- =============================================================================

SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "category":    {"type": "string"},
    "amount":      {"type": "number"},
    "region":      {"type": "string"},
    "ordered_at":  {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "category", "amount", "region"]
}
$$::jsonb);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

INSERT INTO orders (order_id, customer_id, category, amount, region, ordered_at) VALUES
    ('ORD-001', 'C-1', 'hardware', 150.00, 'US-East', '2025-03-15 10:00:00+00'),
    ('ORD-002', 'C-2', 'software', 299.00, 'EU-West', '2025-03-15 10:01:00+00'),
    ('ORD-003', 'C-1', 'hardware',  75.00, 'US-East', '2025-03-15 10:02:00+00'),
    ('ORD-004', 'C-3', 'software', 499.00, 'US-West', '2025-03-15 10:03:00+00'),
    ('ORD-005', 'C-2', 'hardware', 200.00, 'EU-West', '2025-03-15 10:04:00+00'),
    ('ORD-006', 'C-1', 'services', 120.00, 'US-East', '2025-03-15 10:30:00+00'),
    ('ORD-007', 'C-4', 'software', 850.00, 'APAC',    '2025-03-15 11:00:00+00'),
    ('ORD-008', 'C-2', 'hardware',  50.00, 'EU-West', '2025-03-15 11:30:00+00');

-- =============================================================================
-- PIPELINE 1: Running sales totals by product category
-- =============================================================================

-- Output dashboard table
CREATE TABLE IF NOT EXISTS category_dashboard (
    category        TEXT PRIMARY KEY,
    total_revenue   NUMERIC,
    order_count     BIGINT,
    avg_order_value NUMERIC
);

SELECT pgstreams.create_pipeline('sales_by_category', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "sales-agg",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "aggregate": {
          "group_by": "value_json->>'category'",
          "columns": {
            "total_revenue":    "sum((value_json->>'amount')::numeric)",
            "order_count":      "count(*)",
            "avg_order_value":  "avg((value_json->>'amount')::numeric)"
          },
          "state_table": "sales_by_category",
          "emit": "updated_rows"
        }
      }
    ]
  },
  "output": {
    "table": {
      "name": "public.category_dashboard",
      "mode": "upsert",
      "key": "category"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Regional sales with rollup tables
-- =============================================================================
-- Auto-creates rollup tables:
--   pgstreams.regional_sales_5m   (5-minute buckets)
--   pgstreams.regional_sales_1h   (1-hour buckets)

SELECT pgstreams.create_pipeline('regional_sales', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "regional-sales-agg",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "aggregate": {
          "group_by": "value_json->>'region'",
          "columns": {
            "revenue":          "sum((value_json->>'amount')::numeric)",
            "orders":           "count(*)",
            "unique_customers": "count(DISTINCT value_json->>'customer_id')"
          },
          "state_table": "regional_sales",
          "rollups": ["5 minutes", "1 hour"],
          "emit": "input"
        }
      }
    ]
  },
  "output": {
    "drop": {}
  }
}
$$::jsonb);

-- =============================================================================
-- START PIPELINES
-- =============================================================================

SELECT pgstreams.start('sales_by_category');
SELECT pgstreams.start('regional_sales');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Running totals by category
SELECT category, total_revenue, order_count, round(avg_order_value, 2) AS avg_order
FROM category_dashboard
ORDER BY total_revenue DESC;
-- Expected:
-- | category | total_revenue | order_count | avg_order |
-- |----------|---------------|-------------|-----------|
-- | software |       1648.00 |           3 |    549.33 |
-- | hardware |        475.00 |           3 |    158.33 |
-- | services |        120.00 |           1 |    120.00 |

-- State table (internal aggregate state)
SELECT * FROM pgstreams.sales_by_category ORDER BY total_revenue DESC;

-- Regional aggregates
SELECT * FROM pgstreams.regional_sales ORDER BY revenue DESC;
-- Expected:
-- | group_key | revenue | orders | unique_customers |
-- |-----------|---------|--------|------------------|
-- | US-East   |  345.00 |      3 |                1 |
-- | EU-West   |  549.00 |      3 |                1 |
-- | US-West   |  499.00 |      1 |                1 |
-- | APAC      |  850.00 |      1 |                1 |

-- Rollup tables (5-minute granularity)
SELECT * FROM pgstreams.regional_sales_5m ORDER BY bucket;

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('sales_by_category');
SELECT pgstreams.stop('regional_sales');
SELECT pgstreams.drop_pipeline('sales_by_category');
SELECT pgstreams.drop_pipeline('regional_sales');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS category_dashboard;
DROP TABLE IF EXISTS orders;
