-- =============================================================================
-- 06: Change Data Capture (CDC)
-- =============================================================================
-- Capture INSERT, UPDATE, and DELETE operations from PostgreSQL tables using
-- logical replication and process them as a stream.

-- =============================================================================
-- SETUP: Source table and publication
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INT NOT NULL,
    amount       NUMERIC(10,2) NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    region       TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL gives us old row values on UPDATE/DELETE
ALTER TABLE orders REPLICA IDENTITY FULL;

-- PostgreSQL logical replication requires a publication
CREATE PUBLICATION orders_pub FOR TABLE orders;

-- Output topic for the changelog
SELECT pgkafka.create_typed_topic('orders_changelog', $$
{
  "type": "object",
  "properties": {
    "operation":   {"type": "string"},
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "amount":      {"type": "number"},
    "status":      {"type": "string"},
    "old_status":  {"type": "string"},
    "region":      {"type": "string"},
    "changed_at":  {"type": "string", "format": "date-time"},
    "lsn":         {"type": "string"}
  }
}
$$::jsonb);

-- Output topic for status transitions
SELECT pgkafka.create_typed_topic('order_status_transitions', $$
{
  "type": "object",
  "properties": {
    "order_id":   {"type": "string"},
    "old_status": {"type": "string"},
    "new_status": {"type": "string"},
    "changed_at": {"type": "string", "format": "date-time"}
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 1: Stream all order changes to Kafka
-- =============================================================================

SELECT pgstreams.create_pipeline('orders_cdc', $$
{
  "input": {
    "cdc": {
      "table": "public.orders",
      "publication": "orders_pub",
      "slot": "orders_cdc_slot",
      "operations": ["INSERT", "UPDATE", "DELETE"]
    }
  },
  "pipeline": {
    "processors": [
      {"mapping": {
        "operation":   "operation",
        "order_id":    "coalesce(new->>'order_id', old->>'order_id')",
        "customer_id": "coalesce(new->>'customer_id', old->>'customer_id')",
        "amount":      "(new->>'amount')::numeric",
        "status":      "new->>'status'",
        "old_status":  "old->>'status'",
        "region":      "coalesce(new->>'region', old->>'region')",
        "changed_at":  "commit_timestamp",
        "lsn":         "lsn"
      }}
    ]
  },
  "output": {
    "kafka": {
      "topic": "orders_changelog",
      "key": "order_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Only capture status transitions
-- =============================================================================

SELECT pgstreams.create_pipeline('order_status_changes', $$
{
  "input": {
    "cdc": {
      "table": "public.orders",
      "publication": "orders_pub",
      "slot": "order_status_slot",
      "operations": ["UPDATE"]
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "old->>'status' IS DISTINCT FROM new->>'status'"},
      {"mapping": {
        "order_id":    "new->>'order_id'",
        "old_status":  "old->>'status'",
        "new_status":  "new->>'status'",
        "changed_at":  "commit_timestamp"
      }},
      {"log": {
        "level": "info",
        "message": "'Order ' || order_id || ': ' || old_status || ' -> ' || new_status"
      }}
    ]
  },
  "output": {
    "kafka": {
      "topic": "order_status_transitions",
      "key": "order_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 3: CDC to live aggregate (materialized view replacement)
-- =============================================================================

SELECT pgstreams.create_pipeline('orders_by_region_live', $$
{
  "input": {
    "cdc": {
      "table": "public.orders",
      "publication": "orders_pub",
      "slot": "orders_region_slot",
      "operations": ["INSERT"]
    }
  },
  "pipeline": {
    "processors": [
      {
        "aggregate": {
          "group_by": "new->>'region'",
          "columns": {
            "total_orders":  "count(*)",
            "total_revenue": "sum((new->>'amount')::numeric)",
            "avg_order":     "avg((new->>'amount')::numeric)"
          },
          "state_table": "orders_by_region_live",
          "emit": "none"
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
-- START ALL PIPELINES
-- =============================================================================

SELECT pgstreams.start('orders_cdc');
SELECT pgstreams.start('order_status_changes');
SELECT pgstreams.start('orders_by_region_live');

-- =============================================================================
-- GENERATE CHANGES (these trigger CDC)
-- =============================================================================

-- Inserts
INSERT INTO orders (customer_id, amount, status, region) VALUES
    (1, 150.00, 'pending',   'US-East'),
    (2, 750.00, 'pending',   'EU-West'),
    (3, 300.00, 'pending',   'US-East');

-- Wait for CDC to capture inserts
SELECT pg_sleep(2);

-- Status transitions (triggers pipeline 2)
UPDATE orders SET status = 'confirmed', updated_at = now() WHERE order_id = 1;
UPDATE orders SET status = 'confirmed', updated_at = now() WHERE order_id = 2;
UPDATE orders SET status = 'shipped',   updated_at = now() WHERE order_id = 1;

-- Non-status update (should NOT trigger pipeline 2)
UPDATE orders SET amount = 160.00, updated_at = now() WHERE order_id = 3;

-- Delete
DELETE FROM orders WHERE order_id = 3;

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Changelog: should contain all operations
SELECT operation, order_id, status, old_status, amount
FROM orders_changelog
ORDER BY changed_at;
-- Expected (approximately):
-- | operation | order_id | status    | old_status | amount |
-- |-----------|----------|-----------|------------|--------|
-- | INSERT    | 1        | pending   | NULL       | 150.00 |
-- | INSERT    | 2        | pending   | NULL       | 750.00 |
-- | INSERT    | 3        | pending   | NULL       | 300.00 |
-- | UPDATE    | 1        | confirmed | pending    | 150.00 |
-- | UPDATE    | 2        | confirmed | pending    | 750.00 |
-- | UPDATE    | 1        | shipped   | confirmed  | 150.00 |
-- | UPDATE    | 3        | pending   | pending    | 160.00 |
-- | DELETE    | 3        | NULL      | pending    | NULL   |

-- Status transitions: only actual status changes
SELECT order_id, old_status, new_status
FROM order_status_transitions
ORDER BY changed_at;
-- Expected (no row for order 3's amount-only update):
-- | order_id | old_status | new_status |
-- |----------|------------|------------|
-- | 1        | pending    | confirmed  |
-- | 2        | pending    | confirmed  |
-- | 1        | confirmed  | shipped    |

-- Live regional aggregate
SELECT * FROM pgstreams.orders_by_region_live ORDER BY total_revenue DESC;
-- Expected:
-- | group_key | total_orders | total_revenue | avg_order |
-- |-----------|-------------|---------------|-----------|
-- | EU-West   |           1 |        750.00 |    750.00 |
-- | US-East   |           2 |        450.00 |    225.00 |

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('orders_cdc');
SELECT pgstreams.stop('order_status_changes');
SELECT pgstreams.stop('orders_by_region_live');
SELECT pgstreams.drop_pipeline('orders_cdc');
SELECT pgstreams.drop_pipeline('order_status_changes');
SELECT pgstreams.drop_pipeline('orders_by_region_live');
DROP PUBLICATION IF EXISTS orders_pub;
SELECT pgkafka.drop_topic('orders_changelog');
SELECT pgkafka.drop_topic('order_status_transitions');
DROP TABLE IF EXISTS orders_changelog;
DROP TABLE IF EXISTS order_status_transitions;
DROP TABLE IF EXISTS orders CASCADE;
