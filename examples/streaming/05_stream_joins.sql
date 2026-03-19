-- =============================================================================
-- 05: Stream Joins
-- =============================================================================
-- Join two streams together (stream-to-stream) or join a stream with a
-- PostgreSQL table (stream-to-table lookup).

-- =============================================================================
-- SETUP: Typed topics and reference tables
-- =============================================================================

-- Input: orders
SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "amount":      {"type": "number"},
    "region":      {"type": "string"},
    "created_at":  {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "amount"]
}
$$::jsonb);

-- Input: payments
SELECT pgkafka.create_typed_topic('payments', $$
{
  "type": "object",
  "properties": {
    "payment_id": {"type": "string"},
    "order_id":   {"type": "string"},
    "amount":     {"type": "number"},
    "status":     {"type": "string"},
    "paid_at":    {"type": "string", "format": "date-time"}
  },
  "required": ["payment_id", "order_id", "amount", "status"]
}
$$::jsonb);

-- Output topic for order-payment join
SELECT pgkafka.create_typed_topic('order_payment_status', $$
{
  "type": "object",
  "properties": {
    "order_id":       {"type": "string"},
    "order_amount":   {"type": "number"},
    "payment_id":     {"type": "string"},
    "payment_status": {"type": "string"},
    "paid_amount":    {"type": "number"},
    "matched":        {"type": "boolean"}
  }
}
$$::jsonb);

-- Reference table for stream-to-table join
CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL,
    warehouse     TEXT NOT NULL
);

INSERT INTO products VALUES
    ('SKU-001', 'Widget Pro',    'hardware',  29.99, 'NYC'),
    ('SKU-002', 'Gadget Plus',   'hardware',  49.99, 'LAX'),
    ('SKU-003', 'Cloud License', 'software', 199.00, 'N/A')
ON CONFLICT DO NOTHING;

-- Input: line_items for table join
SELECT pgkafka.create_typed_topic('line_items', $$
{
  "type": "object",
  "properties": {
    "order_id":   {"type": "string"},
    "product_id": {"type": "string"},
    "quantity":   {"type": "integer"}
  },
  "required": ["order_id", "product_id", "quantity"]
}
$$::jsonb);

-- Output topic for enriched line items
SELECT pgkafka.create_typed_topic('enriched_line_items', $$
{
  "type": "object",
  "properties": {
    "order_id":     {"type": "string"},
    "product_id":   {"type": "string"},
    "product_name": {"type": "string"},
    "category":     {"type": "string"},
    "quantity":     {"type": "integer"},
    "unit_price":   {"type": "number"},
    "line_total":   {"type": "number"},
    "warehouse":    {"type": "string"}
  }
}
$$::jsonb);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

-- Orders
INSERT INTO orders (order_id, customer_id, amount, region, created_at) VALUES
    ('ORD-100', 'C-1',  750.00, 'US-East', '2025-03-15 10:00:00+00'),
    ('ORD-101', 'C-2',  200.00, 'EU-West', '2025-03-15 10:01:00+00'),
    ('ORD-102', 'C-3', 1500.00, 'US-West', '2025-03-15 10:02:00+00');

-- Payments (ORD-102 has no payment -> left join will show pending)
INSERT INTO payments (payment_id, order_id, amount, status, paid_at) VALUES
    ('PAY-001', 'ORD-100', 750.00, 'completed', '2025-03-15 10:05:00+00'),
    ('PAY-002', 'ORD-101', 200.00, 'completed', '2025-03-15 10:10:00+00');

-- Line items for table join
INSERT INTO line_items (order_id, product_id, quantity) VALUES
    ('ORD-100', 'SKU-001', 10),
    ('ORD-100', 'SKU-003',  2),
    ('ORD-101', 'SKU-002',  4);

-- =============================================================================
-- PIPELINE 1: Stream-to-Stream Join (orders + payments)
-- =============================================================================
-- Left join: all orders appear, unmatched payments show as "pending".

SELECT pgstreams.create_pipeline('order_payment_join', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "order-payment-join",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "join": {
          "topic": "payments",
          "type": "left",
          "on": "value_json->>'order_id' = right_value_json->>'order_id'",
          "window": "1 hour",
          "columns": {
            "order_id":       "value_json->>'order_id'",
            "order_amount":   "(value_json->>'amount')::numeric",
            "payment_id":     "right_value_json->>'payment_id'",
            "payment_status": "coalesce(right_value_json->>'status', 'pending')",
            "paid_amount":    "(right_value_json->>'amount')::numeric",
            "matched":        "right_value_json IS NOT NULL"
          }
        }
      }
    ]
  },
  "output": {
    "kafka": {
      "topic": "order_payment_status",
      "key": "order_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Stream-to-Table Join (line items + products)
-- =============================================================================

SELECT pgstreams.create_pipeline('enrich_with_product', $$
{
  "input": {
    "kafka": {
      "topic": "line_items",
      "group": "product-enrichment",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "table_join": {
          "table": "products",
          "on": "value_json->>'product_id' = products.product_id",
          "type": "left",
          "columns": {
            "order_id":      "value_json->>'order_id'",
            "product_id":    "value_json->>'product_id'",
            "product_name":  "products.name",
            "category":      "products.category",
            "quantity":      "(value_json->>'quantity')::int",
            "unit_price":    "products.unit_price",
            "line_total":    "(value_json->>'quantity')::int * products.unit_price",
            "warehouse":     "products.warehouse"
          }
        }
      }
    ]
  },
  "output": {
    "kafka": {
      "topic": "enriched_line_items",
      "key": "order_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START PIPELINES
-- =============================================================================

SELECT pgstreams.start('order_payment_join');
SELECT pgstreams.start('enrich_with_product');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- Order-payment join results
SELECT order_id, order_amount, payment_id, payment_status, matched
FROM order_payment_status
ORDER BY order_id;
-- Expected:
-- | order_id | order_amount | payment_id | payment_status | matched |
-- |----------|--------------|------------|----------------|---------|
-- | ORD-100  |       750.00 | PAY-001    | completed      | true    |
-- | ORD-101  |       200.00 | PAY-002    | completed      | true    |
-- | ORD-102  |      1500.00 | NULL       | pending        | false   |

-- Enriched line items
SELECT order_id, product_name, quantity, unit_price, line_total, warehouse
FROM enriched_line_items
ORDER BY order_id, product_id;
-- Expected:
-- | order_id | product_name  | quantity | unit_price | line_total | warehouse |
-- |----------|---------------|----------|------------|------------|-----------|
-- | ORD-100  | Widget Pro    |       10 |      29.99 |     299.90 | NYC       |
-- | ORD-100  | Cloud License |        2 |     199.00 |     398.00 | N/A       |
-- | ORD-101  | Gadget Plus   |        4 |      49.99 |     199.96 | LAX       |

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('order_payment_join');
SELECT pgstreams.stop('enrich_with_product');
SELECT pgstreams.drop_pipeline('order_payment_join');
SELECT pgstreams.drop_pipeline('enrich_with_product');
SELECT pgkafka.drop_topic('enriched_line_items');
SELECT pgkafka.drop_topic('order_payment_status');
SELECT pgkafka.drop_topic('line_items');
SELECT pgkafka.drop_topic('payments');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS enriched_line_items;
DROP TABLE IF EXISTS order_payment_status;
DROP TABLE IF EXISTS line_items;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
