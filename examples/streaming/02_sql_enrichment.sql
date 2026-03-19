-- =============================================================================
-- 02: SQL Enrichment
-- =============================================================================
-- Enrich streaming records by joining with data in PostgreSQL tables.
--
-- Scenario: Order events arrive on a typed Kafka topic. We look up customer
-- details from a reference table, then produce enriched records to an
-- output table.

-- =============================================================================
-- SETUP: Reference tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS customers (
    customer_id   INT PRIMARY KEY,
    name          TEXT NOT NULL,
    tier          TEXT NOT NULL DEFAULT 'standard',  -- standard, gold, platinum
    region        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS products (
    product_id    TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL
);

INSERT INTO customers VALUES
    (1, 'Acme Corp',    'platinum', 'US-East'),
    (2, 'Globex Inc',   'gold',     'EU-West'),
    (3, 'Initech',      'standard', 'US-West')
ON CONFLICT DO NOTHING;

INSERT INTO products VALUES
    ('SKU-001', 'Widget Pro',     'hardware',  29.99),
    ('SKU-002', 'Gadget Plus',    'hardware',  49.99),
    ('SKU-003', 'Cloud License',  'software', 199.00)
ON CONFLICT DO NOTHING;

-- =============================================================================
-- SETUP: Typed topics
-- =============================================================================

-- Input: orders
SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":     {"type": "string"},
    "customer_id":  {"type": "integer"},
    "amount":       {"type": "number"},
    "created_at":   {"type": "string", "format": "date-time"}
  },
  "required": ["order_id", "customer_id", "amount"]
}
$$::jsonb);

-- Input: line_items
SELECT pgkafka.create_typed_topic('line_items', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "product_id":  {"type": "string"},
    "quantity":    {"type": "integer"}
  },
  "required": ["order_id", "product_id", "quantity"]
}
$$::jsonb);

-- Output table for enriched line items
CREATE TABLE IF NOT EXISTS order_line_items_enriched (
    id              BIGSERIAL PRIMARY KEY,
    order_id        TEXT,
    product_id      TEXT,
    product_name    TEXT,
    product_category TEXT,
    quantity        INT,
    unit_price      NUMERIC(10,2),
    line_total      NUMERIC(10,2),
    enriched_at     TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

INSERT INTO orders (order_id, customer_id, amount, created_at) VALUES
    ('ORD-100', 1,  350.00, '2025-03-15 10:00:00+00'),
    ('ORD-101', 2,  199.00, '2025-03-15 10:01:00+00'),
    ('ORD-102', 3,   89.97, '2025-03-15 10:02:00+00'),
    ('ORD-103', 99, 500.00, '2025-03-15 10:03:00+00');  -- unknown customer

INSERT INTO line_items (order_id, product_id, quantity) VALUES
    ('ORD-100', 'SKU-001', 5),
    ('ORD-100', 'SKU-003', 1),
    ('ORD-101', 'SKU-002', 2),
    ('ORD-102', 'SKU-001', 3);

-- =============================================================================
-- PIPELINE 1: Enrich orders with customer info
-- =============================================================================

-- Output topic for enriched orders
SELECT pgkafka.create_typed_topic('enriched_orders', $$
{
  "type": "object",
  "properties": {
    "order_id":        {"type": "string"},
    "customer_id":     {"type": "integer"},
    "customer_name":   {"type": "string"},
    "customer_tier":   {"type": "string"},
    "customer_region": {"type": "string"},
    "amount":          {"type": "number"},
    "ordered_at":      {"type": "string"}
  }
}
$$::jsonb);

SELECT pgstreams.create_pipeline('enrich_orders', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "order-enricher",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "sql": {
          "query": "SELECT name, tier, region FROM customers WHERE customer_id = $1",
          "args": ["(value_json->>'customer_id')::int"],
          "result_map": {
            "customer_name": "name",
            "customer_tier": "tier",
            "customer_region": "region"
          },
          "on_empty": "skip"
        }
      },
      {"mapping": {
        "order_id":        "value_json->>'order_id'",
        "customer_id":     "(value_json->>'customer_id')::int",
        "customer_name":   "customer_name",
        "customer_tier":   "customer_tier",
        "customer_region": "customer_region",
        "amount":          "(value_json->>'amount')::numeric",
        "ordered_at":      "value_json->>'created_at'"
      }}
    ]
  },
  "output": {
    "kafka": {
      "topic": "enriched_orders",
      "key": "order_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Enrich line items with product info
-- =============================================================================

SELECT pgstreams.create_pipeline('enrich_line_items', $$
{
  "input": {
    "kafka": {
      "topic": "line_items",
      "group": "line-item-enricher",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "sql": {
          "query": "SELECT product_id, name, category, unit_price FROM products WHERE product_id = $1",
          "args": ["value_json->>'product_id'"],
          "result_map": {
            "product_name":     "name",
            "product_category": "category",
            "unit_price":       "unit_price"
          },
          "on_empty": "null"
        }
      },
      {"mapping": {
        "order_id":          "value_json->>'order_id'",
        "product_id":        "value_json->>'product_id'",
        "product_name":      "product_name",
        "product_category":  "product_category",
        "quantity":          "(value_json->>'quantity')::int",
        "unit_price":        "unit_price::numeric",
        "line_total":        "(value_json->>'quantity')::int * unit_price::numeric"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "public.order_line_items_enriched",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START PIPELINES
-- =============================================================================

SELECT pgstreams.start('enrich_orders');
SELECT pgstreams.start('enrich_line_items');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(2);

-- Enriched orders: should have 3 (ORD-103 skipped, unknown customer)
SELECT order_id, customer_name, customer_tier, amount
FROM enriched_orders
ORDER BY order_id;
-- Expected:
-- | order_id | customer_name | customer_tier | amount |
-- |----------|---------------|---------------|--------|
-- | ORD-100  | Acme Corp     | platinum      | 350.00 |
-- | ORD-101  | Globex Inc    | gold          | 199.00 |
-- | ORD-102  | Initech       | standard      |  89.97 |

-- Enriched line items: products resolved, totals calculated
SELECT order_id, product_name, quantity, unit_price, line_total
FROM order_line_items_enriched
ORDER BY order_id, product_id;
-- Expected:
-- | order_id | product_name  | quantity | unit_price | line_total |
-- |----------|---------------|----------|------------|------------|
-- | ORD-100  | Widget Pro    |        5 |      29.99 |     149.95 |
-- | ORD-100  | Cloud License |        1 |     199.00 |     199.00 |
-- | ORD-101  | Gadget Plus   |        2 |      49.99 |      99.98 |
-- | ORD-102  | Widget Pro    |        3 |      29.99 |      89.97 |

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('enrich_orders');
SELECT pgstreams.stop('enrich_line_items');
SELECT pgstreams.drop_pipeline('enrich_orders');
SELECT pgstreams.drop_pipeline('enrich_line_items');
SELECT pgkafka.drop_topic('enriched_orders');
SELECT pgkafka.drop_topic('line_items');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS enriched_orders;
DROP TABLE IF EXISTS order_line_items_enriched;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS line_items;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;
