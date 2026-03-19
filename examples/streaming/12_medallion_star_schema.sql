-- =============================================================================
-- 12: Medallion Architecture with Star Schema Gold Layer
-- =============================================================================
-- Bronze -> Silver -> Gold pipeline chain implementing a lakehouse-style
-- medallion architecture entirely inside PostgreSQL.
--
-- Bronze: raw ingestion (append-only, no transformation)
-- Silver: cleaned, deduplicated, enriched (upsert by business key)
-- Gold:   star schema with fact and dimension tables (aggregated)
--
-- Domain: e-commerce order analytics

-- =============================================================================
-- SCHEMA SETUP
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =============================================================================
-- REFERENCE DATA (used for enrichment in Silver layer)
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_products (
    product_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    category     TEXT NOT NULL,
    subcategory  TEXT,
    unit_cost    NUMERIC(10,2) NOT NULL,
    supplier     TEXT
);

CREATE TABLE IF NOT EXISTS silver.dim_customers (
    customer_id  TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    email        TEXT,
    tier         TEXT NOT NULL DEFAULT 'standard',
    region       TEXT NOT NULL,
    country      TEXT NOT NULL,
    signup_date  DATE
);

CREATE TABLE IF NOT EXISTS silver.dim_stores (
    store_id     TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    city         TEXT NOT NULL,
    country      TEXT NOT NULL,
    channel      TEXT NOT NULL   -- online, retail, wholesale
);

INSERT INTO silver.dim_products VALUES
    ('SKU-001', 'Widget Pro',     'Hardware', 'Widgets',   12.50, 'Acme'),
    ('SKU-002', 'Gadget Plus',    'Hardware', 'Gadgets',   22.00, 'Globex'),
    ('SKU-003', 'Cloud License',  'Software', 'Licenses', 80.00, 'Initech'),
    ('SKU-004', 'Support Plan',   'Services', 'Support',  40.00, 'Initech')
ON CONFLICT DO NOTHING;

INSERT INTO silver.dim_customers VALUES
    ('C-100', 'Acme Corp',   'acme@example.com',   'platinum', 'US-East',  'US', '2023-01-15'),
    ('C-200', 'Globex Inc',  'globex@example.com',  'gold',     'EU-West',  'DE', '2023-06-01'),
    ('C-300', 'Initech',     'initech@example.com', 'standard', 'US-West',  'US', '2024-02-20'),
    ('C-400', 'Umbrella Co', 'uc@example.com',      'gold',     'APAC',     'JP', '2024-08-10')
ON CONFLICT DO NOTHING;

INSERT INTO silver.dim_stores VALUES
    ('S-01', 'Web Store',       'N/A',      'N/A', 'online'),
    ('S-02', 'NYC Flagship',    'New York',  'US', 'retail'),
    ('S-03', 'Berlin Shop',     'Berlin',    'DE', 'retail'),
    ('S-04', 'Wholesale Direct','N/A',       'N/A', 'wholesale')
ON CONFLICT DO NOTHING;

-- =============================================================================
-- INPUT: Typed topics for orders and line items
-- =============================================================================

SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "store_id":    {"type": "string"},
    "status":      {"type": "string"},
    "currency":    {"type": "string", "maxLength": 3},
    "subtotal":    {"type": "number"},
    "tax":         {"type": "number"},
    "shipping":    {"type": "number"},
    "total":       {"type": "number"},
    "item_count":  {"type": "integer"},
    "ordered_at":  {"type": "string", "format": "date-time"},
    "region":      {"type": "string"}
  },
  "required": ["order_id", "customer_id", "total", "ordered_at"]
}
$$::jsonb);

SELECT pgkafka.create_typed_topic('order_line_items', $$
{
  "type": "object",
  "properties": {
    "line_item_id": {"type": "string"},
    "order_id":     {"type": "string"},
    "product_id":   {"type": "string"},
    "quantity":     {"type": "integer"},
    "unit_price":   {"type": "number"}
  },
  "required": ["line_item_id", "order_id", "product_id", "quantity", "unit_price"]
}
$$::jsonb);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

INSERT INTO orders (order_id, customer_id, store_id, status, currency, subtotal, tax, shipping, total, item_count, ordered_at, region) VALUES
    ('ORD-001', 'C-100', 'S-01', 'confirmed', 'USD', 162.47, 13.00, 5.00, 180.47, 3, '2025-03-15 10:00:00+00', 'US-East'),
    ('ORD-002', 'C-200', 'S-03', 'confirmed', 'EUR', 199.00, 38.00, 0.00, 237.00, 1, '2025-03-15 10:30:00+00', 'EU-West'),
    ('ORD-003', 'C-100', 'S-02', 'confirmed', 'USD', 124.98, 10.00, 0.00, 134.98, 2, '2025-03-15 11:00:00+00', 'US-East'),
    ('ORD-004', 'C-400', 'S-01', 'pending',   'USD', 449.97, 36.00, 8.00, 493.97, 3, '2025-03-15 12:00:00+00', 'APAC');

INSERT INTO order_line_items (line_item_id, order_id, product_id, quantity, unit_price) VALUES
    ('LI-001', 'ORD-001', 'SKU-001', 5,  29.99),   -- 149.95
    ('LI-002', 'ORD-001', 'SKU-004', 1,  12.52),   -- 12.52
    ('LI-003', 'ORD-002', 'SKU-003', 1, 199.00),   -- 199.00
    ('LI-004', 'ORD-003', 'SKU-002', 1,  49.99),   -- 49.99
    ('LI-005', 'ORD-003', 'SKU-001', 3,  24.99),   -- 74.97 (discounted)
    ('LI-006', 'ORD-004', 'SKU-003', 2, 199.00),   -- 398.00
    ('LI-007', 'ORD-004', 'SKU-004', 1,  51.97);   -- 51.97

-- =============================================================================
-- BRONZE LAYER: Raw Ingestion (append-only, no transformation)
-- =============================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_orders (
    id           BIGSERIAL PRIMARY KEY,
    raw_key      TEXT,
    raw_value    JSONB NOT NULL,
    source_topic TEXT,
    kafka_offset BIGINT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.raw_line_items (
    id           BIGSERIAL PRIMARY KEY,
    raw_value    JSONB NOT NULL,
    kafka_offset BIGINT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT pgstreams.create_pipeline('orders_bronze', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "medallion-bronze",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {"mapping": {
        "raw_key":      "key_text",
        "raw_value":    "value_json",
        "source_topic": "source_topic",
        "kafka_offset": "offset_id"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "bronze.raw_orders",
      "mode": "append"
    }
  }
}
$$::jsonb);

SELECT pgstreams.create_pipeline('line_items_bronze', $$
{
  "input": {
    "kafka": {
      "topic": "order_line_items",
      "group": "medallion-bronze-li",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {"mapping": {
        "raw_value":    "value_json",
        "kafka_offset": "offset_id"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "bronze.raw_line_items",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- SILVER LAYER: Cleaned, Deduplicated, Enriched
-- =============================================================================

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id       TEXT PRIMARY KEY,
    customer_id    TEXT NOT NULL,
    store_id       TEXT NOT NULL,
    order_status   TEXT NOT NULL,
    currency       TEXT NOT NULL DEFAULT 'USD',
    subtotal       NUMERIC(12,2),
    tax            NUMERIC(12,2),
    shipping       NUMERIC(12,2),
    total          NUMERIC(12,2) NOT NULL,
    item_count     INT,
    ordered_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT pgstreams.create_pipeline('orders_silver', $$
{
  "input": {
    "table": {
      "name": "bronze.raw_orders",
      "offset_column": "id",
      "poll": "2s"
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "value_json->'raw_value'->>'order_id' IS NOT NULL"},
      {"dedupe": {
        "key": "value_json->'raw_value'->>'order_id'",
        "window": "24 hours"
      }},
      {"mapping": {
        "order_id":     "value_json->'raw_value'->>'order_id'",
        "customer_id":  "value_json->'raw_value'->>'customer_id'",
        "store_id":     "coalesce(value_json->'raw_value'->>'store_id', 'S-01')",
        "order_status": "coalesce(value_json->'raw_value'->>'status', 'pending')",
        "currency":     "coalesce(value_json->'raw_value'->>'currency', 'USD')",
        "subtotal":     "(value_json->'raw_value'->>'subtotal')::numeric",
        "tax":          "coalesce((value_json->'raw_value'->>'tax')::numeric, 0)",
        "shipping":     "coalesce((value_json->'raw_value'->>'shipping')::numeric, 0)",
        "total":        "(value_json->'raw_value'->>'total')::numeric",
        "item_count":   "coalesce((value_json->'raw_value'->>'item_count')::int, 0)",
        "ordered_at":   "(value_json->'raw_value'->>'ordered_at')::timestamptz"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "silver.orders",
      "mode": "upsert",
      "key": "order_id"
    }
  }
}
$$::jsonb);

CREATE TABLE IF NOT EXISTS silver.order_line_items (
    line_item_id   TEXT PRIMARY KEY,
    order_id       TEXT NOT NULL,
    product_id     TEXT NOT NULL,
    product_name   TEXT,
    category       TEXT,
    quantity       INT NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,
    unit_cost      NUMERIC(10,2),
    line_total     NUMERIC(12,2) NOT NULL,
    margin         NUMERIC(12,2),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT pgstreams.create_pipeline('line_items_silver', $$
{
  "input": {
    "table": {
      "name": "bronze.raw_line_items",
      "offset_column": "id",
      "poll": "2s"
    }
  },
  "pipeline": {
    "processors": [
      {"dedupe": {
        "key": "value_json->'raw_value'->>'line_item_id'",
        "window": "24 hours"
      }},
      {
        "sql": {
          "query": "SELECT name, category, unit_cost FROM silver.dim_products WHERE product_id = $1",
          "args": ["value_json->'raw_value'->>'product_id'"],
          "result_map": {
            "product_name": "name",
            "category":     "category",
            "unit_cost":    "unit_cost"
          },
          "on_empty": "null"
        }
      },
      {"mapping": {
        "line_item_id": "value_json->'raw_value'->>'line_item_id'",
        "order_id":     "value_json->'raw_value'->>'order_id'",
        "product_id":   "value_json->'raw_value'->>'product_id'",
        "product_name": "product_name",
        "category":     "category",
        "quantity":     "(value_json->'raw_value'->>'quantity')::int",
        "unit_price":   "(value_json->'raw_value'->>'unit_price')::numeric",
        "unit_cost":    "unit_cost::numeric",
        "line_total":   "(value_json->'raw_value'->>'quantity')::int * (value_json->'raw_value'->>'unit_price')::numeric",
        "margin":       "(value_json->'raw_value'->>'quantity')::int * ((value_json->'raw_value'->>'unit_price')::numeric - coalesce(unit_cost::numeric, 0))"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "silver.order_line_items",
      "mode": "upsert",
      "key": "line_item_id"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- GOLD LAYER: Star Schema
-- =============================================================================

-- Date dimension
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INT PRIMARY KEY,
    full_date    DATE NOT NULL UNIQUE,
    year         INT NOT NULL,
    quarter      INT NOT NULL,
    month        INT NOT NULL,
    month_name   TEXT NOT NULL,
    week         INT NOT NULL,
    day_of_week  INT NOT NULL,
    day_name     TEXT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);

INSERT INTO gold.dim_date
SELECT
    to_char(d, 'YYYYMMDD')::int AS date_key,
    d AS full_date,
    extract(year FROM d)::int,
    extract(quarter FROM d)::int,
    extract(month FROM d)::int,
    to_char(d, 'Month'),
    extract(week FROM d)::int,
    extract(dow FROM d)::int,
    to_char(d, 'Day'),
    extract(dow FROM d)::int IN (0, 6)
FROM generate_series('2024-01-01'::date, '2025-12-31'::date, '1 day') AS d
ON CONFLICT DO NOTHING;

-- Customer, product, store dimensions
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key  SERIAL PRIMARY KEY,
    customer_id   TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    tier          TEXT NOT NULL,
    region        TEXT NOT NULL,
    country       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_id    TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    category      TEXT NOT NULL,
    subcategory   TEXT
);

CREATE TABLE IF NOT EXISTS gold.dim_store (
    store_key     SERIAL PRIMARY KEY,
    store_id      TEXT NOT NULL UNIQUE,
    name          TEXT NOT NULL,
    channel       TEXT NOT NULL,
    country       TEXT NOT NULL
);

-- Populate gold dimensions from silver
INSERT INTO gold.dim_customer (customer_id, name, tier, region, country)
SELECT customer_id, name, tier, region, country FROM silver.dim_customers
ON CONFLICT (customer_id) DO UPDATE SET
    name = EXCLUDED.name, tier = EXCLUDED.tier,
    region = EXCLUDED.region, country = EXCLUDED.country;

INSERT INTO gold.dim_product (product_id, name, category, subcategory)
SELECT product_id, name, category, subcategory FROM silver.dim_products
ON CONFLICT (product_id) DO UPDATE SET
    name = EXCLUDED.name, category = EXCLUDED.category,
    subcategory = EXCLUDED.subcategory;

INSERT INTO gold.dim_store (store_id, name, channel, country)
SELECT store_id, name, channel, country FROM silver.dim_stores
ON CONFLICT (store_id) DO UPDATE SET
    name = EXCLUDED.name, channel = EXCLUDED.channel,
    country = EXCLUDED.country;

-- Fact table
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    fact_id        BIGSERIAL PRIMARY KEY,
    order_id       TEXT NOT NULL,
    line_item_id   TEXT NOT NULL UNIQUE,
    date_key       INT REFERENCES gold.dim_date(date_key),
    customer_key   INT REFERENCES gold.dim_customer(customer_key),
    product_key    INT REFERENCES gold.dim_product(product_key),
    store_key      INT REFERENCES gold.dim_store(store_key),
    quantity       INT NOT NULL,
    unit_price     NUMERIC(10,2) NOT NULL,
    unit_cost      NUMERIC(10,2),
    revenue        NUMERIC(12,2) NOT NULL,
    cost           NUMERIC(12,2),
    margin         NUMERIC(12,2),
    ordered_at     TIMESTAMPTZ NOT NULL
);

SELECT pgstreams.create_pipeline('gold_fact_sales', $$
{
  "input": {
    "table": {
      "name": "silver.order_line_items",
      "offset_column": "line_item_id",
      "poll": "5s"
    }
  },
  "pipeline": {
    "processors": [
      {
        "sql": {
          "query": "SELECT order_id, customer_id, store_id, ordered_at FROM silver.orders WHERE order_id = $1",
          "args": ["value_json->>'order_id'"],
          "result_map": {
            "customer_id": "customer_id",
            "store_id":    "store_id",
            "ordered_at":  "ordered_at"
          },
          "on_empty": "skip"
        }
      },
      {
        "sql": {
          "query": "SELECT customer_key FROM gold.dim_customer WHERE customer_id = $1",
          "args": ["customer_id"],
          "result_map": { "customer_key": "customer_key" },
          "on_empty": "skip"
        }
      },
      {
        "sql": {
          "query": "SELECT product_key FROM gold.dim_product WHERE product_id = $1",
          "args": ["value_json->>'product_id'"],
          "result_map": { "product_key": "product_key" },
          "on_empty": "skip"
        }
      },
      {
        "sql": {
          "query": "SELECT store_key FROM gold.dim_store WHERE store_id = $1",
          "args": ["store_id"],
          "result_map": { "store_key": "store_key" },
          "on_empty": "null"
        }
      },
      {"mapping": {
        "order_id":      "value_json->>'order_id'",
        "line_item_id":  "value_json->>'line_item_id'",
        "date_key":      "to_char(ordered_at::timestamptz, 'YYYYMMDD')::int",
        "customer_key":  "customer_key::int",
        "product_key":   "product_key::int",
        "store_key":     "store_key::int",
        "quantity":      "(value_json->>'quantity')::int",
        "unit_price":    "(value_json->>'unit_price')::numeric",
        "unit_cost":     "(value_json->>'unit_cost')::numeric",
        "revenue":       "(value_json->>'line_total')::numeric",
        "cost":          "(value_json->>'quantity')::int * coalesce((value_json->>'unit_cost')::numeric, 0)",
        "margin":        "(value_json->>'margin')::numeric",
        "ordered_at":    "ordered_at::timestamptz"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "gold.fact_sales",
      "mode": "upsert",
      "key": "line_item_id"
    }
  }
}
$$::jsonb);

-- Gold aggregates: customer lifetime value
SELECT pgstreams.create_pipeline('gold_customer_ltv', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "gold-customer-ltv",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "aggregate": {
          "group_by": "value_json->>'customer_id'",
          "columns": {
            "lifetime_revenue":  "sum((value_json->>'total')::numeric)",
            "lifetime_orders":   "count(*)",
            "avg_order_value":   "avg((value_json->>'total')::numeric)",
            "first_order_at":    "min((value_json->>'ordered_at')::timestamptz)",
            "last_order_at":     "max((value_json->>'ordered_at')::timestamptz)"
          },
          "state_table": "gold_customer_ltv",
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
-- START ALL PIPELINES (in dependency order)
-- =============================================================================

-- Bronze (no dependencies)
SELECT pgstreams.start('orders_bronze');
SELECT pgstreams.start('line_items_bronze');

SELECT pg_sleep(3);  -- let bronze land data

-- Silver (depends on bronze tables)
SELECT pgstreams.start('orders_silver');
SELECT pgstreams.start('line_items_silver');

SELECT pg_sleep(3);  -- let silver process

-- Gold (depends on silver tables)
SELECT pgstreams.start('gold_fact_sales');
SELECT pgstreams.start('gold_customer_ltv');

SELECT pg_sleep(5);  -- let gold build

-- =============================================================================
-- VERIFY EACH LAYER
-- =============================================================================

-- Bronze: raw data landed
SELECT count(*) AS bronze_orders FROM bronze.raw_orders;          -- Expected: 4
SELECT count(*) AS bronze_line_items FROM bronze.raw_line_items;  -- Expected: 7

-- Silver: cleaned and enriched
SELECT order_id, customer_id, total, order_status
FROM silver.orders ORDER BY order_id;
-- Expected: 4 orders

SELECT line_item_id, product_name, category, quantity, unit_price, line_total, margin
FROM silver.order_line_items ORDER BY line_item_id;
-- Expected: 7 line items with product names and margins

-- Gold: star schema fact table
SELECT
    f.order_id, f.line_item_id,
    dp.name AS product, dc.name AS customer,
    f.quantity, f.revenue, f.margin
FROM gold.fact_sales f
JOIN gold.dim_product dp ON f.product_key = dp.product_key
JOIN gold.dim_customer dc ON f.customer_key = dc.customer_key
ORDER BY f.order_id, f.line_item_id;

-- Gold: customer lifetime value
SELECT
    g.group_key AS customer_id,
    c.name,
    c.tier,
    g.lifetime_revenue,
    g.lifetime_orders,
    round(g.avg_order_value, 2) AS avg_order
FROM pgstreams.gold_customer_ltv g
JOIN silver.dim_customers c ON g.group_key = c.customer_id
ORDER BY g.lifetime_revenue DESC;
-- Expected:
-- | customer_id | name        | tier     | lifetime_revenue | lifetime_orders | avg_order |
-- |-------------|-------------|----------|------------------|-----------------|-----------|
-- | C-400       | Umbrella Co | gold     |           493.97 |               1 |    493.97 |
-- | C-100       | Acme Corp   | platinum |           315.45 |               2 |    157.73 |
-- | C-200       | Globex Inc  | gold     |           237.00 |               1 |    237.00 |

-- Revenue by product category
SELECT
    dp.category,
    count(*) AS line_items,
    sum(f.revenue) AS total_revenue,
    sum(f.margin) AS total_margin,
    round(sum(f.margin) / nullif(sum(f.revenue), 0) * 100, 1) AS margin_pct
FROM gold.fact_sales f
JOIN gold.dim_product dp ON f.product_key = dp.product_key
GROUP BY dp.category
ORDER BY total_revenue DESC;

-- Pipeline health
SELECT * FROM pgstreams.status() ORDER BY name;

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('gold_customer_ltv');
SELECT pgstreams.stop('gold_fact_sales');
SELECT pgstreams.stop('line_items_silver');
SELECT pgstreams.stop('orders_silver');
SELECT pgstreams.stop('line_items_bronze');
SELECT pgstreams.stop('orders_bronze');

SELECT pgstreams.drop_pipeline('gold_customer_ltv');
SELECT pgstreams.drop_pipeline('gold_fact_sales');
SELECT pgstreams.drop_pipeline('line_items_silver');
SELECT pgstreams.drop_pipeline('orders_silver');
SELECT pgstreams.drop_pipeline('line_items_bronze');
SELECT pgstreams.drop_pipeline('orders_bronze');

SELECT pgkafka.drop_topic('order_line_items');
SELECT pgkafka.drop_topic('orders');

DROP TABLE IF EXISTS gold.fact_sales;
DROP TABLE IF EXISTS gold.dim_store;
DROP TABLE IF EXISTS gold.dim_product;
DROP TABLE IF EXISTS gold.dim_customer;
DROP TABLE IF EXISTS gold.dim_date;
DROP TABLE IF EXISTS silver.order_line_items;
DROP TABLE IF EXISTS silver.orders;
DROP TABLE IF EXISTS silver.dim_stores;
DROP TABLE IF EXISTS silver.dim_customers;
DROP TABLE IF EXISTS silver.dim_products;
DROP TABLE IF EXISTS bronze.raw_line_items;
DROP TABLE IF EXISTS bronze.raw_orders;
DROP TABLE IF EXISTS order_line_items;
DROP TABLE IF EXISTS orders;

DROP SCHEMA IF EXISTS gold;
DROP SCHEMA IF EXISTS silver;
DROP SCHEMA IF EXISTS bronze;
