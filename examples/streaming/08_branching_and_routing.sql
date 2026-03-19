-- =============================================================================
-- 08: Branching and Routing
-- =============================================================================
-- Route records to different outputs based on content.
-- branch = fan-out (non-exclusive), switch = first-match routing.

-- =============================================================================
-- SETUP: Typed topics and output tables
-- =============================================================================

-- Input: orders
SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "customer_id": {"type": "string"},
    "amount":      {"type": "number"},
    "region":      {"type": "string"}
  },
  "required": ["order_id", "amount", "region"]
}
$$::jsonb);

-- Branch outputs
SELECT pgkafka.create_typed_topic('vip_orders', $$
{
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "amount":   {"type": "number"},
    "reason":   {"type": "string"}
  }
}
$$::jsonb);

SELECT pgkafka.create_typed_topic('international_orders', $$
{
  "type": "object",
  "properties": {
    "order_id": {"type": "string"},
    "region":   {"type": "string"},
    "amount":   {"type": "number"}
  }
}
$$::jsonb);

CREATE TABLE IF NOT EXISTS order_events (
    id           BIGSERIAL PRIMARY KEY,
    order_id     TEXT,
    customer_id  TEXT,
    amount       NUMERIC,
    region       TEXT,
    received_at  TIMESTAMPTZ DEFAULT now()
);

-- Input: alerts for switch routing
SELECT pgkafka.create_typed_topic('alerts', $$
{
  "type": "object",
  "properties": {
    "alert_id": {"type": "string"},
    "service":  {"type": "string"},
    "severity": {"type": "string"},
    "message":  {"type": "string"}
  },
  "required": ["alert_id", "severity", "message"]
}
$$::jsonb);

-- Switch outputs
SELECT pgkafka.create_typed_topic('pagerduty_alerts', $$
{
  "type": "object",
  "properties": {
    "channel":  {"type": "string"},
    "alert_id": {"type": "string"},
    "service":  {"type": "string"},
    "message":  {"type": "string"},
    "priority": {"type": "string"}
  }
}
$$::jsonb);

SELECT pgkafka.create_typed_topic('slack_alerts', $$
{
  "type": "object",
  "properties": {
    "channel":  {"type": "string"},
    "alert_id": {"type": "string"},
    "service":  {"type": "string"},
    "message":  {"type": "string"}
  }
}
$$::jsonb);

CREATE TABLE IF NOT EXISTS alert_log (
    id         BIGSERIAL PRIMARY KEY,
    alert_id   TEXT,
    service    TEXT,
    severity   TEXT,
    message    TEXT,
    logged_at  TIMESTAMPTZ DEFAULT now()
);

-- =============================================================================
-- INSERT TEST DATA
-- =============================================================================

-- Orders: different regions and amounts
INSERT INTO orders (order_id, customer_id, amount, region) VALUES
    ('ORD-001', 'C-1', 1500.00, 'EU-West'),    -- matches: vip + international + analytics
    ('ORD-002', 'C-2',  200.00, 'US-East'),     -- matches: analytics only
    ('ORD-003', 'C-3', 2000.00, 'US-West'),     -- matches: vip + analytics
    ('ORD-004', 'C-4',  800.00, 'APAC');        -- matches: international + analytics

-- Alerts: different severities
INSERT INTO alerts (alert_id, service, severity, message) VALUES
    ('ALT-001', 'api-gateway', 'critical', 'Service down'),
    ('ALT-002', 'db-primary',  'warning',  'High connection count'),
    ('ALT-003', 'cache',       'info',     'Cache eviction rate high');

-- =============================================================================
-- PIPELINE 1: Branch — fan-out orders to multiple destinations
-- =============================================================================
-- Each record can match multiple branches simultaneously.

SELECT pgstreams.create_pipeline('order_router', $$
{
  "input": {
    "kafka": {
      "topic": "orders",
      "group": "order-router",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "branch": {
          "high_value": {
            "filter": "(value_json->>'amount')::numeric > 1000",
            "processors": [
              {"mapping": {
                "order_id": "value_json->>'order_id'",
                "amount":   "(value_json->>'amount')::numeric",
                "reason":   "'high_value'"
              }}
            ],
            "output": {
              "kafka": {"topic": "vip_orders", "key": "order_id"}
            }
          },
          "international": {
            "filter": "value_json->>'region' NOT IN ('US-East', 'US-West')",
            "processors": [
              {"mapping": {
                "order_id": "value_json->>'order_id'",
                "region":   "value_json->>'region'",
                "amount":   "(value_json->>'amount')::numeric"
              }}
            ],
            "output": {
              "kafka": {"topic": "international_orders", "key": "order_id"}
            }
          },
          "analytics": {
            "processors": [
              {"mapping": {
                "order_id":    "value_json->>'order_id'",
                "customer_id": "value_json->>'customer_id'",
                "amount":      "(value_json->>'amount')::numeric",
                "region":      "value_json->>'region'",
                "received_at": "now()"
              }}
            ],
            "output": {
              "table": {
                "name": "public.order_events",
                "mode": "append"
              }
            }
          }
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
-- PIPELINE 2: Switch — exclusive routing by severity
-- =============================================================================

SELECT pgstreams.create_pipeline('alert_router', $$
{
  "input": {
    "kafka": {
      "topic": "alerts",
      "group": "alert-router",
      "start": "earliest"
    }
  },
  "pipeline": {
    "processors": [
      {
        "switch": [
          {
            "check": "value_json->>'severity' = 'critical'",
            "processors": [
              {"mapping": {
                "channel":  "'pagerduty'",
                "alert_id": "value_json->>'alert_id'",
                "service":  "value_json->>'service'",
                "message":  "value_json->>'message'",
                "priority": "'P1'"
              }}
            ],
            "output": {
              "kafka": {"topic": "pagerduty_alerts"}
            }
          },
          {
            "check": "value_json->>'severity' = 'warning'",
            "processors": [
              {"mapping": {
                "channel":  "'slack'",
                "alert_id": "value_json->>'alert_id'",
                "service":  "value_json->>'service'",
                "message":  "value_json->>'message'"
              }}
            ],
            "output": {
              "kafka": {"topic": "slack_alerts"}
            }
          },
          {
            "output": {
              "table": {
                "name": "public.alert_log",
                "mode": "append"
              }
            }
          }
        ]
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

SELECT pgstreams.start('order_router');
SELECT pgstreams.start('alert_router');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- VIP orders: ORD-001 ($1500) and ORD-003 ($2000)
SELECT order_id, amount, reason FROM vip_orders ORDER BY order_id;
-- Expected:
-- | order_id | amount  | reason     |
-- |----------|---------|------------|
-- | ORD-001  | 1500.00 | high_value |
-- | ORD-003  | 2000.00 | high_value |

-- International orders: ORD-001 (EU-West) and ORD-004 (APAC)
SELECT order_id, region, amount FROM international_orders ORDER BY order_id;
-- Expected:
-- | order_id | region  | amount  |
-- |----------|---------|---------|
-- | ORD-001  | EU-West | 1500.00 |
-- | ORD-004  | APAC    |  800.00 |

-- Analytics table: ALL orders
SELECT order_id, customer_id, amount, region FROM order_events ORDER BY order_id;
-- Expected: all 4 orders

-- Alert routing: critical -> pagerduty
SELECT alert_id, channel, priority FROM pagerduty_alerts;
-- Expected: ALT-001, pagerduty, P1

-- Alert routing: warning -> slack
SELECT alert_id, channel FROM slack_alerts;
-- Expected: ALT-002, slack

-- Alert routing: info -> table (default)
SELECT alert_id, severity, message FROM alert_log;
-- Expected: ALT-003, info, Cache eviction rate high

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('order_router');
SELECT pgstreams.stop('alert_router');
SELECT pgstreams.drop_pipeline('order_router');
SELECT pgstreams.drop_pipeline('alert_router');
SELECT pgkafka.drop_topic('pagerduty_alerts');
SELECT pgkafka.drop_topic('slack_alerts');
SELECT pgkafka.drop_topic('alerts');
SELECT pgkafka.drop_topic('international_orders');
SELECT pgkafka.drop_topic('vip_orders');
SELECT pgkafka.drop_topic('orders');
DROP TABLE IF EXISTS pagerduty_alerts;
DROP TABLE IF EXISTS slack_alerts;
DROP TABLE IF EXISTS alerts;
DROP TABLE IF EXISTS international_orders;
DROP TABLE IF EXISTS vip_orders;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS order_events;
DROP TABLE IF EXISTS alert_log;
