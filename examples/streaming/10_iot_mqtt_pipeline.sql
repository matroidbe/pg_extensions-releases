-- =============================================================================
-- 10: IoT MQTT Pipeline
-- =============================================================================
-- Ingest sensor data from MQTT topics, process it, and write to analytics
-- tables and alert topics. End-to-end IoT data pipeline inside PostgreSQL.
--
-- Note: This example uses MQTT input. Sensor devices publish JSON payloads
-- to MQTT topics. pg_mqtt must be enabled and its broker running.

-- =============================================================================
-- SETUP: Analytics tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS sensor_readings (
    id           BIGSERIAL PRIMARY KEY,
    device_id    TEXT NOT NULL,
    location     TEXT,
    temperature  NUMERIC(6,2),
    humidity     NUMERIC(5,2),
    pressure     NUMERIC(7,2),
    battery_pct  INT,
    read_at      TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensor_alerts (
    id          BIGSERIAL PRIMARY KEY,
    device_id   TEXT NOT NULL,
    alert_type  TEXT NOT NULL,
    severity    TEXT NOT NULL,
    message     TEXT NOT NULL,
    reading     JSONB,
    alerted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS sensor_location_avg_5m (
    id              BIGSERIAL PRIMARY KEY,
    location        TEXT,
    window_start    TIMESTAMPTZ,
    window_end      TIMESTAMPTZ,
    avg_temperature NUMERIC(6,2),
    avg_humidity    NUMERIC(5,2),
    avg_pressure    NUMERIC(7,2),
    device_count    BIGINT,
    reading_count   BIGINT
);

-- =============================================================================
-- SIMULATE MQTT DATA
-- =============================================================================
-- In production, devices publish to MQTT. For testing, we publish via pg_mqtt.

SELECT pgmqtt.publish('devices/sensor-001/telemetry', $$
  {"device_id":"sensor-001","location":"warehouse-A","temperature":22.5,"humidity":45.0,"pressure":1013.25,"battery":95,"timestamp":"2025-03-15T10:00:00Z"}
$$);
SELECT pgmqtt.publish('devices/sensor-002/telemetry', $$
  {"device_id":"sensor-002","location":"warehouse-A","temperature":71.0,"humidity":38.0,"pressure":1012.80,"battery":80,"timestamp":"2025-03-15T10:00:00Z"}
$$);
SELECT pgmqtt.publish('devices/sensor-003/telemetry', $$
  {"device_id":"sensor-003","location":"warehouse-B","temperature":88.0,"humidity":25.0,"pressure":1011.50,"battery":12,"timestamp":"2025-03-15T10:00:00Z"}
$$);
SELECT pgmqtt.publish('devices/sensor-004/telemetry', $$
  {"device_id":"sensor-004","location":"warehouse-B","temperature":19.5,"humidity":55.0,"pressure":1013.00,"battery":60,"timestamp":"2025-03-15T10:00:00Z"}
$$);

-- =============================================================================
-- PIPELINE 1: Ingest all sensor telemetry
-- =============================================================================
-- MQTT wildcard topic: devices/+/telemetry matches all device topics.

SELECT pgstreams.create_pipeline('sensor_ingest', $$
{
  "input": {
    "mqtt": {
      "topic": "devices/+/telemetry",
      "qos": 1
    }
  },
  "pipeline": {
    "processors": [
      {"mapping": {
        "device_id":   "value_json->>'device_id'",
        "location":    "value_json->>'location'",
        "temperature": "(value_json->>'temperature')::numeric",
        "humidity":    "(value_json->>'humidity')::numeric",
        "pressure":    "(value_json->>'pressure')::numeric",
        "battery_pct": "(value_json->>'battery')::int",
        "read_at":     "(value_json->>'timestamp')::timestamptz"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "public.sensor_readings",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- PIPELINE 2: Temperature alerts
-- =============================================================================

SELECT pgstreams.create_pipeline('temp_alerts', $$
{
  "input": {
    "mqtt": {
      "topic": "devices/+/telemetry",
      "qos": 1
    }
  },
  "pipeline": {
    "processors": [
      {
        "switch": [
          {
            "check": "(value_json->>'temperature')::numeric > 85",
            "processors": [
              {"mapping": {
                "device_id":  "value_json->>'device_id'",
                "alert_type": "'high_temperature'",
                "severity":   "'critical'",
                "message":    "'Temperature ' || (value_json->>'temperature') || 'C exceeds critical threshold (85C)'",
                "reading":    "value_json"
              }}
            ],
            "output": {
              "broker": {
                "pattern": "fan_out",
                "outputs": [
                  {"table": {"name": "public.sensor_alerts", "mode": "append"}},
                  {"mqtt": {"topic": "alerts/critical", "qos": 1}}
                ]
              }
            }
          },
          {
            "check": "(value_json->>'temperature')::numeric > 70",
            "processors": [
              {"mapping": {
                "device_id":  "value_json->>'device_id'",
                "alert_type": "'high_temperature'",
                "severity":   "'warning'",
                "message":    "'Temperature ' || (value_json->>'temperature') || 'C exceeds warning threshold (70C)'",
                "reading":    "value_json"
              }}
            ],
            "output": {
              "table": {"name": "public.sensor_alerts", "mode": "append"}
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
-- PIPELINE 3: Low battery detection (deduplicated)
-- =============================================================================

SELECT pgstreams.create_pipeline('low_battery', $$
{
  "input": {
    "mqtt": {
      "topic": "devices/+/telemetry",
      "qos": 1
    }
  },
  "pipeline": {
    "processors": [
      {"filter": "(value_json->>'battery')::int < 15"},
      {"dedupe": {
        "key": "value_json->>'device_id'",
        "window": "1 hour"
      }},
      {"mapping": {
        "device_id":   "value_json->>'device_id'",
        "alert_type":  "'low_battery'",
        "severity":    "'warning'",
        "message":     "'Battery at ' || (value_json->>'battery') || '% for device ' || (value_json->>'device_id')",
        "reading":     "value_json"
      }}
    ]
  },
  "output": {
    "table": {
      "name": "public.sensor_alerts",
      "mode": "append"
    }
  }
}
$$::jsonb);

-- =============================================================================
-- START ALL PIPELINES
-- =============================================================================

SELECT pgstreams.start('sensor_ingest');
SELECT pgstreams.start('temp_alerts');
SELECT pgstreams.start('low_battery');

-- =============================================================================
-- VERIFY
-- =============================================================================

SELECT pg_sleep(3);

-- All sensor readings ingested
SELECT device_id, location, temperature, humidity, battery_pct
FROM sensor_readings
ORDER BY device_id;
-- Expected: 4 rows (sensor-001 through sensor-004)

-- Temperature alerts
SELECT device_id, alert_type, severity, message
FROM sensor_alerts
WHERE alert_type = 'high_temperature'
ORDER BY severity;
-- Expected:
-- | device_id  | alert_type       | severity | message                              |
-- |------------|------------------|----------|--------------------------------------|
-- | sensor-003 | high_temperature | critical | Temperature 88.0C exceeds critical.. |
-- | sensor-002 | high_temperature | warning  | Temperature 71.0C exceeds warning..  |

-- Low battery alerts
SELECT device_id, alert_type, message
FROM sensor_alerts
WHERE alert_type = 'low_battery';
-- Expected: sensor-003 (battery=12%)

-- Pipeline status
SELECT * FROM pgstreams.status();

-- =============================================================================
-- CLEANUP
-- =============================================================================

SELECT pgstreams.stop('sensor_ingest');
SELECT pgstreams.stop('temp_alerts');
SELECT pgstreams.stop('low_battery');
SELECT pgstreams.drop_pipeline('sensor_ingest');
SELECT pgstreams.drop_pipeline('temp_alerts');
SELECT pgstreams.drop_pipeline('low_battery');
DROP TABLE IF EXISTS sensor_readings;
DROP TABLE IF EXISTS sensor_alerts;
DROP TABLE IF EXISTS sensor_location_avg_5m;
