# pg_mqtt

MQTT 3.1.1 broker running inside PostgreSQL. IoT devices connect directly to your database.

## Installation

```bash
cargo pgrx install --release
```

Add to `shared_preload_libraries` in postgresql.conf and restart PostgreSQL.

## Schema

`pgmqtt`

## SQL API

### mqtt_status

Get broker status.

```sql
SELECT pgmqtt.mqtt_status();
-- 'MQTT broker running on 0.0.0.0:1883'
```

**Returns:** `TEXT` - Status message

### mqtt_publish

Publish a message to a topic via SQL.

```sql
SELECT pgmqtt.mqtt_publish('sensors/room1/temperature', '22.5');
```

**Parameters:**
- `topic TEXT` - MQTT topic
- `payload TEXT` - Message payload

**Returns:** `BIGINT` - Message ID

### mqtt_messages

Query stored messages with optional topic pattern filtering.

```sql
SELECT * FROM pgmqtt.mqtt_messages('sensors/%', 100);
```

**Parameters:**
- `topic_pattern TEXT` - MQTT wildcard pattern (+ single level, # multi level)
- `limit INTEGER DEFAULT 10` - Maximum messages to return

**Returns:** `TABLE (message_id BIGINT, topic TEXT, payload TEXT, created_at TIMESTAMPTZ)`

### mqtt_subscription_count

Get the current number of active subscriptions.

```sql
SELECT pgmqtt.mqtt_subscription_count();
```

**Returns:** `BIGINT` - Number of active subscriptions

### mqtt_sessions

List active client sessions.

```sql
SELECT * FROM pgmqtt.mqtt_sessions();
```

**Returns:** `TABLE (client_id TEXT, connected_at TIMESTAMPTZ, last_seen TIMESTAMPTZ)`

## Configuration

```sql
-- Enable/disable broker
SET pg_mqtt.enabled = true;

-- Broker port (default: 1883)
SET pg_mqtt.port = 1883;
```

## Architecture

```
┌─────────────────┐     MQTT Protocol       ┌─────────────────┐
│   IoT Device    │ ◄─────────────────────► │    pg_mqtt      │
│   (any client)  │      Port 1883          │  (background    │
└─────────────────┘                         │   worker)       │
                                            └────────┬────────┘
                                                     │ SPI
                                                     ▼
                                            ┌─────────────────┐
                                            │   PostgreSQL    │
                                            │   Tables        │
                                            └─────────────────┘
```

## Features

- MQTT 3.1.1 protocol support
- QoS 0 message delivery
- Wildcard subscriptions (+ and #)
- Message persistence in PostgreSQL
- SQL interface for publishing and querying
- Session tracking

## Topic Wildcards

- `+` - Single level wildcard (matches exactly one level)
- `#` - Multi-level wildcard (matches zero or more levels, must be last)

Examples:
- `sensors/+/temperature` matches `sensors/room1/temperature`, `sensors/room2/temperature`
- `sensors/#` matches `sensors`, `sensors/room1`, `sensors/room1/temperature`

## Example

```sql
-- Query recent temperature readings
SELECT topic, payload, created_at
FROM pgmqtt.mqtt_messages('sensors/+/temperature', 100)
ORDER BY created_at DESC;

-- Publish alert via SQL
SELECT pgmqtt.mqtt_publish('alerts/temperature', 'High temperature detected');
```
