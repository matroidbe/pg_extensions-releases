# pg_mqtt

MQTT 5.0 and 3.1.1 broker running inside PostgreSQL. IoT devices connect directly to your database.

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

Publish a message to a topic via SQL. It is stored, and delivered to the
connected subscribers of the topic when the transaction commits (within about
50 ms). A rolled-back publish is never delivered. A long-running write
transaction elsewhere in the cluster delays delivery until it ends.

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

-- Messages one subscriber may have waiting (default: 10000). A subscriber
-- that falls further behind loses its oldest QoS 0 messages; one whose queue
-- is full of QoS 1/2 messages is disconnected (0x97 Quota exceeded) instead
-- of losing them. Read when the workers start.
ALTER SYSTEM SET pg_mqtt.max_queued_messages = 10000;
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

- MQTT 5.0 and MQTT 3.1.1 clients, side by side (a 3.1.1 subscriber gets an MQTT 5
  message without its properties)
- QoS 0, 1 and 2: PUBACK for QoS 1, PUBREC/PUBREL/PUBCOMP for QoS 2 (a resent
  QoS 2 PUBLISH is routed once), and a subscriber gets each message at the
  lower of the published and the subscribed QoS, within its Receive Maximum
- PUBLISH properties (user properties, content type, payload format, response
  topic, correlation data, message expiry) reach subscribers, retained
  messages included
- Retained messages: a retained PUBLISH replaces the topic's retained message
  (`pgmqtt.retained`), an empty one removes it, and a new subscription gets the
  matching retained messages (Retain Handling, Retain As Published and No Local
  honoured)
- Last Will (with its properties): published when a connection ends without
  DISCONNECT, on DISCONNECT with reason 0x04, on a keep-alive timeout
  (1.5 × keep-alive) and when a new connection takes the client id over
- Sessions end with their connection: CONNACK answers a Session Expiry Interval
  with 0 and Session Present is always 0. Nothing is queued for offline clients.
- Wildcard filters do not match `$` topics; invalid filters and `$share/`
  (shared subscriptions are not supported) are refused in the SUBACK
- Compliance profile: `tests/compliance/` runs the Eclipse Paho MQTT 5 and
  3.1.1 interoperability suites against the broker
- Wildcard subscriptions (+ and #)
- Message persistence in PostgreSQL: PUBLISHes are stored in batches, one
  transaction each, and acknowledged and forwarded only after the commit; a
  full queue stops the broker reading from publishers (backpressure).
  `examples/mqtt_load.rs` measures it.
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
