# pg_kafka

Kafka protocol server backed by PostgreSQL tables. Standard Kafka clients connect and just work.

## Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Rust toolchain
- cargo-pgrx 0.12+
- cmake (for rdkafka client tests)

## Installation

```bash
# Install pgrx if not already installed
cargo install cargo-pgrx
cargo pgrx init --pg16 $(which pg_config)

# Build and install
cd extensions/pg_kafka
cargo pgrx install --release

# Add to shared_preload_libraries (postgresql.conf)
# shared_preload_libraries = 'pg_kafka'
# Restart PostgreSQL
```

## Quickstart

```sql
-- Create extension
CREATE EXTENSION pg_kafka;

-- Check status
SELECT pgkafka.status();

-- Create a topic
SELECT pgkafka.create_topic('events');

-- Produce via SQL
SELECT pgkafka.produce('events', '{"user": "alice"}'::bytea);

-- Or connect any Kafka client
-- Producer: localhost:9092, topic: events
```

See [pg_kafka Manual](../../docs/pg_kafka.md) for protocol details and limitations.

## Schema

`pgkafka`

## SQL API

### status

Get server status as JSON.

```sql
SELECT pgkafka.status();
-- {"enabled": true, "port": 9092, "host": "0.0.0.0", ...}
```

**Returns:** `JSONB` - Server status

### create_topic

Create a new Kafka topic (native or source-backed).

```sql
-- Native topic (messages stored in pgkafka.messages)
SELECT pgkafka.create_topic('events');

-- Source-backed topic (reads from existing table)
SELECT pgkafka.create_topic(
    'orders-topic',
    source_table => 'public.orders',
    offset_column => 'id',
    value_column => 'payload'
);
```

**Parameters:**
- `name TEXT` - Topic name
- `source_table TEXT DEFAULT NULL` - Source table (schema.table)
- `offset_column TEXT DEFAULT NULL` - Column to use as Kafka offset
- `value_column TEXT DEFAULT NULL` - Column for message value
- `value_expr TEXT DEFAULT NULL` - SQL expression for value (alternative to value_column)
- `key_column TEXT DEFAULT NULL` - Column for message key
- `key_expr TEXT DEFAULT NULL` - SQL expression for key
- `config JSONB DEFAULT NULL` - Additional configuration

**Returns:** `INTEGER` - Topic ID

### create_topic_from_table

Create a source-backed topic with validation of source table structure.

```sql
SELECT pgkafka.create_topic_from_table(
    'orders-topic',
    'public.orders',
    'order_id',
    value_expr => 'row_to_json(orders)',
    key_expr => 'customer_id::text',
    timestamp_column => 'created_at'
);
```

**Parameters:**
- `name TEXT` - Topic name
- `source_table TEXT` - Source table (schema.table)
- `offset_column TEXT` - Column for Kafka offset (must be integer with unique constraint)
- `value_column TEXT DEFAULT NULL` - Column for message value
- `value_expr TEXT DEFAULT NULL` - SQL expression for value
- `key_column TEXT DEFAULT NULL` - Column for message key
- `key_expr TEXT DEFAULT NULL` - SQL expression for key
- `timestamp_column TEXT DEFAULT NULL` - Column for message timestamp

**Returns:** `INTEGER` - Topic ID

### drop_topic

Drop a topic and optionally delete its messages.

```sql
SELECT pgkafka.drop_topic('events');
SELECT pgkafka.drop_topic('events', delete_messages => false);
```

**Parameters:**
- `name TEXT` - Topic name
- `delete_messages BOOLEAN DEFAULT true` - Whether to delete messages

**Returns:** `BOOLEAN` - True if dropped

### enable_topic_writes

Enable Kafka Produce API writes to a source-backed topic.

```sql
-- Stream mode: append to underlying table
SELECT pgkafka.enable_topic_writes('orders-topic', 'stream');

-- Table mode: upsert by key
SELECT pgkafka.enable_topic_writes(
    'orders-topic',
    'table',
    key_column => 'customer_id',
    kafka_offset_column => 'kafka_offset'
);
```

**Parameters:**
- `topic_name TEXT` - Topic name
- `write_mode TEXT DEFAULT 'stream'` - 'stream' (append) or 'table' (upsert)
- `key_column TEXT DEFAULT NULL` - Key column for table mode
- `kafka_offset_column TEXT DEFAULT NULL` - Column to store Kafka offset

**Returns:** `BOOLEAN` - True if enabled

### disable_topic_writes

Disable writes for a source-backed topic.

```sql
SELECT pgkafka.disable_topic_writes('orders-topic');
```

**Parameters:**
- `topic_name TEXT` - Topic name

**Returns:** `BOOLEAN` - True if disabled

### produce

Produce a message to a native topic via SQL.

```sql
SELECT pgkafka.produce('events', '{"event": "user_created"}'::bytea);
SELECT pgkafka.produce(
    'events',
    '{"event": "order_placed"}'::bytea,
    key => 'user-123'::bytea,
    headers => '{"source": "api"}'::jsonb
);
```

**Parameters:**
- `topic TEXT` - Topic name
- `value BYTEA` - Message value
- `key BYTEA DEFAULT NULL` - Message key
- `headers JSONB DEFAULT NULL` - Message headers

**Returns:** `BIGINT` - Message offset

## Configuration

```sql
-- Enable/disable server
SET pg_kafka.enabled = true;

-- Server port (default: 9092)
SET pg_kafka.port = 9092;

-- Bind address
SET pg_kafka.host = '0.0.0.0';

-- Advertised host for clients
SET pg_kafka.advertised_host = 'kafka.example.com';
```

## Architecture

```
┌─────────────────┐     Kafka Protocol      ┌─────────────────┐
│  Kafka Client   │ ◄─────────────────────► │    pg_kafka     │
│  (any language) │      Port 9092          │  (background    │
└─────────────────┘                         │   worker)       │
                                            └────────┬────────┘
                                                     │ SPI
                                                     ▼
                                            ┌─────────────────┐
                                            │   PostgreSQL    │
                                            │   Tables        │
                                            └─────────────────┘
```

## Supported Kafka APIs

| API | Status | Notes |
|-----|--------|-------|
| Metadata | Implemented | Topic discovery |
| Produce | Implemented | Single partition |
| Fetch | Implemented | Consumer reads |
| ListOffsets | Implemented | Offset queries |
| FindCoordinator | Implemented | Group coordination |
| JoinGroup | Implemented | Consumer groups |
| SyncGroup | Implemented | Partition assignment |
| Heartbeat | Implemented | Group liveness |
| LeaveGroup | Implemented | Clean disconnect |
| OffsetFetch | Implemented | Committed offsets |
| OffsetCommit | Implemented | Offset persistence |
| ApiVersions | Implemented | Protocol negotiation |
