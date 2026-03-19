# pg_kafka User Manual

Kafka protocol server backed by PostgreSQL tables.

## Overview

pg_kafka implements the Kafka binary protocol, allowing any Kafka client to connect and produce/consume messages stored in PostgreSQL tables. This brings the entire Kafka ecosystem to Postgres without running a separate Kafka cluster.

### Key Features

- **Standard Protocol**: Any Kafka client works (rdkafka, kafka-python, confluent-kafka, kafkajs, etc.)
- **SQL Storage**: Messages stored in PostgreSQL tables, queryable via SQL
- **Source-Backed Topics**: Expose existing tables as Kafka topics
- **Consumer Groups**: Offset tracking and group coordination
- **ACID Guarantees**: Messages persisted with PostgreSQL durability

### Architecture

```
+-------------------+     Kafka Protocol      +-------------------+
|   Kafka Client    | <--------------------> |    pg_kafka       |
|   (any language)  |      Port 9092         |  (background      |
+-------------------+                         |   worker)         |
                                              +---------+---------+
                                                        | SPI
                                                        v
                                              +-------------------+
                                              |   PostgreSQL      |
                                              |   Tables          |
                                              +-------------------+
```

pg_kafka runs as a PostgreSQL background worker that accepts Kafka protocol connections. When a client produces a message, it's stored in PostgreSQL tables. When a client consumes, messages are read from those tables using PostgreSQL's SPI (Server Programming Interface).

### When to Use pg_kafka

**Good fit:**
- Need Kafka ecosystem tools against Postgres data
- Want SQL access to message streams
- Building event sourcing on Postgres
- Prototyping before deploying real Kafka
- Edge deployments where Kafka is too heavy

**Consider real Kafka instead when:**
- Need multi-broker replication
- Processing millions of messages per second
- Require exactly-once semantics
- Need multi-datacenter replication

## Concepts

### Topics

pg_kafka supports two types of topics:

#### Native Topics

Messages stored directly in the `pgkafka.messages` table. Create with:

```sql
SELECT pgkafka.create_topic('events');
```

Native topics support full Kafka semantics:
- Produce messages via Kafka protocol or SQL
- Consume messages via Kafka protocol
- Offsets managed automatically

#### Source-Backed Topics

Expose existing PostgreSQL tables as Kafka topics. No data duplication - the table IS the topic.

```sql
SELECT pgkafka.create_topic(
    'orders',
    source_table => 'public.orders',
    offset_column => 'id',
    value_column => 'payload'
);
```

Source-backed topics:
- Read from the underlying table on Fetch
- Optionally support writes (append or upsert)
- Use any column as the message offset
- Transform columns via SQL expressions

### Consumer Groups

pg_kafka tracks consumer group membership and committed offsets:

```sql
-- View group membership
SELECT * FROM pgkafka.consumer_groups;

-- View committed offsets
SELECT * FROM pgkafka.consumer_group_offsets;
```

Consumer group features:
- JoinGroup/SyncGroup for partition assignment
- OffsetCommit/OffsetFetch for offset management
- Heartbeat for liveness detection
- LeaveGroup for clean disconnects

### Partitions

**pg_kafka uses a single partition per topic.** This is a deliberate design choice:
- Simplifies the implementation
- PostgreSQL tables are the natural unit of parallelism
- Avoids the complexity of partition rebalancing

For scale-out, create multiple topics or use PostgreSQL table partitioning.

## Getting Started

### 1. Install the Extension

```sql
CREATE EXTENSION pg_kafka CASCADE;
```

### 2. Configure and Enable

```sql
-- Set the listening port (default: 9092)
ALTER SYSTEM SET pg_kafka.port = 9092;

-- Set bind address (default: 127.0.0.1)
ALTER SYSTEM SET pg_kafka.host = '0.0.0.0';

-- Enable the server (default: false)
ALTER SYSTEM SET pg_kafka.enabled = true;

-- Reload configuration
SELECT pg_reload_conf();
```

Note: `pg_kafka.enabled` requires a server restart to take effect.

### 3. Create a Topic

```sql
SELECT pgkafka.create_topic('my-events');
```

### 4. Connect with Any Kafka Client

**Python (kafka-python):**
```python
from kafka import KafkaProducer, KafkaConsumer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('my-events', b'Hello from Python!')

consumer = KafkaConsumer('my-events', bootstrap_servers='localhost:9092')
for message in consumer:
    print(message.value)
```

**Node.js (kafkajs):**
```javascript
const { Kafka } = require('kafkajs');
const kafka = new Kafka({ brokers: ['localhost:9092'] });

const producer = kafka.producer();
await producer.send({
  topic: 'my-events',
  messages: [{ value: 'Hello from Node!' }],
});
```

**Rust (rdkafka):**
```rust
use rdkafka::producer::{FutureProducer, FutureRecord};

let producer: FutureProducer = ClientConfig::new()
    .set("bootstrap.servers", "localhost:9092")
    .create()?;

producer.send(
    FutureRecord::to("my-events").payload("Hello from Rust!"),
    Duration::from_secs(0),
).await?;
```

### 5. Query Messages via SQL

```sql
-- View all messages
SELECT * FROM pgkafka.messages ORDER BY offset DESC LIMIT 10;

-- Produce via SQL
SELECT pgkafka.produce('my-events', '{"event": "test"}'::bytea);
```

## Protocol Support

pg_kafka implements the Kafka protocol for client compatibility. This section details what's supported.

### Supported APIs

| API | Key | Versions | Description |
|-----|-----|----------|-------------|
| ApiVersions | 18 | 0-3 | Protocol version negotiation |
| Metadata | 3 | 0-12 | Broker/topic discovery |
| Produce | 0 | 0-9 | Message production |
| Fetch | 1 | 0-13 | Message consumption |
| ListOffsets | 2 | 0-7 | Query earliest/latest offsets |
| FindCoordinator | 10 | 0-4 | Locate group coordinator |
| JoinGroup | 11 | 0-7 | Join a consumer group |
| SyncGroup | 14 | 0-5 | Receive partition assignment |
| Heartbeat | 12 | 0-4 | Keep group membership alive |
| LeaveGroup | 13 | 0-4 | Exit consumer group cleanly |
| OffsetFetch | 9 | 0-8 | Get committed offsets |
| OffsetCommit | 8 | 0-8 | Persist consumer offsets |

### API Details

#### Metadata (API Key 3)
Returns broker and topic information. pg_kafka advertises itself as a single broker.

**Supported:**
- Topic list with partition metadata
- Auto-topic creation (configurable)

**Response behavior:**
- Always returns 1 partition per topic
- Partition leader is always broker 0 (this server)

#### Produce (API Key 0)
Stores messages in PostgreSQL tables.

**Supported:**
- Record batches with multiple messages
- Message keys and values
- Headers
- Timestamps (CreateTime)

**Behavior:**
- Messages inserted into `pgkafka.messages` (native) or source table (source-backed)
- Returns offset of last message in batch

#### Fetch (API Key 1)
Retrieves messages from a topic starting at an offset.

**Supported:**
- Offset-based consumption
- Max bytes limit
- Multiple partitions in one request

**Behavior:**
- Reads from `pgkafka.messages` or source table
- Returns messages as Kafka RecordBatch format

#### Consumer Group APIs
Full support for cooperative consumer groups:

- **JoinGroup**: Registers consumer with group, returns member ID
- **SyncGroup**: Distributes partition assignments (always 1 partition)
- **Heartbeat**: Maintains group membership
- **LeaveGroup**: Clean exit from group

### Unsupported APIs

The following Kafka APIs are not implemented. Use SQL functions instead.

| API | Alternative |
|-----|-------------|
| CreateTopics | `pgkafka.create_topic()` |
| DeleteTopics | `pgkafka.drop_topic()` |
| CreatePartitions | N/A (single partition design) |
| DescribeConfigs | `pgkafka.status()` |
| AlterConfigs | `SET pg_kafka.*` GUCs |
| DeleteRecords | SQL: `DELETE FROM pgkafka.messages` |
| InitProducerIdRequest | Transactions not supported |
| AddPartitionsToTxn | Transactions not supported |
| EndTxn | Transactions not supported |

### Protocol Version Negotiation

Clients negotiate versions via ApiVersions request. pg_kafka returns supported version ranges, and clients select compatible versions. Most modern clients (librdkafka 1.0+, kafka-python 2.0+, kafkajs 2.0+) work without configuration.

## Limitations

Understanding these limitations helps you decide when pg_kafka is the right choice.

### Single Partition Per Topic

pg_kafka uses one partition per topic. This means:
- No partition-level parallelism within a topic
- Consumer groups don't distribute partitions
- All consumers in a group see all messages

**Workaround:** Create multiple topics for parallelism, or use PostgreSQL table partitioning for large datasets.

### No Transactions

Kafka's transactional producer (exactly-once semantics) is not supported:
- No InitProducerIdRequest
- No AddPartitionsToTxn
- No EndTxn

**Workaround:** Use PostgreSQL transactions directly via SQL, or accept at-least-once semantics.

### No Authentication/Authorization

pg_kafka v1 has no built-in authentication:
- No SASL support
- No ACLs
- Relies on network-level security

**Workaround:** Use firewall rules, VPN, or SSH tunnels. Authentication planned for v2.

### No Replication

pg_kafka is single-node:
- No multi-broker clusters
- No partition replicas
- Durability depends on PostgreSQL backup/replication

**Workaround:** Use PostgreSQL streaming replication for HA.

### Message Size Limits

Messages are limited by PostgreSQL column size (1GB) and practical memory constraints. Very large messages may cause memory pressure.

**Recommendation:** Keep messages under 1MB for best performance.

### Consumer Group Rebalancing

Rebalancing is simpler than real Kafka:
- Only one partition to assign
- Range and RoundRobin assignors work identically
- Sticky assignor not implemented

### Ordering Guarantees

Messages within a topic are ordered by offset. However:
- Concurrent producers may interleave
- Source-backed topics order by `offset_column`

## Best Practices

### Topic Design

1. **Use native topics for event streams**: When you need standard Kafka semantics with message persistence.

2. **Use source-backed topics for table CDC**: When you want Kafka clients to read from existing tables.

3. **Naming convention**: Use kebab-case (`user-events`, `order-updates`) for topic names.

### Source-Backed Topics

1. **Choose the right offset column**: Must be unique, sequential, and indexed.

   ```sql
   -- Good: SERIAL or BIGSERIAL primary key
   CREATE TABLE events (
       id BIGSERIAL PRIMARY KEY,
       data JSONB
   );
   ```

2. **Use expressions for complex values**:

   ```sql
   SELECT pgkafka.create_topic_from_table(
       'users-json',
       'public.users',
       'id',
       value_expr => 'row_to_json(users)'
   );
   ```

3. **Enable writes carefully**: Only enable if the source table schema matches message format.

### Consumer Groups

1. **Use meaningful group IDs**: `myapp-consumers`, `analytics-pipeline`

2. **Handle rebalances gracefully**: Commit offsets before shutdown.

3. **Monitor committed offsets**:

   ```sql
   SELECT
       g.group_id,
       o.topic_name,
       o.committed_offset,
       m.max_offset,
       m.max_offset - o.committed_offset AS lag
   FROM pgkafka.consumer_group_offsets o
   JOIN pgkafka.consumer_groups g ON g.id = o.group_id
   LEFT JOIN (
       SELECT topic_id, MAX(offset) as max_offset
       FROM pgkafka.messages
       GROUP BY topic_id
   ) m ON m.topic_id = o.topic_id;
   ```

### Performance Tuning

1. **Batch produce**: Send multiple messages per Produce request.

2. **Batch fetch**: Use larger `fetch.max.bytes` for throughput.

3. **Index source tables**: Ensure offset column is indexed for source-backed topics.

4. **Vacuum regularly**: Keep table bloat under control for read performance.

### Monitoring

Enable Prometheus metrics for observability:

```sql
-- Enable metrics endpoint
ALTER SYSTEM SET pg_kafka.metrics_enabled = true;
ALTER SYSTEM SET pg_kafka.metrics_port = 9187;
SELECT pg_reload_conf();
```

Available metrics:
- `pgkafka_messages_produced_total` - Total messages produced
- `pgkafka_messages_consumed_total` - Total messages consumed
- `pgkafka_active_connections` - Current Kafka client connections
- `pgkafka_produce_latency_seconds` - Produce request latency

## Configuration Reference

See [configuration.md](configuration.md#pg_kafka) for the complete list of GUC parameters.

Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `pg_kafka.enabled` | `false` | Enable Kafka server (restart required) |
| `pg_kafka.port` | `9092` | Kafka protocol port |
| `pg_kafka.host` | `127.0.0.1` | Bind address |
| `pg_kafka.advertised_host` | (empty) | Host advertised to clients |
| `pg_kafka.metrics_enabled` | `false` | Enable Prometheus endpoint |
| `pg_kafka.metrics_port` | `9187` | Prometheus metrics port |

## Troubleshooting

See [troubleshooting.md](troubleshooting.md#pg_kafka) for common issues and solutions.

Quick checks:

```sql
-- Check server status
SELECT pgkafka.status();

-- View topics
SELECT * FROM pgkafka.topics;

-- Check for connection issues
SELECT * FROM pg_stat_activity WHERE application_name LIKE 'pg_kafka%';
```
