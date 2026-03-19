# pg_streaming Examples

Example SQL files demonstrating the pg_streaming extension — a declarative stream processing engine running inside PostgreSQL.

**Every example is fully self-contained**: creates typed topics, inserts test data, runs pipelines, verifies output, and cleans up.

## Prerequisites

```sql
CREATE EXTENSION pg_streaming;
-- For Kafka examples:
CREATE EXTENSION pg_kafka;
-- For MQTT examples:
CREATE EXTENSION pg_mqtt;
```

## Typed Topics

These examples use **typed topics** — the PostgreSQL-native way to work with Kafka topics in pg_kafka. A typed topic is a regular PostgreSQL table that is automatically exposed as a Kafka topic:

```sql
-- Creates a table + Kafka topic + JSON Schema in one step
SELECT pgkafka.create_typed_topic('orders', $$
{
  "type": "object",
  "properties": {
    "order_id":    {"type": "string"},
    "amount":      {"type": "number"},
    "customer_id": {"type": "integer"}
  }
}
$$::jsonb);

-- Insert data with regular SQL (appears on Kafka topic automatically)
INSERT INTO orders (order_id, amount, customer_id)
VALUES ('ORD-001', 150.00, 42);

-- Query the topic as a table
SELECT * FROM orders;
```

### JSON Schema Type Mapping

| JSON Schema | PostgreSQL |
|-------------|------------|
| `{"type": "string"}` | `TEXT` |
| `{"type": "string", "maxLength": 3}` | `VARCHAR(3)` |
| `{"type": "string", "format": "date-time"}` | `TIMESTAMPTZ` |
| `{"type": "string", "format": "date"}` | `DATE` |
| `{"type": "string", "format": "uuid"}` | `UUID` |
| `{"type": "integer"}` | `BIGINT` |
| `{"type": "number"}` | `NUMERIC` |
| `{"type": "boolean"}` | `BOOLEAN` |
| `{"type": "object"}` | `JSONB` |

## Examples

| File | Description | Input |
|------|-------------|-------|
| [01_basic_filter_and_map.sql](01_basic_filter_and_map.sql) | Filter and reshape records between typed topics | Kafka |
| [02_sql_enrichment.sql](02_sql_enrichment.sql) | Enrich streaming records with database lookups | Kafka |
| [03_windowed_aggregation.sql](03_windowed_aggregation.sql) | Tumbling, sliding, session, and count windows | Kafka |
| [04_unbounded_aggregation.sql](04_unbounded_aggregation.sql) | Kappa-style incremental aggregation with rollups | Kafka |
| [05_stream_joins.sql](05_stream_joins.sql) | Stream-to-stream and stream-to-table joins | Kafka |
| [06_cdc_pipeline.sql](06_cdc_pipeline.sql) | Change Data Capture from PostgreSQL tables | CDC |
| [07_cep_pattern_matching.sql](07_cep_pattern_matching.sql) | Complex Event Processing for detecting event sequences | Kafka |
| [08_branching_and_routing.sql](08_branching_and_routing.sql) | Route records to different outputs based on conditions | Kafka |
| [09_table_to_table.sql](09_table_to_table.sql) | Poll a table, transform, and write to another table | Table |
| [10_iot_mqtt_pipeline.sql](10_iot_mqtt_pipeline.sql) | IoT sensor data from MQTT through to analytics tables | MQTT |
| [11_observability.sql](11_observability.sql) | Monitor pipelines with status, metrics, errors, and lag | Kafka |
| [12_medallion_star_schema.sql](12_medallion_star_schema.sql) | Full Bronze -> Silver -> Gold medallion architecture | Kafka |

## Example Structure

Every example follows the same pattern:

```
SETUP       → Create typed topics, reference tables, output tables
INSERT DATA → Seed test records into input topics/tables
PIPELINES   → Define and start streaming pipelines
VERIFY      → Query output tables to confirm results
CLEANUP     → Stop pipelines, drop topics, drop tables
```

## Record Model

Every record flowing through a pipeline has these fields available in SQL expressions:

| Field | Type | Description |
|-------|------|-------------|
| `key_text` | `TEXT` | Message key as UTF-8 |
| `key_json` | `JSONB` | Key parsed as JSON |
| `value_text` | `TEXT` | Message value as UTF-8 |
| `value_json` | `JSONB` | Value parsed as JSON |
| `headers` | `JSONB` | Message headers |
| `offset_id` | `BIGINT` | Source offset |
| `created_at` | `TIMESTAMPTZ` | Message timestamp |
| `source_topic` | `TEXT` | Origin topic |
