# pg_registry

Schema registry for PostgreSQL extensions with JSON Schema validation.

## Schema

`pgregistry`

## SQL API

### register_schema

Register a new JSON Schema.

```sql
SELECT pgregistry.register_schema(
    'orders-value',
    '{"type": "object", "properties": {"id": {"type": "integer"}}}'::jsonb,
    'Schema for order events'
);
```

**Parameters:**
- `subject TEXT` - Schema subject name
- `schema_def JSONB` - JSON Schema definition
- `description TEXT DEFAULT NULL` - Optional description

**Returns:** `INTEGER` - Schema ID

### register_schema_from_table

Generate and register a schema from a PostgreSQL table.

```sql
SELECT pgregistry.register_schema_from_table(
    'orders-value',
    'public.orders',
    'Auto-generated from orders table'
);
```

**Parameters:**
- `subject TEXT` - Schema subject name
- `table_name TEXT` - Source table (schema.table)
- `description TEXT DEFAULT NULL` - Optional description

**Returns:** `INTEGER` - Schema ID

### generate_schema_from_table

Generate a JSON Schema from a table without registering it.

```sql
SELECT pgregistry.generate_schema_from_table('public.orders');
```

**Parameters:**
- `table_name TEXT` - Source table (schema.table)

**Returns:** `JSONB` - JSON Schema definition

### get_schema

Get a schema by ID.

```sql
SELECT pgregistry.get_schema(1);
```

**Parameters:**
- `schema_id INTEGER` - Schema ID

**Returns:** `JSONB` - Schema definition or NULL

### get_latest_schema

Get the latest version of a schema for a subject.

```sql
SELECT pgregistry.get_latest_schema('orders-value');
```

**Parameters:**
- `subject TEXT` - Schema subject name

**Returns:** `JSONB` - Schema definition or NULL

### drop_schema

Delete a schema.

```sql
SELECT pgregistry.drop_schema(1);
```

**Parameters:**
- `schema_id INTEGER` - Schema ID

**Returns:** `BOOLEAN` - True if deleted

### bind_schema_to_topic

Bind a schema to a Kafka topic for validation.

```sql
SELECT pgregistry.bind_schema_to_topic(
    'orders',
    1,
    'value',
    'STRICT'
);
```

**Parameters:**
- `topic_name TEXT` - Kafka topic name
- `schema_id INTEGER` - Schema ID
- `schema_type TEXT DEFAULT 'value'` - 'key' or 'value'
- `validation_mode TEXT DEFAULT 'STRICT'` - 'STRICT' or 'LOG'

**Returns:** `INTEGER` - Binding ID

### unbind_schema_from_topic

Remove a schema binding from a topic.

```sql
SELECT pgregistry.unbind_schema_from_topic('orders', 'value');
```

**Parameters:**
- `topic_name TEXT` - Kafka topic name
- `schema_type TEXT DEFAULT 'value'` - 'key' or 'value'

**Returns:** `BOOLEAN` - True if removed

### get_topic_schema

Get the schema bound to a topic.

```sql
SELECT pgregistry.get_topic_schema('orders', 'value');
```

**Parameters:**
- `topic_name TEXT` - Kafka topic name
- `schema_type TEXT DEFAULT 'value'` - 'key' or 'value'

**Returns:** `JSONB` - Schema definition or NULL

### validate

Validate JSON data against a schema.

```sql
SELECT pgregistry.validate(1, '{"order_id": 123}'::jsonb);
```

**Parameters:**
- `schema_id INTEGER` - Schema ID
- `data JSONB` - Data to validate

**Returns:** `BOOLEAN` - True if valid

### validate_for_topic

Validate data against a topic's bound schema.

```sql
SELECT pgregistry.validate_for_topic('orders', '{"order_id": 123}'::jsonb, 'value');
```

**Parameters:**
- `topic_name TEXT` - Kafka topic name
- `data JSONB` - Data to validate
- `schema_type TEXT DEFAULT 'value'` - 'key' or 'value'

**Returns:** `BOOLEAN` - True if valid (or no schema bound)

### create_table_from_schema

Create a PostgreSQL table from a registered JSON Schema.

```sql
SELECT pgregistry.create_table_from_schema('orders', 'orders-value', true);
```

**Parameters:**
- `table_name TEXT` - Target table name
- `schema_subject TEXT` - Schema subject name
- `add_id_column BOOLEAN DEFAULT true` - Add serial ID column

**Returns:** `BOOLEAN` - True if created

### bind_table_to_topic

Bind a table to a Kafka topic for bidirectional data flow.

```sql
SELECT pgregistry.bind_table_to_topic('orders', 'orders-topic', 'stream');
```

**Parameters:**
- `table_name TEXT` - Table name
- `kafka_topic TEXT` - Kafka topic name
- `mode TEXT DEFAULT 'stream'` - 'stream' (append) or 'table' (upsert)

**Returns:** `INTEGER` - Binding ID

### unbind_table_from_topic

Remove a table-to-topic binding.

```sql
SELECT pgregistry.unbind_table_from_topic('orders');
```

**Parameters:**
- `table_name TEXT` - Table name

**Returns:** `BOOLEAN` - True if removed

### get_table_binding

Get binding info for a table.

```sql
SELECT pgregistry.get_table_binding('orders');
```

**Parameters:**
- `table_name TEXT` - Table name

**Returns:** `JSONB` - Binding info or NULL

## Type Mapping

When generating schemas from PostgreSQL tables:

| PostgreSQL Type | JSON Schema Type |
|-----------------|------------------|
| integer, bigint, smallint | `{"type": "integer"}` |
| numeric, real, double precision | `{"type": "number"}` |
| boolean | `{"type": "boolean"}` |
| text, varchar, char | `{"type": "string"}` |
| json, jsonb | `{}` (any) |
| timestamp, timestamptz | `{"type": "string", "format": "date-time"}` |
| date | `{"type": "string", "format": "date"}` |
| uuid | `{"type": "string", "format": "uuid"}` |
| bytea | `{"type": "string", "contentEncoding": "base64"}` |
| array types | `{"type": "array", "items": {...}}` |

## Validation Modes

- **STRICT** - Reject invalid messages
- **LOG** - Log validation errors but accept messages
