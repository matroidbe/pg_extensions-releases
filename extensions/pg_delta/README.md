# pg_delta

Delta Lake streaming integration for PostgreSQL. Stream data bidirectionally between Postgres and Delta Lake tables.

## Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Rust toolchain
- cargo-pgrx 0.12+
- Cloud storage credentials (AWS/Azure/GCS)

## Installation

```bash
# Install pgrx if not already installed
cargo install cargo-pgrx
cargo pgrx init --pg16 $(which pg_config)

# Build and install
cd extensions/pg_delta
cargo pgrx install --release

# Add to shared_preload_libraries (postgresql.conf)
# shared_preload_libraries = 'pg_delta'
# Restart PostgreSQL
```

## Quickstart

```sql
-- Create extension
CREATE EXTENSION pg_delta;

-- Configure AWS (or Azure/GCS)
SET delta.aws_region = 'us-east-1';
SET delta.aws_access_key_id = 'your-key';
SET delta.aws_secret_access_key = 'your-secret';

-- Read Delta table
SELECT delta.read('s3://bucket/table', 'public.staging');

-- Create a stream
SELECT delta.stream_create(
    'my_stream',
    's3://bucket/source',
    'public.target',
    'ingest'
);
```

See [pg_delta Manual](../../docs/pg_delta.md) for cloud storage setup.

## Schema

`delta`

## Function Overview

| Function | Description |
|----------|-------------|
| **Status & Config** | |
| `status()` | Get extension status |
| `test_storage(uri)` | Test storage connectivity |
| **One-Shot Operations** | |
| `read(uri, table)` | Read Delta table into Postgres |
| `write(uri, query/table)` | Write Postgres data to Delta |
| `info(uri)` | Get Delta table metadata |
| `schema(uri)` | Get Delta table schema |
| `history(uri)` | Get Delta table history |
| **Streaming** | |
| `stream_create(...)` | Create a stream |
| `stream_start(name)` | Start a stream |
| `stream_stop(name)` | Stop a stream |
| `stream_drop(name)` | Drop a stream |
| `stream_reset(name)` | Reset stream progress |
| **Managed Tables (Delta → Postgres)** | |
| `create_table(uri, table)` | Create Postgres table from Delta |
| `refresh(table)` | Refresh managed table |
| `drop_table(table)` | Drop managed table registration |
| `list_tables()` | List managed tables |
| **Export Tables (Postgres → Delta)** | |
| `export_table(table, uri)` | Register table for export |
| `export(table)` | Export changes to Delta |
| `drop_export(table)` | Drop export registration |
| `list_exports()` | List export registrations |

## SQL API

### status

Get extension status as JSON.

```sql
SELECT delta.status();
-- {"enabled": true, "aws_configured": true, ...}
```

**Returns:** `JSONB` - Extension status

### stream_create

Create a new stream configuration.

```sql
-- Ingest stream (Delta → Postgres)
SELECT delta.stream_create(
    'sales_ingest',                    -- stream name
    's3://bucket/sales',               -- Delta table URI
    'public.sales',                    -- target Postgres table
    'ingest',                          -- direction
    mode => 'append',                  -- append | upsert | replace
    poll_interval_ms => 5000           -- poll every 5 seconds
);

-- Export stream (Postgres → Delta)
SELECT delta.stream_create(
    'orders_export',
    's3://bucket/orders',
    'public.orders',
    'export',
    mode => 'incremental',             -- incremental | snapshot | cdc
    tracking_column => 'updated_at',
    batch_size => 10000
);
```

**Parameters:**
- `name TEXT` - Unique stream identifier
- `uri TEXT` - Delta table URI (s3://, az://, gs://, file://)
- `pg_table TEXT` - Postgres table (schema.table)
- `direction TEXT` - 'ingest' or 'export'
- `mode TEXT DEFAULT 'append'` - Sync mode
- `poll_interval_ms INTEGER DEFAULT NULL` - Poll interval for ingest
- `batch_size INTEGER DEFAULT NULL` - Rows per batch for export
- `flush_interval_ms INTEGER DEFAULT NULL` - Max time between flushes
- `start_version BIGINT DEFAULT NULL` - Delta version to start from
- `tracking_column TEXT DEFAULT NULL` - Column for incremental export
- `key_columns TEXT[] DEFAULT NULL` - Primary key for upsert mode
- `storage_options JSONB DEFAULT NULL` - Per-stream credential override
- `enabled BOOLEAN DEFAULT true` - Start immediately

**Returns:** `INTEGER` - Stream ID

### stream_start

Start a stopped stream.

```sql
SELECT delta.stream_start('sales_ingest');
```

**Returns:** `BOOLEAN` - True if started

### stream_stop

Stop a running stream.

```sql
SELECT delta.stream_stop('sales_ingest');
```

**Returns:** `BOOLEAN` - True if stopped

### stream_drop

Drop a stream configuration.

```sql
SELECT delta.stream_drop('sales_ingest');
SELECT delta.stream_drop('sales_ingest', force => true);
```

**Returns:** `BOOLEAN` - True if dropped

### stream_reset

Reset stream progress to re-sync.

```sql
SELECT delta.stream_reset('sales_ingest');
SELECT delta.stream_reset('sales_ingest', version => 0);
```

**Returns:** `BOOLEAN` - True if reset

### read

Read a Delta table into Postgres (one-time).

```sql
-- Latest version
SELECT delta.read('s3://bucket/table', 'public.staging');

-- Specific version (time travel)
SELECT delta.read(
    's3://bucket/table',
    'public.staging',
    version => 42
);
```

**Returns:** `BIGINT` - Rows inserted

### write

Write Postgres data to Delta (one-time).

```sql
-- From query
SELECT delta.write(
    's3://bucket/output',
    query => 'SELECT * FROM orders WHERE date > ''2024-01-01'''
);

-- From table
SELECT delta.write(
    's3://bucket/customers',
    pg_table => 'public.customers'
);
```

**Returns:** `BIGINT` - Rows written

### info

Get Delta table metadata.

```sql
SELECT delta.info('s3://bucket/table');
-- {"version": 42, "num_files": 10, ...}
```

**Returns:** `JSONB` - Table metadata

### schema

Get Delta table schema.

```sql
SELECT delta.schema('s3://bucket/table');
-- {"columns": [{"name": "id", "type": "LONG", ...}]}
```

**Returns:** `JSONB` - Table schema

### history

Get Delta table transaction history.

```sql
SELECT delta.history('s3://bucket/table', limit => 10);
```

**Returns:** `JSONB` - Transaction history

### test_storage

Test storage connectivity.

```sql
SELECT delta.test_storage('s3://bucket/path');
-- {"can_list": true, "can_read": true, ...}
```

**Returns:** `JSONB` - Connectivity test results

### create_table

Create a Postgres table from a Delta Lake table (managed table pattern).

```sql
-- Create table from Delta source
SELECT delta.create_table(
    's3://bucket/sales',
    'public.sales'
);

-- With custom storage credentials
SELECT delta.create_table(
    's3://bucket/sales',
    'public.sales',
    storage_options => '{"aws_access_key_id": "xxx"}'::jsonb
);
```

**Parameters:**
- `uri TEXT` - Delta table URI (s3://, az://, gs://, file://)
- `pg_table TEXT` - Target Postgres table (will be created)
- `storage_options JSONB DEFAULT NULL` - Per-table storage credentials override

**Returns:** `BIGINT` - Rows loaded

### refresh

Refresh a managed Delta table, syncing only changed partitions.

```sql
-- Incremental refresh (only changed partitions)
SELECT delta.refresh('public.sales');

-- Full refresh (re-sync all data)
SELECT delta.refresh('public.sales', true);
```

**Parameters:**
- `pg_table TEXT` - Postgres table (must be created via create_table)
- `full_refresh BOOLEAN DEFAULT false` - If true, re-sync all data

**Returns:** `BIGINT` - Rows synced

### drop_table

Drop a managed Delta table registration.

```sql
-- Unregister only (keep Postgres table)
SELECT delta.drop_table('public.sales');

-- Unregister and drop Postgres table
SELECT delta.drop_table('public.sales', true);
```

**Parameters:**
- `pg_table TEXT` - Postgres table name
- `drop_table BOOLEAN DEFAULT false` - If true, also DROP the Postgres table

**Returns:** `BOOLEAN` - True if unregistered

### list_tables

List all managed Delta tables.

```sql
SELECT delta.list_tables();
-- {"tables": [{"pg_table": "public.sales", "delta_uri": "s3://...", ...}]}
```

**Returns:** `JSONB` - Array of managed table registrations

### export_table

Export a Postgres table to Delta Lake (creates export registration).

```sql
-- Export with partitioning and change tracking
SELECT delta.export_table(
    'public.orders',
    's3://bucket/orders',
    partition_by => ARRAY['year', 'month'],
    tracking_column => 'updated_at'
);

-- Simple export without partitioning
SELECT delta.export_table('public.users', 's3://bucket/users');
```

**Parameters:**
- `pg_table TEXT` - Source Postgres table (schema.table)
- `uri TEXT` - Delta table URI (s3://, az://, gs://, file://)
- `partition_by TEXT[] DEFAULT NULL` - Columns to partition by in Delta
- `tracking_column TEXT DEFAULT NULL` - Column to detect changes (e.g., 'updated_at')
- `storage_options JSONB DEFAULT NULL` - Per-table storage credentials override

**Returns:** `BIGINT` - Rows exported

### export

Export changed rows from a managed Postgres table to Delta.

```sql
-- Incremental export (only changed rows)
SELECT delta.export('public.orders');

-- Full export (all rows)
SELECT delta.export('public.orders', true);
```

**Parameters:**
- `pg_table TEXT` - Postgres table (must be registered via export_table)
- `full_export BOOLEAN DEFAULT false` - If true, re-export all data

**Returns:** `BIGINT` - Rows exported

### drop_export

Drop an export table registration.

```sql
SELECT delta.drop_export('public.orders');
```

**Parameters:**
- `pg_table TEXT` - Postgres table name

**Returns:** `BOOLEAN` - True if unregistered

**Note:** Does NOT delete the Delta table or Postgres table.

### list_exports

List all export table registrations.

```sql
SELECT delta.list_exports();
-- {"exports": [{"pg_table": "public.orders", "delta_uri": "s3://...", ...}]}
```

**Returns:** `JSONB` - Array of export table registrations

## Configuration

### Worker Settings

```sql
SET delta.enabled = true;              -- Enable stream manager
SET delta.worker_log_level = 'info';   -- debug, info, warn, error
SET delta.poll_interval_ms = 10000;    -- Default poll interval
SET delta.batch_size = 10000;          -- Default batch size
SET delta.flush_interval_ms = 60000;   -- Default flush interval
```

### AWS S3

```sql
SET delta.aws_region = 'us-east-1';
SET delta.aws_access_key_id = 'xxx';
SET delta.aws_secret_access_key = 'xxx';

-- Or use instance profile
SET delta.aws_use_instance_profile = true;

-- For S3-compatible storage (MinIO)
SET delta.aws_endpoint = 'http://localhost:9000';
SET delta.aws_allow_http = true;
```

### Azure Blob Storage

```sql
SET delta.azure_storage_account = 'myaccount';
SET delta.azure_storage_key = 'xxx';

-- Or use managed identity
SET delta.azure_use_managed_identity = true;
```

### Google Cloud Storage

```sql
SET delta.gcs_service_account_path = '/path/to/key.json';

-- Or use default credentials
SET delta.gcs_use_default_credentials = true;
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      PostgreSQL                              │
│  ┌─────────────┐    ┌──────────────────┐    ┌────────────┐  │
│  │ Source      │───▶│  pg_delta BGW    │───▶│ Target     │  │
│  │ Tables      │    │                  │    │ Tables     │  │
│  └─────────────┘    │  • Ingest stream │    └────────────┘  │
│                     │  • Export stream │                     │
│                     │  • Progress track│                     │
│                     └──────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
           │                                        ▲
           ▼                                        │
    ┌──────────────────────────────────────────────────────┐
    │                   Delta Lake                          │
    │   s3://bucket/table/                                  │
    │   ├── _delta_log/          (transaction log)         │
    │   └── *.parquet            (data files)              │
    └──────────────────────────────────────────────────────┘
```

## Sync Modes

### Ingest (Delta → Postgres)

| Mode | Description |
|------|-------------|
| `append` | INSERT new rows |
| `upsert` | INSERT ON CONFLICT UPDATE (requires key_columns) |
| `replace` | TRUNCATE and reload |

### Export (Postgres → Delta)

| Mode | Description |
|------|-------------|
| `incremental` | Poll based on tracking_column |
| `snapshot` | Full table dump |
| `cdc` | Logical replication (not yet implemented) |

## VACUUM and Compaction Handling

Delta Lake's VACUUM and OPTIMIZE (compaction) operations change physical file layout without changing logical data. This creates challenges for incremental sync:

- **Compaction**: Files A + B → File C (same data, different file)
- **VACUUM**: Removes old files that are no longer referenced

pg_delta handles this by tracking processed files and detecting when files are removed by VACUUM. When a compacted file is detected, pg_delta will re-process it to ensure data consistency.

### Idempotent Loading

For idempotent loading with compaction, use `upsert` mode with `key_columns`:

```sql
-- Create table with primary key
CREATE TABLE ml_features (
    user_id BIGINT PRIMARY KEY,
    embedding FLOAT8[],
    feature_1 FLOAT8
);

-- Stream with upsert mode
SELECT delta.stream_create(
    'ml_features_ingest',
    's3://ml-lakehouse/features',
    'public.ml_features',
    'ingest',
    mode => 'upsert',
    key_columns => ARRAY['user_id']  -- Required for upsert
);
```

### Mode Selection Guide

| Mode | Use Case | Compaction Safe? |
|------|----------|------------------|
| `append` | Append-only tables with no compaction | No - may duplicate |
| `upsert` | Tables with primary keys (recommended) | Yes - deduplicates |
| `replace` | Full refresh each time | Yes - replaces all |

**For production use with Delta Lake compaction, always use `upsert` mode with `key_columns`.**

## Metadata Tables

### delta.streams

Stream configuration:

```sql
SELECT name, direction, uri, pg_table, mode, status
FROM delta.streams;
```

### delta.stream_progress

Sync progress:

```sql
SELECT s.name, p.delta_version, p.rows_synced, p.last_sync_at
FROM delta.stream_progress p
JOIN delta.streams s ON s.id = p.stream_id;
```

### delta.stream_errors

Error log:

```sql
SELECT s.name, e.error_type, e.error_message, e.occurred_at
FROM delta.stream_errors e
JOIN delta.streams s ON s.id = e.stream_id
ORDER BY e.occurred_at DESC
LIMIT 10;
```

## Examples

### ML Feature Pipeline

```sql
-- Data scientists produce features in Delta Lake
-- Stream them into Postgres for real-time serving

CREATE TABLE ml_features (
    user_id BIGINT PRIMARY KEY,
    embedding FLOAT8[],
    feature_1 FLOAT8,
    updated_at TIMESTAMPTZ
);

SELECT delta.stream_create(
    'ml_features_ingest',
    's3://ml-lakehouse/features/user_embeddings',
    'public.ml_features',
    'ingest',
    mode => 'upsert',
    key_columns => ARRAY['user_id'],
    poll_interval_ms => 60000  -- Check every minute
);
```

### OLTP to Analytics Export

```sql
-- Export order data to Delta Lake for analytics

SELECT delta.stream_create(
    'orders_to_lakehouse',
    's3://analytics-lake/orders',
    'public.orders',
    'export',
    mode => 'incremental',
    tracking_column => 'updated_at',
    batch_size => 50000
);
```

### One-time Data Load

```sql
-- Load historical data from Delta Lake
SELECT delta.read(
    's3://archive/historical_orders',
    'public.historical_orders',
    mode => 'replace'
);

-- Export to Delta Lake
SELECT delta.write(
    's3://backup/customers',
    pg_table => 'public.customers',
    mode => 'overwrite'
);
```
