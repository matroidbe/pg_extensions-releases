# pg_delta User Manual

Delta Lake streaming integration for PostgreSQL.

## Overview

pg_delta enables bidirectional data streaming between PostgreSQL and Delta Lake tables stored in cloud object storage (S3, Azure Blob, GCS). Use it to sync data between your operational database and data lakehouse.

### Key Features

- **Ingest**: Stream Delta Lake changes into PostgreSQL tables
- **Export**: Stream PostgreSQL changes to Delta Lake
- **Time Travel**: Query specific Delta Lake versions
- **Multi-Cloud**: AWS S3, Azure Blob Storage, Google Cloud Storage

### Use Cases

**Serve ML features to applications:**
```
Delta Lake (features) --> pg_delta --> PostgreSQL --> Application
```

**Archive operational data:**
```
PostgreSQL (transactions) --> pg_delta --> Delta Lake --> Analytics
```

**Sync OLTP and lakehouse:**
```
PostgreSQL <-- pg_delta --> Delta Lake
```

### Architecture

```
+-----------------------------------------------------------------+
|                      PostgreSQL                                   |
|  +--------------+    +------------------+    +--------------+    |
|  | Source       |--->|  pg_delta BGW    |--->| Target       |    |
|  | Tables       |    |                  |    | Tables       |    |
|  +--------------+    |  - Ingest stream |    +--------------+    |
|                      |  - Export stream |                        |
|                      |  - Progress track|                        |
|                      +------------------+                        |
+-----------------------------------------------------------------+
           |                                        ^
           v                                        |
    +-----------------------------------------------------+
    |                   Delta Lake                         |
    |   s3://bucket/table/                                 |
    |   +-- _delta_log/          (transaction log)        |
    |   +-- *.parquet            (data files)             |
    +-----------------------------------------------------+
```

pg_delta runs as a PostgreSQL background worker that manages stream synchronization. It uses the delta-rs Rust library for Delta Lake operations.

## Concepts

### Streams

A stream is a continuous synchronization between a PostgreSQL table and a Delta Lake table. Streams have:

- **Direction**: `ingest` (Delta -> Postgres) or `export` (Postgres -> Delta)
- **Mode**: How to handle updates (`append`, `upsert`, `replace`, `incremental`, `snapshot`)
- **URI**: Cloud storage path to the Delta table
- **Status**: `running`, `stopped`, `error`

Create a stream:

```sql
SELECT delta.stream_create(
    'sales_sync',                    -- name
    's3://bucket/sales',             -- Delta table URI
    'public.sales',                  -- PostgreSQL table
    'ingest',                        -- direction
    mode => 'upsert',
    key_columns => ARRAY['id']
);
```

### Sync Modes

#### Ingest Modes (Delta to PostgreSQL)

| Mode | Description | Use When |
|------|-------------|----------|
| `append` | INSERT new rows | Immutable data (logs, events) |
| `upsert` | INSERT ON CONFLICT UPDATE | Mutable data with known keys |
| `replace` | TRUNCATE and reload | Full refresh needed |

#### Export Modes (PostgreSQL to Delta)

| Mode | Description | Use When |
|------|-------------|----------|
| `incremental` | Poll `tracking_column` for changes | Tables with updated_at timestamp |
| `snapshot` | Full table dump | Small tables, periodic refresh |
| `cdc` | Logical replication | Real-time sync (not yet implemented) |

### Delta Lake Concepts

**Versions**: Each Delta Lake write creates a new version. pg_delta tracks which version has been processed.

**Time Travel**: Query historical data by version number.

**Transaction Log**: `_delta_log/` directory contains commit history as JSON files.

## Getting Started

### 1. Install the Extension

```sql
CREATE EXTENSION pg_delta CASCADE;
```

### 2. Configure Cloud Storage

See [Cloud Storage Setup](#cloud-storage-setup) for your provider.

```sql
-- Example: AWS S3 with access keys
SET delta.aws_region = 'us-east-1';
SET delta.aws_access_key_id = 'AKIA...';
SET delta.aws_secret_access_key = 'your-secret-key';
```

### 3. Test Connectivity

```sql
SELECT delta.test_storage('s3://your-bucket/test-path');
-- Returns: {"can_list": true, "can_read": true, ...}
```

### 4. Create Target Table (Ingest)

```sql
CREATE TABLE ml_features (
    user_id BIGINT PRIMARY KEY,
    embedding FLOAT8[],
    feature_1 FLOAT8,
    updated_at TIMESTAMPTZ
);
```

### 5. Create and Start Stream

```sql
SELECT delta.stream_create(
    'features_sync',
    's3://ml-lakehouse/features/user_embeddings',
    'public.ml_features',
    'ingest',
    mode => 'upsert',
    key_columns => ARRAY['user_id'],
    poll_interval_ms => 60000  -- Check every minute
);

-- Stream starts automatically if enabled => true (default)
```

### 6. Monitor Progress

```sql
SELECT s.name, p.delta_version, p.rows_synced, p.last_sync_at
FROM delta.stream_progress p
JOIN delta.streams s ON s.id = p.stream_id;
```

## Cloud Storage Setup

### AWS S3

#### Prerequisites

1. S3 bucket containing Delta Lake tables
2. IAM credentials with appropriate permissions

#### IAM Policy

Minimum required permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket",
        "arn:aws:s3:::your-bucket/*"
      ]
    }
  ]
}
```

#### Configuration Options

**Option 1: Access Keys (development)**

```sql
SET delta.aws_region = 'us-east-1';
SET delta.aws_access_key_id = 'AKIAIOSFODNN7EXAMPLE';
SET delta.aws_secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY';
```

**Option 2: Instance Profile (production on EC2/ECS)**

```sql
SET delta.aws_use_instance_profile = true;
SET delta.aws_region = 'us-east-1';
```

The IAM role attached to your EC2 instance or ECS task must have the required S3 permissions.

**Option 3: Assume Role**

```sql
SET delta.aws_region = 'us-east-1';
SET delta.aws_role_arn = 'arn:aws:iam::123456789012:role/DeltaLakeAccess';
-- Uses instance profile or environment credentials for initial auth
```

**Option 4: S3-Compatible Storage (MinIO, LocalStack)**

```sql
SET delta.aws_endpoint = 'http://localhost:9000';
SET delta.aws_access_key_id = 'minioadmin';
SET delta.aws_secret_access_key = 'minioadmin';
SET delta.aws_allow_http = true;  -- Required for non-HTTPS endpoints
SET delta.aws_s3_force_path_style = true;  -- Required for MinIO
```

#### Per-Stream Credentials

Override global credentials for specific streams:

```sql
SELECT delta.stream_create(
    'cross_account_sync',
    's3://other-account-bucket/data',
    'public.data',
    'ingest',
    storage_options => '{
        "aws_access_key_id": "AKIA...",
        "aws_secret_access_key": "...",
        "aws_region": "eu-west-1"
    }'::jsonb
);
```

### Azure Blob Storage

#### Prerequisites

1. Azure Storage Account
2. Container with Delta Lake tables
3. Access credentials (key, SAS token, or managed identity)

#### Configuration Options

**Option 1: Storage Account Key**

```sql
SET delta.azure_storage_account = 'mystorageaccount';
SET delta.azure_storage_key = 'your-storage-key';
```

**Option 2: SAS Token**

```sql
SET delta.azure_storage_account = 'mystorageaccount';
SET delta.azure_sas_token = 'sv=2021-06-08&ss=bfqt&srt=sco...';
```

**Option 3: Managed Identity (Azure VMs, AKS)**

```sql
SET delta.azure_storage_account = 'mystorageaccount';
SET delta.azure_use_managed_identity = true;
```

#### URI Format

```sql
-- ABFS format (recommended for Azure Databricks compatibility)
's3://mystorageaccount.blob.core.windows.net/container/path'

-- az:// format
'az://container/path'
```

### Google Cloud Storage

#### Prerequisites

1. GCS bucket with Delta Lake tables
2. Service account with Storage Object permissions

#### Configuration Options

**Option 1: Service Account JSON File**

```sql
SET delta.gcs_service_account_path = '/path/to/service-account.json';
```

**Option 2: Application Default Credentials (GCE, GKE)**

```sql
SET delta.gcs_use_default_credentials = true;
```

This uses the credentials from `GOOGLE_APPLICATION_CREDENTIALS` environment variable or the GCE metadata service.

#### URI Format

```
'gs://your-bucket/path/to/table'
```

#### Required Roles

The service account needs:
- `roles/storage.objectViewer` (read-only)
- `roles/storage.objectCreator` (write)

Or for full access:
- `roles/storage.objectAdmin`

## Streaming Patterns

### ML Feature Serving

Stream features from data science lakehouse to application database:

```sql
-- Create target table matching Delta schema
CREATE TABLE ml_features (
    user_id BIGINT PRIMARY KEY,
    embedding FLOAT8[128],
    churn_score FLOAT8,
    ltv_prediction FLOAT8,
    updated_at TIMESTAMPTZ
);

-- Create ingest stream with upsert
SELECT delta.stream_create(
    'ml_features_ingest',
    's3://ml-lakehouse/features/user_features',
    'public.ml_features',
    'ingest',
    mode => 'upsert',
    key_columns => ARRAY['user_id'],
    poll_interval_ms => 60000  -- Refresh every minute
);
```

### OLTP to Analytics

Export operational data for analytics workloads:

```sql
-- Export orders incrementally based on updated_at
SELECT delta.stream_create(
    'orders_to_lakehouse',
    's3://analytics-lake/orders',
    'public.orders',
    'export',
    mode => 'incremental',
    tracking_column => 'updated_at',
    batch_size => 50000,
    flush_interval_ms => 300000  -- Flush every 5 minutes
);
```

### One-Time Data Loads

For bulk operations without continuous streaming:

```sql
-- Load Delta table into PostgreSQL
SELECT delta.read(
    's3://archive/historical_orders',
    'public.historical_orders'
);

-- Export PostgreSQL table to Delta
SELECT delta.write(
    's3://backup/customers',
    pg_table => 'public.customers'
);

-- Export query result to Delta
SELECT delta.write(
    's3://reports/monthly_summary',
    query => 'SELECT date_trunc(''month'', order_date) as month,
                     SUM(amount) as total
              FROM orders
              GROUP BY 1'
);
```

### Time Travel Queries

Access historical versions:

```sql
-- Read specific version
SELECT delta.read(
    's3://bucket/table',
    'public.staging',
    version => 42
);

-- View table history
SELECT delta.history('s3://bucket/table', limit => 10);
```

## Error Handling and Recovery

### Stream Errors

View recent errors:

```sql
SELECT s.name, e.error_type, e.error_message, e.occurred_at
FROM delta.stream_errors e
JOIN delta.streams s ON s.id = e.stream_id
ORDER BY e.occurred_at DESC
LIMIT 10;
```

Common error types:
- `storage_error`: Cloud provider connectivity issues
- `schema_mismatch`: Delta schema doesn't match PostgreSQL table
- `permission_denied`: Insufficient cloud credentials
- `version_not_found`: Requested Delta version doesn't exist

### Automatic Retry

Streams automatically retry transient errors with exponential backoff:
- First retry: 1 second
- Second retry: 2 seconds
- Third retry: 4 seconds
- Maximum: 5 retries, then stream stops

### Manual Recovery

```sql
-- Stop a failing stream
SELECT delta.stream_stop('my_stream');

-- Fix the issue (credentials, schema, etc.)

-- Reset progress to resync
SELECT delta.stream_reset('my_stream', version => 0);

-- Restart
SELECT delta.stream_start('my_stream');
```

### Schema Evolution

When Delta schema changes:

1. Additive changes (new columns) work automatically if PostgreSQL column exists with matching type
2. Type changes require manual intervention
3. Column removals are ignored (NULL values in PostgreSQL)

**Handling schema changes:**

```sql
-- Stop stream
SELECT delta.stream_stop('my_stream');

-- Update PostgreSQL table
ALTER TABLE public.my_table ADD COLUMN new_column TEXT;

-- Restart stream
SELECT delta.stream_start('my_stream');
```

## Best Practices

### Stream Design

1. **Match schemas carefully**: Ensure PostgreSQL types can hold Delta values.

2. **Use upsert for mutable data**: Prevents duplicate rows on retries.

3. **Index key columns**: Required for upsert performance.

   ```sql
   CREATE UNIQUE INDEX ON my_table (id);
   ```

4. **Use incremental export**: Avoids full table scans on each batch.

### Performance Tuning

1. **Tune batch_size**: Larger batches = fewer transactions, more memory.

   ```sql
   -- For large tables, increase batch size
   SELECT delta.stream_create(
       ...,
       batch_size => 100000
   );
   ```

2. **Tune poll_interval**: Balance freshness vs. load.

3. **Use partitioned PostgreSQL tables**: For very large datasets.

4. **Monitor stream progress**:

   ```sql
   -- Check stream lag
   SELECT s.name,
          i.version as delta_version,
          p.delta_version as synced_version,
          i.version - p.delta_version as lag
   FROM delta.streams s
   JOIN delta.stream_progress p ON p.stream_id = s.id
   CROSS JOIN LATERAL (
       SELECT (delta.info(s.uri)->>'version')::bigint as version
   ) i;
   ```

### Security

1. **Use instance profiles/managed identity in production**: Avoid hardcoding credentials.

2. **Use separate service accounts**: One per stream for audit trails.

3. **Encrypt in transit**: HTTPS is default for all cloud providers.

4. **Restrict bucket access**: Limit IAM policies to required paths.

### Monitoring

Check stream health:

```sql
SELECT s.name, s.direction, s.status,
       p.rows_synced, p.last_sync_at,
       EXTRACT(EPOCH FROM (NOW() - p.last_sync_at)) as seconds_since_sync
FROM delta.streams s
LEFT JOIN delta.stream_progress p ON p.stream_id = s.id;
```

Set up alerts for:
- Streams in `error` status
- Large `seconds_since_sync` values
- Growing error counts

## Configuration Reference

See [configuration.md](configuration.md#pg_delta) for the complete list of GUC parameters.

Key settings:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `delta.enabled` | `true` | Enable stream manager |
| `delta.poll_interval_ms` | `10000` | Default poll interval |
| `delta.batch_size` | `10000` | Default batch size |
| `delta.aws_region` | (empty) | AWS region for S3 |
| `delta.aws_use_instance_profile` | `false` | Use EC2 instance profile |
| `delta.azure_storage_account` | (empty) | Azure storage account |
| `delta.gcs_use_default_credentials` | `false` | Use GCP default credentials |

## Troubleshooting

See [troubleshooting.md](troubleshooting.md#pg_delta) for common issues and solutions.

Quick diagnostics:

```sql
-- Check extension status
SELECT delta.status();

-- Test storage connectivity
SELECT delta.test_storage('s3://your-bucket/path');

-- View Delta table info
SELECT delta.info('s3://your-bucket/table');

-- Check stream status
SELECT name, status, direction FROM delta.streams;

-- View recent errors
SELECT * FROM delta.stream_errors ORDER BY occurred_at DESC LIMIT 5;
```
