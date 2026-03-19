# Configuration Reference

Complete reference for all pg_extensions GUC (Grand Unified Configuration) parameters.

## How to Set Configuration

### postgresql.conf (persistent)

```ini
# Server startup required for shared_preload_libraries changes
shared_preload_libraries = 'pg_kafka,pg_delta,pg_ml'

# Runtime changeable (sighup context)
pg_kafka.port = 9092
pg_kafka.enabled = true
```

### SET command (session)

```sql
SET pg_kafka.port = 9092;
```

### ALTER SYSTEM (persistent without editing files)

```sql
ALTER SYSTEM SET pg_kafka.port = 9092;
SELECT pg_reload_conf();
```

## GUC Context Reference

| Context | When Changes Take Effect |
|---------|--------------------------|
| `postmaster` | PostgreSQL restart required |
| `sighup` | After `pg_reload_conf()` or `pg_ctl reload` |
| `suset` | Superuser can change; takes effect immediately |
| `userset` | Any user can change; takes effect immediately |

---

## pg_kafka Configuration

Kafka protocol server backed by PostgreSQL tables.

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_kafka.port` | integer | 9092 | sighup | TCP port for Kafka protocol server (range: 1-65535) |
| `pg_kafka.host` | string | 0.0.0.0 | sighup | IP address or hostname that pg_kafka binds to |
| `pg_kafka.advertised_host` | string | (same as host) | sighup | Hostname/IP that clients use to connect. Defaults to pg_kafka.host if not set |
| `pg_kafka.enabled` | boolean | true | sighup | Enable the Kafka protocol server |
| `pg_kafka.worker_count` | integer | 4 | sighup | Number of background workers handling Kafka connections (range: 1-32). Each handler binds to the same port with SO_REUSEPORT |
| `pg_kafka.database` | string | postgres | postmaster | Database where pg_kafka extension is installed and topics are stored |
| `pg_kafka.metrics_port` | integer | 9187 | sighup | Port for Prometheus metrics endpoint (range: 1024-65535). Only worker 0 starts the endpoint |
| `pg_kafka.metrics_enabled` | boolean | false | sighup | Enable Prometheus metrics endpoint |

### Example Configuration

```ini
# postgresql.conf
shared_preload_libraries = 'pg_kafka'

pg_kafka.port = 9092
pg_kafka.host = '0.0.0.0'
pg_kafka.enabled = true
pg_kafka.worker_count = 4
pg_kafka.database = 'mydb'
pg_kafka.metrics_enabled = true
pg_kafka.metrics_port = 9187
```

---

## pg_delta Configuration

Delta Lake streaming integration for PostgreSQL.

### Worker Settings

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `delta.enabled` | boolean | true | sighup | Enable the Delta Lake stream manager |
| `delta.worker_log_level` | string | (none) | sighup | Log level for the stream manager worker. One of: debug, info, warn, error |

### Default Stream Settings

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `delta.poll_interval_ms` | integer | 10000 | sighup | Default poll interval for ingest streams in milliseconds (range: 100-600000) |
| `delta.batch_size` | integer | 10000 | sighup | Default batch size for export streams - number of rows to buffer before writing (range: 1-1000000) |
| `delta.flush_interval_ms` | integer | 60000 | sighup | Default flush interval for export streams in milliseconds (range: 1000-3600000) |

### AWS S3 Configuration

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `delta.aws_region` | string | (none) | suset | AWS region for S3 access (e.g., us-east-1, eu-west-1) |
| `delta.aws_access_key_id` | string | (none) | suset | AWS access key ID for static credentials |
| `delta.aws_secret_access_key` | string | (none) | suset | AWS secret access key for static credentials |
| `delta.aws_session_token` | string | (none) | suset | AWS session token for temporary credentials |
| `delta.aws_endpoint` | string | (none) | suset | Custom S3 endpoint URL for S3-compatible storage like MinIO |
| `delta.aws_use_instance_profile` | boolean | false | suset | Use EC2 instance profile for AWS credentials (recommended for production on EC2/ECS) |
| `delta.aws_allow_http` | boolean | false | suset | Allow HTTP (non-HTTPS) connections to S3 (required for local S3-compatible storage) |

### Azure Blob Storage Configuration

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `delta.azure_storage_account` | string | (none) | suset | Azure storage account name |
| `delta.azure_storage_key` | string | (none) | suset | Azure storage account access key |
| `delta.azure_sas_token` | string | (none) | suset | Azure Shared Access Signature token |
| `delta.azure_use_managed_identity` | boolean | false | suset | Use Azure Managed Identity for credentials (recommended for production on Azure VMs) |

### Google Cloud Storage Configuration

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `delta.gcs_service_account_path` | string | (none) | suset | Path to GCS service account JSON file |
| `delta.gcs_service_account_key` | string | (none) | suset | Full GCS service account JSON key content (inline) |
| `delta.gcs_use_default_credentials` | boolean | false | suset | Use GCS Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS or GCE metadata) |

### Example Configuration

```ini
# postgresql.conf
shared_preload_libraries = 'pg_delta'

delta.enabled = true
delta.poll_interval_ms = 5000
delta.batch_size = 10000

# AWS S3 (choose one auth method)
delta.aws_region = 'us-east-1'
delta.aws_use_instance_profile = true  # Recommended for EC2

# Or static credentials
# delta.aws_access_key_id = 'AKIA...'
# delta.aws_secret_access_key = 'secret'
```

---

## pg_ml Configuration

Machine learning with PyCaret integration.

### Python Environment Settings

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_ml.venv_path` | string | /var/lib/postgresql/pg_ml | suset | Directory where pg_ml stores Python virtual environment |
| `pg_ml.uv_path` | string | (auto-detected) | suset | Override the bundled uv binary location |
| `pg_ml.auto_setup` | boolean | true | suset | Automatically create venv when first ML function is called |

### Training Worker Settings

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_ml.training_worker_enabled` | boolean | true | postmaster | Enable async training background worker |
| `pg_ml.training_poll_interval` | integer | 1000 | postmaster | Job polling interval in milliseconds (range: 100-60000) |
| `pg_ml.training_database` | string | postgres | postmaster | Database name the training worker uses for SPI connections |

### MLflow Settings

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_ml.mlflow_enabled` | boolean | false | postmaster | Enable MLflow UI server (starts background worker) |
| `pg_ml.mlflow_port` | integer | 5000 | postmaster | HTTP port for MLflow UI (range: 1024-65535) |
| `pg_ml.mlflow_host` | string | 127.0.0.1 | postmaster | IP address or hostname that MLflow binds to |
| `pg_ml.mlflow_tracking` | boolean | false | userset | Enable MLflow tracking for training operations (session-level) |

### Example Configuration

```ini
# postgresql.conf
shared_preload_libraries = 'pg_ml'

pg_ml.venv_path = '/var/lib/postgresql/pg_ml'
pg_ml.auto_setup = true
pg_ml.training_worker_enabled = true
pg_ml.training_poll_interval = 1000

# Optional: Enable MLflow UI
pg_ml.mlflow_enabled = true
pg_ml.mlflow_port = 5000
pg_ml.mlflow_host = '0.0.0.0'
```

---

## pg_mqtt Configuration

MQTT 5.0 broker backed by PostgreSQL tables.

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_mqtt.enabled` | boolean | true | sighup | Enable the MQTT broker |
| `pg_mqtt.port` | integer | 1883 | sighup | TCP port for MQTT connections (range: 1-65535) |
| `pg_mqtt.worker_count` | integer | 4 | postmaster | Number of background workers handling MQTT connections (range: 1-32) |

### Example Configuration

```ini
# postgresql.conf
shared_preload_libraries = 'pg_mqtt'

pg_mqtt.enabled = true
pg_mqtt.port = 1883
pg_mqtt.worker_count = 4
```

---

## pg_feature Configuration

Automated feature engineering using Deep Feature Synthesis.

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_feature.default_depth` | integer | 2 | userset | Default max_depth for Deep Feature Synthesis - controls how many primitives can be stacked (range: 1-10) |
| `pg_feature.default_agg_primitives` | string | sum,mean,count,max,min | userset | Default aggregation primitives (comma-separated) |
| `pg_feature.default_trans_primitives` | string | year,month,weekday | userset | Default transform primitives (comma-separated) |
| `pg_feature.max_features` | integer | 500 | userset | Maximum number of features to generate - prevents feature explosion (range: 10-10000) |

### Example Configuration

```sql
-- Session-level configuration
SET pg_feature.default_depth = 3;
SET pg_feature.default_agg_primitives = 'sum,mean,count,max,min,std';
SET pg_feature.max_features = 1000;
```

---

## pg_transform Configuration

Python-based data transformations using sentence-transformers.

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_transform.venv_path` | string | /var/lib/postgresql/pg_transform | suset | Directory where pg_transform stores Python virtual environment |
| `pg_transform.uv_path` | string | (auto-detected) | suset | Override the bundled uv binary location |
| `pg_transform.auto_setup` | boolean | true | suset | Automatically create venv when first function is called |

### Example Configuration

```ini
# postgresql.conf
pg_transform.venv_path = '/var/lib/postgresql/pg_transform'
pg_transform.auto_setup = true
```

---

## pg_ortools Configuration

Constraint optimization using Google OR-Tools CP-SAT solver.

| Parameter | Type | Default | Context | Description |
|-----------|------|---------|---------|-------------|
| `pg_ortools.venv_path` | string | /var/lib/postgresql/pg_ortools | suset | Directory where the Python venv with ortools will be created |
| `pg_ortools.uv_path` | string | (auto-detected) | suset | Override the default uv binary location |
| `pg_ortools.auto_setup` | boolean | true | suset | Automatically set up Python environment on first use |

### Example Configuration

```ini
# postgresql.conf
pg_ortools.venv_path = '/var/lib/postgresql/pg_ortools'
pg_ortools.auto_setup = true
```

---

## Quick Reference: All GUCs by Extension

| Extension | GUC Count | Key GUCs |
|-----------|-----------|----------|
| pg_kafka | 8 | port, host, enabled, worker_count, database, metrics_* |
| pg_delta | 18 | enabled, poll_interval_ms, aws_*, azure_*, gcs_* |
| pg_ml | 10 | venv_path, training_*, mlflow_* |
| pg_mqtt | 3 | enabled, port, worker_count |
| pg_feature | 4 | default_depth, *_primitives, max_features |
| pg_transform | 3 | venv_path, uv_path, auto_setup |
| pg_ortools | 3 | venv_path, uv_path, auto_setup |

**Total: 49 GUCs across 7 extensions**
