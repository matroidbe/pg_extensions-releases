# Troubleshooting Guide

Common issues and solutions for pg_extensions.

## Quick Diagnostics

```sql
-- Check extension status
SELECT pgkafka.status();
SELECT delta.status();
SELECT pgml.status();
SELECT pgmcp.status();
SELECT pgmqtt.mqtt_status();
SELECT pgforms.status();

-- Check if background workers are running
SELECT pid, application_name, state, backend_type
FROM pg_stat_activity
WHERE backend_type = 'background worker';

-- Check PostgreSQL logs
-- Location varies by installation:
-- Debian/Ubuntu: /var/log/postgresql/
-- RHEL/CentOS: /var/lib/pgsql/data/log/
-- macOS Homebrew: /usr/local/var/log/postgresql@16.log
```

---

## Installation Issues

### Extension file not found

**Symptom:**
```
ERROR: could not access file "$libdir/pg_kafka": No such file or directory
```

**Cause:** Extension shared library not installed or wrong PostgreSQL version.

**Solution:**
1. Verify the extension file exists:
   ```bash
   ls $(pg_config --pkglibdir)/pg_kafka*
   ```
2. If missing, reinstall the extension:
   ```bash
   cd extensions/pg_kafka
   cargo pgrx install --release
   ```
3. Verify PostgreSQL version matches pgrx target:
   ```bash
   pg_config --version
   cargo pgrx info
   ```

---

### shared_preload_libraries not taking effect

**Symptom:** Extension background workers not starting even though `shared_preload_libraries` is set.

**Cause:** PostgreSQL must be fully restarted (not just reloaded) for shared_preload_libraries changes.

**Solution:**
1. Verify configuration:
   ```bash
   grep shared_preload_libraries $(pg_config --sysconfdir)/postgresql.conf
   ```
2. Restart PostgreSQL (not just reload):
   ```bash
   # systemd
   sudo systemctl restart postgresql

   # pg_ctl
   pg_ctl restart -D /path/to/data
   ```
3. Verify library loaded:
   ```sql
   SHOW shared_preload_libraries;
   ```

---

### shared_preload_libraries syntax error

**Symptom:**
```
FATAL: could not load library "pg_kafka,pg_delta": cannot open shared object file
```

**Cause:** Missing quotes or incorrect syntax in postgresql.conf.

**Solution:**

Correct syntax:
```ini
# Correct - comma-separated, single-quoted
shared_preload_libraries = 'pg_kafka,pg_delta,pg_ml'

# Wrong - spaces cause issues
shared_preload_libraries = 'pg_kafka, pg_delta, pg_ml'

# Wrong - separate entries not supported
shared_preload_libraries = 'pg_kafka'
shared_preload_libraries = 'pg_delta'
```

---

### Missing system dependencies

**Symptom:** Compilation errors during `cargo pgrx install`:
```
error: failed to run custom build command for `rdkafka-sys`
--- stderr
cmake: not found
```

**Cause:** Missing build dependencies.

**Solution:**

Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y cmake libssl-dev pkg-config libclang-dev
```

RHEL/CentOS:
```bash
sudo yum install -y cmake openssl-devel pkgconfig llvm-devel clang
```

macOS:
```bash
brew install cmake openssl pkg-config llvm
```

---

### PostgreSQL version mismatch

**Symptom:**
```
error: PostgreSQL version 16 is not supported
```
or
```
FATAL: incompatible library: missing magic block
```

**Cause:** Extension compiled for different PostgreSQL version than running.

**Solution:**
1. Check running PostgreSQL version:
   ```sql
   SELECT version();
   ```
2. Set pgrx to match:
   ```bash
   cargo pgrx init --pg16 $(which pg_config)
   ```
3. Rebuild extension:
   ```bash
   cargo pgrx install --release --pg16
   ```

---

## pg_kafka Issues

### Port already in use

**Symptom:**
```
pg_kafka worker 0: server error: Address already in use (os error 98)
```

**Cause:** Port 9092 (or configured port) is already bound by another process.

**Solution:**
1. Find what's using the port:
   ```bash
   sudo lsof -i :9092
   # or
   sudo netstat -tlnp | grep 9092
   ```
2. Either stop the conflicting service or change pg_kafka port:
   ```sql
   ALTER SYSTEM SET pg_kafka.port = 9093;
   SELECT pg_reload_conf();
   ```

---

### Client connection refused

**Symptom:** Kafka client cannot connect:
```
Connection refused (os error 111)
```

**Cause:** pg_kafka not running or bound to wrong interface.

**Solution:**
1. Verify pg_kafka is enabled:
   ```sql
   SHOW pg_kafka.enabled;
   -- Should be 'true'
   ```
2. Check background workers running:
   ```sql
   SELECT * FROM pg_stat_activity WHERE application_name LIKE 'pg_kafka%';
   ```
3. Verify host binding:
   ```sql
   SHOW pg_kafka.host;
   -- For external access, set to '0.0.0.0'
   ```
4. Check PostgreSQL logs for startup errors.

---

### Metadata request timeout

**Symptom:** Kafka client hangs on metadata request or times out.

**Cause:** Client connected but pg_kafka worker not responding.

**Solution:**
1. Check worker count is sufficient:
   ```sql
   SHOW pg_kafka.worker_count;
   -- Increase if many concurrent clients
   ```
2. Check PostgreSQL connection limit:
   ```sql
   SHOW max_connections;
   -- Workers use one connection each
   ```
3. Look for errors in PostgreSQL logs.

---

### Consumer not receiving messages

**Symptom:** Consumer subscribes successfully but receives no messages.

**Cause:** Various - offset management, topic filter, or message format.

**Solution:**
1. Verify messages exist in topic:
   ```sql
   SELECT COUNT(*) FROM pgkafka.messages WHERE topic = 'your-topic';
   ```
2. Check consumer offset:
   ```sql
   SELECT * FROM pgkafka.consumer_offsets WHERE group_id = 'your-group';
   ```
3. Reset consumer offset if needed:
   ```sql
   DELETE FROM pgkafka.consumer_offsets WHERE group_id = 'your-group';
   ```

---

### Topic not found errors

**Symptom:**
```
UNKNOWN_TOPIC_OR_PARTITION: This server does not host this topic-partition
```

**Cause:** Topic doesn't exist or wasn't created.

**Solution:**
1. Check if topic exists:
   ```sql
   SELECT * FROM pgkafka.topics WHERE name = 'your-topic';
   ```
2. Create topic if missing:
   ```sql
   INSERT INTO pgkafka.topics (name, partitions, replication_factor)
   VALUES ('your-topic', 1, 1);
   ```

---

### information_schema type casting errors

**Symptom:**
```
IncompatibleTypes error when querying information_schema
```

**Cause:** information_schema uses domain types that don't map directly to Rust types.

**Solution:**
Always cast information_schema columns to `::text`:
```sql
-- Wrong
SELECT table_schema, table_name FROM information_schema.tables;

-- Correct
SELECT table_schema::text, table_name::text FROM information_schema.tables;
```

This is a known pattern documented in the project CLAUDE.md.

---

## pg_delta Issues

### AWS credentials not configured

**Symptom:**
```
Error: AWS credentials not found
```
or
```
CredentialsError: No credentials in chain
```

**Cause:** AWS authentication not configured for pg_delta.

**Solution:**

Choose one authentication method:

1. **Instance profile (recommended for EC2):**
   ```sql
   ALTER SYSTEM SET delta.aws_use_instance_profile = true;
   SELECT pg_reload_conf();
   ```

2. **Static credentials:**
   ```sql
   ALTER SYSTEM SET delta.aws_region = 'us-east-1';
   ALTER SYSTEM SET delta.aws_access_key_id = 'AKIA...';
   ALTER SYSTEM SET delta.aws_secret_access_key = 'your-secret';
   SELECT pg_reload_conf();
   ```

---

### S3 access denied

**Symptom:**
```
AccessDenied: Access Denied
```

**Cause:** IAM permissions insufficient for S3 bucket/objects.

**Solution:**
1. Verify bucket name and region match:
   ```sql
   SHOW delta.aws_region;
   ```
2. Check IAM policy has required permissions:
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "s3:GetObject",
       "s3:PutObject",
       "s3:ListBucket",
       "s3:DeleteObject"
     ],
     "Resource": [
       "arn:aws:s3:::your-bucket",
       "arn:aws:s3:::your-bucket/*"
     ]
   }
   ```
3. Verify Delta table path is correct (should match bucket structure).

---

### Azure authentication errors

**Symptom:**
```
AuthenticationError: Invalid storage account credentials
```

**Cause:** Azure storage credentials not configured or invalid.

**Solution:**

Choose one authentication method:

1. **Managed identity (recommended for Azure VMs):**
   ```sql
   ALTER SYSTEM SET delta.azure_storage_account = 'mystorageaccount';
   ALTER SYSTEM SET delta.azure_use_managed_identity = true;
   SELECT pg_reload_conf();
   ```

2. **Storage key:**
   ```sql
   ALTER SYSTEM SET delta.azure_storage_account = 'mystorageaccount';
   ALTER SYSTEM SET delta.azure_storage_key = 'your-key';
   SELECT pg_reload_conf();
   ```

---

### GCS permission errors

**Symptom:**
```
PermissionDenied: Caller does not have storage.objects.get access
```

**Cause:** GCS credentials missing or insufficient permissions.

**Solution:**

1. **Service account file:**
   ```sql
   ALTER SYSTEM SET delta.gcs_service_account_path = '/path/to/sa.json';
   SELECT pg_reload_conf();
   ```

2. **Default credentials (GCE or GOOGLE_APPLICATION_CREDENTIALS):**
   ```sql
   ALTER SYSTEM SET delta.gcs_use_default_credentials = true;
   SELECT pg_reload_conf();
   ```

Ensure service account has Storage Object Viewer/Creator roles.

---

### Stream stuck in pending state

**Symptom:** Stream shows status "pending" and never processes data.

**Cause:** Stream manager worker not running or stream misconfigured.

**Solution:**
1. Check worker is running:
   ```sql
   SELECT * FROM pg_stat_activity
   WHERE application_name = 'pg_delta stream manager';
   ```
2. Verify stream is enabled:
   ```sql
   SELECT name, enabled, status, error_message FROM delta.streams;
   ```
3. Check for errors:
   ```sql
   SELECT * FROM delta.stream_errors WHERE stream_id = 1 ORDER BY occurred_at DESC LIMIT 5;
   ```

---

### Arrow type conversion errors

**Symptom:**
```
ArrowError: Cannot convert data type X to Arrow type Y
```

**Cause:** PostgreSQL column type not supported for Delta Lake conversion.

**Solution:**
1. Check column types in source table:
   ```sql
   \d your_table
   ```
2. Supported types: integer, bigint, float, double precision, text, boolean, timestamp, date, bytea, uuid
3. Unsupported: arrays, composites, ranges, geometric types
4. Cast unsupported types in the export query:
   ```sql
   SELECT col1, col2::text AS col2_text FROM your_table;
   ```

---

## pg_ml Issues

### Python venv setup failed

**Symptom:**
```
Error: Failed to create virtual environment
```

**Cause:** uv not available or filesystem permissions issue.

**Solution:**
1. Run setup manually:
   ```sql
   SELECT pgml.setup();
   ```
2. Check venv directory is writable:
   ```bash
   ls -la /var/lib/postgresql/pg_ml
   ```
3. Verify uv is installed:
   ```bash
   which uv
   # or
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

---

### uv not found

**Symptom:**
```
Error: uv not found. Install: curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Cause:** uv package manager not in PATH.

**Solution:**
1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
2. Or specify uv path:
   ```sql
   ALTER SYSTEM SET pg_ml.uv_path = '/home/postgres/.cargo/bin/uv';
   SELECT pg_reload_conf();
   ```

---

### PyCaret import errors

**Symptom:**
```
ModuleNotFoundError: No module named 'pycaret'
```

**Cause:** PyCaret not installed in the virtual environment.

**Solution:**
1. Force reinstall dependencies:
   ```sql
   SELECT pgml.setup(force := true);
   ```
2. Check venv manually:
   ```bash
   /var/lib/postgresql/pg_ml/bin/pip list | grep pycaret
   ```

---

### Training job stuck

**Symptom:** Training job stays in "queued" or "training" state indefinitely.

**Cause:** Training worker not running or Python error.

**Solution:**
1. Check training worker is running:
   ```sql
   SELECT * FROM pg_stat_activity
   WHERE application_name = 'pg_ml_training';
   ```
2. Check worker is enabled:
   ```sql
   SHOW pg_ml.training_worker_enabled;
   ```
3. Check job status:
   ```sql
   SELECT id, state, error_message FROM pgml.training_jobs ORDER BY id DESC LIMIT 5;
   ```
4. Check PostgreSQL logs for Python errors.

---

### Model not found

**Symptom:**
```
Error: Model 'my_model' not found
```

**Cause:** Model name incorrect or model not deployed.

**Solution:**
1. List available models:
   ```sql
   SELECT project_name, algorithm, deployed FROM pgml.models ORDER BY created_at DESC;
   ```
2. Deploy a model:
   ```sql
   SELECT pgml.deploy('my_project', 1);  -- Model ID
   ```

---

### Memory errors during training

**Symptom:**
```
MemoryError: Unable to allocate array
```
or PostgreSQL crashes during training.

**Cause:** Dataset too large for available memory.

**Solution:**
1. Reduce training data:
   ```sql
   SELECT pgml.train(
     'my_model',
     'SELECT * FROM large_table LIMIT 100000'
   );
   ```
2. Increase PostgreSQL memory limits:
   ```ini
   work_mem = '1GB'
   maintenance_work_mem = '2GB'
   ```
3. Use sampling in training:
   ```sql
   SELECT pgml.train(
     'my_model',
     'SELECT * FROM large_table TABLESAMPLE SYSTEM (10)'
   );
   ```

---

## General Debugging Tips

### Enable verbose logging

```ini
# postgresql.conf
log_min_messages = debug1
log_statement = 'all'
```

### Check background worker status

```sql
SELECT pid, application_name, state, wait_event, backend_type
FROM pg_stat_activity
WHERE backend_type = 'background worker';
```

### Monitor extension memory usage

```sql
SELECT pg_backend_pid(), pg_size_pretty(pg_backend_memory_contexts());
```

### Reload configuration without restart

```sql
SELECT pg_reload_conf();
```

### Find PostgreSQL log location

```sql
SHOW log_directory;
SHOW log_filename;
```

---

## Getting Help

If issues persist:

1. Check PostgreSQL logs for detailed error messages
2. Verify extension version matches PostgreSQL version
3. Search existing issues on GitHub
4. Open a new issue with:
   - PostgreSQL version (`SELECT version();`)
   - Extension version
   - Full error message
   - Steps to reproduce
