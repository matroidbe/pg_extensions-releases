# pg_augur

Pure-Rust ML for PostgreSQL, powered by [Augur](../../augur).

A sibling of `pg_ml` that exposes the same SQL surface under the
`pgaugur` schema — no Python, no PyCaret, no venv.

See the [design doc](../../design/pg_augur/README.md) for the full
architecture, SQL API matrix, and phased delivery plan.

## Quick start

```bash
# Build prerequisites (LightGBM)
sudo apt install -y cmake build-essential

# Install the extension
cargo pgrx install --package pg_augur

# Preload in postgresql.conf
shared_preload_libraries = 'pg_augur'
pg_augur.training_database = 'mydb'

# In the database
CREATE EXTENSION pg_augur;

-- Sync training
SELECT pgaugur.setup('public.my_table', 'target_col');
SELECT model_id FROM pgaugur.create_model('my_table_proj', 'xgboost');
SELECT pgaugur.predict('my_table_proj', ARRAY[1.0, 2.0, 3.0]);

-- Or async
SELECT pgaugur.start_training(
    project_name => 'my_proj',
    source_table => 'public.my_table',
    target_column => 'target_col',
    algorithm => 'xgboost'
);
SELECT * FROM pgaugur.training_status(<job_id>);
```

## Run tests

```bash
# Unit + pg_test (in-transaction)
cargo test -p pg_augur --features pg16

# End-to-end with a real PostgreSQL (bg worker)
./extensions/pg_augur/test.sh
```

## Status

- Phase 1 (sync train + predict): ✅ shipped
- Phase 2 (async bg worker): ✅ shipped
- Phase 3 (AutoML breadth): ⏳ planned
- Phase 4 (time series): ⏳ planned

See the design doc for details.
