#!/usr/bin/env bash
# End-to-end test of pg_ml on a throwaway cluster.
#
# Unlike test.sh, this never touches ~/.pgrx/data-<N> or the shared pgrx tree:
# pg_ml is installed into a private hardlinked copy of the pgrx PostgreSQL tree
# (scripts/lib/pg-scratch.sh) and runs on a cluster created for this run.
#
# It covers what the pg_test suite cannot:
#   1. pgml.setup_venv() building the venv from nothing, from the embedded
#      requirements.lock.txt, and that the result is exactly the lock;
#   2. synchronous training and prediction on that venv;
#   3. async training through the background worker (needs committed rows,
#      so it cannot run inside a pg_test transaction);
#   4. a running job is visible and cancel_training() takes effect at once;
#   5. worker_status() reports the training worker.
#
# Usage: ./e2e.sh            (PG_MAJOR=18 by default)
# Needs: uv, cargo-pgrx, the build venv (.venv, created by `uv sync --locked`)
set -euo pipefail

EXT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXT_DIR/../.." && pwd)"
# shellcheck source=../../scripts/lib/pg-scratch.sh
source "$REPO_ROOT/scripts/lib/pg-scratch.sh"

PG_MAJOR="${PG_MAJOR:-18}"
WORK="$REPO_ROOT/target/pg-ml-e2e"
VENV="$WORK/venv"
DB=pg_ml_e2e

log() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

PG_CONFIG_SRC=$(resolve_pg_config "$PG_MAJOR")
[[ -n "$PG_CONFIG_SRC" ]] || fail "no pgrx PostgreSQL $PG_MAJOR (cargo pgrx init)"

trap cleanup_scratch EXIT
rm -rf "$WORK"
mkdir -p "$WORK"

# --- build: link against the build venv's libpython, like Makefile/test.sh ---
log "syncing the build venv (uv sync --locked)"
(cd "$EXT_DIR" && uv sync --locked -q)
BUILD_PY="$EXT_DIR/.venv/bin/python"
PY_LIBDIR=$("$BUILD_PY" -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")

make_scratch_tree "$PG_CONFIG_SRC" "$WORK/pg"
unlink_extension pg_ml
log "building and installing pg_ml into $WORK/pg"
(cd "$EXT_DIR" && env -u PG_CONFIG PYO3_PYTHON="$BUILD_PY" PG_ML_VENV_PATH="$VENV" \
    RUSTFLAGS="-C link-arg=-Wl,-rpath,$PY_LIBDIR" \
    cargo pgrx install --pg-config "$SCRATCH_CONFIG" >"$WORK/install.log" 2>&1) \
    || { tail -30 "$WORK/install.log" >&2; fail "install (log: $WORK/install.log)"; }

# --- cluster: worker preloaded, venv path pointing at nothing yet ------------
start_scratch_cluster "$WORK/data" "$WORK"
# Create the database before pg_ml is preloaded: a worker that cannot connect
# counts as a supervised failure
scratch_psql -d postgres -c "CREATE DATABASE $DB"
stop_scratch_cluster
cat >>"$SCRATCH_PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_ml'
max_worker_processes = 16
pg_ml.database = '$DB'
pg_ml.venv_path = '$VENV'
pg_ml.auto_setup = off
pg_ml.training_poll_interval = 500
EOF
start_scratch_cluster "$WORK/data" "$WORK"
export PGOPTIONS="-c client_min_messages=warning"
q() { scratch_psql -d "$DB" -At "$@"; }

q -c "CREATE EXTENSION pg_ml"
[[ ! -e "$VENV" ]] || fail "venv exists before setup_venv()"

# --- 1. setup_venv() from the embedded lock ----------------------------------
log "pgml.setup_venv() (installs ~300 packages)"
q -c "SELECT pgml.setup_venv()"
[[ -x "$VENV/bin/python" ]] || fail "setup_venv() created no python"
[[ ! -e "$VENV/requirements.lock.txt" ]] || fail "temp requirements file left in venv"

log "venv matches requirements.lock.txt"
check=$(uv pip install --dry-run --require-hashes --python "$VENV/bin/python" \
    -r "$EXT_DIR/requirements.lock.txt" 2>&1)
grep -q "Would make no changes" <<<"$check" || { echo "$check" >&2; fail "venv differs from lock"; }
uv pip check --python "$VENV/bin/python" >/dev/null || fail "uv pip check"
extras=$(comm -23 \
    <(uv pip freeze --python "$VENV/bin/python" | sed 's/==.*//' | tr '_' '-' | tr 'A-Z' 'a-z' | sort) \
    <(grep -oE '^[A-Za-z0-9._-]+==' "$EXT_DIR/requirements.lock.txt" | sed 's/==//' | tr '_' '-' | tr 'A-Z' 'a-z' | sort) \
    | grep -vx uv || true)
[[ -z "$extras" ]] || fail "packages outside the lock: $extras"

# --- 2. sync train + predict --------------------------------------------------
log "load_dataset + setup + create_model + predict"
q -c "SELECT pgml.load_dataset('iris')" >/dev/null
# One session: the PyCaret experiment from setup() lives in the backend
q -c "SELECT experiment_id FROM pgml.setup('pgml_samples.iris', 'species',
        project_name => 'e2e_sync', exclude_columns => ARRAY['id'])" \
  -c "SELECT model_id FROM pgml.create_model('e2e_sync', 'lr')" >/dev/null
pred=$(q -c "SELECT pgml.predict('e2e_sync', ARRAY[5.1, 3.5, 1.4, 0.2])")
[[ "$pred" == *setosa* ]] || fail "sync prediction for a setosa sample: '$pred'"
log "  predicted $pred"

# --- 3. async training via the background worker -----------------------------
log "start_training via background worker"
job=$(q -c "SELECT pgml.start_training(project_name => 'e2e_async',
        source_table => 'pgml_samples.iris', target_column => 'species',
        algorithm => 'lr', exclude_columns => ARRAY['id'])")
state=""
for _ in $(seq 1 180); do
    state=$(q -c "SELECT state FROM pgml.training_status($job)")
    case "$state" in completed|failed|cancelled) break ;; esac
    sleep 1
done
[[ "$state" == completed ]] || {
    q -c "SELECT * FROM pgml.training_status($job)" >&2
    fail "async job $job ended in '$state'"; }
pred=$(q -c "SELECT pgml.predict('e2e_async', ARRAY[6.7, 3.0, 5.2, 2.3])")
[[ "$pred" == *virginica* ]] || fail "async prediction for a virginica sample: '$pred'"
log "  predicted $pred"

# --- 4. a running job is visible and can be cancelled --------------------------
# The worker claims, trains and stores in separate transactions, so the job's
# state shows while it runs, and cancel_training() neither waits for the
# training to finish nor gets overwritten by it.
log "cancel a running AutoML job"
job=$(q -c "SELECT pgml.start_training(project_name => 'e2e_cancel',
        source_table => 'pgml_samples.iris', target_column => 'species',
        automl => true, budget_time => 60, exclude_columns => ARRAY['id'])")
seen=""
for _ in $(seq 1 240); do
    state=$(q -c "SELECT state FROM pgml.training_status($job)")
    case "$state" in
        setup|training) seen=$state; break ;;
        completed|failed|cancelled) fail "job $job ended ($state) before it was seen running" ;;
    esac
    sleep 0.5
done
[[ -n "$seen" ]] || fail "job $job was never seen running"
log "  seen running ($seen)"
cancelled=$(PGOPTIONS="$PGOPTIONS -c statement_timeout=5000" q -c "SELECT pgml.cancel_training($job)") \
    || fail "cancel_training() blocked on the running job"
[[ "$cancelled" == t ]] || fail "cancel_training() returned '$cancelled'"

# A later job runs once the worker is free; by then the cancelled one is done
next=$(q -c "SELECT pgml.start_training(project_name => 'e2e_after_cancel',
        source_table => 'pgml_samples.iris', target_column => 'species',
        algorithm => 'lr', exclude_columns => ARRAY['id'])")
for _ in $(seq 1 600); do
    state=$(q -c "SELECT state FROM pgml.training_status($next)")
    case "$state" in completed|failed|cancelled) break ;; esac
    sleep 1
done
[[ "$state" == completed ]] || fail "job $next after the cancel ended in '$state'"
state=$(q -c "SELECT state FROM pgml.training_status($job)")
[[ "$state" == cancelled ]] || fail "cancelled job $job ended as '$state'"
models=$(q -c "SELECT count(*) FROM pgml.models m JOIN pgml.projects p ON p.id = m.project_id
        WHERE p.name = 'e2e_cancel'")
[[ "$models" == 0 ]] || fail "cancelled job $job stored $models model(s)"

# --- 5. supervision reports the worker ------------------------------------------
row=$(q -F '|' -c "SELECT name, state, failures FROM pgml.worker_status() WHERE name = 'pg_ml_training'")
[[ "$row" == "pg_ml_training|running|0" ]] || fail "worker_status(): '$row'"

log "OK: pg_ml end-to-end"
