#!/usr/bin/env bash
# End-to-end test of background worker supervision (design/bgworker-supervision)
# on a throwaway cluster: a private copy of the pgrx PostgreSQL tree
# (scripts/lib/pg-scratch.sh), never ~/.pgrx/data-<N>.
#
# Another process holds pg_kafka's port, so every worker run fails to bind.
# With pg_kafka.max_worker_failures = 2 the worker must end up `failed` with
# the bind error as its reason. Once the port is free, reset_workers()
# (superuser only) must bring the broker up.
#
# Usage: ./supervision-e2e.sh   (PG_MAJOR=18 by default)
set -euo pipefail

EXT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXT_DIR/../.." && pwd)"
# shellcheck source=../../scripts/lib/pg-scratch.sh
source "$REPO_ROOT/scripts/lib/pg-scratch.sh"

PG_MAJOR="${PG_MAJOR:-18}"
WORK="$REPO_ROOT/target/pg-kafka-supervision-e2e"

log() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

PG_CONFIG_SRC=$(resolve_pg_config "$PG_MAJOR")
[[ -n "$PG_CONFIG_SRC" ]] || fail "no pgrx PostgreSQL $PG_MAJOR (cargo pgrx init)"

HOLDER=""
cleanup() {
    [[ -n "$HOLDER" ]] && kill "$HOLDER" 2>/dev/null || true
    cleanup_scratch
}
trap cleanup EXIT
rm -rf "$WORK"
mkdir -p "$WORK"

make_scratch_tree "$PG_CONFIG_SRC" "$WORK/pg"
unlink_extension pg_kafka
log "building and installing pg_kafka into $WORK/pg"
(cd "$EXT_DIR" && env -u PG_CONFIG cargo pgrx install --pg-config "$SCRATCH_CONFIG" \
    >"$WORK/install.log" 2>&1) \
    || { tail -30 "$WORK/install.log" >&2; fail "install (log: $WORK/install.log)"; }

# --- hold the broker port -------------------------------------------------
KPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1])')
python3 -c "
import socket, time
s = socket.socket(); s.bind(('0.0.0.0', $KPORT)); s.listen()
time.sleep(3600)" &
HOLDER=$!
sleep 0.5
log "port $KPORT held by pid $HOLDER"

# --- cluster ----------------------------------------------------------------
start_scratch_cluster "$WORK/data" "$WORK"
stop_scratch_cluster
cat >>"$SCRATCH_PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_kafka'
pg_kafka.database = 'postgres'
pg_kafka.port = $KPORT
pg_kafka.worker_count = 1
pg_kafka.max_worker_failures = 2
EOF
start_scratch_cluster "$WORK/data" "$WORK"
q() { scratch_psql -d postgres -At "$@"; }
q -c "CREATE EXTENSION pg_kafka"

# --- 1. repeated bind failures end in `failed` --------------------------------
log "waiting for the worker to give up (2 failures, ~20s)"
row=""
for _ in $(seq 1 120); do
    row=$(q -F '|' -c "SELECT state, failures, last_failure FROM pgkafka.worker_status()")
    [[ "$row" == failed* ]] && break
    sleep 1
done
echo "    $row"
[[ "$row" == "failed|2|"* ]] || fail "expected failed after 2 failures, got '$row'"
[[ "$row" == *"$KPORT"* && "$row" == *[Aa]ddress* ]] || fail "reason should name the bind error: '$row'"
grep -q "will not be restarted" "$WORK/postgres.log" || fail "no WARNING in the server log"

# Still failed a few seconds later: no more retries
sleep 8
row=$(q -F '|' -c "SELECT state, restarts FROM pgkafka.worker_status()")
[[ "$row" == failed* ]] || fail "left the failed state on its own: '$row'"

# --- 2. reset_workers() is superuser only ------------------------------------
q -c "CREATE ROLE kafka_e2e_user LOGIN" -c "GRANT USAGE ON SCHEMA pgkafka TO kafka_e2e_user"
if scratch_psql -d postgres -At -U kafka_e2e_user -c "SELECT pgkafka.reset_workers()" 2>"$WORK/nonsuper.err"; then
    fail "a non-superuser could reset workers"
fi
grep -q "requires superuser" "$WORK/nonsuper.err" || fail "unexpected error: $(cat "$WORK/nonsuper.err")"

# --- 3. fix the cause, reset, and the broker serves ----------------------------
kill "$HOLDER"; wait "$HOLDER" 2>/dev/null || true; HOLDER=""
reset=$(q -c "SELECT pgkafka.reset_workers()")
[[ "$reset" == 1 ]] || fail "reset_workers() returned '$reset', expected 1"

log "waiting for the broker on port $KPORT"
up=""
for _ in $(seq 1 30); do
    if python3 -c "import socket; socket.create_connection(('127.0.0.1', $KPORT), 1)" 2>/dev/null; then
        up=yes; break
    fi
    sleep 1
done
[[ -n "$up" ]] || fail "broker not listening after reset"
row=$(q -F '|' -c "SELECT state, failures FROM pgkafka.worker_status()")
echo "    $row"
[[ "$row" == "running|0" ]] || fail "expected running with 0 failures, got '$row'"

log "OK: pg_kafka worker supervision"
