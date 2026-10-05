#!/usr/bin/env bash
# End-to-end test of the WMS background worker on a throwaway cluster: a
# private copy of the pgrx PostgreSQL tree (scripts/lib/pg-scratch.sh), never
# ~/.pgrx/data-<N>.
#
#   1. pg_xarray.wms_enabled = on, applied with pg_reload_conf(), starts the
#      listener (GetCapabilities answers);
#   2. a new pg_xarray.wms_port, applied by reload, moves the listener;
#   3. a port held by another process makes every run fail: with
#      pg_xarray.max_worker_failures = 2 the worker ends up `failed` with the
#      bind error as its reason (design/bgworker-supervision);
#   4. after fixing the port by reload, reset_workers() brings the WMS back.
#
# Usage: ./wms-e2e.sh   (PG_MAJOR=18 by default)
set -euo pipefail

EXT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$EXT_DIR/../.." && pwd)"
# shellcheck source=../../scripts/lib/pg-scratch.sh
source "$REPO_ROOT/scripts/lib/pg-scratch.sh"

PG_MAJOR="${PG_MAJOR:-18}"
WORK="$REPO_ROOT/target/pg-xarray-wms-e2e"

log() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }
free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1])'; }
capabilities() {
    curl -s -m 5 "http://127.0.0.1:$1/wms?service=WMS&request=GetCapabilities" 2>/dev/null \
        | grep -q "WMS_Capabilities"
}
wait_for() { # wait_for <seconds> <command…>
    local n=$1; shift
    for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done
    return 1
}

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
unlink_extension pg_xarray
log "building and installing pg_xarray into $WORK/pg"
(cd "$EXT_DIR" && env -u PG_CONFIG cargo pgrx install --pg-config "$SCRATCH_CONFIG" \
    >"$WORK/install.log" 2>&1) \
    || { tail -30 "$WORK/install.log" >&2; fail "install (log: $WORK/install.log)"; }

P1=$(free_port); P2=$(free_port); P3=$(free_port)
start_scratch_cluster "$WORK/data" "$WORK"
stop_scratch_cluster
cat >>"$SCRATCH_PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_xarray'
pg_xarray.database = 'postgres'
pg_xarray.wms_port = $P1
pg_xarray.max_worker_failures = 2
EOF
start_scratch_cluster "$WORK/data" "$WORK"
q() { scratch_psql -d postgres -At "$@"; }
reload_with() { q -c "ALTER SYSTEM SET $1" -c "SELECT pg_reload_conf()" >/dev/null; }
q -c "CREATE EXTENSION pg_xarray CASCADE" >/dev/null

# --- 1. enable by reload ---------------------------------------------------------
capabilities "$P1" && fail "WMS answers while disabled"
log "enable by pg_reload_conf()"
reload_with "pg_xarray.wms_enabled = on"
wait_for 20 capabilities "$P1" || fail "WMS not serving on $P1 after enabling by reload"

# --- 2. move the port by reload ------------------------------------------------
log "move to port $P2 by pg_reload_conf()"
reload_with "pg_xarray.wms_port = $P2"
wait_for 20 capabilities "$P2" || fail "WMS not serving on $P2 after the port change"
capabilities "$P1" && fail "WMS still serving on the old port $P1"

# --- 3. a held port ends in `failed` -------------------------------------------
python3 -c "
import socket, time
s = socket.socket(); s.bind(('127.0.0.1', $P3)); s.listen()
time.sleep(3600)" &
HOLDER=$!
sleep 0.5
log "move to port $P3, held by pid $HOLDER: waiting for the worker to give up"
reload_with "pg_xarray.wms_port = $P3"
row=""
for _ in $(seq 1 120); do
    row=$(q -F '|' -c "SELECT state, failures, last_failure FROM pgx.worker_status()")
    [[ "$row" == failed* ]] && break
    sleep 1
done
echo "    $row"
[[ "$row" == "failed|2|"*"$P3"* ]] || fail "expected failed after 2 bind failures on $P3, got '$row'"

# --- 4. fix by reload, then reset --------------------------------------------------
log "fix the port by reload, then reset_workers()"
reload_with "pg_xarray.wms_port = $P2"
sleep 2
capabilities "$P2" && fail "a failed worker served before reset_workers()"
reset=$(q -c "SELECT pgx.reset_workers()")
[[ "$reset" == 1 ]] || fail "reset_workers() returned '$reset', expected 1"
wait_for 30 capabilities "$P2" || fail "WMS not serving on $P2 after reset_workers()"
row=$(q -F '|' -c "SELECT state, failures FROM pgx.worker_status()")
[[ "$row" == "running|0" ]] || fail "expected running with 0 failures, got '$row'"

log "OK: pg_xarray WMS worker"
