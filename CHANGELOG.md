# Changelog

All notable changes to this repository. A release tag (`vX.Y.Z`) names a
snapshot of the whole repo; each extension also carries its own version in its
`Cargo.toml` (`default_version` in the control file).

## v0.5.1 — 2026-10-07

**pg_ortools local search solves rosters.** Local search assigns each item
exactly one slot; a roster is the other way round — every shift gets exactly
one employee, and an employee takes many shifts. A new typed constraint
`assignment` with `{"each": "slot"}` declares that shape: the loader hands the
engine the transposed problem and maps the solution back, with constraint
configs staying in the caller's terms (`no_overlap` becomes a slot-conflict
rule, `skill_match` is transposed, costs move sides, `pin_current` is per
slot; `group_balance` is refused). A typed constraint whose config does not
parse now fails the solve instead of being skipped. The design doc's
typed-constraint tables, which documented field-name configs the parser never
read, now give the real shapes (design/pg_ortools/metaheuristic.md).

pg_git's integration test serves git-HTTP on a free port instead of a fixed
5433, which collided with a PostgreSQL on the same host.

Extension versions: pg_ortools 0.3.3 (ortools_core 0.3.1). Every other
extension is unchanged from v0.5.0.

## v0.5.0 — 2026-10-05

**Supervised background workers.** Every extension with background workers
now supervises them (design/bgworker-supervision). After a failure a worker
backs off (5s, doubling to 60s); after `<ext>.max_worker_failures`
consecutive failures (default 10, `0` = never give up) it stays idle in a
`failed` state until `SELECT <schema>.reset_workers()` (superuser) or a
server restart. `<schema>.worker_status()` shows each worker's state, failure
count, restarts and last failure reason. A worker stopped on purpose
(SIGTERM, disabled) is not counted, and 60s of healthy running resets the
count.

**pg_kafka advertised listener.** New `pg_kafka.advertised_port` for clients
that reach the broker through a remapped port (NAT, proxy, a container
publishing 9092 elsewhere). An unset `pg_kafka.advertised_host` now means the
address the client connected to, never `0.0.0.0`. Both follow
`pg_reload_conf()`, including on open connections
(design/pg_kafka/advertised-listener).

**pg_ml installs a locked Python environment.** `make install` and
`pgml.setup_venv()` install the hashed export of `uv.lock` embedded in the
extension, so every venv has exactly the packages that were tested
(design/pg_ml/python-environment).

Extension versions: **pg_delta 0.3.3, pg_git 0.3.2, pg_kafka 0.3.2, pg_ml
0.3.2, pg_mqtt 0.3.2, pg_ortools 0.3.2, pg_s3 0.3.2, pg_streaming 0.3.2,
pg_swarm 0.3.2, pg_xarray 0.4.2**; all others unchanged. Upgrade with
`ALTER EXTENSION <ext> UPDATE`. A server restart is needed for supervision:
it uses shared memory that is set up when the library is preloaded.

### Fixed
- Background workers never applied `pg_reload_conf()`: pgrx only flags
  SIGHUP and nothing reloaded the configuration. `pg_xarray.wms_enabled`,
  documented as toggled by reload, needed a server restart.
- A background worker stopped by `pg_terminate_backend()` or
  `DROP DATABASE … WITH (FORCE)` exited with code 0 and was never started
  again until a server restart. pg_xarray's WMS worker was never restarted
  after any exit.
- `DROP DATABASE` could hang on background workers that waited without
  servicing interrupts (pg_delta, pg_git, pg_ml, pg_streaming, pg_swarm, and
  shared waits in every extension).
- pg_kafka, pg_mqtt, pg_s3, pg_git: a listener that could not bind its port
  left the worker idling; it is now a reported failure that is retried.
- pg_kafka (#125): the advertised host was read once at startup and
  defaulted to `0.0.0.0`, so remote clients could not connect.
- pg_ml: training jobs ran in one transaction, so `training_status()` showed
  `queued` until a job finished and `cancel_training()` waited for the
  training to end. Jobs left running by a worker that died are now failed.
- pg_git: the HTTP worker logged from a tokio thread.
- pg_swarm: the scheduler could take another node's id as its own.
- Tests: integration suites skipped, and so passed, when their server was
  down; `test.sh` now fails instead. pg_git's integration tests had never run.
- CI: the pgrx test cluster has its own port, so other projects' CI on the
  same runner host no longer collides with it.

## v0.4.1 — 2026-10-03

**Background-worker configuration.** Every extension with background workers
reads `<prefix>.database` (pg_delta: `delta.database`; new for pg_delta and
pg_swarm, which always used `postgres`; pg_ortools and pg_ml keep
`solver_database` / `training_database` as deprecated aliases). A worker whose
database does not have the extension yet now logs once and waits for
`CREATE EXTENSION` instead of exiting and being restarted in a loop. Bottles
declare the database and listen addresses in `[postgresql.settings]`, so
`pgx install --configure --set <ext>.database=<db>` (pgbrew) configures them
(design/bgworker-config).

Install bottles with pgx from pgbrew 1b25cc4 or later: `--set` needs
pgbrew#8, and older pgx wrote values such as `0.0.0.0` unquoted into the
conf.d drop-in, which stops PostgreSQL from starting (pgbrew#9).

Extension versions: **pg_delta 0.3.2, pg_git 0.3.1, pg_kafka 0.3.1, pg_mqtt
0.3.1, pg_ml 0.3.1, pg_ortools 0.3.1, pg_s3 0.3.1, pg_streaming 0.3.1,
pg_swarm 0.3.1, pg_xarray 0.4.1**; all others unchanged. Upgrade with
`ALTER EXTENSION <ext> UPDATE`.

### Fixed
- Bottle verification left files in the CI checkout that the runner could
  not delete, failing every later CI run at checkout.
- Bottles: the builder image pins Rust to CI's toolchain, and pgx no longer
  packs a stale install script of an earlier version from the build cache
  (pgbrew#10).

## v0.4.0 — 2026-10-02

**PostgreSQL 18 is the default target.** Every crate's default feature is now
`pg18`, CI builds and tests against PostgreSQL 18, and so do `test.sh` and
`scripts/check-upgrade-path.sh`. Other majors still build with
`--no-default-features --features pgNN` plus the crate's other default
features.

**Prebuilt bottles.** Each release attaches a bottle of every extension except
pg_ml (PostgreSQL 18, linux-amd64, glibc 2.36+) and a `SHA256SUMS` to the
public GitHub release, installable with `pgx install --bottle <url>`
(design/bottles). v0.4.0 is the first release with bottles. The pg_solid
bottle needs OpenCASCADE 7.6 on the host (Debian 12, Ubuntu 24.04).

Extension versions: **pg_delta 0.3.1, pg_sheet 0.3.1, pg_xarray 0.4.0**; all
others unchanged at 0.3.0. Upgrade with `ALTER EXTENSION <ext> UPDATE`.

### Fixed
- pg_delta: the index-mode FDW did not compile against PostgreSQL 18.
- pg_sheet: `create_sheet` failed on databases without an `app_user` role; the
  grants to it are now made only when the role exists.
- pg_xarray: chunks that differ only in their `chunk_key` (Zarr levels and
  spatial tiles of one time step) collapsed into one catalog row. The dedupe
  key now includes `chunk_key`; re-register affected Zarr variables after
  `ALTER EXTENSION pg_xarray UPDATE`.
- CI: pg_image and pg_solid are linted and upgrade-checked again (only pg_ml
  is still skipped), and a reused CI workspace registers the PostgreSQL major
  the crates target.
- Tests: a pg_registry unit test no longer calls into pgrx off the main
  thread, and eidos_oauth's JWKS cache tests no longer race each other.

## v0.3.0 — 2026-10-01

**Upgrade baseline.** v0.3.0 is the last release that replaces extensions in
place without upgrade scripts, and it is where every extension's upgrade chain
starts. Databases on any earlier build — including images built from `develop`
that report `0.2.0` — must `DROP EXTENSION … CASCADE` / `CREATE EXTENSION`
(or dump and restore their data): `ALTER EXTENSION … UPDATE` from 0.2.0 is not
supported because no 0.2.0 SQL state exists in the wild to upgrade from. From
v0.3.0 on, each extension is versioned on its own and every version change
ships a `sql/<ext>--<old>--<new>.sql` upgrade script.

All extensions are at **0.3.0**.

### Removed
- **pg_augur** — now maintained in its own repository (`matroidbe/augur`).

### Added
- **pg_kafka** — consumer-group coordinator (JoinGroup/SyncGroup/Heartbeat/
  LeaveGroup, offset commit/fetch) and admin APIs.
- **pg_ortools** — CP-SAT scheduling engine (pure Rust, via Pumpkin), in the
  default build (`cpsat` feature, on by default): CP scheduling SQL surface,
  async `solve_cp` that is time-bounded and cancellable; Eidos live catalog
  (`eidos_catalog_*`).
- **pg_streaming** — `call` output connector (with `set_role`), Modbus TCP and
  Siemens S7 input connectors, Modbus and S7 write sinks.
- **pg_delta** — index mode: catalog a Delta table's `_delta_log`, prune files,
  and query them in place through the `pg_delta_server` FDW, without copying.
- **pg_ml** — `predict_proba_row`, the by-name probability entry point.
- **pg_fsm** — idempotent seeding via `if_not_exists`.
- **prob_core** — pure-Rust distribution core extracted from pg_prob; pg_prob
  now wraps it.
- `eidos_catalog` contract shipped for pg_prob, pg_solid and pg_ml.
- Background-worker extensions declare their required PostgreSQL configuration;
  pg_solid declares its OpenCASCADE dependency (for pgbrew).

### Fixed
- **pg_spi** — SPI parameters are bound instead of spliced into SQL text
  (SQL-injection fix); correct `rows_affected`, error resilience, and read-only
  SPI selection.
- **pg_ortools** — no panic on infeasible CP scheduling models.
- **pg_streaming** — connector errors no longer crash-loop the executor;
  `set_role` is assumed inside the guarded call.
- **pg_solid** — builds against OCCT 7.6+ and relocatable prefixes.
- Build: machine-specific cargo settings and local patches kept out of the
  committed manifests; clippy clean with `--all-targets` across the workspace.

## v0.2.0 — 2026-06-29

New extensions: pg_xarray, pg_solid, pg_streaming, pg_augur, pg_currency,
pg_sequence, pg_sheet, pg_git, pg_swarm. All extensions bumped to 0.2.0.
Background workers idle on `wait_latch` and absorb `ProcSignalBarrier`, so
`DROP DATABASE` and shutdown no longer hang.

## v0.1.0 — 2025-12-18

Initial release.
