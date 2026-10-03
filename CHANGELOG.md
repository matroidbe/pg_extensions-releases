# Changelog

All notable changes to this repository. A release tag (`vX.Y.Z`) names a
snapshot of the whole repo; each extension also carries its own version in its
`Cargo.toml` (`default_version` in the control file).

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
