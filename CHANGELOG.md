# Changelog

All notable changes to this repository. A release tag (`vX.Y.Z`) names a
snapshot of the whole repo; each extension also carries its own version in its
`Cargo.toml` (`default_version` in the control file).

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
