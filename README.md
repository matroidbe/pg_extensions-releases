# pg_extensions

> **Status: Proof of Concept**
> This project explores a PostgreSQL-first approach to application infrastructure — bringing compute to data instead of data to compute. It is not production-ready, not supported, and not accepting contributions. APIs will change without notice.

PostgreSQL extensions that bring compute to data. Stop extracting data into external services — run ML, stream processing, optimization, and business logic where your data already lives.

Built with Rust and [pgrx](https://github.com/pgcentralfoundation/pgrx). Standard `CREATE EXTENSION` installation. No forks, no sidecars, no external daemons.

## Extensions

Self-contained tools — no dependencies on other pg_extensions, useful in any context.

### Data & Streaming

| Extension | Description |
|-----------|-------------|
| [pg_kafka](extensions/pg_kafka/) | Kafka protocol server backed by PostgreSQL tables |
| [pg_mqtt](extensions/pg_mqtt/) | MQTT 5.0 broker backed by PostgreSQL tables |
| [pg_delta](extensions/pg_delta/) | Bidirectional streaming between PostgreSQL and Delta Lake |
| [pg_s3](extensions/pg_s3/) | S3-compatible object storage with metadata in Postgres, binary on disk |
| [pg_streaming](extensions/pg_streaming/) | Declarative stream-processing engine — sources, transforms, and sinks inside PostgreSQL |
| [pg_registry](extensions/pg_registry/) | JSON Schema registry with Kafka topic binding |

### Intelligence

| Extension | Description |
|-----------|-------------|
| [pg_ml](extensions/pg_ml/) | Machine learning with PyCaret — async training via background worker |
| [pg_augur](extensions/pg_augur/) | Pure-Rust AutoML — model training and forecasting without Python |
| [pg_feature](extensions/pg_feature/) | Automated feature engineering using Deep Feature Synthesis |
| [pg_prob](extensions/pg_prob/) | Probabilistic data types with Monte Carlo simulation |
| [pg_ortools](extensions/pg_ortools/) | Constraint optimization using HiGHS MIP solver |
| [pg_image](extensions/pg_image/) | Image processing, EXIF extraction, perceptual hashing, ONNX detection |

### Business Logic

| Extension | Description |
|-----------|-------------|
| [pg_fsm](extensions/pg_fsm/) | Transactional finite state machines with guards and triggers |
| [pg_ledger](extensions/pg_ledger/) | Double-entry accounting engine |
| [pg_calendar](extensions/pg_calendar/) | Working calendar with culture-specific holidays and exceptions |
| [pg_uom](extensions/pg_uom/) | Unit of measure conversions with dimensional analysis |
| [pg_currency](extensions/pg_currency/) | Multi-currency exchange rates with base-relative triangulation |
| [pg_sequence](extensions/pg_sequence/) | ERP document numbering — formatted, scoped, auto-incrementing sequences |
| [pg_sheet](extensions/pg_sheet/) | Domain-aware spreadsheet overlays — formulas, snapshots, audit |

### Scientific & Geospatial

| Extension | Description |
|-----------|-------------|
| [pg_xarray](extensions/pg_xarray/) | Catalog & query layer for chunked scientific arrays (NetCDF, Zarr, HDF5, GRIB, COG, SELAFIN) over object storage |
| [pg_solid](extensions/pg_solid/) | 3D solid geometry (CAD/BIM) powered by OpenCASCADE — STEP/IGES/IFC import, GiST spatial index, glTF export |

### Infrastructure

| Extension | Description |
|-----------|-------------|
| [eidos_oauth](extensions/eidos_oauth/) | OAuth 2.0 / JWT validator module |
| [pg_git](extensions/pg_git/) | Git version control backed by PostgreSQL tables |
| [pg_swarm](extensions/pg_swarm/) | Distributed task-processing swarm with background workers |

## Quick Start

### Prerequisites

- PostgreSQL 14, 15, 16, or 17
- Rust toolchain ([rustup](https://rustup.rs/))
- [cargo-pgrx](https://github.com/pgcentralfoundation/pgrx)

### Installation

```bash
cargo install cargo-pgrx
cargo pgrx init

git clone https://github.com/matroidbe/pg_extensions.git
cd pg_extensions

# Build and install any extension
cd extensions/pg_kafka
cargo pgrx install --release
```

```sql
CREATE EXTENSION pg_kafka;

SELECT pgkafka.create_topic('events');
SELECT pgkafka.produce('events', '{"event": "test"}'::bytea);
-- Connect any Kafka client to localhost:9092
```

## Architecture

Every extension runs inside PostgreSQL — no external processes. Network-facing extensions (pg_kafka, pg_mqtt, pg_s3, pg_xarray) use background workers with async I/O via Tokio.

```
PostgreSQL
├── pg_kafka     ── Kafka protocol ──── Kafka clients
├── pg_mqtt      ── MQTT 5.0 ───────── IoT devices
├── pg_s3        ── S3 REST API ────── S3 clients
├── pg_delta     ── Delta Lake ─────── Cloud storage (S3/Azure/GCS)
├── pg_xarray    ── WMS / arrays ───── Scientific data (NetCDF/Zarr/GRIB)
├── pg_ml        ── PyCaret ────────── ML models
├── pg_solid     ── OpenCASCADE ────── 3D CAD / BIM (STEP/IFC/glTF)
├── pg_fsm       ─┐
├── pg_ledger     │ Business logic extensions
├── pg_calendar   │ operate on PostgreSQL tables
├── pg_uom       ─┘
│
└── PostgreSQL Tables (single source of truth)
```

## Status

This is a proof of concept demonstrating the "PostgreSQL as application platform" approach. It exists to show what's possible, not to provide production software.

- **Not production-ready** — expect bugs, missing features, and breaking changes
- **Not supported** — no issue tracking, no SLA, no guarantees
- **Not open to contributions** — source is available for learning and evaluation

If the approach interests you, watch the repo for releases.

## License

**Matroid Source Available License v1.0** — See [LICENSE](LICENSE)

Free for internal use. If you run Postgres with these extensions to operate your own business — go ahead. If you want to build a commercial product or service on top of them, you need a commercial license.

For commercial licensing: tom@matroid.be
