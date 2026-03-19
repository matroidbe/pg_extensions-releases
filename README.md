# pg_extensions

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
| [pg_registry](extensions/pg_registry/) | JSON Schema registry with Kafka topic binding |

### Intelligence

| Extension | Description |
|-----------|-------------|
| [pg_ml](extensions/pg_ml/) | Machine learning with PyCaret — async training via background worker |
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

### Infrastructure

| Extension | Description |
|-----------|-------------|
| [eidos_oauth](extensions/eidos_oauth/) | OAuth 2.0 / JWT validator module |

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

Every extension runs inside PostgreSQL — no external processes. Network-facing extensions (pg_kafka, pg_mqtt, pg_s3) use background workers with async I/O via Tokio.

```
PostgreSQL
├── pg_kafka     ── Kafka protocol ──── Kafka clients
├── pg_mqtt      ── MQTT 5.0 ───────── IoT devices
├── pg_s3        ── S3 REST API ────── S3 clients
├── pg_delta     ── Delta Lake ─────── Cloud storage (S3/Azure/GCS)
├── pg_ml        ── PyCaret ────────── ML models
├── pg_fsm       ─┐
├── pg_ledger     │ Business logic extensions
├── pg_calendar   │ operate on PostgreSQL tables
├── pg_uom       ─┘
│
└── PostgreSQL Tables (single source of truth)
```

## License

**Matroid Source Available License v1.0** — See [LICENSE](LICENSE)

Free for internal use. If you run Postgres with these extensions to operate your own business — go ahead. If you want to build a commercial product or service on top of them, you need a commercial license.

For commercial licensing: tom@matroid.be
