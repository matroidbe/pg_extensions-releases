# pg_extensions Examples

End-to-end examples demonstrating how pg_extensions work together to solve real business problems.

## Available Examples

### [retail-forecasting/](retail-forecasting/)

**Sales Forecasting with Probabilistic P&L**

Predict monthly sales revenue with uncertainty quantification and build a P&L statement that propagates uncertainty through all calculations.

**Extensions used:**
- `pg_feature` - Automated feature engineering (Deep Feature Synthesis)
- `pg_ml` - Machine learning with conformal prediction (PyCaret/AutoML)
- `pg_prob` - Probabilistic data types with Monte Carlo simulation

**Business questions answered:**
- "What's the probability we hit our $80K revenue target?"
- "What's our worst-case scenario (Value at Risk)?"
- "If marketing spend increases, how does profit uncertainty change?"

**Quick start:**
```bash
cd retail-forecasting
docker compose up -d
docker compose exec postgres psql -U postgres
# Then run SQL files 00-07 in order
```

---

## Prerequisites

### Docker (Recommended)

Each example includes a `Dockerfile` and `docker-compose.yml` that builds PostgreSQL with all required extensions pre-installed.

```bash
# Start any example
cd <example-name>
docker compose up -d

# Connect
docker compose exec postgres psql -U postgres

# Cleanup
docker compose down -v
```

### Manual Installation

If running without Docker, you need:

1. **PostgreSQL 16+**
2. **Rust toolchain** (for pgrx extensions)
3. **pgbrew** (pgx) for extension installation:
   ```bash
   # Install pgbrew
   go install github.com/matroidbe/pgbrew/cmd/pgx@latest

   # Install extensions
   pgx install github.com/matroidbe/pg_extensions/extensions/pg_feature
   pgx install github.com/matroidbe/pg_extensions/extensions/pg_ml
   pgx install github.com/matroidbe/pg_extensions/extensions/pg_prob
   ```

---

## Example Structure

Each example follows a consistent structure:

```
<example-name>/
├── README.md           # Business scenario and instructions
├── Dockerfile          # Multi-stage build with pgbrew
├── docker-compose.yml  # Easy startup
└── NN-*.sql            # Numbered SQL files to run in order
```

SQL files are numbered to indicate execution order:
- `00-extensions.sql` - Install and configure extensions
- `01-schema.sql` - Create database schema
- `02-*.sql` onwards - Business logic, data, analysis

---

## Philosophy

These examples demonstrate the **"Postgres as Platform"** approach:

1. **Compute moves to data** - ML, feature engineering, and analytics run inside PostgreSQL
2. **Extensions are composable** - Multiple extensions work together seamlessly
3. **SQL is the interface** - No external tools or languages required for the workflow
4. **Uncertainty is first-class** - Probabilistic types enable better decision-making

See [ideas/postgres-as-platform.md](../ideas/postgres-as-platform.md) for the full vision.

---

## Contributing

To add a new example:

1. Create a new directory under `examples/`
2. Include `Dockerfile`, `docker-compose.yml`, and numbered SQL files
3. Write a README explaining the business scenario
4. Update this file with the new example

---

## License

Part of the pg_extensions project. See repository root for license details.
