# =============================================================================
# Dockerfile for TechMart Retail Forecasting Example
# =============================================================================
# Single-stage build that installs pg_migrate, pg_feature, pg_ml, and pg_prob
# extensions using pgbrew (pgx) from the remote GitHub repository.
#
# NOTE: We use a single stage (not multi-stage builder) because pg_ml's Python
# venv has dependencies on system libraries that must be present at runtime.
#
# Usage:
#   docker compose up -d
#   docker compose exec postgres psql -U postgres
#
# =============================================================================

FROM postgres:16-bookworm

# Install build dependencies (Rust, Go, Python, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gnupg2 \
    curl \
    ca-certificates \
    git \
    build-essential \
    pkg-config \
    libssl-dev \
    libclang-dev \
    clang \
    python3 \
    python3-pip \
    python3-venv \
    libpython3-dev \
    postgresql-server-dev-16 \
    && rm -rf /var/lib/apt/lists/*

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install Go for pgbrew
RUN curl -LO https://go.dev/dl/go1.23.4.linux-amd64.tar.gz \
    && tar -C /usr/local -xzf go1.23.4.linux-amd64.tar.gz \
    && rm go1.23.4.linux-amd64.tar.gz
ENV PATH="${PATH}:/usr/local/go/bin:/root/go/bin"

# Install uv (Python package manager, required for pg_ml)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="${PATH}:/root/.local/bin"

# Install pgbrew (pgx)
RUN go install github.com/matroidbe/pgbrew/cmd/pgx@latest

# Set PG_CONFIG for pgx
ENV PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config

# Install cargo-pgrx (required for pgrx extension detection)
RUN cargo install cargo-pgrx --version 0.16.1 --locked

# Initialize pgrx for the target PostgreSQL
RUN cargo pgrx init --pg16 /usr/lib/postgresql/16/bin/pg_config

# Verify pgx installation
RUN pgx doctor

# Install extensions from GitHub using pgx (from develop branch)
# pgx automatically handles cargo-pgrx version management
# pg_ml's Makefile will create the Python venv with PyCaret
RUN pgx install github.com/matroidbe/pg_extensions/extensions/pg_migrate@develop
RUN pgx install github.com/matroidbe/pg_extensions/extensions/pg_feature@develop
RUN pgx install github.com/matroidbe/pg_extensions/extensions/pg_prob@develop
RUN pgx install github.com/matroidbe/pg_extensions/extensions/pg_ml@develop

# Clean up build dependencies to reduce image size (optional)
# Keeping them allows rebuilding extensions if needed
# RUN apt-get purge -y build-essential pkg-config libssl-dev libclang-dev clang \
#     && apt-get autoremove -y \
#     && rm -rf /var/lib/apt/lists/*

# Create init scripts directory
RUN mkdir -p /docker-entrypoint-initdb.d

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD pg_isready -U postgres || exit 1

# Default command
CMD ["postgres"]
