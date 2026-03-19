# pg_ml Makefile - Production Installation
#
# This Makefile is for PRODUCTION deployment only.
# For development, use ./test.sh which manages the local .venv folder.
#
# Requires: PG_CONFIG environment variable pointing to pg_config binary
# Example:  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install
#
# For system PostgreSQL (apt-get), the venv is created with sudo and owned by postgres.
# uv is installed into the venv so the postgres background worker can install packages.

# Validate PG_CONFIG is set
ifndef PG_CONFIG
$(error PG_CONFIG is not set. Set it to the path of pg_config, e.g., PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config)
endif

# Derive paths from pg_config
SHAREDIR := $(shell $(PG_CONFIG) --sharedir)
VENV_PATH ?= $(SHAREDIR)/pg_ml
PYTHON_VERSION ?= 3.11

# Detect if we need sudo (system PostgreSQL where SHAREDIR is not writable)
NEED_SUDO := $(shell test -w $(SHAREDIR) 2>/dev/null && echo "" || echo "sudo")

# Detect if this is a system PostgreSQL (apt-get, yum) that runs as postgres user
# vs user-installed (Homebrew, Postgres.app) that runs as current user
# System piaths: /usr/share/postgresql, /usr/pgsql-*
IS_SYSTEM_PG := $(shell echo $(SHAREDIR) | grep -qE '^/usr/(share/postgresql|pgsql-)' && echo "yes" || echo "")

# postgres user/group for ownership (only used for system PostgreSQL)
PG_USER ?= postgres
PG_GROUP ?= postgres

# Directory containing this Makefile (for finding pyproject.toml)
MAKEFILE_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

.PHONY: all install clean clean-all info help check-uv create-venv

all: install

# Check that uv is installed
check-uv:
	@command -v uv >/dev/null 2>&1 || { \
		echo "Error: 'uv' is not installed."; \
		echo "Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh"; \
		exit 1; \
	}

# Create fresh venv (always removes existing to ensure deps match pyproject.toml)
# UV_PYTHON_INSTALL_DIR is set to SHAREDIR so managed Python is accessible to all users
create-venv: check-uv
	@if [ -d "$(VENV_PATH)" ]; then \
		echo "==> Removing existing venv at $(VENV_PATH)..."; \
		$(NEED_SUDO) rm -rf $(VENV_PATH); \
	fi
	@echo "==> Creating Python $(PYTHON_VERSION) virtual environment at $(VENV_PATH)..."
	@$(NEED_SUDO) mkdir -p $(VENV_PATH)
	@$(NEED_SUDO) UV_PYTHON_INSTALL_DIR=$(SHAREDIR)/.uv-python uv venv --python $(PYTHON_VERSION) --managed-python $(VENV_PATH)
	@echo "==> Installing dependencies from pyproject.toml (this may take a few minutes)..."
	@cd $(MAKEFILE_DIR) && $(NEED_SUDO) uv pip install --python $(VENV_PATH)/bin/python .
	@echo "==> Installing uv into venv (for runtime package installation)..."
	@$(NEED_SUDO) uv pip install --python $(VENV_PATH)/bin/python uv
	@echo "==> Virtual environment ready at $(VENV_PATH)"

# Python library directory (for rpath linking)
# Note: This is evaluated lazily, so venv must exist when install runs
PYTHON_LIB_DIR = $(shell $(VENV_PATH)/bin/python -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))")

# Build and install extension
# Uses rpath to embed Python library path into the .so
# For system PostgreSQL: uses --sudo flag for pgrx to copy .so files
# After build, sets venv ownership to postgres for runtime access
install: create-venv
	@echo "==> Building pg_ml linked against $(VENV_PATH)/bin/python..."
	@echo "==> Python lib directory: $(PYTHON_LIB_DIR)"
	PYO3_PYTHON=$(VENV_PATH)/bin/python \
	PG_ML_VENV_PATH=$(VENV_PATH) \
	RUSTFLAGS="-C link-arg=-Wl,-rpath,$(PYTHON_LIB_DIR)" \
	cargo pgrx install --pg-config=$(PG_CONFIG) --release $(if $(NEED_SUDO),--sudo,)
	@if [ -n "$(IS_SYSTEM_PG)" ]; then \
		echo "==> System PostgreSQL detected, setting venv ownership to $(PG_USER):$(PG_GROUP)..."; \
		sudo chown -R $(PG_USER):$(PG_GROUP) $(VENV_PATH); \
	fi

# Clean build artifacts (keeps venv)
clean:
	cargo clean

# Full clean including venv
clean-all: clean
	$(NEED_SUDO) rm -rf $(VENV_PATH)

# Show configuration
info:
	@echo "pg_ml build configuration:"
	@echo "  PG_CONFIG=$(PG_CONFIG)"
	@echo "  SHAREDIR=$(SHAREDIR)"
	@echo "  VENV_PATH=$(VENV_PATH)"
	@echo "  PYTHON_VERSION=$(PYTHON_VERSION)"
	@echo "  NEED_SUDO=$(NEED_SUDO)"
	@echo "  IS_SYSTEM_PG=$(IS_SYSTEM_PG)"
	@echo "  PG_USER=$(PG_USER)"

# Show help
help:
	@echo "pg_ml Makefile - Production Installation"
	@echo ""
	@echo "For DEVELOPMENT, use ./test.sh instead (manages local .venv folder)"
	@echo ""
	@echo "Required environment variable:"
	@echo "  PG_CONFIG  - Path to pg_config binary"
	@echo ""
	@echo "Optional environment variables:"
	@echo "  VENV_PATH      - Override venv location (default: SHAREDIR/pg_ml)"
	@echo "  PYTHON_VERSION - Python version (default: 3.11)"
	@echo "  PG_USER        - User to own venv (default: postgres)"
	@echo "  PG_GROUP       - Group to own venv (default: postgres)"
	@echo ""
	@echo "Targets:"
	@echo "  make install   - Build and install (recreates venv for fresh deps)"
	@echo "  make clean     - Clean build artifacts (keeps venv)"
	@echo "  make clean-all - Clean everything including venv"
	@echo "  make info      - Show build configuration"
	@echo ""
	@echo "Example:"
	@echo "  PG_CONFIG=/usr/lib/postgresql/16/bin/pg_config make install"
	@echo ""
	@echo "The venv is always recreated on install to ensure dependencies"
	@echo "match pyproject.toml. For system PostgreSQL, sudo is used automatically."
