#!/usr/bin/env bash
# Keep pg_ml's pinned Python environment in step with pyproject.toml.
#
# uv.lock is the resolution; requirements.lock.txt is its hashed export, which
# the extension embeds (src/venv.rs) and the Makefile installs, so every venv
# gets exactly the packages CI tested (design/pg_ml/python-environment.md).
#
#   ./check-python-lock.sh          fail if either file is stale
#   ./check-python-lock.sh --fix    relock (no upgrades) and re-export
#
# To take newer packages: uv lock --upgrade && ./check-python-lock.sh --fix
set -euo pipefail

cd "$(dirname "$0")"

EXPORT=(uv export --frozen --no-dev --no-emit-project --format requirements-txt)

if [[ "${1:-}" == "--fix" ]]; then
    uv lock
    "${EXPORT[@]}" > requirements.lock.txt
    echo "requirements.lock.txt regenerated"
    exit 0
fi

if ! uv lock --check; then
    echo "uv.lock is out of date with pyproject.toml; run ./check-python-lock.sh --fix" >&2
    exit 1
fi

if ! diff -u requirements.lock.txt <("${EXPORT[@]}"); then
    echo "requirements.lock.txt does not match uv.lock; run ./check-python-lock.sh --fix" >&2
    exit 1
fi

echo "pg_ml Python lock is up to date"
