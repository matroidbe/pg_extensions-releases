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
#
# The lock is read and written with one pinned uv: uv releases change how the
# export spells platform markers (0.13.3 differs from 0.13.1), so an unpinned
# uv makes the check fail whenever a new uv ships. To move the pin, change
# UV_VERSION and run --fix in the same commit.
set -euo pipefail

cd "$(dirname "$0")"

UV_VERSION=0.13.1
uv_found=$(uv --version 2>/dev/null || true)
if [[ "$uv_found" == "uv $UV_VERSION" || "$uv_found" == "uv $UV_VERSION "* ]]; then
    UV=(uv)
else
    UV=(uvx --quiet --from "uv==$UV_VERSION" uv)
fi

EXPORT=("${UV[@]}" export --frozen --no-dev --no-emit-project --format requirements-txt)

if [[ "${1:-}" == "--fix" ]]; then
    "${UV[@]}" lock
    "${EXPORT[@]}" > requirements.lock.txt
    echo "requirements.lock.txt regenerated"
    exit 0
fi

if ! "${UV[@]}" lock --check; then
    echo "uv.lock is out of date with pyproject.toml; run ./check-python-lock.sh --fix" >&2
    exit 1
fi

if ! diff -u requirements.lock.txt <("${EXPORT[@]}"); then
    echo "requirements.lock.txt does not match uv.lock; run ./check-python-lock.sh --fix" >&2
    exit 1
fi

echo "pg_ml Python lock is up to date"
