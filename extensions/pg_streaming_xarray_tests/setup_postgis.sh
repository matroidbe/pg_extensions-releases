#!/usr/bin/env bash
# setup_postgis.sh — install PostGIS into the pgrx-managed PostgreSQL.
#
# pg_xarray's catalog schema uses GEOMETRY columns and requires PostGIS.
# Most distros ship PostGIS via apt/dnf/brew, but the binaries are
# linked against the system Postgres, NOT against the pgrx-built PG in
# $HOME/.pgrx. This script bridges that gap.
#
# Strategy (priority order, falls through automatically):
#
#   1. If PostGIS is already installed for the pgrx PG, do nothing.
#   2. If brew has a postgis bottle for the same PG major version, copy
#      its .so + .sql + .control files into the pgrx install. Fast.
#   3. Otherwise, build PostGIS from source against the pgrx PG. Needs
#      brew geos + brew proj + system libxml2-dev. Slower (~5 min).
#
# Usage:
#   ./setup_postgis.sh                  # auto, PG 16
#   PG_VERSION=17 ./setup_postgis.sh    # other PG major version
#
# Idempotent — re-running with PostGIS already present is a no-op.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
log()   { echo -e "${GREEN}==>${NC} $1"; }
warn()  { echo -e "${YELLOW}Warning:${NC} $1"; }
error() { echo -e "${RED}Error:${NC} $1" >&2; }

PGRX_HOME="${PGRX_HOME:-$HOME/.pgrx}"
PG_VERSION="${PG_VERSION:-16}"
PG_INSTALL="$(ls -d "$PGRX_HOME"/${PG_VERSION}.*/pgrx-install 2>/dev/null | head -1 || true)"
if [[ -z "$PG_INSTALL" ]]; then
    error "No pgrx PostgreSQL $PG_VERSION install found under $PGRX_HOME."
    error "Run: cargo pgrx init --pg${PG_VERSION} download"
    exit 1
fi
PG_CONFIG="$PG_INSTALL/bin/pg_config"
SHARE_DIR="$($PG_CONFIG --sharedir)"
PKGLIB_DIR="$($PG_CONFIG --pkglibdir)"

# -----------------------------------------------------------------------------
# 1. Short-circuit if PostGIS is already in the pgrx install.
# -----------------------------------------------------------------------------
if [[ -f "$SHARE_DIR/extension/postgis.control" \
   && -f "$PKGLIB_DIR/postgis-3.so" ]]; then
    log "PostGIS already installed for pgrx PG $PG_VERSION at:"
    log "  $SHARE_DIR/extension/postgis.control"
    log "  $PKGLIB_DIR/postgis-3.so"
    exit 0
fi

# -----------------------------------------------------------------------------
# 2. Try copying from a brew install (fast path).
# -----------------------------------------------------------------------------
BREW_PREFIX=""
if command -v brew >/dev/null 2>&1; then
    BREW_PREFIX="$(brew --prefix 2>/dev/null || true)"
fi

if [[ -n "$BREW_PREFIX" ]] && [[ -d "$BREW_PREFIX/Cellar/postgis" ]]; then
    BREW_POSTGIS="$(ls -d "$BREW_PREFIX"/Cellar/postgis/*/ 2>/dev/null | head -1)"
    BREW_POSTGIS="${BREW_POSTGIS%/}"
    log "Found brew PostGIS at $BREW_POSTGIS"

    BREW_SHARE="$BREW_POSTGIS/share/postgresql@$PG_VERSION/extension"
    BREW_LIB="$BREW_POSTGIS/lib/postgresql@$PG_VERSION"

    if [[ -d "$BREW_SHARE" && -d "$BREW_LIB" ]]; then
        log "Copying brew PostGIS files for PG $PG_VERSION into pgrx install..."
        mkdir -p "$SHARE_DIR/extension" "$PKGLIB_DIR"
        cp "$BREW_SHARE"/postgis*.control \
           "$BREW_SHARE"/postgis*.sql \
           "$SHARE_DIR/extension/" 2>/dev/null || true
        cp "$BREW_LIB"/postgis*.so "$PKGLIB_DIR/" 2>/dev/null || true

        if [[ -f "$SHARE_DIR/extension/postgis.control" \
           && -f "$PKGLIB_DIR/postgis-3.so" ]]; then
            log "✓ PostGIS copied from brew."
            exit 0
        else
            warn "brew copy didn't produce a complete install — falling through."
        fi
    else
        log "brew has PostGIS but no @$PG_VERSION bottle (has $(ls "$BREW_POSTGIS/lib" 2>/dev/null | tr '\n' ' ')) — building from source instead."
    fi
fi

# -----------------------------------------------------------------------------
# 3. Build from source.
# -----------------------------------------------------------------------------
POSTGIS_VERSION="${POSTGIS_VERSION:-3.5.2}"

GEOS_CONFIG="$(command -v geos-config || true)"
if [[ -z "$GEOS_CONFIG" && -n "$BREW_PREFIX" ]]; then
    GEOS_CONFIG="$BREW_PREFIX/opt/geos/bin/geos-config"
fi
if [[ ! -x "$GEOS_CONFIG" ]]; then
    error "geos-config not found. Install with:"
    error "  brew install geos     # (or: sudo apt-get install libgeos-dev)"
    exit 2
fi

PROJ_PREFIX=""
if [[ -n "$BREW_PREFIX" ]]; then
    PROJ_PREFIX="$BREW_PREFIX/opt/proj"
fi
if [[ ! -d "$PROJ_PREFIX" ]]; then
    error "PROJ not found (expected at $PROJ_PREFIX). Install with:"
    error "  brew install proj     # (or: sudo apt-get install libproj-dev)"
    exit 2
fi

# libxml2 headers — try system + brew.
LIBXML_CFLAGS=""
LIBXML_LIBS="-lxml2"
if pkg-config --exists libxml-2.0 2>/dev/null; then
    LIBXML_CFLAGS="$(pkg-config --cflags libxml-2.0)"
    LIBXML_LIBS="$(pkg-config --libs libxml-2.0)"
fi

BUILD_DIR="${POSTGIS_BUILD_DIR:-/tmp/postgis-src-$$}"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

if [[ ! -d "postgis-$POSTGIS_VERSION" ]]; then
    log "Downloading PostGIS $POSTGIS_VERSION source..."
    curl -sSL "https://download.osgeo.org/postgis/source/postgis-${POSTGIS_VERSION}.tar.gz" \
        -o postgis.tar.gz
    tar xzf postgis.tar.gz
fi

cd "postgis-$POSTGIS_VERSION"

if [[ ! -f Makefile ]]; then
    log "Configuring PostGIS for pgrx PG $PG_VERSION..."
    PG_CONFIG="$PG_CONFIG" \
    LDFLAGS="-L$BREW_PREFIX/lib -Wl,-rpath,$BREW_PREFIX/lib" \
    CPPFLAGS="-I$BREW_PREFIX/include $LIBXML_CFLAGS" \
    ./configure \
        --with-pgconfig="$PG_CONFIG" \
        --with-geosconfig="$GEOS_CONFIG" \
        --with-projdir="$PROJ_PREFIX" \
        --without-protobuf --without-json --without-gdal \
        --without-sfcgal --without-topology --without-raster \
        --without-pcre \
        > /tmp/postgis-configure-$$.log 2>&1 \
        || { error "configure failed; see /tmp/postgis-configure-$$.log"; exit 3; }
fi

log "Building PostGIS (this takes a few minutes)..."
LD_LIBRARY_PATH="$BREW_PREFIX/lib:${LD_LIBRARY_PATH:-}" \
    make -j"$(nproc)" > /tmp/postgis-build-$$.log 2>&1 \
    || { error "build failed; see /tmp/postgis-build-$$.log"; exit 3; }

log "Installing PostGIS extension into pgrx PG $PG_VERSION..."
# `make install` from the top level tries to install man pages to
# /usr/local — skip that step and install only the extension dir.
LD_LIBRARY_PATH="$BREW_PREFIX/lib:${LD_LIBRARY_PATH:-}" \
    make -C extensions install > /tmp/postgis-install-$$.log 2>&1 \
    || { error "install failed; see /tmp/postgis-install-$$.log"; exit 3; }

# The .so for the core postgis library is installed by `make install`
# from the libpgcommon/postgis dir, but we skipped that. Install the
# loaded library manually.
SO_SRC="$(find "$BUILD_DIR/postgis-$POSTGIS_VERSION" -name 'postgis-3.so' \
            -not -path '*/extensions/*' 2>/dev/null | head -1)"
if [[ -n "$SO_SRC" ]]; then
    cp "$SO_SRC" "$PKGLIB_DIR/"
fi

# Also install address_standardizer if it was built (a postgis extension).
ADDR_SO="$(find "$BUILD_DIR/postgis-$POSTGIS_VERSION" -name 'address_standardizer-3.so' 2>/dev/null | head -1)"
if [[ -n "$ADDR_SO" ]]; then
    cp "$ADDR_SO" "$PKGLIB_DIR/" 2>/dev/null || true
fi

if [[ -f "$SHARE_DIR/extension/postgis.control" \
   && -f "$PKGLIB_DIR/postgis-3.so" ]]; then
    log "✓ PostGIS $POSTGIS_VERSION built and installed for pgrx PG $PG_VERSION."
else
    error "Install completed but the expected files are missing:"
    error "  $SHARE_DIR/extension/postgis.control"
    error "  $PKGLIB_DIR/postgis-3.so"
    exit 4
fi
