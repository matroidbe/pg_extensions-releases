#!/usr/bin/env bash
# Build HDF5 1.14 + netcdf-c into a private prefix for pg_xarray's
# `reader-netcdf` feature, on hosts that have no HDF5 1.x to link against.
#
# The hdf5-metno-sys build script (pulled in by the netcdf crate) accepts only
# HDF5 1.8-1.14. Debian/Ubuntu ship 1.10, so Linux uses the system libraries;
# Homebrew ships only HDF5 2.x, so macOS needs this. (The netcdf crate's own
# `static` feature builds both from source too, but that build cannot open
# NetCDF-4/HDF5 files on macOS, only NetCDF-3.)
#
# Autotools builds give the dylibs absolute install names, so a pg_xarray
# linked against them loads inside postgres without any rpath.
#
# Usage:
#   ./setup_hdf5.sh            # build into $PREFIX if not already there
# Prints nothing but progress; test.sh exports HDF5_DIR/NETCDF_DIR=$PREFIX.
#
# Environment:
#   PGX_HDF5_PREFIX   install prefix (default: $PGRX_HOME/hdf5-1.14-netcdf)
#   HDF5_VERSION_SRC  default 1.14.6
#   NETCDF_VERSION    default 4.9.3

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
log()   { echo -e "${GREEN}==>${NC} $1"; }
error() { echo -e "${RED}Error:${NC} $1" >&2; }

PGRX_HOME="${PGRX_HOME:-$HOME/.pgrx}"
PREFIX="${PGX_HDF5_PREFIX:-$PGRX_HOME/hdf5-1.14-netcdf}"
HDF5_VERSION_SRC="${HDF5_VERSION_SRC:-1.14.6}"
NETCDF_VERSION="${NETCDF_VERSION:-4.9.3}"

if [[ -f "$PREFIX/include/hdf5.h" && -f "$PREFIX/include/netcdf.h" ]]; then
    log "HDF5 + netcdf-c already in $PREFIX"
    exit 0
fi

JOBS="$(getconf _NPROCESSORS_ONLN)"
BUILD_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pgx-hdf5.XXXXXX")"
trap 'rm -rf "$BUILD_DIR"' EXIT
cd "$BUILD_DIR"

hdf5_series="${HDF5_VERSION_SRC%.*}"       # 1.14
hdf5_tag="${HDF5_VERSION_SRC//./_}"         # 1_14_6
log "Downloading HDF5 $HDF5_VERSION_SRC..."
curl -fsSL "https://support.hdfgroup.org/releases/hdf5/v${hdf5_series//./_}/v${hdf5_tag}/downloads/hdf5-${HDF5_VERSION_SRC}.tar.gz" | tar xz
log "Building HDF5 $HDF5_VERSION_SRC into $PREFIX (a few minutes)..."
( cd "hdf5-$HDF5_VERSION_SRC" \
    && ./configure --prefix="$PREFIX" --enable-hl --with-zlib --disable-fortran --disable-cxx \
        --disable-tests --disable-tools >"$BUILD_DIR/hdf5-configure.log" 2>&1 \
    && make -j"$JOBS" >"$BUILD_DIR/hdf5-build.log" 2>&1 \
    && make install >"$BUILD_DIR/hdf5-install.log" 2>&1 ) \
    || { error "HDF5 build failed:"; tail -30 "$BUILD_DIR"/hdf5-*.log >&2; exit 3; }

log "Downloading netcdf-c $NETCDF_VERSION..."
curl -fsSL "https://downloads.unidata.ucar.edu/netcdf-c/${NETCDF_VERSION}/netcdf-c-${NETCDF_VERSION}.tar.gz" | tar xz
log "Building netcdf-c $NETCDF_VERSION against it..."
( cd "netcdf-c-$NETCDF_VERSION" \
    && CPPFLAGS="-I$PREFIX/include" LDFLAGS="-L$PREFIX/lib" \
        ./configure --prefix="$PREFIX" --enable-netcdf-4 --disable-dap --disable-byterange \
        --disable-libxml2 --disable-nczarr --disable-plugins --disable-testsets --disable-utilities \
        >"$BUILD_DIR/netcdf-configure.log" 2>&1 \
    && make -j"$JOBS" >"$BUILD_DIR/netcdf-build.log" 2>&1 \
    && make install >"$BUILD_DIR/netcdf-install.log" 2>&1 ) \
    || { error "netcdf-c build failed:"; tail -30 "$BUILD_DIR"/netcdf-*.log >&2; exit 3; }

log "✓ HDF5 $HDF5_VERSION_SRC + netcdf-c $NETCDF_VERSION installed in $PREFIX"
