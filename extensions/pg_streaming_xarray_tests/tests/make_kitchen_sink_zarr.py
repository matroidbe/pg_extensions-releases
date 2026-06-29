#!/usr/bin/env python3
"""make_kitchen_sink_zarr.py — generate a richer synthetic Zarr v3
tree on disk that exercises every dimension + dtype + composite path
the pg_xarray + pgx_zarr_walker shipped so far.

Standard library only — no Python package install required.

Layout written under <root>/:

    weather/                       ← geographic, default SRID 4326
        zarr.json + the coord arrays below
        valid_time/                ← CF time axis
            attributes.units = "hours since 2024-01-01 00:00:00"
        level/                     ← Z axis (4 pressure levels)
        latitude/
        longitude/
        t2m_packed/                ← 4D int16 + CF packing
            shape [n_time, n_level, n_lat, n_lon]
            attributes: units=K, scale_factor=0.01, add_offset=273.15,
                        _FillValue=-9999, standard_name=air_temperature,
                        long_name="2-metre temperature"
        u/                         ← 3D float32 wind component
            shape [n_time, n_lat, n_lon]
            attributes: units="m s-1", standard_name="eastward_wind"
        v/                         ← same as u, value-flipped sign
            attributes: units="m s-1", standard_name="northward_wind"

    sim/                           ← Cartesian (SRID 0), engineering coords
        zarr.json + coord arrays
        x/                         (m)
        y/                         (m)
        pressure_field/            ← 2D float32 scalar
            shape [n_y, n_x]
            attributes: units="Pa", standard_name="pressure"

Usage:
    python3 make_kitchen_sink_zarr.py <root>
"""
import json
import os
import struct
import sys
from pathlib import Path


N_TIME  = 4   # 4 hourly snapshots
N_LEVEL = 4   # 4 pressure levels: 1000, 850, 500, 250 hPa
N_LAT   = 3   # 50, 51, 52
N_LON   = 4   # 0, 1, 2, 3
N_Y     = 3   # 0, 1, 2 (metres)
N_X     = 4   # 0, 1, 2, 3 (metres)

PRESSURE_LEVELS = [1000.0, 850.0, 500.0, 250.0]


def write_meta(path: Path, meta: dict):
    path.mkdir(parents=True, exist_ok=True)
    (path / "zarr.json").write_text(json.dumps(meta))


def write_chunk(var_dir: Path, chunk_index, bytes_: bytes):
    """Write the chunk file at c/<i>/<j>/<k>/... given a tuple."""
    chunk_dir = var_dir / "c"
    for idx in chunk_index[:-1]:
        chunk_dir = chunk_dir / str(idx)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    chunk_dir = chunk_dir / str(chunk_index[-1])
    chunk_dir.write_bytes(bytes_)


def write_axis(store_root: Path, name: str, values: list[float], dtype="float32"):
    """1-D coord axis, single chunk."""
    dir_ = store_root / name
    n = len(values)
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [n],
        "data_type": dtype,
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [n]}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
    }
    write_meta(dir_, meta)
    fmt = "<f" if dtype == "float32" else "<d"
    body = b"".join(struct.pack(fmt, v) for v in values)
    write_chunk(dir_, [0], body)


def write_time_axis(store_root: Path, name: str, values: list[float], units: str):
    """1-D time axis with CF `units` attribute. Stored as float64."""
    dir_ = store_root / name
    n = len(values)
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [n],
        "data_type": "float64",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [n]}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        "attributes": {"units": units},
    }
    write_meta(dir_, meta)
    body = b"".join(struct.pack("<d", v) for v in values)
    write_chunk(dir_, [0], body)


def write_t2m_packed(store_root: Path):
    """4D int16 + CF packing — the headline real-world ERA5 shape.
    One chunk per (time, level) slice = N_TIME*N_LEVEL chunks. Cell
    formula (stored ints): stored[t][k][j][i] = t*100 + k*10 + j*4 + i.
    physical = stored * 0.01 + 273.15 (CF packing).
    """
    dir_ = store_root / "t2m_packed"
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [N_TIME, N_LEVEL, N_LAT, N_LON],
        "data_type": "int16",
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": [1, 1, N_LAT, N_LON]},
        },
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": -9999,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        "dimension_names": ["valid_time", "level", "latitude", "longitude"],
        "attributes": {
            "units": "K",
            "standard_name": "air_temperature",
            "long_name": "2-metre temperature",
            "scale_factor": 0.01,
            "add_offset": 273.15,
            "_FillValue": -9999,
        },
    }
    write_meta(dir_, meta)
    for t in range(N_TIME):
        for k in range(N_LEVEL):
            payload = bytearray()
            for j in range(N_LAT):
                for i in range(N_LON):
                    stored = t * 100 + k * 10 + j * 4 + i
                    payload.extend(struct.pack("<h", stored))  # signed int16 LE
            write_chunk(dir_, [t, k, 0, 0], bytes(payload))


def write_wind_component(store_root: Path, name: str, sign: int, standard_name: str):
    """3D float32 — shape (time, lat, lon). One chunk per time slice.
    Values: sign * (t*100 + j*4 + i). Sign-flip lets us distinguish u from v.
    """
    dir_ = store_root / name
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [N_TIME, N_LAT, N_LON],
        "data_type": "float32",
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": [1, N_LAT, N_LON]},
        },
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        "dimension_names": ["valid_time", "latitude", "longitude"],
        "attributes": {
            "units": "m s-1",
            "standard_name": standard_name,
        },
    }
    write_meta(dir_, meta)
    for t in range(N_TIME):
        payload = bytearray()
        for j in range(N_LAT):
            for i in range(N_LON):
                v = float(sign * (t * 100 + j * 4 + i))
                payload.extend(struct.pack("<f", v))
        write_chunk(dir_, [t, 0, 0], bytes(payload))


def write_pressure_field(store_root: Path):
    """2D Cartesian float32 — shape (y, x). One single chunk.
    Cell values: p[j][i] = j*N_X + i.
    """
    dir_ = store_root / "pressure_field"
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": [N_Y, N_X],
        "data_type": "float32",
        "chunk_grid": {
            "name": "regular",
            "configuration": {"chunk_shape": [N_Y, N_X]},
        },
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        "dimension_names": ["y", "x"],
        "attributes": {
            "units": "Pa",
            "standard_name": "pressure",
        },
    }
    write_meta(dir_, meta)
    payload = bytearray()
    for j in range(N_Y):
        for i in range(N_X):
            v = float(j * N_X + i)
            payload.extend(struct.pack("<f", v))
    write_chunk(dir_, [0, 0], bytes(payload))


def main():
    if len(sys.argv) != 2:
        print("usage: make_kitchen_sink_zarr.py <root>", file=sys.stderr)
        sys.exit(2)
    root = Path(sys.argv[1]).resolve()
    weather = root / "weather"
    sim = root / "sim"
    weather.mkdir(parents=True, exist_ok=True)
    sim.mkdir(parents=True, exist_ok=True)

    # --- Geographic store ---
    write_axis(weather, "latitude", [50.0 + k for k in range(N_LAT)])
    write_axis(weather, "longitude", [float(k) for k in range(N_LON)])
    write_axis(weather, "level", PRESSURE_LEVELS, dtype="float64")
    write_time_axis(
        weather,
        "valid_time",
        [float(t) for t in range(N_TIME)],
        "hours since 2024-01-01 00:00:00",
    )
    write_t2m_packed(weather)
    write_wind_component(weather, "u", sign=+1, standard_name="eastward_wind")
    write_wind_component(weather, "v", sign=-1, standard_name="northward_wind")

    # --- Cartesian store ---
    write_axis(sim, "x", [float(k) for k in range(N_X)])
    write_axis(sim, "y", [float(k) for k in range(N_Y)])
    write_pressure_field(sim)

    print(f"Wrote kitchen-sink Zarr v3 stores at {root}")
    print(f"  weather/")
    print(f"    t2m_packed  shape=[{N_TIME}, {N_LEVEL}, {N_LAT}, {N_LON}] int16+CF (K via scale/offset)")
    print(f"    u           shape=[{N_TIME}, {N_LAT}, {N_LON}] float32 m/s eastward")
    print(f"    v           shape=[{N_TIME}, {N_LAT}, {N_LON}] float32 m/s northward (sign-flipped)")
    print(f"    valid_time  4 hourly snapshots since 2024-01-01")
    print(f"    level       {PRESSURE_LEVELS} hPa")
    print(f"  sim/")
    print(f"    pressure_field shape=[{N_Y}, {N_X}] float32 Pa (Cartesian XYZ, SRID 0)")


if __name__ == "__main__":
    main()
