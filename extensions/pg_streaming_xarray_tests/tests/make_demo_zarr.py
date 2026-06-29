#!/usr/bin/env python3
"""
make_demo_zarr.py — generate a tiny synthetic Zarr v3 store on disk.

Used by test.sh's demo step to produce a real file tree that
example_pipeline.sql can ingest. Standard library only — no Python
package install required.

The store layout matches what pg_xarray's reader + the xarray_header
processor expect:

    <root>/
        t2m/
            zarr.json         (array metadata; bytes codec, f32 LE)
            c/0/0             (single chunk; 3 lat × 4 lon = 12 cells)
        latitude/
            zarr.json
            c/0               (3 lats: 50.0, 51.0, 52.0)
        longitude/
            zarr.json
            c/0               (4 lons: 2.0, 3.0, 4.0, 5.0)

Cell values are deterministic: value[j][i] = j * 4 + i (0..11).

Usage:
    python3 make_demo_zarr.py <root_dir>
"""
import json
import os
import struct
import sys
from pathlib import Path


N_LAT = 3
N_LON = 4


def write_array(
    path: Path,
    shape: list[int],
    chunk_shape: list[int],
    values: bytes,
    dim_names: list[str] | None = None,
):
    path.mkdir(parents=True, exist_ok=True)
    meta = {
        "zarr_format": 3,
        "node_type": "array",
        "shape": shape,
        "data_type": "float32",
        "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": chunk_shape}},
        "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
        "fill_value": 0,
        "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
    }
    if dim_names is not None:
        meta["dimension_names"] = dim_names
    (path / "zarr.json").write_text(json.dumps(meta))

    # Single-chunk store: write the values at c/0 (for 1D) or c/0/0 (for 2D).
    chunk_dir = path / "c"
    for idx in [0] * len(shape):
        chunk_dir = chunk_dir / str(idx)
    chunk_dir.parent.mkdir(parents=True, exist_ok=True)
    chunk_dir.write_bytes(values)


def main():
    if len(sys.argv) != 2:
        print("usage: make_demo_zarr.py <root_dir>", file=sys.stderr)
        sys.exit(2)
    root = Path(sys.argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=True)

    # t2m: 3 × 4 grid of f32, values = j*4 + i
    t2m_bytes = b"".join(
        struct.pack("<f", float(j * N_LON + i)) for j in range(N_LAT) for i in range(N_LON)
    )
    write_array(
        root / "t2m",
        shape=[N_LAT, N_LON],
        chunk_shape=[N_LAT, N_LON],
        values=t2m_bytes,
        dim_names=["latitude", "longitude"],
    )

    # 1D coord axes
    lat_bytes = b"".join(struct.pack("<f", 50.0 + k) for k in range(N_LAT))
    write_array(root / "latitude", shape=[N_LAT], chunk_shape=[N_LAT], values=lat_bytes)

    lon_bytes = b"".join(struct.pack("<f", 2.0 + k) for k in range(N_LON))
    write_array(root / "longitude", shape=[N_LON], chunk_shape=[N_LON], values=lon_bytes)

    print(f"Wrote synthetic Zarr v3 store at {root}")
    print(f"  Variable t2m  shape=[{N_LAT}, {N_LON}] values=0..{N_LAT * N_LON - 1}")
    print(f"  Coord latitude  values={[50.0 + k for k in range(N_LAT)]}")
    print(f"  Coord longitude values={[2.0 + k for k in range(N_LON)]}")


if __name__ == "__main__":
    main()
