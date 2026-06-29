#!/usr/bin/env python3
"""make_fixture.py — kitchen-sink Zarr v3 fixture for pg_xarray's
end-to-end test.sh. Same shape as the visible demo fixture in
pg_streaming_xarray_tests, but lives here so pg_xarray's tests are
self-contained (no pg_streaming dependency).

Standard library only. Writes two stores under <root>/:

    weather/   (geographic, default SRID 4326)
      t2m_packed     4D int16 + CF (scale=0.01, offset=273.15, fill=-9999)
                     dims: (valid_time, level, latitude, longitude)
      u, v           3D float32 wind components (sign-flipped v)
      valid_time     "hours since 2024-01-01 00:00:00"
      level          [1000, 850, 500, 250] hPa
      latitude       50..52
      longitude      0..3

    sim/       (Cartesian SRID 0)
      pressure_field 2D float32 (y, x)
      x              0..3 (m)
      y              0..2 (m)
"""
import json
import struct
import sys
from pathlib import Path


N_TIME, N_LEVEL, N_LAT, N_LON = 4, 4, 3, 4
N_Y, N_X = 3, 4
PRESSURE_LEVELS = [1000.0, 850.0, 500.0, 250.0]


def write_meta(path, meta):
    path.mkdir(parents=True, exist_ok=True)
    (path / "zarr.json").write_text(json.dumps(meta))


def write_chunk(var_dir, chunk_index, bytes_):
    chunk_dir = var_dir / "c"
    for idx in chunk_index[:-1]:
        chunk_dir = chunk_dir / str(idx)
    chunk_dir.mkdir(parents=True, exist_ok=True)
    (chunk_dir / str(chunk_index[-1])).write_bytes(bytes_)


def write_axis(store_root, name, values, dtype="float32"):
    dir_ = store_root / name
    n = len(values)
    write_meta(
        dir_,
        {
            "zarr_format": 3,
            "node_type": "array",
            "shape": [n],
            "data_type": dtype,
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [n]}},
            "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
            "fill_value": 0,
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
        },
    )
    fmt = "<f" if dtype == "float32" else "<d"
    write_chunk(dir_, [0], b"".join(struct.pack(fmt, v) for v in values))


def write_time_axis(store_root, name, values, units):
    dir_ = store_root / name
    n = len(values)
    write_meta(
        dir_,
        {
            "zarr_format": 3,
            "node_type": "array",
            "shape": [n],
            "data_type": "float64",
            "chunk_grid": {"name": "regular", "configuration": {"chunk_shape": [n]}},
            "chunk_key_encoding": {"name": "default", "configuration": {"separator": "/"}},
            "fill_value": 0,
            "codecs": [{"name": "bytes", "configuration": {"endian": "little"}}],
            "attributes": {"units": units},
        },
    )
    write_chunk(dir_, [0], b"".join(struct.pack("<d", v) for v in values))


def write_t2m_packed(store_root):
    """Cell formula: stored[t][k][j][i] = t*100 + k*10 + j*4 + i;
    physical = stored * 0.01 + 273.15 (CF packing)."""
    dir_ = store_root / "t2m_packed"
    write_meta(
        dir_,
        {
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
        },
    )
    for t in range(N_TIME):
        for k in range(N_LEVEL):
            payload = bytearray()
            for j in range(N_LAT):
                for i in range(N_LON):
                    stored = t * 100 + k * 10 + j * 4 + i
                    payload.extend(struct.pack("<h", stored))
            write_chunk(dir_, [t, k, 0, 0], bytes(payload))


def write_wind_component(store_root, name, sign, standard_name):
    """u/v: values[t][j][i] = sign * (t*100 + j*4 + i)."""
    dir_ = store_root / name
    write_meta(
        dir_,
        {
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
            "attributes": {"units": "m s-1", "standard_name": standard_name},
        },
    )
    for t in range(N_TIME):
        payload = bytearray()
        for j in range(N_LAT):
            for i in range(N_LON):
                v = float(sign * (t * 100 + j * 4 + i))
                payload.extend(struct.pack("<f", v))
        write_chunk(dir_, [t, 0, 0], bytes(payload))


def write_pressure_field(store_root):
    dir_ = store_root / "pressure_field"
    write_meta(
        dir_,
        {
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
            "attributes": {"units": "Pa", "standard_name": "pressure"},
        },
    )
    payload = bytearray()
    for j in range(N_Y):
        for i in range(N_X):
            payload.extend(struct.pack("<f", float(j * N_X + i)))
    write_chunk(dir_, [0, 0], bytes(payload))


def main():
    if len(sys.argv) != 2:
        print("usage: make_fixture.py <root>", file=sys.stderr)
        sys.exit(2)
    root = Path(sys.argv[1]).resolve()
    weather = root / "weather"
    sim = root / "sim"
    weather.mkdir(parents=True, exist_ok=True)
    sim.mkdir(parents=True, exist_ok=True)

    write_axis(weather, "latitude", [50.0 + k for k in range(N_LAT)])
    write_axis(weather, "longitude", [float(k) for k in range(N_LON)])
    write_axis(weather, "level", PRESSURE_LEVELS, dtype="float64")
    write_time_axis(weather, "valid_time",
                    [float(t) for t in range(N_TIME)],
                    "hours since 2024-01-01 00:00:00")
    write_t2m_packed(weather)
    write_wind_component(weather, "u", +1, "eastward_wind")
    write_wind_component(weather, "v", -1, "northward_wind")

    write_axis(sim, "x", [float(k) for k in range(N_X)])
    write_axis(sim, "y", [float(k) for k in range(N_Y)])
    write_pressure_field(sim)

    # NetCDF fixtures:
    #   * weather.nc          — NC3 classic, contiguous (one HDF5-equivalent
    #                           chunk = whole variable). Exercises the V1
    #                           "register one chunk per variable" fallback.
    #   * weather_chunked.nc  — NC4 with explicit HDF5 chunking. Exercises
    #                           the V2 "one catalog row per HDF5 chunk"
    #                           slicing path — the only way 100 GB ERA5
    #                           files become tractable.
    write_nc(root / "weather.nc", chunked=False)
    write_nc(root / "weather_chunked.nc", chunked=True)

    # GRIB2 fixture — two messages of the same parameter (TMP at level
    # 500 hPa, two forecast hours) so walk_grib produces two catalog
    # rows. Generated via eccodes' GRIB2.tmpl sample.
    write_grib2(root / "weather.grib2")

    # SELAFIN fixture — TELEMAC's binary unstructured-mesh format.
    # 4 nodes / 2 triangles, two variables ("WATER DEPTH" and
    # "VELOCITY U"), two timesteps. Pure stdlib (struct + bytes) — no
    # external lib needed.
    write_selafin(root / "flood.slf")

    print(f"Wrote pg_xarray fixture at {root}")


def write_selafin(path):
    """Build a tiny SELAFIN file with 4 nodes, 2 triangles, two
    variables (WATER DEPTH, VELOCITY U), two timesteps. Big-endian
    Fortran sequential unformatted records per TELEMAC spec."""

    def rec(payload: bytes) -> bytes:
        """Wrap a record in Fortran sequential length prefix + suffix."""
        n = struct.pack(">I", len(payload))
        return n + payload + n

    def s32(s: str) -> bytes:
        """Pad / truncate a string to exactly 32 ASCII bytes."""
        b = s.encode("ascii")
        return b[:32].ljust(32, b" ")

    out = bytearray()
    # 1) Title — 80 bytes
    out += rec(b"pg_xarray e2e SELAFIN fixture".ljust(80, b" "))
    # 2) NBV1, NBV2 — 2 big-endian int32
    out += rec(struct.pack(">ii", 2, 0))
    # 3) Variable names + units — 2 * 32 bytes
    out += rec(s32("WATER DEPTH     M") + s32("VELOCITY U      M/S"))
    # 4) IPARAM (10 i32). IPARAM[10]=1 → DATE record follows. (1-indexed
    #    in TELEMAC docs == [9] zero-indexed.)
    iparam = [0] * 10
    iparam[9] = 1
    out += rec(struct.pack(">10i", *iparam))
    # 4b) DATE — year/month/day/hour/min/sec
    out += rec(struct.pack(">6i", 2024, 1, 1, 0, 0, 0))
    # 5) NELEM, NPOIN, NDP, _ikle_extra
    nelem, npoin, ndp = 2, 4, 3
    out += rec(struct.pack(">4i", nelem, npoin, ndp, 1))
    # 6) IKLE (1-based) — two triangles {1,2,3} and {1,3,4}
    ikle = [1, 2, 3,
            1, 3, 4]
    out += rec(struct.pack(">%di" % (nelem * ndp), *ikle))
    # 7) IPOBO — boundary flags (all 0 = interior; harmless for tests)
    out += rec(struct.pack(">%di" % npoin, *[0] * npoin))
    # 8) X coords
    xs = [0.0, 1.0, 1.0, 0.0]
    out += rec(struct.pack(">%df" % npoin, *xs))
    # 9) Y coords
    ys = [0.0, 0.0, 1.0, 1.0]
    out += rec(struct.pack(">%df" % npoin, *ys))

    # Timesteps. t = 0s and t = 3600s (1h after DATE epoch).
    # WATER DEPTH (depth in m): linear ramp with node index, growing
    # slightly over time so the test can distinguish.
    # VELOCITY U (m/s): zero everywhere at t=0, 1.0 everywhere at t=1h.
    for step, t_secs in enumerate([0.0, 3600.0]):
        out += rec(struct.pack(">f", t_secs))
        depth = [1.0 + node + 0.1 * step for node in range(npoin)]  # 1..4, 1.1..4.1
        out += rec(struct.pack(">%df" % npoin, *depth))
        vel_u = [0.0 if step == 0 else 1.0] * npoin
        out += rec(struct.pack(">%df" % npoin, *vel_u))

    with open(str(path), "wb") as f:
        f.write(bytes(out))


def write_grib2(path):
    """Build a 2-message GRIB2 file using eccodes' GRIB2 sample template.
    Both messages encode 2-metre temperature on a small lat/lon grid at
    pressure level 500 hPa, one at forecast hour 0 and one at hour 3.
    """
    import eccodes as ec
    import numpy as np

    # Tiny 3 x 4 lat/lon grid matching the Zarr fixture's spatial extent.
    n_lat, n_lon = 3, 4
    lat_first, lat_last = 52.0, 50.0  # GRIB convention: first lat = north
    lon_first, lon_last = 0.0, 3.0
    base_date = "20240101"  # YYYYMMDD
    base_time = "0000"      # HHMM

    with open(str(path), "wb") as out:
        for fhour in (0, 3):
            handle = ec.codes_grib_new_from_samples("GRIB2")
            try:
                # Header — minimal GRIB2 conventions.
                ec.codes_set(handle, "centre", "ecmf")
                ec.codes_set(handle, "dataDate", int(base_date))
                ec.codes_set(handle, "dataTime", int(base_time))
                ec.codes_set(handle, "forecastTime", fhour)
                ec.codes_set(handle, "indicatorOfUnitOfTimeRange", 1)  # hours

                # Parameter: 2-metre temperature (discipline=0, category=0,
                # number=0 is generic Temperature; we use that with a
                # level + units that say "K").
                ec.codes_set(handle, "discipline", 0)
                ec.codes_set(handle, "parameterCategory", 0)  # Meteorological
                ec.codes_set(handle, "parameterNumber", 0)    # Temperature

                # Vertical level: isobaric surface 500 hPa.
                ec.codes_set(handle, "typeOfFirstFixedSurface", 100)  # isobaric
                ec.codes_set(handle, "scaleFactorOfFirstFixedSurface", 0)
                ec.codes_set(handle, "scaledValueOfFirstFixedSurface", 50000)  # Pa

                # Grid: regular lat/lon, 3 x 4.
                ec.codes_set(handle, "gridType", "regular_ll")
                ec.codes_set(handle, "Ni", n_lon)
                ec.codes_set(handle, "Nj", n_lat)
                ec.codes_set(handle, "latitudeOfFirstGridPointInDegrees", lat_first)
                ec.codes_set(handle, "longitudeOfFirstGridPointInDegrees", lon_first)
                ec.codes_set(handle, "latitudeOfLastGridPointInDegrees", lat_last)
                ec.codes_set(handle, "longitudeOfLastGridPointInDegrees", lon_last)
                ec.codes_set(handle, "iDirectionIncrementInDegrees", 1.0)
                ec.codes_set(handle, "jDirectionIncrementInDegrees", 1.0)
                ec.codes_set(handle, "iScansNegatively", 0)
                ec.codes_set(handle, "jScansPositively", 0)

                # Values — same shape as the NC fixture so users can
                # compare physical numbers across formats.
                values = np.zeros((n_lat, n_lon), dtype=np.float64)
                for j in range(n_lat):
                    for i in range(n_lon):
                        # GRIB orders rows top-to-bottom (north → south),
                        # so j=0 == lat 52, j=2 == lat 50. Use the
                        # northern-grid index to keep numbers comparable.
                        lat_idx_from_south = n_lat - 1 - j
                        values[j, i] = 275.0 + 0.1 * fhour + 0.05 * lat_idx_from_south + 0.05 * i
                ec.codes_set_values(handle, values.flatten())

                msg = ec.codes_get_message(handle)
                out.write(msg)
            finally:
                ec.codes_release(handle)


def write_nc(path, chunked):
    """Write a CF-compliant NetCDF file with a t2m variable + lat/lon/time
    coord vars via netCDF4-python (handles NC3 + NC4 + HDF5 chunking).
    When `chunked=True` writes NC4 with a (1, lat, 2) chunk shape →
    multiple HDF5 chunks per file so the per-chunk catalog pruning
    actually has something to prune."""
    import netCDF4
    import numpy as np
    fmt = "NETCDF4" if chunked else "NETCDF3_64BIT_OFFSET"
    nc = netCDF4.Dataset(str(path), "w", format=fmt)
    nc.history = "pg_xarray e2e fixture"
    nc.createDimension("time", N_TIME)
    nc.createDimension("latitude", N_LAT)
    nc.createDimension("longitude", N_LON)

    time_v = nc.createVariable("time", "f8", ("time",))
    time_v.units = "hours since 2024-01-01 00:00:00"
    time_v.standard_name = "time"
    time_v[:] = np.arange(N_TIME, dtype=np.float64)

    lat_v = nc.createVariable("latitude", "f4", ("latitude",))
    lat_v.units = "degrees_north"
    lat_v.standard_name = "latitude"
    lat_v[:] = np.array([50.0 + k for k in range(N_LAT)], dtype=np.float32)

    lon_v = nc.createVariable("longitude", "f4", ("longitude",))
    lon_v.units = "degrees_east"
    lon_v.standard_name = "longitude"
    lon_v[:] = np.array([float(k) for k in range(N_LON)], dtype=np.float32)

    # int16 + CF packing — same conventions the Zarr fixture uses.
    # Chunk shape (1, N_LAT, 2): one chunk per (time, lon-strip) pair →
    # 4 time * 2 lon-strips = 8 HDF5 chunks for the 4 x 3 x 4 file.
    create_kwargs = {"fill_value": np.int16(-9999)}
    if chunked:
        create_kwargs["chunksizes"] = (1, N_LAT, 2)
    t2m = nc.createVariable(
        "t2m", "i2", ("time", "latitude", "longitude"), **create_kwargs
    )
    t2m.units = "K"
    t2m.standard_name = "air_temperature"
    t2m.long_name = "2-metre temperature"
    t2m.scale_factor = np.float64(0.01)
    t2m.add_offset = np.float64(273.15)
    # netCDF4-python auto-PACKS on write when scale_factor + add_offset
    # are set as attributes — give it the physical values directly and
    # it stores (physical - add_offset) / scale_factor under the hood.
    # Assigning pre-packed int16 would double-pack and silently corrupt.
    phys = np.zeros((N_TIME, N_LAT, N_LON), dtype=np.float64)
    for t in range(N_TIME):
        for j in range(N_LAT):
            for i in range(N_LON):
                phys[t, j, i] = 275.0 + 0.1 * t + 0.05 * j + 0.05 * i
    t2m[:] = phys
    nc.close()


if __name__ == "__main__":
    main()
