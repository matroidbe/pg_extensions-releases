#!/usr/bin/env python3
"""build_wave_channel.py — generate a meaty synthetic SELAFIN file for
the §5.6 demo step.

This stands in for a "downloaded" TELEMAC file because real-world
open-data SELAFINs (Malpasset dam break, etc.) live inside the
openTELEMAC source tarball without stable per-file URLs. The
generated file uses the exact same binary format (big-endian Fortran
sequential unformatted records per the TELEMAC v8 spec), so the
pgx.register_file('selafin') path treats it identically to anything
the official TELEMAC toolchain would produce.

Output: `demo/fixtures/wave_channel.slf`
  - 21 × 11 = 231 nodes on a 200 m × 100 m channel (Δ = 10 m)
  - 400 triangles (each rectangular cell split diagonally)
  - 30 timesteps, 60 s apart  → 0 s .. 1740 s
  - Variables:
      * WATER DEPTH (m)  — propagating sine wave in x
      * VELOCITY U (m/s) — wave-derived horizontal velocity
      * VELOCITY V (m/s) — zero (1-D wave; here for the flow-arrows pair)

Pure stdlib — no numpy, no pyproj, no telemac-mascaret install.

Usage:
    /usr/bin/python3 demo/build_wave_channel.py
"""

from __future__ import annotations

import math
import struct
import sys
from pathlib import Path


# Mesh + wave parameters — tweak here, not in the demo SQL.
NX = 21                 # nodes in x  → 0, 10, 20, ..., 200 m
NY = 11                 # nodes in y  → 0, 10, 20, ..., 100 m
DX = 10.0               # m
DY = 10.0               # m
N_STEPS = 30
DT = 60.0               # s

MEAN_DEPTH = 2.0        # m
AMPLITUDE = 0.6         # m
WAVELENGTH = 80.0       # m
PERIOD = 600.0          # s  → propagates eastward at 80/600 ≈ 0.13 m/s
WAVE_SPEED = WAVELENGTH / PERIOD  # phase speed for U calc


def main() -> int:
    out_path = Path(__file__).parent / "fixtures" / "wave_channel.slf"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_selafin(out_path)
    print(f"Wrote SELAFIN fixture: {out_path}")
    print(
        f"  {NX*NY} nodes, {2 * (NX - 1) * (NY - 1)} triangles, "
        f"{N_STEPS} timesteps, 3 variables"
    )
    return 0


def write_selafin(path: Path) -> None:
    def rec(payload: bytes) -> bytes:
        """Wrap in Fortran sequential length-prefix + length-suffix."""
        n = struct.pack(">I", len(payload))
        return n + payload + n

    def s32(s: str) -> bytes:
        """Pad / truncate a string to exactly 32 ASCII bytes."""
        b = s.encode("ascii")
        return b[:32].ljust(32, b" ")

    # ---- nodes + triangulation ----
    xs: list[float] = []
    ys: list[float] = []
    for j in range(NY):
        for i in range(NX):
            xs.append(i * DX)
            ys.append(j * DY)
    npoin = len(xs)
    assert npoin == NX * NY

    # Triangulate each grid cell into 2 triangles. Node IDs are 1-based
    # in the IKLE record per TELEMAC convention.
    ikle: list[int] = []
    for j in range(NY - 1):
        for i in range(NX - 1):
            a = j * NX + i + 1
            b = j * NX + (i + 1) + 1
            c = (j + 1) * NX + i + 1
            d = (j + 1) * NX + (i + 1) + 1
            # Two CCW triangles: (a, b, d), (a, d, c)
            ikle.extend([a, b, d, a, d, c])
    nelem = 2 * (NX - 1) * (NY - 1)
    ndp = 3
    assert len(ikle) == nelem * ndp

    out = bytearray()
    # 1) Title — 80 bytes
    out += rec(b"pg_xarray wave_channel.slf (synthetic)".ljust(80, b" "))
    # 2) NBV1 (number of physical vars), NBV2 (clipped vars; 0 for us)
    out += rec(struct.pack(">ii", 3, 0))
    # 3) Variable names + units — 3 * 32 bytes
    out += rec(
        s32("WATER DEPTH     M")
        + s32("VELOCITY U      M/S")
        + s32("VELOCITY V      M/S")
    )
    # 4) IPARAM (10 i32). IPARAM[10]=1 → DATE record follows (1-indexed
    #    in TELEMAC docs == [9] zero-indexed).
    iparam = [0] * 10
    iparam[9] = 1
    out += rec(struct.pack(">10i", *iparam))
    # 4b) DATE — year/month/day/hour/min/sec
    out += rec(struct.pack(">6i", 2024, 6, 1, 0, 0, 0))
    # 5) NELEM, NPOIN, NDP, _ikle_extra
    out += rec(struct.pack(">4i", nelem, npoin, ndp, 1))
    # 6) IKLE — 1-based vertex indices, 3 per triangle, big-endian i32.
    out += rec(struct.pack(">%di" % len(ikle), *ikle))
    # 7) IPOBO — boundary flags (all 0 = interior for this demo).
    out += rec(struct.pack(">%di" % npoin, *([0] * npoin)))
    # 8) X coords
    out += rec(struct.pack(">%df" % npoin, *xs))
    # 9) Y coords
    out += rec(struct.pack(">%df" % npoin, *ys))

    # ---- per-timestep records ----
    # Wave equation: η(x, t) = A * sin(2π (x/L - t/T))
    #   depth(x, t) = mean + η
    #   U(x, t)     = (c/h) * η ≈ (c/mean) * η for shallow water linearisation
    #   V(x, t)     = 0
    two_pi = 2.0 * math.pi
    for step in range(N_STEPS):
        t_secs = step * DT
        out += rec(struct.pack(">f", t_secs))

        depth = []
        vel_u = []
        vel_v = []
        for j in range(NY):
            for i in range(NX):
                x = i * DX
                phase = two_pi * (x / WAVELENGTH - t_secs / PERIOD)
                eta = AMPLITUDE * math.sin(phase)
                d = MEAN_DEPTH + eta
                u = (WAVE_SPEED / MEAN_DEPTH) * eta * 10.0  # exaggerate for arrows
                depth.append(d)
                vel_u.append(u)
                vel_v.append(0.0)

        out += rec(struct.pack(">%df" % npoin, *depth))
        out += rec(struct.pack(">%df" % npoin, *vel_u))
        out += rec(struct.pack(">%df" % npoin, *vel_v))

    with open(str(path), "wb") as f:
        f.write(bytes(out))


if __name__ == "__main__":
    sys.exit(main())
