-- ============================================================================
-- pg_xarray Demo · Chapter 5 — Unstructured meshes (SELAFIN / TELEMAC)
-- ============================================================================
--
-- Hydraulic / hydrological / FEM data lives on unstructured meshes —
-- triangles or polygons, not a regular lat/lon grid. pg_xarray indexes
-- them in `pgx.meshes` / `pgx.mesh_versions` / `pgx.mesh_nodes` /
-- `pgx.mesh_cells`, and `pgx.fetch_mesh(dataset, variable, ...)` joins
-- chunk values to the mesh geometry.
--
-- This chapter uses the SELAFIN fixture from make_fixture.py — a tiny
-- 4-node, 2-triangle mesh with WATER DEPTH + VELOCITY U at two
-- timesteps. The same `pgx.register_file` call enumerates messages /
-- chunks / messages, auto-creates the mesh + version, and populates
-- nodes + cells from the file's X/Y/IKLE arrays.
--
-- ============================================================================

\set ON_ERROR_STOP on

\echo
\echo '==> Chapter 5: unstructured mesh (SELAFIN)'
\echo

-- -----------------------------------------------------------------------------
-- 5.1 — Register a SELAFIN file (auto-creates mesh + version + nodes + cells)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 5.1 Register the file — one call per variable; the mesh is created once'
\echo

SELECT pgx.register_dataset('demo-flood', 'selafin', default_srid := 0);
SELECT pgx.register_file('demo-flood', 'WATER DEPTH', 'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/flood.slf', 'selafin');
SELECT pgx.register_file('demo-flood', 'VELOCITY U',  'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/flood.slf', 'selafin');

-- -----------------------------------------------------------------------------
-- 5.2 — Inspect the mesh
-- -----------------------------------------------------------------------------
\echo
\echo '-- 5.2 Mesh nodes (X/Y → PostGIS Point):'
SELECT mn.node_id, ST_AsText(mn.geom) AS geom
FROM   pgx.mesh_nodes mn
JOIN   pgx.mesh_versions mv ON mv.id = mn.mesh_version_id
JOIN   pgx.meshes        m  ON m.id  = mv.mesh_id
JOIN   pgx.datasets      d  ON d.id  = m.dataset_id
WHERE  d.name = 'demo-flood'
ORDER  BY mn.node_id;

\echo
\echo '-- Mesh cells (triangles with node_ids[] connectivity + centroid):'
SELECT mc.cell_id, mc.node_ids, ST_AsText(mc.centroid) AS centroid
FROM   pgx.mesh_cells mc
JOIN   pgx.mesh_versions mv ON mv.id = mc.mesh_version_id
JOIN   pgx.meshes        m  ON m.id  = mv.mesh_id
JOIN   pgx.datasets      d  ON d.id  = m.dataset_id
WHERE  d.name = 'demo-flood'
ORDER  BY mc.cell_id;

-- -----------------------------------------------------------------------------
-- 5.3 — Fetch per-node values at a specific timestep
-- -----------------------------------------------------------------------------
\echo
\echo '-- 5.3 fetch_mesh — values joined to node geometry'
\echo

\echo '-- WATER DEPTH at t=0:'
SELECT node_id, geom_wkt, value::numeric(6,2) AS depth_m
FROM   pgx.fetch_mesh('demo-flood', 'WATER DEPTH',
                      at_time := '2024-01-01 00:00:00+00'::timestamptz)
ORDER  BY node_id;

\echo
\echo '-- VELOCITY U at t=01:00 — uniform 1.0 m/s in this fixture:'
SELECT node_id, geom_wkt, value::numeric(6,2) AS vel_u_mps
FROM   pgx.fetch_mesh('demo-flood', 'VELOCITY U',
                      at_time := '2024-01-01 01:00:00+00'::timestamptz)
ORDER  BY node_id;

-- -----------------------------------------------------------------------------
-- 5.4 — Spatial query: WATER DEPTH inside a bbox
-- -----------------------------------------------------------------------------
\echo
\echo '-- 5.4 Bbox prune — only nodes inside the requested envelope'
\echo

SELECT node_id, geom_wkt, value::numeric(6,2) AS depth_m
FROM   pgx.fetch_mesh('demo-flood', 'WATER DEPTH',
                      bbox_wkt := 'POLYGON((-0.1 -0.1, 1.1 -0.1, 1.1 0.5, -0.1 0.5, -0.1 -0.1))',
                      at_time  := '2024-01-01 00:00:00+00'::timestamptz)
ORDER  BY node_id;

-- -----------------------------------------------------------------------------
-- 5.5 — Export the animated water surface as glTF Binary (GLB)
-- -----------------------------------------------------------------------------
\echo
\echo '-- 5.5 Visualisation — emit an animated GLB of WATER DEPTH + flow arrows.'
\echo '--      Open the resulting file in https://gltf-viewer.donmccurdy.com/'
\echo '--      or the Khronos glTF Sample Viewer to see the colour-graded'
\echo '--      surface morph between timesteps with VELOCITY U as flow arrows.'
\echo

-- Surface coloured by WATER DEPTH, flow arrows from VELOCITY U.
-- z_scale exaggerates the displacement so the morph is visible at this fixture's
-- (4-node) scale; in production tune to taste or pass 1.0.
\echo '-- GLB byte length:'
SELECT length(pgx.xarray_to_glb(
    'demo-flood',
    'WATER DEPTH',
    flow_uv  => ARRAY['VELOCITY U'],
    z_scale  => 10.0,
    colormap => 'viridis'
)) AS glb_bytes;

-- The asset.extras block carries dataset / colormap / vmin / vmax so an
-- external viewer can render a legend without round-tripping to the catalog.
-- Write the bytes to /tmp via \lo_export-style escape if your psql supports it;
-- otherwise pipe via COPY ... TO PROGRAM or a small client.
\echo
\echo '== Try this =='
\echo '-- Write the GLB to disk (psql >= 16):'
\echo "  \\copy (SELECT pgx.xarray_to_glb('demo-flood', 'WATER DEPTH',"
\echo "                                   flow_uv => ARRAY['VELOCITY U'],"
\echo "                                   z_scale => 10.0)) TO '/tmp/flood.glb' (FORMAT binary);"
\echo
\echo '-- Then open /tmp/flood.glb at https://gltf-viewer.donmccurdy.com/'

\echo
\echo '== Try this =='
\echo '-- Cell-level features: average WATER DEPTH per triangle (manual join):'
\echo
\echo '  SELECT mc.cell_id,'
\echo '         ST_AsText(mc.centroid) AS centroid,'
\echo '         avg(v.value)::numeric(6,2) AS mean_depth_m'
\echo "  FROM   pgx.fetch_mesh('demo-flood', 'WATER DEPTH',"
\echo "                        at_time := '2024-01-01 00:00:00+00'::timestamptz) v"
\echo '  JOIN   pgx.mesh_cells mc ON v.node_id = ANY(mc.node_ids)'
\echo '  GROUP  BY mc.cell_id, mc.centroid'
\echo '  ORDER  BY mc.cell_id;'
\echo
\echo '-- The catalog model is the same for FEM exodus / ParaView XDMF / MED —'
\echo '-- each format is one walker function returning (nodes, cells, chunks).'

-- -----------------------------------------------------------------------------
-- 5.6 — A meatier SELAFIN: register, query a subset via the index, then GLB.
-- -----------------------------------------------------------------------------
--
-- `flood.slf` is a 4-node toy — useful for round-trip tests, less so for
-- visual demos. Here we generate a 231-node / 400-triangle / 30-timestep
-- synthetic SELAFIN that simulates a sine wave propagating through a
-- 200 m × 100 m channel. Real TELEMAC datasets (Malpasset dam break etc.)
-- have the same shape; swap the `\!` line below for a `curl` against any
-- open-data SELAFIN you have access to.
--
-- The narrative of this step:
--   1. Generate (or download) the file
--   2. Register it as a dataset — pgx.register_file does the mesh discovery
--   3. Use the catalog index to filter the spatial subset we care about
--   4. Export that subset to GLB and open it in a viewer
-- ============================================================================

\echo
\echo '==> Chapter 5.6: 231-node wave channel — generate, subset, export GLB'
\echo

-- 5.6.1 — Build the fixture (one-shot; idempotent, file is cached on disk).
\echo '-- 5.6.1 Generate wave_channel.slf (idempotent)'
\! /usr/bin/python3 /home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/build_wave_channel.py

-- 5.6.2 — Register each variable. Same pattern as 5.1 — the mesh is
-- created once on the first call, subsequent calls share it.
\echo
\echo '-- 5.6.2 Register the wave-channel SELAFIN'
SELECT pgx.register_dataset('wave-channel', 'selafin', default_srid := 0);
SELECT pgx.register_file(
    'wave-channel', 'WATER DEPTH',
    'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/wave_channel.slf',
    'selafin'
);
SELECT pgx.register_file(
    'wave-channel', 'VELOCITY U',
    'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/wave_channel.slf',
    'selafin'
);
SELECT pgx.register_file(
    'wave-channel', 'VELOCITY V',
    'fs:///home/ubuntu/dev/pg_extensions/extensions/pg_xarray/demo/fixtures/wave_channel.slf',
    'selafin'
);

-- 5.6.3 — Demonstrate index pruning. The mesh covers x ∈ [0, 200], y ∈ [0, 100].
-- A bbox of x ∈ [50, 150], y ∈ [20, 80] should keep ~half the nodes.
\echo
\echo '-- 5.6.3 Index-pruned subset query — node counts inside vs outside bbox:'
SELECT
    count(*) FILTER (WHERE in_bbox) AS nodes_inside,
    count(*) FILTER (WHERE NOT in_bbox) AS nodes_outside
FROM (
    SELECT
        public.ST_X(geom) BETWEEN 50 AND 150
        AND public.ST_Y(geom) BETWEEN 20 AND 80 AS in_bbox
    FROM   pgx.mesh_nodes mn
    JOIN   pgx.mesh_versions mv ON mv.id = mn.mesh_version_id
    JOIN   pgx.meshes m  ON m.id = mv.mesh_id
    JOIN   pgx.datasets d ON d.id = m.dataset_id
    WHERE  d.name = 'wave-channel'
) t;

-- The same bbox applied to pgx.fetch_mesh — the index does the pruning
-- before the chunk-decode step (see [src/srf/fetch_mesh.rs] for the JOIN
-- to mesh_nodes that drops rows outside the bbox).
\echo
\echo '-- WATER DEPTH samples inside the bbox at t=600s (5 rows):'
SELECT node_id, geom_wkt, value::numeric(6,3) AS depth_m
FROM   pgx.fetch_mesh(
           'wave-channel', 'WATER DEPTH',
           at_time  := '2024-06-01 00:10:00+00'::timestamptz,
           bbox_wkt := 'POLYGON((50 20, 150 20, 150 80, 50 80, 50 20))'
       )
ORDER  BY node_id
LIMIT  5;

-- 5.6.4 — Translate the bbox subset to an animated GLB.
\echo
\echo '-- 5.6.4 Export the bbox subset to an animated GLB'
\echo '--   • surface_var = WATER DEPTH (drives Z displacement + colour)'
\echo '--   • flow_uv     = (VELOCITY U, VELOCITY V) — animated LINES arrows'
\echo '--   • bbox_wkt    = 100 m × 60 m window centred on the channel'
\echo '--   • z_scale     = 5 — exaggerate so the wave is visible at this scale'
\echo '--   • time_scale  = 60 — sim time / 60 → 29 minutes of physics plays in 29 s'
\echo

SELECT length(pgx.xarray_to_glb(
    'wave-channel',
    'WATER DEPTH',
    flow_uv  => ARRAY['VELOCITY U', 'VELOCITY V'],
    bbox_wkt => 'POLYGON((50 20, 150 20, 150 80, 50 80, 50 20))',
    z_scale  => 5.0,
    colormap => 'viridis',
    options  => '{"arrow_scale": 8.0, "time_scale": 60}'::jsonb
)) AS bbox_glb_bytes;

\echo
\echo '== Try this =='
\echo '-- Save the full-mesh + bbox-subset GLBs side-by-side and compare:'
\echo "  \\copy (SELECT pgx.xarray_to_glb('wave-channel', 'WATER DEPTH',"
\echo "                                   flow_uv => ARRAY['VELOCITY U', 'VELOCITY V'],"
\echo "                                   z_scale => 5.0,"
\echo "                                   options => '{\"arrow_scale\": 8.0, \"time_scale\": 60}'::jsonb))"
\echo "        TO '/tmp/wave_channel_full.glb' (FORMAT binary);"
\echo
\echo "  \\copy (SELECT pgx.xarray_to_glb('wave-channel', 'WATER DEPTH',"
\echo "                                   flow_uv  => ARRAY['VELOCITY U', 'VELOCITY V'],"
\echo "                                   bbox_wkt => 'POLYGON((50 20, 150 20, 150 80, 50 80, 50 20))',"
\echo "                                   z_scale  => 5.0,"
\echo "                                   options  => '{\"arrow_scale\": 8.0, \"time_scale\": 60}'::jsonb))"
\echo "        TO '/tmp/wave_channel_bbox.glb' (FORMAT binary);"
\echo
\echo '-- Then open both at https://gltf-viewer.donmccurdy.com/ — the bbox'
\echo '-- file should contain only the centre 100 m × 60 m strip of the channel,'
\echo '-- proving the catalog index pruned the GLB output as well as the query.'

-- -----------------------------------------------------------------------------
-- 5.7 — 2D raster + WMS endpoint (GIS-tooling compliant, no GeoServer)
-- -----------------------------------------------------------------------------
--
-- Where the GLB pipeline above targets 3D viewers (three.js, glTF Sample
-- Viewer), this section covers the 2D side of the same data: PNG raster
-- output for dashboards, map portals, and GIS clients (QGIS, ArcGIS,
-- Leaflet, OpenLayers) that already speak OGC standards.
--
-- Two surfaces, same catalog:
--   1. `pgx.xarray_to_png(...)` — single-call raster export. Returns a
--      PNG bytea for the requested timestep / bbox. Composable in SQL.
--   2. WMS 1.3.0 HTTP endpoint — `pg_xarray.wms_enabled = on` in
--      postgresql.conf brings up a read-only HTTP server inside a
--      Postgres bgworker. QGIS adds it as a WMS layer directly; no
--      separate GeoServer / JVM in the loop.
--
-- The WMS server uses the same bbox + time + CRS plumbing the catalog
-- already exposes — `GetMap?BBOX=...&TIME=...` maps 1:1 onto the
-- `pgx.fetch_mesh` index. Pre-rendering / cache absorption is up to a
-- reverse proxy (every GetMap response carries `Cache-Control: max-age`).
-- ============================================================================

\echo
\echo '==> Chapter 5.7: 2D raster + WMS endpoint'
\echo

-- 5.7.1 — Single PNG via SQL.
\echo '-- 5.7.1 Render a single timestep as PNG via SQL (no HTTP needed):'
SELECT length(pgx.xarray_to_png(
    'wave-channel',
    'WATER DEPTH',
    at_time  => '2024-06-01 00:10:00+00'::timestamptz,
    bbox_wkt => 'POLYGON((50 20, 150 20, 150 80, 50 80, 50 20))',
    width    => 800,
    height   => 400,
    colormap => 'viridis'
)) AS png_bytes;

\echo
\echo '== Try this =='
\echo '-- Save a PNG of the central strip at t=10min:'
\echo "  \\copy (SELECT pgx.xarray_to_png('wave-channel', 'WATER DEPTH',"
\echo "             at_time  => '2024-06-01 00:10:00+00'::timestamptz,"
\echo "             bbox_wkt => 'POLYGON((50 20, 150 20, 150 80, 50 80, 50 20))',"
\echo "             width    => 800, height => 400)) TO '/tmp/wave_t10.png' (FORMAT binary);"
\echo

-- 5.7.2 — WMS endpoint, no separate server.
\echo
\echo '-- 5.7.2 WMS 1.3.0 endpoint — pg_xarray IS the WMS server.'
\echo '--'
\echo '-- Required postgresql.conf:'
\echo '--   shared_preload_libraries = ''pg_xarray'''
\echo '--   pg_xarray.wms_enabled    = on'
\echo '--   pg_xarray.wms_port       = 7800        # default'
\echo '--   pg_xarray.wms_bind_host  = ''127.0.0.1''  # loopback only — put nginx in front'
\echo '--   pg_xarray.wms_cache_seconds = 60       # Cache-Control max-age'
\echo '--   pg_xarray.database       = ''pg_xarray_demo''  # whichever DB has the catalog'
\echo
\echo '-- Then from the shell:'
\echo "--   curl 'http://localhost:7800/wms?SERVICE=WMS&REQUEST=GetCapabilities'"
\echo "--   curl -o tile.png 'http://localhost:7800/wms?SERVICE=WMS&REQUEST=GetMap&LAYERS=wave-channel:WATER%20DEPTH&BBOX=0,0,200,100&WIDTH=800&HEIGHT=400&CRS=EPSG:0&FORMAT=image/png&TIME=2024-06-01T00:10:00Z'"
\echo
\echo '-- Or from QGIS: Layer → Add Layer → Add WMS Layer →'
\echo "--   New Connection → URL http://localhost:7800/wms → wave-channel:WATER DEPTH"
\echo
\echo '-- Performance note: every GetMap response carries Cache-Control:'
\echo '-- max-age=N; put nginx/Varnish/CDN in front for steady-state read traffic.'
\echo '-- Single bgworker today; for >1000 tiles/sec → PG read replicas.'
