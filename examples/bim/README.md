# BIM: IFC Import with pg_solid

Import IFC (Industry Foundation Classes) building models into PostgreSQL with full semantic metadata and 3D geometry stored as `pg_solid` Solid types.

**Pure SQL** — no Python, no ifcopenshell, no external dependencies. The IFC parser runs entirely inside the pg_solid extension.

## Features

- **Type-to-table mapping** — IfcWall -> `walls`, IfcSlab -> `slabs`, etc. Overridable via SQL.
- **Adjacency list** — All IFC relationships in `ifc_edges` for graph traversal via recursive CTEs.
- **Spatial structure** — Building hierarchy (project -> site -> building -> storey).
- **3D geometry** — Elements parsed to `Solid` type for volume, distance, intersection queries.
- **Catch-all** — Unmapped types go to the `elements` table.

## Quick Start

A test file `minimal_building.ifc` is included. It contains a project with one site, one building, one storey, and a wall with geometry, material, and properties.

```bash
# Apply schema (creates tables + type mappings)
psql -d mydb -f schema.sql

# Import the test file (use absolute path readable by the PostgreSQL server)
psql -d mydb -v ifc_file="'$(pwd)/minimal_building.ifc'" -f import.sql
```

## Usage

```bash
# Import any IFC file (path must be readable by the PostgreSQL server process)
psql -d mydb -v ifc_file="'/path/to/model.ifc'" -f import.sql
```

Or directly in psql:

```sql
-- Parse elements directly (no persistent tables needed)
SELECT ifc_type, name, solid_volume(solid)
FROM pgsolid.ifc_elements('/path/to/model.ifc')
WHERE solid IS NOT NULL;

-- View spatial hierarchy
SELECT * FROM pgsolid.ifc_spatial_structure('/path/to/model.ifc');

-- View relationships
SELECT rel_type, count(*)
FROM pgsolid.ifc_relationships('/path/to/model.ifc')
GROUP BY rel_type;
```

## SQL Functions

pg_solid provides three table-returning functions for IFC files:

| Function | Returns |
|----------|---------|
| `ifc_elements(filepath)` | global_id, ifc_type, name, material, storey, solid, properties |
| `ifc_relationships(filepath)` | source_id, target_id, rel_type, ordinal, properties |
| `ifc_spatial_structure(filepath)` | global_id, ifc_type, name, parent_global_id, elevation |

These parse the IFC file on every call. For repeated queries, import into tables using `import.sql`.

## Override Mappings

Mappings live in the `ifc_type_mapping` table. Change them before import:

```sql
-- Route walls to a custom table
UPDATE ifc_type_mapping SET target_table = 'load_bearing_walls' WHERE ifc_type = 'IfcWall';

-- Add a new mapping
INSERT INTO ifc_type_mapping (ifc_type, target_table) VALUES ('IfcPipe', 'pipes');
```

## Example Queries

### Basic element queries

```sql
-- All walls with volume
SELECT name, solid_volume(solid) FROM walls;

-- Element count by type table
SELECT 'walls' AS source, count(*) FROM walls
UNION ALL SELECT 'slabs', count(*) FROM slabs
UNION ALL SELECT 'columns', count(*) FROM columns;

-- Total volume per storey
SELECT storey, sum(solid_volume(solid)) AS total_volume
FROM walls GROUP BY storey;
```

### Spatial queries with pg_solid

```sql
-- Find walls near a point
SELECT name, solid_distance(solid, solid_box(0.01, 0.01, 0.01)) AS dist
FROM walls ORDER BY dist LIMIT 10;

-- Walls that intersect a region
SELECT name FROM walls
WHERE solid_intersects(solid, solid_translate(solid_box(5, 5, 3), 10, 0, 0));
```

### Graph traversal (adjacency list)

```sql
-- All elements contained in a storey
SELECT target_id FROM ifc_edges
WHERE source_id = '0004_STOREY__GUID'
  AND rel_type = 'IfcRelContainedInSpatialStructure';

-- What fills the openings in a wall?
SELECT w.name AS wall, d.name AS door
FROM ifc_edges void_rel
JOIN ifc_edges fill_rel ON void_rel.target_id = fill_rel.source_id
JOIN walls w ON void_rel.source_id = w.global_id
JOIN doors d ON fill_rel.target_id = d.global_id
WHERE void_rel.rel_type = 'IfcRelVoidsElement'
  AND fill_rel.rel_type = 'IfcRelFillsElement';

-- Recursive: all elements reachable from a starting element
WITH RECURSIVE graph AS (
    SELECT target_id, rel_type, 1 AS depth
    FROM ifc_edges WHERE source_id = '0004_STOREY__GUID'
    UNION ALL
    SELECT e.target_id, e.rel_type, g.depth + 1
    FROM ifc_edges e JOIN graph g ON e.source_id = g.target_id
    WHERE g.depth < 10
)
SELECT * FROM graph;

-- Building hierarchy
WITH RECURSIVE tree AS (
    SELECT global_id, name, ifc_type, 0 AS depth
    FROM ifc_spatial_structure WHERE parent_global_id IS NULL
    UNION ALL
    SELECT s.global_id, s.name, s.ifc_type, t.depth + 1
    FROM ifc_spatial_structure s JOIN tree t ON s.parent_global_id = t.global_id
)
SELECT repeat('  ', depth) || name AS hierarchy, ifc_type FROM tree;
```

## Schema Overview

| Table | Purpose |
|-------|---------|
| `ifc_type_mapping` | IFC type -> table routing |
| `ifc_spatial_structure` | Building hierarchy (project/site/building/storey) |
| `ifc_edges` | Adjacency list for all IFC relationships |
| `elements` | Catch-all for unmapped IFC types |
| `walls`, `slabs`, ... | Auto-created per mapping, all share same schema |
