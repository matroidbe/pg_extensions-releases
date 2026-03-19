-- BIM Schema for IFC Import (pure SQL — no Python required)
-- Requires: pg_solid extension
--
-- Tables:
--   ifc_type_mapping     — configurable IFC type → table routing
--   ifc_spatial_structure — building hierarchy (project → site → building → storey)
--   ifc_edges            — adjacency list for all IFC relationships
--   elements             — catch-all table for unmapped IFC types
--
-- Element tables (walls, slabs, columns, ...) are auto-created by import.sql
-- based on the mappings in ifc_type_mapping.

CREATE EXTENSION IF NOT EXISTS pg_solid;

-- ============================================================
-- Type Mapping: IFC type → target table (SQL-overridable)
-- ============================================================

CREATE TABLE IF NOT EXISTS ifc_type_mapping (
    ifc_type      TEXT PRIMARY KEY,       -- e.g. IfcWall, IfcSlab
    target_table  TEXT NOT NULL           -- e.g. walls, slabs
);

-- Standard defaults (override with UPDATE, extend with INSERT)
INSERT INTO ifc_type_mapping (ifc_type, target_table) VALUES
    ('IfcWall',                  'walls'),
    ('IfcWallStandardCase',      'walls'),
    ('IfcSlab',                  'slabs'),
    ('IfcColumn',                'columns'),
    ('IfcBeam',                  'beams'),
    ('IfcDoor',                  'doors'),
    ('IfcWindow',                'windows'),
    ('IfcStair',                 'stairs'),
    ('IfcRoof',                  'roofs'),
    ('IfcPlate',                 'plates'),
    ('IfcMember',                'members'),
    ('IfcRailing',               'railings'),
    ('IfcCurtainWall',           'curtain_walls'),
    ('IfcCovering',              'coverings'),
    ('IfcFurnishingElement',     'furniture'),
    ('IfcBuildingElementProxy',  'proxies')
ON CONFLICT (ifc_type) DO NOTHING;

-- ============================================================
-- Spatial Structure: building hierarchy
-- ============================================================

CREATE TABLE IF NOT EXISTS ifc_spatial_structure (
    id                SERIAL PRIMARY KEY,
    global_id         TEXT NOT NULL UNIQUE,
    ifc_type          TEXT NOT NULL,         -- IfcProject, IfcSite, IfcBuilding, IfcBuildingStorey
    name              TEXT,
    parent_global_id  TEXT,
    elevation         DOUBLE PRECISION       -- storey elevation (meters)
);

CREATE INDEX IF NOT EXISTS idx_spatial_parent
    ON ifc_spatial_structure (parent_global_id);

-- ============================================================
-- Adjacency List: all IFC relationships
-- ============================================================

CREATE TABLE IF NOT EXISTS ifc_edges (
    id          SERIAL PRIMARY KEY,
    source_id   TEXT NOT NULL,              -- global_id of relating element
    target_id   TEXT NOT NULL,              -- global_id of related element
    rel_type    TEXT NOT NULL,              -- IfcRelVoidsElement, IfcRelAggregates, ...
    ordinal     INT,                        -- ordering within relationship group
    properties  JSONB
);

CREATE INDEX IF NOT EXISTS idx_edges_source  ON ifc_edges (source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target  ON ifc_edges (target_id);
CREATE INDEX IF NOT EXISTS idx_edges_reltype ON ifc_edges (rel_type);

-- ============================================================
-- Catch-all element table for unmapped IFC types
-- ============================================================

CREATE TABLE IF NOT EXISTS elements (
    id          SERIAL PRIMARY KEY,
    global_id   TEXT NOT NULL UNIQUE,
    ifc_type    TEXT NOT NULL,
    name        TEXT,
    material    TEXT,
    storey      TEXT,
    solid       pgsolid.Solid,
    properties  JSONB
);

CREATE INDEX IF NOT EXISTS idx_elements_type ON elements (ifc_type);
