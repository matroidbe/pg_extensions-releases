-- IFC Import — pure SQL (no Python, no ifcopenshell)
--
-- Imports an IFC building model into the BIM schema using pg_solid's
-- built-in ifc_elements(), ifc_relationships(), and ifc_spatial_structure()
-- functions. These parse the IFC file entirely within the extension.
--
-- Prerequisites:
--   1. pg_solid extension installed
--   2. schema.sql applied (creates tables + type mappings)
--
-- Usage:
--   psql -d mydb -v ifc_file="'/path/to/model.ifc'" -f import.sql
--
-- The :ifc_file variable must be a path readable by the PostgreSQL server process.

SET search_path TO public, pgsolid;

-- ============================================================
-- 1. Spatial structure
-- ============================================================

INSERT INTO ifc_spatial_structure (global_id, ifc_type, name, parent_global_id, elevation)
SELECT global_id, ifc_type, name, parent_global_id, elevation
FROM pgsolid.ifc_spatial_structure(:ifc_file)
ON CONFLICT (global_id) DO UPDATE SET
    ifc_type          = EXCLUDED.ifc_type,
    name              = EXCLUDED.name,
    parent_global_id  = EXCLUDED.parent_global_id,
    elevation         = EXCLUDED.elevation;

-- ============================================================
-- 2. Relationships (adjacency list)
-- ============================================================

INSERT INTO ifc_edges (source_id, target_id, rel_type, ordinal, properties)
SELECT source_id, target_id, rel_type, ordinal, properties
FROM pgsolid.ifc_relationships(:ifc_file);

-- ============================================================
-- 3. Elements — route to mapped tables via DO block
-- ============================================================
-- For each IFC type that has a mapping in ifc_type_mapping, create
-- the target table (if needed) and INSERT elements into it.
-- Unmapped types go into the catch-all 'elements' table.

DO $import$
DECLARE
    r RECORD;
    tbl TEXT;
    cnt INT := 0;
BEGIN
    -- Create mapped tables that don't exist yet
    FOR r IN SELECT DISTINCT target_table FROM ifc_type_mapping LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I (
                id          SERIAL PRIMARY KEY,
                global_id   TEXT NOT NULL UNIQUE,
                ifc_type    TEXT NOT NULL,
                name        TEXT,
                material    TEXT,
                storey      TEXT,
                solid       pgsolid.Solid,
                properties  JSONB
            )', r.target_table);
        EXECUTE format(
            'CREATE INDEX IF NOT EXISTS idx_%I_type ON %I (ifc_type)',
            r.target_table, r.target_table);
    END LOOP;

    -- Insert elements into their mapped tables
    FOR r IN
        SELECT
            e.global_id, e.ifc_type, e.name, e.material, e.storey, e.solid, e.properties,
            COALESCE(m.target_table, 'elements') AS target_table
        FROM pgsolid.ifc_elements(:ifc_file) e
        LEFT JOIN ifc_type_mapping m ON m.ifc_type = e.ifc_type
    LOOP
        EXECUTE format(
            'INSERT INTO %I (global_id, ifc_type, name, material, storey, solid, properties)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (global_id) DO UPDATE SET
                ifc_type   = EXCLUDED.ifc_type,
                name       = EXCLUDED.name,
                material   = EXCLUDED.material,
                storey     = EXCLUDED.storey,
                solid      = EXCLUDED.solid,
                properties = EXCLUDED.properties',
            r.target_table)
        USING r.global_id, r.ifc_type, r.name, r.material, r.storey, r.solid, r.properties;
        cnt := cnt + 1;
    END LOOP;

    RAISE NOTICE 'Imported % elements', cnt;
END
$import$;

-- ============================================================
-- 4. Summary
-- ============================================================

SELECT 'spatial_structure' AS table_name, count(*) AS rows FROM ifc_spatial_structure
UNION ALL
SELECT 'ifc_edges', count(*) FROM ifc_edges
UNION ALL
SELECT 'elements', count(*) FROM elements;
