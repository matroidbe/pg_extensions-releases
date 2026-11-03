-- pg_image 0.3.0 -> 0.3.1
-- Recreate the PostGIS bridge with schema-qualified PostGIS names (#181).
-- Also creates it for databases where PostGIS was installed after pg_image.

DO $bridge$
DECLARE
    gis text;
BEGIN
    -- Only create PostGIS bridge functions if PostGIS is available. During
    -- CREATE EXTENSION the search_path is pgimg alone, so every PostGIS name
    -- is qualified with the schema PostGIS lives in.
    SELECT extnamespace::regnamespace::text INTO gis
    FROM pg_extension WHERE extname = 'postgis';

    IF gis IS NOT NULL THEN
        EXECUTE format($fn$
            CREATE OR REPLACE FUNCTION pgimg.img_gps_point(data bytea) RETURNS %1$s.geometry AS $$
                SELECT CASE
                    WHEN pgimg.img_gps_lon(data) IS NOT NULL AND pgimg.img_gps_lat(data) IS NOT NULL
                    THEN %1$s.ST_SetSRID(%1$s.ST_MakePoint(pgimg.img_gps_lon(data), pgimg.img_gps_lat(data)), 4326)
                    ELSE NULL
                END;
            $$ LANGUAGE SQL IMMUTABLE STRICT;
        $fn$, gis);

        EXECUTE format($fn$
            CREATE OR REPLACE FUNCTION pgimg.img_gps_pointz(data bytea) RETURNS %1$s.geometry AS $$
                SELECT CASE
                    WHEN pgimg.img_gps_lon(data) IS NOT NULL AND pgimg.img_gps_lat(data) IS NOT NULL
                    THEN %1$s.ST_SetSRID(%1$s.ST_MakePoint(
                        pgimg.img_gps_lon(data),
                        pgimg.img_gps_lat(data),
                        COALESCE(pgimg.img_gps_altitude(data), 0)
                    ), 4326)
                    ELSE NULL
                END;
            $$ LANGUAGE SQL IMMUTABLE STRICT;
        $fn$, gis);

        RAISE NOTICE 'pg_image: PostGIS bridge functions created (img_gps_point, img_gps_pointz)';
    ELSE
        RAISE NOTICE 'pg_image: PostGIS not found, skipping bridge functions. Install PostGIS, then DROP and CREATE EXTENSION pg_image to enable them.';
    END IF;
END
$bridge$;
