-- =============================================================================
-- example_pipeline.sql
--
-- A complete pg_streaming pipeline expressed as SQL. Reads a Zarr v3
-- store from the local filesystem (the source), fans each variable's
-- chunks out via the xarray_header processor, and upserts them into
-- pg_xarray's catalog via the xarray_index sink.
--
-- IMPORTANT: the pipeline indexes METADATA only — for each chunk it
-- writes one row into pgx.chunks recording the URI, byte range, and
-- bbox of that chunk. The actual array bytes never land in Postgres.
-- pgx.fetch reads them on demand from the original location via
-- OpenDAL when you query, so the source files must remain reachable
-- at the URI captured by xarray_index for as long as you want to query
-- them.
--
-- After this script runs, the catalog is populated and you can query
-- pgx.fetch(...) to pull cells back.
--
-- Variables substituted by test.sh (psql -v):
--   :store_root      — absolute path of the Zarr store root
--   :dataset_name    — the dataset name to register in pgx.datasets
--   :pipeline_name   — the pg_streaming pipeline name
-- =============================================================================

\set ON_ERROR_STOP on

-- Show what's registered before we start. xarray_index + xarray_header
-- come from pg_streaming compiled with --features xarray (folded into
-- the same .so, not a separate extension).
SELECT 'sinks' AS kind,      name FROM pgstreams.list_custom_sinks()
UNION ALL
SELECT 'processors' AS kind, name FROM pgstreams.list_custom_processors()
ORDER BY kind, name;

-- ---------------------------------------------------------------------
-- The actual pipeline definition.
--
-- Built with jsonb_build_object so JSON quoting is automatically safe
-- for any path string. Equivalent JSON written out:
--
--   {
--     "input": {
--       "opendal": {
--         "service": "fs",
--         "config":  {"root": "<store_root>"},
--         "path":    "t2m/zarr.json",
--         "parse_as":"bytes",
--         "mode":    "watch",
--         "watch":   {"poll": "500ms"}
--       }
--     },
--     "pipeline": {
--       "processors": [
--         { "mapping": { "uri": "'fs://<store_root>'" } },
--         { "custom":  {
--             "name":   "xarray_header",
--             "config": { "format": "zarr", "variables": ["t2m"] }
--           }
--         }
--       ]
--     },
--     "output": {
--       "custom": {
--         "name":   "xarray_index",
--         "config": {
--           "dataset":     "<dataset_name>",
--           "format":      "zarr",
--           "mesh_kind":   "regular_grid",
--           "mesh_motion": "fixed",
--           "auto_create": true
--         }
--       }
--     }
--   }
-- ---------------------------------------------------------------------
SELECT pgstreams.create_pipeline(
    :'pipeline_name',
    jsonb_build_object(
        'input', jsonb_build_object(
            'opendal', jsonb_build_object(
                'service',  'fs',
                'config',   jsonb_build_object('root', :'store_root'),
                'path',     't2m/zarr.json',
                'parse_as', 'bytes',
                'mode',     'watch',
                'watch',    jsonb_build_object('poll', '500ms')
            )
        ),
        'pipeline', jsonb_build_object(
            'processors', jsonb_build_array(
                jsonb_build_object(
                    'mapping', jsonb_build_object(
                        -- SQL string expression: emit a literal URI per record.
                        'uri', quote_literal('fs://' || :'store_root')
                    )
                ),
                jsonb_build_object(
                    'custom', jsonb_build_object(
                        'name',   'xarray_header',
                        'config', jsonb_build_object(
                            'format',    'zarr',
                            'variables', jsonb_build_array('t2m')
                        )
                    )
                )
            )
        ),
        'output', jsonb_build_object(
            'custom', jsonb_build_object(
                'name',   'xarray_index',
                'config', jsonb_build_object(
                    'dataset',     :'dataset_name',
                    'format',      'zarr',
                    'mesh_kind',   'regular_grid',
                    'mesh_motion', 'fixed',
                    'auto_create', true
                )
            )
        )
    )
);

SELECT pgstreams.start(:'pipeline_name');

\echo
\echo Pipeline started. Bg worker should now be reading the Zarr store
\echo and populating pgx.chunks. Verify with:
\echo   SELECT pgx.chunk_count(:'dataset_name');
\echo   SELECT * FROM pgx.fetch(:'dataset_name', 't2m') LIMIT 5;
