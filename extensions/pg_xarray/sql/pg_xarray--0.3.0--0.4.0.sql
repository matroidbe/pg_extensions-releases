-- pg_xarray 0.3.0 -> 0.4.0
--
-- The chunk dedupe key gains chunk_key. Zarr chunks share the store URI and
-- carry no byte_offset, so without it every level and spatial tile of a time
-- step collapsed into one catalog row. The new key only adds a column, so
-- existing rows cannot violate it.
DROP INDEX pgx.chunks_dedupe_idx;
CREATE UNIQUE INDEX chunks_dedupe_idx
    ON pgx.chunks (variable_id, uri, chunk_key, byte_offset, time_range) NULLS NOT DISTINCT;
