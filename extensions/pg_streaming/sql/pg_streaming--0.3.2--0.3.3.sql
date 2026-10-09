-- pg_streaming 0.3.2 -> 0.3.3
-- Raw-zone ingest: the opendal source gains `order: lexicographic` (a
-- constant-size, row-precise cursor), `parse_as: parquet` and `parse_as:
-- listing`; S3 is a default backend. replay() rewinds a stopped pipeline's
-- source cursor (sovereign-data-platform.md, Pillar 1 "The raw zone").

CREATE FUNCTION "replay"(
	"name" TEXT,
	"after" TEXT DEFAULT NULL
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'replay_wrapper';
