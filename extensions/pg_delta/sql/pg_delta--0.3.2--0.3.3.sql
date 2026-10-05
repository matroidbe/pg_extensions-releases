-- pg_delta 0.3.2 -> 0.3.3
-- Background workers apply pg_reload_conf(), restart after termination, and
-- are supervised: backoff, a failed state, worker_status() and
-- reset_workers() (design/bgworker-supervision).

CREATE FUNCTION "worker_status"() RETURNS TABLE (
	"worker" INT,
	"name" TEXT,
	"state" TEXT,
	"failures" INT,
	"restarts" INT,
	"pid" INT,
	"since" timestamp with time zone,
	"last_failure" TEXT
)
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'worker_status_wrapper';

CREATE FUNCTION "reset_workers"() RETURNS INT
STRICT
LANGUAGE c
AS 'MODULE_PATHNAME', 'reset_workers_wrapper';
