-- pg_mqtt 0.3.2 -> 0.4.0
-- mqtt_publish() reaches connected subscribers, and MQTT publishes reach
-- subscribers on other workers, through a fan-out queue every worker polls
-- (design/pg_mqtt/fanout.md, #26).

CREATE UNLOGGED TABLE IF NOT EXISTS outbox (
    id BIGSERIAL PRIMARY KEY,
    xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    topic TEXT NOT NULL,
    payload BYTEA,
    origin_worker INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_outbox_xid ON outbox(xid);
