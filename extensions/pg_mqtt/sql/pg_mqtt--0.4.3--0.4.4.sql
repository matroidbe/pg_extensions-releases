-- pg_mqtt 0.4.3 -> 0.4.4
-- Last Will, retained messages, QoS 1 and 2, PUBLISH properties and No Local.
--   retained.qos, retained.properties      how a retained message was published
--   subscriptions.retain_as_published      the subscription's Retain As Published option
--   subscriptions.no_local                 the subscription's No Local option
--   outbox.qos, outbox.retain,             carried to subscribers on other workers
--   outbox.properties, outbox.origin_client
ALTER TABLE pgmqtt.retained ADD COLUMN IF NOT EXISTS qos SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE pgmqtt.retained ADD COLUMN IF NOT EXISTS properties BYTEA;
ALTER TABLE pgmqtt.subscriptions
    ADD COLUMN IF NOT EXISTS retain_as_published BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE pgmqtt.subscriptions ADD COLUMN IF NOT EXISTS no_local BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE pgmqtt.outbox ADD COLUMN IF NOT EXISTS qos SMALLINT NOT NULL DEFAULT 0;
ALTER TABLE pgmqtt.outbox ADD COLUMN IF NOT EXISTS retain BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE pgmqtt.outbox ADD COLUMN IF NOT EXISTS properties BYTEA;
ALTER TABLE pgmqtt.outbox ADD COLUMN IF NOT EXISTS origin_client TEXT;
