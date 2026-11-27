-- Sink: one row per unique event (dedup key = event_id).
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      uuid        PRIMARY KEY,
    user_id       text        NOT NULL,
    seq           bigint      NOT NULL,
    type          text        NOT NULL,
    produced_at   timestamptz NOT NULL,
    consumer_pod  text        NOT NULL,
    topic         text        NOT NULL,
    kafka_partition int       NOT NULL,
    kafka_offset  bigint      NOT NULL,
    -- monotonically increasing write order, used to detect order violations
    write_id      bigserial   NOT NULL,
    written_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS processed_events_user_write ON processed_events (user_id, write_id);

-- Every attempt to write an event_id that is already in processed_events
-- (redelivery after a crash / rebalance) is logged here; count(*) = dropped duplicates.
CREATE TABLE IF NOT EXISTS duplicate_deliveries (
    id            bigserial   PRIMARY KEY,
    event_id      uuid        NOT NULL,
    user_id       text        NOT NULL,
    seq           bigint      NOT NULL,
    consumer_pod  text        NOT NULL,
    topic         text        NOT NULL,
    kafka_partition int       NOT NULL,
    kafka_offset  bigint      NOT NULL,
    seen_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);
