CREATE TABLE IF NOT EXISTS events (
    event_id        TEXT        PRIMARY KEY,
    user_id         TEXT        NOT NULL,
    seq             BIGINT      NOT NULL,
    type            TEXT        NOT NULL,
    produced_at     TIMESTAMPTZ NOT NULL,
    pod_id          TEXT        NOT NULL,
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INT         NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    consumed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_user_seq_idx ON events (user_id, seq);
CREATE INDEX IF NOT EXISTS events_consumed_at_idx ON events (consumed_at);

CREATE TABLE IF NOT EXISTS consumer_stats (
    pod_id          TEXT        PRIMARY KEY,
    applied         BIGINT      NOT NULL DEFAULT 0,
    dedup_dropped   BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);