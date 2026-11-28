CREATE TABLE IF NOT EXISTS events_sink (
    write_order  SERIAL PRIMARY KEY,
    event_id     UUID NOT NULL UNIQUE,
    user_id      TEXT NOT NULL,
    seq          BIGINT NOT NULL CHECK (seq >= 1),
    consumer_id  TEXT NOT NULL,
    topic        TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id    BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_events_sink_user_seq
    ON events_sink (user_id, seq);

CREATE TABLE IF NOT EXISTS consumer_stats (
    consumer_id         TEXT PRIMARY KEY,
    duplicates_dropped  BIGINT NOT NULL DEFAULT 0
);
