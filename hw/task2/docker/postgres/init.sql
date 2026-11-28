CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    seq BIGINT NOT NULL CHECK (seq >= 1),
    consumer_id INTEGER NOT NULL,
    topic TEXT NOT NULL,
    "partition" INTEGER NOT NULL CHECK ("partition" >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE processed_events
    ADD COLUMN IF NOT EXISTS sink_seq BIGSERIAL;

CREATE INDEX IF NOT EXISTS idx_processed_events_user_seq
    ON processed_events (user_id, seq);

CREATE INDEX IF NOT EXISTS idx_processed_events_processed_at
    ON processed_events (processed_at);

CREATE INDEX IF NOT EXISTS idx_processed_events_sink_seq
    ON processed_events (sink_seq);

CREATE TABLE IF NOT EXISTS consumer_stats (
    consumer_id INTEGER PRIMARY KEY,
    duplicate_count BIGINT NOT NULL DEFAULT 0 CHECK (duplicate_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
