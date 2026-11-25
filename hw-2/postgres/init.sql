CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    type TEXT NOT NULL,
    seq BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    payload JSONB NOT NULL,
    consumer_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    "offset" BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_stats (
    consumer_id TEXT PRIMARY KEY,
    duplicates BIGINT NOT NULL DEFAULT 0
);
