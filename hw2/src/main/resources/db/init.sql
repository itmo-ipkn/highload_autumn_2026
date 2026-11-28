CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    user_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    consumer_pod_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS processed_events_user_seq_idx
    ON processed_events (user_id, seq);

CREATE TABLE IF NOT EXISTS duplicate_delivery_stats (
    id SMALLINT PRIMARY KEY CHECK (id = 1),
    dropped_duplicates BIGINT NOT NULL DEFAULT 0
);

INSERT INTO duplicate_delivery_stats (id, dropped_duplicates)
VALUES (1, 0)
ON CONFLICT (id) DO NOTHING;
