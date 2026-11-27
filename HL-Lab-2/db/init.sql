CREATE TABLE processed_events (
    sink_order BIGINT GENERATED ALWAYS AS IDENTITY,
    event_id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    seq BIGINT NOT NULL,
    consumer_pod TEXT NOT NULL,
    source_topic TEXT NOT NULL,
    source_partition INTEGER NOT NULL,
    source_offset BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE duplicate_attempts (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    count BIGINT NOT NULL DEFAULT 0
);

INSERT INTO duplicate_attempts (id) VALUES (1);
