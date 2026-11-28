CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    users_per_producer INTEGER NOT NULL CHECK (users_per_producer > 0),
    events_per_user INTEGER NOT NULL CHECK (events_per_user > 0),
    key_mode TEXT NOT NULL CHECK (key_mode IN ('user_id', 'none')),
    poison_rate DOUBLE PRECISION NOT NULL CHECK (poison_rate BETWEEN 0 AND 1),
    seed BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS producer_runs (
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    producer_id INTEGER NOT NULL CHECK (producer_id BETWEEN 1 AND 3),
    status TEXT NOT NULL CHECK (status IN ('running', 'complete', 'interrupted', 'failed')),
    acknowledged BIGINT NOT NULL DEFAULT 0,
    errors BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, producer_id)
);

CREATE TABLE IF NOT EXISTS user_state (
    user_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    write_count BIGINT NOT NULL DEFAULT 0,
    duplicates BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS processed_events (
    event_id UUID PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES runs(run_id),
    user_id TEXT NOT NULL REFERENCES user_state(user_id),
    seq BIGINT NOT NULL CHECK (seq > 0),
    consumer_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    write_order BIGINT NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (user_id, write_order)
);
CREATE INDEX IF NOT EXISTS processed_events_run_user
    ON processed_events (run_id, user_id, write_order);
