CREATE TABLE IF NOT EXISTS sink_events (
    id BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    user_id VARCHAR(64) NOT NULL,
    seq BIGINT NOT NULL,
    event_type VARCHAR(32) NOT NULL,
    processor_pod VARCHAR(64) NOT NULL,
    topic VARCHAR(128) NOT NULL,
    partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    produced_at TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    payload JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sink_user_seq ON sink_events (user_id, seq);
CREATE INDEX IF NOT EXISTS idx_sink_user_id_order ON sink_events (user_id, id);

CREATE TABLE IF NOT EXISTS dedup_stats (
    stat_key VARCHAR(64) PRIMARY KEY,
    rejected_duplicates BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO dedup_stats (stat_key, rejected_duplicates)
VALUES ('global', 0)
ON CONFLICT (stat_key) DO NOTHING;
