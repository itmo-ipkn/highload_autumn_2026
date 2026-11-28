CREATE TABLE IF NOT EXISTS events (
    event_id     TEXT PRIMARY KEY,
    user_id      TEXT        NOT NULL,
    seq          BIGINT      NOT NULL,
    type         TEXT        NOT NULL,
    produced_at  TIMESTAMPTZ NOT NULL,
    payload      JSONB,
    processed_by INT         NOT NULL,
    topic        TEXT        NOT NULL,
    partition    INT         NOT NULL,
    kafka_offset       BIGINT      NOT NULL,
    inserted_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_user_seq_idx      ON events (user_id, seq);
CREATE INDEX IF NOT EXISTS events_user_inserted_idx ON events (user_id, inserted_at);

CREATE TABLE IF NOT EXISTS metrics (
    id         INT PRIMARY KEY DEFAULT 1,
    duplicates BIGINT NOT NULL DEFAULT 0
);
INSERT INTO metrics (id, duplicates) VALUES (1, 0) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS poison_attempts (
    event_id   TEXT PRIMARY KEY,
    attempts   INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
