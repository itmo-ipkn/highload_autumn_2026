CREATE TABLE IF NOT EXISTS sink_events (
    event_id      UUID        PRIMARY KEY,
    write_id      BIGSERIAL   NOT NULL,
    user_id       UUID        NOT NULL,
    seq           BIGINT      NOT NULL,
    type          TEXT        NOT NULL,
    produced_at   TIMESTAMPTZ NOT NULL,
    consumer_pod  INT         NOT NULL,
    src_topic     TEXT        NOT NULL,
    src_partition INT         NOT NULL,
    src_offset    BIGINT      NOT NULL,
    written_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
CREATE INDEX IF NOT EXISTS sink_events_user_seq ON sink_events (user_id, seq);
CREATE INDEX IF NOT EXISTS sink_events_write_id ON sink_events (write_id);

CREATE TABLE IF NOT EXISTS sink_counters (
    name  TEXT   PRIMARY KEY,
    value BIGINT NOT NULL
);