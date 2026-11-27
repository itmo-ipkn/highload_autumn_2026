-- id - порядок записи в sink: по нему verify считает нарушения порядка.
CREATE TABLE IF NOT EXISTS events (
    id           bigserial PRIMARY KEY,
    event_id     uuid        NOT NULL UNIQUE,
    user_id      text        NOT NULL,
    seq          bigint      NOT NULL,
    type         text        NOT NULL,
    poison       boolean     NOT NULL DEFAULT false,
    producer_id  text        NOT NULL,
    consumer_id  text        NOT NULL,
    topic        text        NOT NULL,
    partition    integer     NOT NULL,
    "offset"     bigint      NOT NULL,
    produced_at  timestamptz NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS events_user_id_id ON events (user_id, id);

-- Повторные доставки, отброшенные по event_id.
CREATE TABLE IF NOT EXISTS duplicates (
    id           bigserial PRIMARY KEY,
    event_id     uuid        NOT NULL,
    consumer_id  text        NOT NULL,
    topic        text        NOT NULL,
    partition    integer     NOT NULL,
    "offset"     bigint      NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
