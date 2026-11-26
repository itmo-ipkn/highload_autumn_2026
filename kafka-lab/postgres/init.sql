BEGIN;

CREATE TABLE IF NOT EXISTS processed_events (
    event_id uuid NOT NULL,
    user_id text NOT NULL,
    seq bigint NOT NULL,
    consumer_id integer NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    record_offset bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS processed_events_event_id_unique
    ON processed_events (event_id);

CREATE TABLE IF NOT EXISTS consumer_counters (
    name text PRIMARY KEY,
    value bigint NOT NULL
);

INSERT INTO consumer_counters (name, value)
VALUES ('discarded_duplicates', 0)
ON CONFLICT (name) DO NOTHING;

COMMIT;
