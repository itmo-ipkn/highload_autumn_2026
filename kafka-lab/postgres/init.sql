CREATE TABLE IF NOT EXISTS processed_events (
    event_id uuid NOT NULL,
    user_id text NOT NULL,
    seq bigint NOT NULL,
    consumer_id integer NOT NULL,
    topic text NOT NULL,
    partition_id integer NOT NULL,
    record_offset bigint NOT NULL
);
