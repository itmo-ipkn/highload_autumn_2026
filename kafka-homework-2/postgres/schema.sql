CREATE TABLE producer_progress (
    user_id text PRIMARY KEY,
    mode text NOT NULL CHECK (mode IN ('user_id', 'none')),
    confirmed_seq integer NOT NULL DEFAULT 0 CHECK (confirmed_seq >= 0),
    pending jsonb
);

CREATE TABLE processed_events (
    write_id bigint GENERATED ALWAYS AS IDENTITY,
    event_id uuid PRIMARY KEY,
    user_id text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('user_id', 'none')),
    seq integer NOT NULL CHECK (seq > 0),
    event_type text NOT NULL,
    device_code text NOT NULL,
    consumer_id integer NOT NULL,
    topic text NOT NULL,
    partition integer NOT NULL,
    kafka_offset bigint NOT NULL
);

CREATE INDEX processed_events_user_order ON processed_events (user_id, write_id);

CREATE TABLE duplicate_deliveries (
    event_id uuid PRIMARY KEY REFERENCES processed_events (event_id),
    count bigint NOT NULL CHECK (count > 0)
);
