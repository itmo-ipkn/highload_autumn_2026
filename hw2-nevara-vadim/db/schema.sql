CREATE TABLE IF NOT EXISTS producer_state (
    user_id text PRIMARY KEY,
    owner_id text NOT NULL,
    last_acked_seq bigint NOT NULL DEFAULT 0,
    pending_event jsonb
);

CREATE TABLE IF NOT EXISTS processed_events (
    write_order bigserial PRIMARY KEY,
    event_id uuid NOT NULL UNIQUE,
    user_id text NOT NULL,
    seq bigint NOT NULL,
    consumer_id text NOT NULL,
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    duplicates bigint NOT NULL DEFAULT 0,
    processed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS processing_failures (
    source_topic text NOT NULL,
    source_partition integer NOT NULL,
    source_offset bigint NOT NULL,
    attempts integer NOT NULL,
    last_error text NOT NULL,
    PRIMARY KEY (source_topic, source_partition, source_offset)
);
