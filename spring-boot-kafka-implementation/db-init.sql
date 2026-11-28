CREATE TABLE IF NOT EXISTS dlt_events (
  id bigserial PRIMARY KEY,
  event_id uuid NOT NULL UNIQUE,
  user_id varchar(64) NOT NULL,
  seq bigint NOT NULL,
  event_type varchar(32) NOT NULL,
  processor_pod varchar(64) NOT NULL,
  dlt_topic varchar(128) NOT NULL DEFAULT 'events.dlt',
  dlt_partition integer NOT NULL,
  dlt_offset bigint NOT NULL,
  original_topic varchar(128) NOT NULL DEFAULT 'events',
  original_partition integer NOT NULL,
  original_offset bigint NOT NULL,
  error_reason text NOT NULL,
  retry_attempts integer NOT NULL DEFAULT 3,
  headers jsonb NOT NULL,
  payload jsonb NOT NULL,
  produced_at timestamptz NOT NULL,
  routed_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dlt_user_seq ON dlt_events (user_id, seq);

CREATE TABLE IF NOT EXISTS kafka_log_records (
  id bigserial PRIMARY KEY,
  topic varchar(128) NOT NULL DEFAULT 'events',
  partition integer NOT NULL,
  kafka_offset bigint NOT NULL,
  record_key varchar(64),
  event_id uuid NOT NULL,
  user_id varchar(64) NOT NULL,
  seq bigint NOT NULL,
  event_type varchar(32) NOT NULL,
  producer_pod varchar(64) NOT NULL,
  is_poison boolean NOT NULL DEFAULT false,
  retry_count integer NOT NULL DEFAULT 0,
  status varchar(32) NOT NULL DEFAULT 'PENDING',
  produced_at timestamptz NOT NULL DEFAULT now(),
  payload jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kafka_log_tp_offset ON kafka_log_records (topic, partition, kafka_offset);

CREATE TABLE IF NOT EXISTS cluster_state (
  state_key varchar(64) PRIMARY KEY,
  key_mode varchar(32) NOT NULL DEFAULT 'user_id',
  processing_delay_ms integer NOT NULL DEFAULT 80,
  producer_rate_ms integer NOT NULL DEFAULT 250,
  poison_probability_pct integer NOT NULL DEFAULT 2,
  github_login varchar(128) NOT NULL DEFAULT 'itmo-ddia-student',
  brokers_json jsonb NOT NULL,
  producers_json jsonb NOT NULL,
  consumers_json jsonb NOT NULL,
  partitions_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS system_logs (
  id bigserial PRIMARY KEY,
  component varchar(64) NOT NULL,
  level varchar(16) NOT NULL,
  message text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sink_user_seq ON sink_events (user_id, seq);
CREATE INDEX IF NOT EXISTS idx_sink_user_id_order ON sink_events (user_id, id);
