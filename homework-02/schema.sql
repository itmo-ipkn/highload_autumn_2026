CREATE TABLE generated (
  user_id text PRIMARY KEY,
  seq bigint NOT NULL,
  pending text
);
CREATE TABLE sink (
  write_no bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id uuid UNIQUE NOT NULL,
  user_id text NOT NULL,
  seq bigint NOT NULL,
  pod int NOT NULL,
  topic text NOT NULL,
  partition_id int NOT NULL,
  kafka_offset bigint NOT NULL
);
CREATE INDEX ON sink(user_id, seq);
CREATE TABLE duplicates (pod int PRIMARY KEY, total bigint NOT NULL);
