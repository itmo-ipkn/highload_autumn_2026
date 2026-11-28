import time
import psycopg
from app.common import DATABASE_URL, LOG

SCHEMA = '''
CREATE TABLE IF NOT EXISTS producer_progress (
    user_id TEXT PRIMARY KEY,
    last_seq BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sink (
    write_order BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    user_id TEXT NOT NULL,
    seq BIGINT NOT NULL,
    consumer_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    message_offset BIGINT NOT NULL,
    written_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_sink_user_seq ON sink (user_id, seq);
CREATE TABLE IF NOT EXISTS stats (
    name TEXT PRIMARY KEY,
    value BIGINT NOT NULL DEFAULT 0
);
INSERT INTO stats (name, value) VALUES ('duplicates', 0) ON CONFLICT DO NOTHING;
'''
for attempt in range(60):
    try:
        with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
            conn.execute(SCHEMA)
        LOG.info('DB schema initialized')
        break
    except psycopg.OperationalError:
        if attempt == 59: raise
        time.sleep(2)
