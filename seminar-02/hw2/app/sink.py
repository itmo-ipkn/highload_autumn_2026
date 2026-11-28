"""Postgres sink: one row per processed event, idempotent by event_id.

Idempotency = `INSERT ... ON CONFLICT (event_id) DO NOTHING`; rowcount == 0 means
the event_id was already stored, i.e. this delivery was a duplicate dropped by
deduplication (counted per pod in dedup_stats).

deliveries keeps every attempt (including duplicates) with a monotonic
attempt_seq, so `verify` can measure per-user write order (out-of-order count).
poison_attempts keeps attempts per (topic, partition, offset) so the "3 attempts"
claim is verifiable after the run.
"""
from __future__ import annotations

import os
import time

import psycopg

SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS delivery_seq;

CREATE TABLE IF NOT EXISTS events (
    event_id     uuid PRIMARY KEY,
    user_id      text NOT NULL,
    seq          int  NOT NULL,
    pod_id       text NOT NULL,
    topic        text NOT NULL,
    "partition"  int  NOT NULL,
    kafka_offset bigint NOT NULL,
    processed_at timestamptz NOT NULL,
    inserted_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS events_user_seq_idx ON events (user_id, seq);

CREATE TABLE IF NOT EXISTS deliveries (
    attempt_seq   bigint PRIMARY KEY DEFAULT nextval('delivery_seq'),
    event_id      uuid NOT NULL,
    user_id       text NOT NULL,
    seq           int  NOT NULL,
    pod_id        text NOT NULL,
    topic         text NOT NULL,
    "partition"   int  NOT NULL,
    kafka_offset  bigint NOT NULL,
    is_duplicate  boolean NOT NULL DEFAULT false,
    inserted_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS deliveries_user_order_idx ON deliveries (user_id, attempt_seq);

CREATE TABLE IF NOT EXISTS dedup_stats (
    pod_id   text PRIMARY KEY,
    attempts int NOT NULL DEFAULT 0,
    dropped  int NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS poison_attempts (
    topic        text NOT NULL,
    "partition"  int NOT NULL,
    kafka_offset bigint NOT NULL,
    pod_id       text NOT NULL,
    attempts     int NOT NULL DEFAULT 0,
    PRIMARY KEY (topic, "partition", kafka_offset)
);

CREATE TABLE IF NOT EXISTS producer_state (
    pod_id    text NOT NULL,
    user_id   text NOT NULL,
    last_seq  int NOT NULL,
    PRIMARY KEY (pod_id, user_id)
);
"""


def conninfo() -> str:
    return (
        f"host={os.environ.get('PGHOST', 'postgres')} port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ.get('PGDATABASE', 'lab2')} user={os.environ.get('PGUSER', 'lab2')} "
        f"password={os.environ.get('PGPASSWORD', 'lab2-demo-password')}"
    )


class Sink:
    def __init__(self, connect_retries: int = 30) -> None:
        last_error = None
        for attempt in range(1, connect_retries + 1):
            try:
                # autocommit: psycopg3's `with conn:` closes the connection at block
                # exit, so we never use it as a context manager here.
                self.conn = psycopg.connect(conninfo(), autocommit=True)
                break
            except psycopg.Error as exc:
                last_error = exc
                time.sleep(min(2 ** min(attempt, 5), 5))
        else:
            raise SystemExit(f'sink: cannot connect to Postgres: {last_error}')
        # Several pods run the schema concurrently; IF NOT EXISTS can still race on
        # the catalog (unique violation), so each statement is retried a few times.
        for statement in [s for s in SCHEMA.split(';') if s.strip()]:
            for attempt in range(1, 6):
                try:
                    self.conn.execute(statement)
                    break
                except (psycopg.errors.UniqueViolation, psycopg.errors.DuplicateTable,
                        psycopg.errors.DuplicateObject, psycopg.errors.DeadlockDetected) as exc:
                    if attempt == 5:
                        raise
                    time.sleep(0.2 * attempt)

    def record_delivery(self, event_id: str, user_id: str, seq: int, pod_id: str,
                        topic: str, partition: int, offset: int, processed_at: str) -> bool:
        """Insert one attempt; returns True when it was a duplicate (dedup drop).

        The events insert runs first; its rowcount tells whether event_id was new.
        Every attempt is still recorded in deliveries (audit + attempt ordering),
        and dedup_stats counts dropped duplicates per pod.
        """
        # autocommit is on, so each execute is its own transaction; the connection
        # must never be used as a context manager (psycopg3 closes it on block exit).
        cur = self.conn.execute(
            'INSERT INTO events (event_id, user_id, seq, pod_id, topic, "partition", '
            'kafka_offset, processed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) '
            'ON CONFLICT (event_id) DO NOTHING',
            (event_id, user_id, seq, pod_id, topic, partition, offset, processed_at))
        duplicate = cur.rowcount == 0
        self.conn.execute(
            'INSERT INTO deliveries (event_id, user_id, seq, pod_id, topic, "partition", '
            'kafka_offset, is_duplicate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)',
            (event_id, user_id, seq, pod_id, topic, partition, offset, duplicate))
        self.conn.execute(
            'INSERT INTO dedup_stats (pod_id, attempts, dropped) VALUES (%s, 1, %s) '
            'ON CONFLICT (pod_id) DO UPDATE SET attempts = dedup_stats.attempts + 1, '
            'dropped = dedup_stats.dropped + %s',
            (pod_id, int(duplicate), int(duplicate)))
        return duplicate

    def persist_state(self, pod_id: str, user_id: str, last_seq: int) -> None:
        self.conn.execute(
            'INSERT INTO producer_state (pod_id, user_id, last_seq) VALUES (%s, %s, %s) '
            'ON CONFLICT (pod_id, user_id) DO UPDATE SET last_seq = EXCLUDED.last_seq',
            (pod_id, user_id, last_seq))

    def load_state(self, pod_id: str, users: list[str]) -> dict[str, int]:
        rows = self.conn.execute(
            'SELECT user_id, last_seq FROM producer_state WHERE pod_id = %s', (pod_id,)).fetchall()
        return {r[0]: r[1] for r in rows}

    def record_attempt(self, topic: str, partition: int, offset: int, pod_id: str) -> int:
        row = self.conn.execute(
            'INSERT INTO poison_attempts (topic, "partition", kafka_offset, pod_id, attempts) '
            'VALUES (%s, %s, %s, %s, 1) ON CONFLICT (topic, "partition", kafka_offset) '
            'DO UPDATE SET attempts = poison_attempts.attempts + 1 RETURNING attempts',
            (topic, partition, offset, pod_id)).fetchone()
        return row[0]
