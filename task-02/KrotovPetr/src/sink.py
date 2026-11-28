"""Only the isolated PostgreSQL service named 'postgres' is accepted."""
import os

import psycopg


def connect():
    if os.getenv("PGHOST") != "postgres" or os.getenv("PGDATABASE") != "events":
        raise ValueError("Only the lab's postgres/events database is supported")
    return psycopg.connect(
        autocommit=True, connect_timeout=5,
        options="-c statement_timeout=10000 -c lock_timeout=5000",
    )


def claim_producer(cfg, producer_id):
    manifest = (cfg.users, cfg.events, cfg.key_mode, cfg.poison_rate, cfg.seed)
    with connect() as conn, conn.transaction():
        conn.execute(
            "INSERT INTO runs (run_id, users_per_producer, events_per_user, key_mode, poison_rate, seed) "
            "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (cfg.run_id, *manifest),
        )
        existing = conn.execute(
            "SELECT users_per_producer, events_per_user, key_mode, poison_rate, seed "
            "FROM runs WHERE run_id = %s", (cfg.run_id,),
        ).fetchone()
        if existing != manifest:
            raise ValueError("RUN_ID already exists with different settings; choose a new RUN_ID")
        claimed = conn.execute(
            "INSERT INTO producer_runs (run_id, producer_id, status) VALUES (%s, %s, 'running') "
            "ON CONFLICT DO NOTHING RETURNING producer_id", (cfg.run_id, producer_id),
        ).fetchone()
        if not claimed:
            raise ValueError("This producer has already run; use a new RUN_ID instead of resetting seq")


def finish_producer(cfg, producer_id, status, acknowledged, errors):
    with connect() as conn:
        conn.execute(
            "UPDATE producer_runs SET status=%s, acknowledged=%s, errors=%s, "
            "updated_at=clock_timestamp() WHERE run_id=%s AND producer_id=%s",
            (status, acknowledged, errors, cfg.run_id, producer_id),
        )


class Sink:
    def __init__(self):
        self.conn = None

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def store(self, event, consumer_id, topic, partition, offset):
        if self.conn is None or self.conn.closed:
            self.conn = connect()
        user = event["user_id"]
        run_id = event["payload"]["run_id"]
        with self.conn.transaction():
            self.conn.execute(
                "INSERT INTO user_state (user_id, run_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (user, run_id),
            )
            count = self.conn.execute(
                "SELECT write_count FROM user_state WHERE user_id=%s FOR UPDATE", (user,),
            ).fetchone()[0]
            inserted = self.conn.execute(
                "INSERT INTO processed_events "
                "(event_id, run_id, user_id, seq, consumer_id, topic, partition, kafka_offset, write_order) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (event_id) DO NOTHING RETURNING event_id",
                (event["event_id"], run_id, user, event["seq"], consumer_id, topic, partition, offset, count + 1),
            ).fetchone()
            if inserted:
                self.conn.execute("UPDATE user_state SET write_count=write_count+1 WHERE user_id=%s", (user,))
            else:
                previous = self.conn.execute(
                    "SELECT run_id, user_id, seq FROM processed_events WHERE event_id=%s",
                    (event["event_id"],),
                ).fetchone()
                if previous != (run_id, user, event["seq"]):
                    raise ValueError("event_id collision with different event data")
                self.conn.execute("UPDATE user_state SET duplicates=duplicates+1 WHERE user_id=%s", (user,))
        return inserted is not None
