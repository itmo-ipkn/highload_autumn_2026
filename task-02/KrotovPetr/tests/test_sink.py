from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import os
import unittest
import uuid

try:
    from src.sink import Sink, claim_producer, connect, finish_producer
except ImportError:
    Sink = None

from src.config import Config
from src.events import make_event


@unittest.skipUnless(Sink is not None and os.getenv("PGHOST") == "postgres", "Requires the local Compose database")
class SinkTests(unittest.TestCase):
    def setUp(self):
        self.cfg = replace(Config.load(), run_id=f"test_{uuid.uuid4().hex}")
        claim_producer(self.cfg, 1)

    def tearDown(self):
        with connect() as conn, conn.transaction():
            for table in ("processed_events", "user_state", "producer_runs", "runs"):
                # Table names are fixed above, never supplied by a caller.
                conn.execute(f"DELETE FROM {table} WHERE run_id=%s", (self.cfg.run_id,))

    def test_concurrent_duplicate_is_inserted_once(self):
        event = make_event(self.cfg, 1, 1, 1)

        def store(_):
            sink = Sink()
            try:
                return sink.store(event, "test", "events", 0, 0)
            finally:
                sink.close()

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(store, range(8)))
        self.assertEqual(sum(results), 1)
        with connect() as conn:
            state = conn.execute("SELECT write_count, duplicates FROM user_state WHERE run_id=%s", (self.cfg.run_id,)).fetchone()
        self.assertEqual(state, (1, 7))

    def test_restart_cannot_reset_sequences(self):
        finish_producer(self.cfg, 1, "complete", 1000, 0)
        with self.assertRaises(ValueError):
            claim_producer(self.cfg, 1)

    def test_manifest_settings_cannot_change(self):
        with self.assertRaises(ValueError):
            claim_producer(replace(self.cfg, events=self.cfg.events + 1), 2)

    def test_sink_records_actual_order(self):
        sink = Sink()
        try:
            for seq in (3, 1, 2):
                sink.store(make_event(self.cfg, 1, 1, seq), "test", "events", 0, seq)
        finally:
            sink.close()
        with connect() as conn:
            rows = conn.execute("SELECT seq, write_order FROM processed_events WHERE run_id=%s ORDER BY write_order", (self.cfg.run_id,)).fetchall()
        self.assertEqual(rows, [(3, 1), (1, 2), (2, 3)])


if __name__ == "__main__":
    unittest.main()
