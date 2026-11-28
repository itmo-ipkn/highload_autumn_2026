import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from unittest.mock import Mock, patch
from uuid import UUID

from confluent_kafka import TopicPartition

from app import verify


EVENT_ID = UUID("e7185266-90e6-42cd-8d33-a6710e15764c")


class SummaryTests(unittest.TestCase):
    def test_sink_and_dlt_fill_sequences_and_report_missing_tail(self):
        stored = [
            verify.StoredEvent(1, "p1-u1", 1, 2),
            verify.StoredEvent(2, "p1-u1", 3, 0),
        ]
        dead_letter = verify.DeadLetter(("events", 1, 2), (EVENT_ID, "p1-u1", 2))

        report = verify.summarize({"p1-u1": 5}, stored, [dead_letter, dead_letter])

        self.assertEqual(report["missing_sequences"], 2)
        self.assertEqual(report["missing_ranges"], [{"user_id": "p1-u1", "start": 4, "end": 5}])
        self.assertEqual(report["filtered_duplicates"], 2)
        self.assertEqual(report["dlt_events"], 1)
        self.assertEqual(report["dlt_duplicates"], 1)
        self.assertEqual(report["write_order_violations"], 0)

    def test_inversions_count_pairs_separately_for_each_user(self):
        stored = [
            verify.StoredEvent(1, "p1-u1", 4, 0),
            verify.StoredEvent(2, "p1-u2", 1, 0),
            verify.StoredEvent(3, "p1-u1", 3, 0),
            verify.StoredEvent(4, "p1-u1", 1, 0),
            verify.StoredEvent(5, "p1-u1", 2, 0),
        ]

        report = verify.summarize({"p1-u1": 4, "p1-u2": 1}, stored, [])

        self.assertEqual(report["missing_sequences"], 0)
        self.assertEqual(report["write_order_violations"], 5)
        self.assertEqual(report["late_events"], 3)


class VerifyTests(unittest.TestCase):
    def run_check(self, initial_end=3, final_end=3, group_offset=3, pending=(), dlt=()):
        reader = Mock()
        reader.committed.return_value = [TopicPartition("events", 0, group_offset)]
        with (
            patch.object(verify, "topic_bounds", side_effect=[
                {0: (0, initial_end)},
                {0: (0, 0)},
                {0: (0, final_end)},
            ]),
            patch.object(verify, "read_dlt", return_value=dlt),
            patch.object(verify, "read_db", return_value=(
                {"p1-u1": 1},
                pending,
                [verify.StoredEvent(1, "p1-u1", 1, 0)],
            )),
        ):
            return verify.check(reader, "unused", final=True)

    def test_final_incomplete_if_producer_has_pending_event(self):
        report, exit_code = self.run_check(pending=["p2-u1"])

        self.assertEqual((report["status"], exit_code), ("INCOMPLETE", 2))
        self.assertEqual(report["pending_producers"], ["p2-u1"])

    def test_final_incomplete_if_group_is_behind_source_end(self):
        report, exit_code = self.run_check(group_offset=2)

        self.assertEqual((report["status"], exit_code), ("INCOMPLETE", 2))
        self.assertEqual(report["group_lag"], {0: 1})

    def test_final_incomplete_if_source_grew_in_dlt_scan(self):
        report, exit_code = self.run_check(final_end=4)

        self.assertEqual((report["status"], exit_code), ("INCOMPLETE", 2))
        self.assertTrue(report["source_changed"])

    def test_final_pass_when_drained(self):
        report, exit_code = self.run_check()

        self.assertEqual((report["status"], exit_code), ("PASS", 0))

    def test_final_fails_if_dlt_record_was_not_identifiable(self):
        report, exit_code = self.run_check(dlt=[verify.DeadLetter(("events", 1, 2), None)])

        self.assertEqual((report["status"], exit_code), ("FAIL", 1))

    def test_fixed_dlt_end_excludes_appends(self):
        reader = Mock()
        beyond_end = Mock()
        beyond_end.error.return_value = None
        beyond_end.partition.return_value = 0
        beyond_end.offset.return_value = 8
        in_range = Mock()
        in_range.error.return_value = None
        in_range.partition.return_value = 1
        in_range.offset.return_value = 5
        in_range.headers.return_value = [
            ("source-topic", b"events"),
            ("source-partition", b"2"),
            ("source-offset", b"4"),
        ]
        in_range.value.return_value = json.dumps({
            "event_id": str(EVENT_ID),
            "user_id": "p1-u1",
            "seq": 2,
        }).encode()
        reader.poll.side_effect = [beyond_end, in_range]

        letters = verify.read_dlt(reader, {0: (7, 8), 1: (5, 6), 2: (0, 0)})

        self.assertEqual(letters, [verify.DeadLetter(("events", 2, 4), (EVENT_ID, "p1-u1", 2))])
        offsets = reader.assign.call_args.args[0]
        self.assertEqual([(offset.partition, offset.offset) for offset in offsets], [(0, 7), (1, 5)])

    def test_login_and_timezone_are_on_first_stdout_line(self):
        output = io.StringIO()
        with (
            patch("sys.argv", ["verify", "--final"]),
            patch.dict("os.environ", {
                "GITHUB_LOGIN": "test-login",
                "KAFKA_BOOTSTRAP_SERVERS": "unused",
                "DATABASE_URL": "unused",
            }),
            patch.object(verify, "Consumer"),
            patch.object(verify, "check", return_value=({"status": "PASS"}, 0)),
            redirect_stdout(output),
        ):
            exit_code = verify.main()

        self.assertEqual(exit_code, 0)
        first, second = output.getvalue().splitlines()
        self.assertTrue(first.startswith("github=test-login time="))
        self.assertIsNotNone(datetime.fromisoformat(first.split("time=")[1]).utcoffset())
        self.assertEqual(json.loads(second), {"status": "PASS"})
