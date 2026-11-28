from dataclasses import replace
import unittest

from src.config import Config
from src.events import expected_events, make_event, process_event, PoisonEvent
from src.verification import analyze


class VerificationTests(unittest.TestCase):
    def setUp(self):
        self.expected = list(expected_events("test", 1, 4, 42, 0))
        self.sink = [{**e, "write_order": e["seq"]} for e in self.expected]

    def test_complete_run(self):
        result = analyze(self.expected, self.sink, [], 7)
        self.assertTrue(result["delivery_ok"])
        self.assertEqual(result["duplicates_discarded"], 7)
        self.assertEqual(result["order_violations"], 0)

    def test_missing_tail_is_detected(self):
        result = analyze(self.expected, self.sink[:-1], [], 0)
        self.assertFalse(result["delivery_ok"])
        self.assertEqual(result["missing_count"], 1)
        self.assertEqual(result["missing_examples"][0]["seq"], 4)

    def test_wholly_missing_user_is_detected(self):
        result = analyze(self.expected, self.sink[4:], [], 0)
        self.assertEqual(result["missing_count"], 4)
        users = {row["user_id"]: row for row in result["per_user"]}
        self.assertEqual(len(users), 3)
        self.assertEqual(users["test:p1:u0001"]["missing_seq"], [1, 2, 3, 4])
        self.assertEqual(users["test:p2:u0001"]["missing_seq"], [])

    def test_per_user_report_contains_dlt_and_partition_distribution(self):
        self.expected[0]["poison"] = True
        self.sink[1]["partition"] = 0
        self.sink[2]["partition"] = 2
        result = analyze(self.expected, self.sink[1:], [self.expected[0]] * 2, 0)
        user = result["per_user"][0]
        self.assertEqual(user["expected"], 4)
        self.assertEqual(user["sink"], 3)
        self.assertEqual(user["dlt"], 1)
        self.assertEqual(user["missing_seq"], [])
        self.assertEqual(user["sink_partitions"], [0, 2])

    def test_poison_in_dlt_is_not_missing(self):
        self.expected[0]["poison"] = True
        result = analyze(self.expected, self.sink[1:], [self.expected[0]], 0)
        self.assertTrue(result["delivery_ok"])

    def test_poison_in_sink_fails(self):
        self.expected[0]["poison"] = True
        self.assertEqual(analyze(self.expected, self.sink, [], 0)["wrong_destination_count"], 1)

    def test_dlt_redelivery_does_not_inflate_coverage(self):
        self.expected[0]["poison"] = True
        result = analyze(self.expected, self.sink[1:], [self.expected[0]] * 3, 0)
        self.assertTrue(result["delivery_ok"])
        self.assertEqual(result["dlt_unique"], 1)
        self.assertEqual(result["dlt_duplicates"], 2)

    def test_late_events_counted_once_each(self):
        # Actual writes for the first user: 3, 1, 2, 4. Both 1 and 2 arrive late.
        for record, order in zip(self.sink[:4], (2, 3, 1, 4)):
            record["write_order"] = order
        result = analyze(self.expected, self.sink, [], 0)
        self.assertEqual(result["order_violations"], 2)

    def test_wrong_event_identity_fields_fail(self):
        self.sink[0]["seq"] = 999
        self.assertEqual(analyze(self.expected, self.sink, [], 0)["mismatched_count"], 1)

    def test_unexpected_event_fails(self):
        extra = {**self.sink[0], "event_id": "unexpected", "write_order": 5}
        self.assertFalse(analyze(self.expected, self.sink + [extra], [], 0)["delivery_ok"])


class EventTests(unittest.TestCase):
    def test_disjoint_producers_and_full_sequences(self):
        events = list(expected_events("run", 10, 100, 42, 0.02))
        self.assertEqual(len({e["user_id"] for e in events}), 30)
        self.assertEqual(len({e["event_id"] for e in events}), 3000)
        self.assertEqual(events, list(expected_events("run", 10, 100, 42, 0.02)))
        poison = sum(e["poison"] for e in events)
        self.assertTrue(30 < poison < 100, poison)

    def test_poison_always_raises(self):
        cfg = replace(Config.load(), poison_rate=1)
        event = make_event(cfg, 1, 1, 1)
        for _ in range(3):
            with self.assertRaises(PoisonEvent):
                process_event(event)


if __name__ == "__main__":
    unittest.main()
