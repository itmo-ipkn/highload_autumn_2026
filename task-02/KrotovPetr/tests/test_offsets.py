from types import SimpleNamespace
import unittest

try:
    from confluent_kafka import KafkaError, KafkaException
    from src.consumer import Offsets
except ImportError:
    Offsets = None


class FakeConsumer:
    def __init__(self):
        self.calls = []
        self.fail = False

    def commit(self, *, offsets, asynchronous):
        self.calls.append([(p.topic, p.partition, p.offset) for p in offsets])
        if self.fail:
            raise KafkaException(KafkaError(KafkaError._TIMED_OUT))
        return offsets


def message(partition, offset):
    return SimpleNamespace(topic=lambda: "events", partition=lambda: partition, offset=lambda: offset)


@unittest.skipUnless(Offsets is not None, "Requires the app's Kafka client")
class OffsetTests(unittest.TestCase):
    def setUp(self):
        self.consumer = FakeConsumer()
        self.offsets = Offsets(self.consumer, "test")

    def test_commits_next_position_only_after_done(self):
        self.offsets.commit()
        self.assertEqual(self.consumer.calls, [])
        self.offsets.done(message(1, 8))
        self.offsets.commit()
        self.assertEqual(self.consumer.calls, [[("events", 1, 9)]])
        self.assertEqual(self.offsets.ready, {})

    def test_commit_error_keeps_completed_position(self):
        self.consumer.fail = True
        self.offsets.done(message(1, 8))
        self.offsets.commit()
        self.assertEqual(self.offsets.ready, {("events", 1): 9})
        self.consumer.fail = False
        self.offsets.commit()
        self.assertEqual(self.offsets.ready, {})

    def test_revoke_commits_only_revoked_partitions(self):
        self.offsets.done(message(0, 4))
        self.offsets.done(message(1, 8))
        self.offsets.revoked(self.consumer, [SimpleNamespace(topic="events", partition=0)])
        self.assertEqual(self.consumer.calls, [[("events", 0, 5)]])
        self.assertEqual(self.offsets.ready, {("events", 1): 9})

    def test_lost_partition_is_never_committed(self):
        self.offsets.done(message(1, 8))
        self.offsets.lost(self.consumer, [SimpleNamespace(topic="events", partition=1)])
        self.offsets.commit()
        self.assertEqual(self.consumer.calls, [])


if __name__ == "__main__":
    unittest.main()
