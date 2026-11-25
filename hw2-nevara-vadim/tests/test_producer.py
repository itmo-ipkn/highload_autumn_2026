import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock, patch
from uuid import UUID

from confluent_kafka import KafkaError, Producer

from app import producer


class ProducerTests(unittest.TestCase):
    def setUp(self):
        self.settings = producer.Settings("p1", 2, 0, 0.02, "user_id")
        self.event = producer.make_event("p1-u1", 1, 0.02)

    def test_owner_ids_are_disjoint(self):
        ids = [
            producer.Settings(owner, 2, 0, 0.02, "user_id").user_ids()
            for owner in ("p1", "p2", "p3")
        ]
        self.assertEqual(
            ids, [["p1-u1", "p1-u2"], ["p2-u1", "p2-u2"], ["p3-u1", "p3-u2"]]
        )

    def test_poison_preserves_event_identity(self):
        event = producer.make_event("p3-u2", 7, 1)
        self.assertEqual(UUID(event["event_id"]).version, 4)
        self.assertEqual(event["user_id"], "p3-u2")
        self.assertEqual(event["seq"], 7)
        self.assertIn(event["type"], ("page_view", "add_to_cart", "purchase"))
        self.assertEqual(
            datetime.fromisoformat(event["produced_at"]).utcoffset(), timedelta()
        )
        self.assertIs(event["payload"]["poison"], True)

    def test_prepare_reuses_pending_event_without_allocating_sequence(self):
        connection = MagicMock()
        register = Mock()
        read = Mock()
        read.fetchone.return_value = (0, self.event)
        connection.execute.side_effect = [register, read]

        with patch.object(producer, "make_event") as make_event:
            result = producer.prepare_event(connection, self.settings, "p1-u1")

        self.assertEqual(result, self.event)
        make_event.assert_not_called()
        self.assertEqual(connection.execute.call_count, 2)

    def test_wire_key_matches_mode(self):
        for mode, expected in [("user_id", b"p1-u1"), ("none", None)]:
            with self.subTest(mode=mode):
                kafka = Mock(spec=Producer)
                delivery = producer.Delivery(self.event)

                producer.enqueue(kafka, delivery, mode)

                topic, = kafka.produce.call_args.args
                args = kafka.produce.call_args.kwargs
                self.assertEqual(topic, "events")
                self.assertEqual(args["key"], expected)
                self.assertEqual(json.loads(args["value"]), self.event)
                self.assertEqual(args["on_delivery"], delivery.on_delivery)
                self.assertNotIn("partition", args)

    def test_failed_delivery_retries_same_event_before_new_sequence(self):
        connection = Mock()
        connection.execute.return_value = [(self.event,)]
        kafka = Mock(spec=Producer)
        shutdown = producer.Shutdown()
        error = KafkaError(KafkaError._MSG_TIMED_OUT)

        def complete(_timeout):
            callback = kafka.produce.call_args.kwargs["on_delivery"]
            if kafka.produce.call_count == 1:
                callback(error, Mock())
                return

            callback(None, Mock())
            shutdown.request(None, None)

        kafka.poll.side_effect = complete
        with (
            patch.object(producer, "prepare_event") as prepare_event,
            patch.object(producer, "acknowledge") as acknowledge,
            self.assertLogs(producer.LOGGER, "WARNING") as logs,
        ):
            producer.run_producer(kafka, connection, self.settings, shutdown)

        self.assertEqual(kafka.produce.call_count, 2)
        first, retry = kafka.produce.call_args_list
        self.assertEqual(first.kwargs["value"], retry.kwargs["value"])
        prepare_event.assert_not_called()
        acknowledge.assert_called_once_with(connection, "p1", self.event)
        self.assertIn("pending event retained", logs.output[0])

    def test_shutdown_flushes_and_only_advances_acknowledged_event(self):
        for delivered_during_flush in (True, False):
            with self.subTest(delivered_during_flush=delivered_during_flush):
                connection = Mock()
                connection.execute.return_value = [(self.event,)]
                kafka = Mock(spec=Producer)
                shutdown = producer.Shutdown()

                def flush(_timeout):
                    if delivered_during_flush:
                        callback = kafka.produce.call_args.kwargs["on_delivery"]
                        callback(None, Mock())
                        return 0
                    return 1

                kafka.poll.side_effect = lambda _timeout: shutdown.request(None, None)
                kafka.flush.side_effect = flush
                with (
                    patch.object(producer, "acknowledge") as acknowledge,
                    patch.object(producer.LOGGER, "warning") as warning,
                ):
                    producer.run_producer(kafka, connection, self.settings, shutdown)

                kafka.flush.assert_called_once_with(6)
                if delivered_during_flush:
                    acknowledge.assert_called_once_with(connection, "p1", self.event)
                    warning.assert_not_called()
                else:
                    acknowledge.assert_not_called()
                    self.assertIn("pending event", warning.call_args.args[0])


if __name__ == "__main__":
    unittest.main()
