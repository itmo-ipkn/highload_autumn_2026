import json
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

import psycopg
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from app import consumer


EVENT_ID = "e7185266-90e6-42cd-8d33-a6710e15764c"
SOURCE = ("events", 1, 17)


def source_message(poison=False):
    event = {
        "event_id": EVENT_ID,
        "user_id": "p1-u1",
        "seq": 4,
        "type": "page_view",
        "produced_at": "2026-11-25T13:01:23Z",
        "payload": {"poison": poison},
    }
    message = Mock()
    message.topic.return_value, message.partition.return_value, message.offset.return_value = SOURCE
    message.error.return_value = None
    message.key.return_value = b"p1-u1"
    message.value.return_value = json.dumps(event).encode()
    return message


class ProcessMessageTests(unittest.TestCase):
    def test_store_receives_source_coordinates(self):
        sink, producer = Mock(), Mock()
        sink.store_event.return_value = True

        self.assertTrue(consumer.process_message(source_message(), sink, producer, "c1"))

        sink.store_event.assert_called_once_with(UUID(EVENT_ID), "p1-u1", 4, "c1", SOURCE)
        sink.record_failure.assert_not_called()
        producer.produce.assert_not_called()

    def test_dlt_after_three_failures_with_original_bytes_and_identity(self):
        message = source_message(poison=True)
        sink, producer = Mock(), Mock()
        sink.record_failure.side_effect = [1, 2, 3]

        def acknowledge(_):
            producer.produce.call_args.kwargs["on_delivery"](None, message)

        producer.flush.side_effect = acknowledge
        for _ in range(2):
            self.assertFalse(consumer.process_message(message, sink, producer, "c1"))
            producer.produce.assert_not_called()
        self.assertTrue(consumer.process_message(message, sink, producer, "c1"))

        self.assertEqual(sink.record_failure.call_count, 3)
        sink.store_event.assert_not_called()
        producer.produce.assert_called_once()
        args, kwargs = producer.produce.call_args
        self.assertEqual(args, ("events.dlt",))
        self.assertEqual(kwargs["value"], message.value())
        self.assertEqual(kwargs["key"], message.key())
        self.assertEqual(kwargs["headers"], {
            "error-type": b"PoisonEvent",
            "error-message": b"payload.poison is true",
            "processing-attempts": b"3",
            "source-topic": b"events",
            "source-partition": b"1",
            "source-offset": b"17",
            "event-id": EVENT_ID.encode(),
            "user-id": b"p1-u1",
            "seq": b"4",
        })

    def test_unacknowledged_dlt_does_not_complete_processing(self):
        sink, producer = Mock(), Mock()
        sink.record_failure.return_value = 3

        with self.assertRaises(consumer.DltUnavailable):
            consumer.process_message(source_message(poison=True), sink, producer, "c1")

    def test_database_failure_is_not_a_poison_attempt(self):
        sink, producer = Mock(), Mock()
        sink.store_event.side_effect = psycopg.OperationalError("database unavailable")

        with self.assertRaises(psycopg.OperationalError):
            consumer.process_message(source_message(), sink, producer, "c1")

        sink.record_failure.assert_not_called()
        producer.produce.assert_not_called()

    def test_retry_seeks_back_before_consumer_polls_again(self):
        kafka_consumer, producer, sink = Mock(), Mock(), Mock()
        callbacks = {}
        message = source_message()

        def register(signum, handler):
            callbacks[signum] = handler

        def poll(_):
            if kafka_consumer.poll.call_count == 1:
                return message
            kafka_consumer.seek.assert_called_once()
            callbacks[consumer.signal.SIGTERM](consumer.signal.SIGTERM, None)
            return None

        kafka_consumer.poll.side_effect = poll
        sink.store_event.side_effect = psycopg.OperationalError("database unavailable")
        with patch.object(consumer.signal, "signal", side_effect=register), patch.object(
            consumer.time, "sleep"
        ):
            consumer.run_consumer(kafka_consumer, producer, sink, "c1", 0.1)

        partition = kafka_consumer.seek.call_args.args[0]
        self.assertEqual((partition.topic, partition.partition, partition.offset), SOURCE)
        kafka_consumer.commit.assert_not_called()


class ReadyOffsetsTests(unittest.TestCase):
    def test_logs_only_first_record_of_each_assigned_partition(self):
        ready = consumer.ReadyOffsets()
        kafka = Mock()
        first_partition = TopicPartition("events", 1)
        second_partition = TopicPartition("events", 2)
        with self.assertLogs(consumer.LOG, level="INFO") as logs:
            ready.on_assign(kafka, [first_partition, second_partition])
            ready.log_first_record(("events", 1, 17))
            ready.log_first_record(("events", 1, 18))
            ready.log_first_record(("events", 2, 40))
            ready.on_revoke(kafka, [first_partition])
            ready.on_assign(kafka, [first_partition])
            ready.log_first_record(("events", 1, 19))
            ready.log_first_record(("events", 2, 41))

        first_records = [
            record.getMessage()
            for record in logs.records
            if record.getMessage().startswith("first consumed")
        ]
        self.assertEqual(first_records, [
            "first consumed topic=events partition=1 offset=17",
            "first consumed topic=events partition=2 offset=40",
            "first consumed topic=events partition=1 offset=19",
        ])

    def test_consumer_flushes_at_ten_messages_and_after_one_idle_second(self):
        kafka, producer, sink = Mock(), Mock(), Mock()
        callbacks = {}
        now = 100.0
        first_offset = SOURCE[2]

        def register(signum, handler):
            callbacks[signum] = handler

        def poll(timeout):
            nonlocal now
            index = kafka.poll.call_count - 1
            if index < 11:
                self.assertEqual(kafka.commit.call_count, int(index >= 10))
                message = source_message()
                message.offset.return_value = first_offset + index
                return message
            if index == 11:
                self.assertEqual(kafka.commit.call_count, 1)
                self.assertEqual(timeout, 1.0)
                now += timeout
            else:
                self.assertEqual(kafka.commit.call_count, 2)
                callbacks[consumer.signal.SIGTERM](consumer.signal.SIGTERM, None)
            return None

        def commit(*, offsets, asynchronous):
            self.assertFalse(asynchronous)
            self.assertEqual(offsets[0].offset, first_offset + sink.store_event.call_count)
            return offsets

        kafka.poll.side_effect = poll
        kafka.commit.side_effect = commit
        with (
            patch.object(consumer.signal, "signal", side_effect=register),
            patch.object(consumer.time, "monotonic", side_effect=lambda: now),
            patch.object(consumer.time, "sleep"),
        ):
            consumer.run_consumer(kafka, producer, sink, "c1", 0.1)

        self.assertEqual(
            [call.kwargs["offsets"][0].offset for call in kafka.commit.call_args_list],
            [first_offset + 10, first_offset + 11],
        )

    def test_shutdown_commits_ready_assigned_offsets_before_close(self):
        kafka, producer, sink = Mock(), Mock(), Mock()
        callbacks = {}
        kafka.assignment.return_value = [TopicPartition("events", 1)]

        def register(signum, handler):
            callbacks[signum] = handler

        def poll(_):
            if kafka.poll.call_count == 1:
                return source_message()
            kafka.commit.assert_not_called()
            callbacks[consumer.signal.SIGTERM](consumer.signal.SIGTERM, None)
            return None

        def close():
            kafka.commit.assert_called_once()
            kafka.subscribe.call_args.kwargs["on_revoke"](kafka, kafka.assignment.return_value)

        kafka.poll.side_effect = poll
        kafka.close.side_effect = close
        kafka.commit.return_value = [TopicPartition("events", 1, 18)]
        with (
            patch.object(consumer.signal, "signal", side_effect=register),
            patch.object(consumer.time, "monotonic", return_value=100.0),
            patch.object(consumer.time, "sleep"),
        ):
            consumer.run_consumer(kafka, producer, sink, "c1", 0.1)

        kafka.commit.assert_called_once()
        offset = kafka.commit.call_args.kwargs["offsets"][0]
        self.assertEqual((offset.topic, offset.partition, offset.offset), ("events", 1, 18))

    def test_revoke_commits_only_ready_revoked_partitions(self):
        ready = consumer.ReadyOffsets()
        kafka = Mock()
        kafka.commit.return_value = [TopicPartition("events", 1, 18)]
        ready.mark(SOURCE)
        ready.mark(("events", 2, 9))

        ready.on_revoke(kafka, [TopicPartition("events", 0), TopicPartition("events", 1)])

        kafka.commit.assert_called_once()
        commit = kafka.commit.call_args.kwargs
        self.assertFalse(commit["asynchronous"])
        self.assertEqual([(offset.partition, offset.offset) for offset in commit["offsets"]], [(1, 18)])
        self.assertEqual(ready.offsets, {("events", 2): 10})

    def test_lost_never_commits(self):
        ready = consumer.ReadyOffsets()
        kafka = Mock()
        ready.mark(SOURCE)

        ready.on_lost(kafka, [TopicPartition("events", 1)])

        kafka.commit.assert_not_called()
        self.assertEqual(ready.offsets, {})

    def test_commit_error_retains_offset_until_retry(self):
        ready = consumer.ReadyOffsets()
        kafka = Mock()
        kafka.commit.side_effect = [
            KafkaException(KafkaError(KafkaError._TIMED_OUT)),
            [TopicPartition("events", 1, 18)],
        ]
        ready.mark(SOURCE)

        ready.commit(kafka)
        self.assertEqual(ready.offsets, {("events", 1): 18})
        ready.commit(kafka)
        self.assertEqual(ready.offsets, {})
