#!/usr/bin/env python3

from __future__ import annotations

import json
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable

import psycopg2
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from psycopg2.extras import RealDictCursor


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "WARNING").upper(),
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("verify")
logging.getLogger("kafka").setLevel(logging.CRITICAL)

SINK_DB = {
    "host": os.getenv("SINK_DB_HOST", "kafka-sink"),
    "port": int(os.getenv("SINK_DB_PORT", "5432")),
    "dbname": os.getenv("SINK_DB_NAME", "sink_db"),
    "user": os.getenv("SINK_DB_USER", "kafka"),
    "password": os.getenv("SINK_DB_PASSWORD", "kafka"),
    "connect_timeout": 10,
}

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
).split(",")
EVENTS_DLT_TOPIC = os.getenv("EVENTS_DLT_TOPIC", "events.dlt")
DLT_READ_TIMEOUT_SECONDS = float(os.getenv("DLT_READ_TIMEOUT_SECONDS", "120"))
STRICT = os.getenv("VERIFY_STRICT", "0").strip().lower() in {"1", "true", "yes"}






def connect_sink():
    return psycopg2.connect(**SINK_DB)


def query_sink() -> list[dict[str, Any]]:
    connection = connect_sink()
    try:
        try:
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT sink_seq,
                           event_id::text AS event_id,
                           user_id,
                           seq,
                           consumer_id,
                           topic,
                           "partition",
                           kafka_offset,
                           processed_at
                    FROM processed_events
                    ORDER BY sink_seq
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
        except psycopg2.errors.UndefinedColumn:
            connection.rollback()
            logger.warning(
                "processed_events.sink_seq is missing; ordering by processed_at"
            )
            with connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT event_id::text AS event_id,
                           user_id,
                           seq,
                           consumer_id,
                           topic,
                           "partition",
                           kafka_offset,
                           processed_at
                    FROM processed_events
                    ORDER BY processed_at, event_id
                    """
                )
                return [dict(row) for row in cursor.fetchall()]
    finally:
        connection.close()


def query_duplicate_count() -> int:
    connection = connect_sink()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(SUM(duplicate_count), 0) FROM consumer_stats")
            return int(cursor.fetchone()[0])
    finally:
        connection.close()


def query_consumer_rows() -> dict[int, int]:
    connection = connect_sink()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT consumer_id, COUNT(*) AS processed
                FROM processed_events
                GROUP BY consumer_id
                ORDER BY consumer_id
                """
            )
            return {int(row[0]): int(row[1]) for row in cursor.fetchall()}
    finally:
        connection.close()






def _decode_headers(raw_headers: Any) -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in raw_headers or []:
        if isinstance(key, bytes):
            key = key.decode("utf-8", errors="replace")
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        result[str(key)] = str(value)
    return result


def query_dlt() -> list[dict[str, Any]]:
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        consumer_timeout_ms=5_000,
        request_timeout_ms=20_000,
        value_deserializer=None,
        key_deserializer=None,
    )
    try:
        partitions = consumer.partitions_for_topic(EVENTS_DLT_TOPIC)
        if not partitions:
            logger.warning("Topic %s does not exist or has no partitions", EVENTS_DLT_TOPIC)
            return []

        topic_partitions = [
            TopicPartition(EVENTS_DLT_TOPIC, partition) for partition in sorted(partitions)
        ]
        consumer.assign(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        for tp in topic_partitions:
            consumer.seek_to_beginning(tp)

        result: list[dict[str, Any]] = []
        deadline = datetime.now(timezone.utc).timestamp() + DLT_READ_TIMEOUT_SECONDS

        while True:
            records = consumer.poll(timeout_ms=500)
            for messages in records.values():
                for message in messages:
                    try:
                        event = json.loads(message.value.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        logger.warning(
                            "Invalid JSON in DLT at partition=%s offset=%s",
                            message.partition,
                            message.offset,
                        )
                        continue
                    if not isinstance(event, dict):
                        logger.warning(
                            "Non-object DLT value at partition=%s offset=%s",
                            message.partition,
                            message.offset,
                        )
                        continue

                    if "original_event" in event and isinstance(event["original_event"], dict):
                        original_event = event["original_event"]
                    else:
                        original_event = event
                    result.append(
                        {
                            "event": original_event,
                            "headers": _decode_headers(message.headers),
                            "dlt_partition": message.partition,
                            "dlt_offset": message.offset,
                        }
                    )

            positions = {tp: consumer.position(tp) for tp in topic_partitions}
            if all(positions[tp] >= end_offsets[tp] for tp in topic_partitions):
                break
            if datetime.now(timezone.utc).timestamp() > deadline:
                logger.error(
                    "Timed out reading DLT; positions=%s end_offsets=%s",
                    positions,
                    end_offsets,
                )
                break

        return result
    finally:
        consumer.close(autocommit=False)


def unique_dlt_events(dlt_records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[str, dict[str, Any]] = {}
    for item in dlt_records:
        event = item["event"]
        event_id = event.get("event_id")
        if not event_id:
            continue
        unique.setdefault(str(event_id), event)
    return list(unique.values())






def collect_user_sequences(
    sink_events: list[dict[str, Any]],
    dlt_events: list[dict[str, Any]],
) -> dict[str, set[int]]:
    sequences: dict[str, set[int]] = defaultdict(set)
    for event in sink_events:
        sequences[event["user_id"]].add(int(event["seq"]))
    for event in dlt_events:
        sequences[event["user_id"]].add(int(event["seq"]))
    return sequences


def check_seq_integrity(
    sink_events: list[dict[str, Any]],
    dlt_events: list[dict[str, Any]],
):
    sequences = collect_user_sequences(sink_events, dlt_events)
    missing_total = 0
    gap_total = 0
    details: list[str] = []

    for user_id in sorted(sequences):
        seqs = sequences[user_id]
        if not seqs:
            continue
        max_seq = max(seqs)
        missing = sorted(set(range(1, max_seq + 1)) - seqs)
        ordered = sorted(seqs)
        gaps = [
            (previous, current)
            for previous, current in zip(ordered, ordered[1:])
            if current > previous + 1
        ]
        missing_total += len(missing)
        gap_total += len(gaps)
        if missing:
            preview = ", ".join(str(value) for value in missing[:20])
            suffix = " ..." if len(missing) > 20 else ""
            details.append(
                f"{user_id}: missing seq [{preview}{suffix}] "
                f"(max seq={max_seq}, {len(missing)} missing)"
            )





    reuse = 0
    per_user_seqs: dict[str, Counter] = defaultdict(Counter)
    for event in sink_events:
        per_user_seqs[event["user_id"]][int(event["seq"])] += 1
    for user_id, counter in per_user_seqs.items():
        duplicated = {seq: count for seq, count in counter.items() if count > 1}
        if duplicated:
            reuse += sum(count - 1 for count in duplicated.values())
            details.append(
                f"{user_id}: seq reused in sink {sorted(duplicated)[:10]} "
                f"({len(duplicated)} seq values)"
            )

    return missing_total, gap_total, details, reuse


def count_inversions(values: list[int]) -> int:
    if len(values) < 2:
        return 0

    def merge_sort(items: list[int]) -> tuple[list[int], int]:
        if len(items) < 2:
            return items, 0
        middle = len(items) // 2
        left, left_inv = merge_sort(items[:middle])
        right, right_inv = merge_sort(items[middle:])
        merged: list[int] = []
        inversions = left_inv + right_inv
        i = j = 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                merged.append(left[i])
                i += 1
            else:
                merged.append(right[j])
                inversions += len(left) - i
                j += 1
        merged.extend(left[i:])
        merged.extend(right[j:])
        return merged, inversions

    _, inversions = merge_sort(values)
    return inversions


def check_order_violations(sink_events: list[dict[str, Any]]):
    per_user: dict[str, list[int]] = defaultdict(list)
    for event in sink_events:
        per_user[event["user_id"]].append(int(event["seq"]))

    total = 0
    per_user_counts: list[tuple[int, str]] = []
    details: list[str] = []
    for user_id in sorted(per_user):
        inversions = count_inversions(per_user[user_id])
        total += inversions
        if inversions:
            per_user_counts.append((inversions, user_id))
            details.append(
                f"{user_id}: {inversions} inverted pair(s) over "
                f"{len(per_user[user_id])} sink rows"
            )

    per_user_counts.sort(reverse=True)
    return total, per_user_counts[:10], details






def print_header() -> None:
    login = os.getenv("GITHUB_LOGIN", "").strip()
    if not login:
        login = "unknown-github-login"
        logger.warning("GITHUB_LOGIN is not set; first line will contain a placeholder")
    now = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"{login} {now}")


def main() -> int:
    print_header()

    try:
        sink_events = query_sink()
        dlt_records = query_dlt()
        duplicate_count = query_duplicate_count()
        consumer_rows = query_consumer_rows()
    except Exception as exc:
        logger.exception("Verification could not be completed: %s", exc)
        print(f"\nERROR: {exc}")
        return 1

    dlt_events = unique_dlt_events(dlt_records)
    missing, gaps, gap_details, seq_reuse = check_seq_integrity(sink_events, dlt_events)
    order_violations, per_user_order, order_details = check_order_violations(sink_events)

    dlt_error_reasons = Counter(
        item["headers"].get("error_reason", "<not set>").split(":")[0]
        for item in dlt_records
    )

    print()
    print("=== Verification results ===")
    print(f"Sink rows: {len(sink_events)}")
    print(f"DLT records: {len(dlt_records)} (unique event_id: {len(dlt_events)})")
    print(f"Duplicate deliveries suppressed: {duplicate_count}")
    print(f"Sequence gaps: {gaps}")
    print(f"Events missing from both sink and DLT: {missing}")
    print(f"Seq values reused in sink: {seq_reuse}")
    print(f"Order violations (all inverted pairs): {order_violations}")

    if consumer_rows:
        distribution = ", ".join(
            f"consumer-{consumer_id}={count}"
            for consumer_id, count in sorted(consumer_rows.items())
        )
        print(f"Sink rows by consumer: {distribution}")

    if dlt_error_reasons:
        reasons = ", ".join(
            f"{reason!r}={count}" for reason, count in dlt_error_reasons.most_common(5)
        )
        print(f"DLT error reasons: {reasons}")

    if gap_details:
        print("\n--- Sequence details ---")
        for line in gap_details[:20]:
            print(line)
        if len(gap_details) > 20:
            print(f"... and {len(gap_details) - 20} more")

    if order_details:
        print("\n--- Order violation details (top users) ---")
        for line in order_details[:10]:
            print(line)

    if per_user_order:
        print(
            "Top users by inversions: "
            + ", ".join(f"{user}={count}" for count, user in per_user_order[:5])
        )

    checks = {
        "no seq gaps": gaps == 0,
        "no lost events": missing == 0,
        "no order violations": order_violations == 0,
        "no reused seq values": seq_reuse == 0,
    }
    failed = [name for name, passed in checks.items() if not passed]
    print()
    for name, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")

    if failed:
        print(f"\nRESULT: FAIL ({', '.join(failed)})")
        return 2 if STRICT else 0

    print("\nRESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
