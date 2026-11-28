import argparse
import json
import os
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition

from .consumer import DLT_TOPIC, GROUP_ID, SOURCE_TOPIC, InvalidEvent, Source, decode_event, event_identity


SAMPLE_LIMIT = 10
KAFKA_TIMEOUT = 5
DLT_IDLE_TIMEOUT = 30


class VerificationUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class StoredEvent:
    write_order: int
    user_id: str
    seq: int
    duplicates: int


@dataclass(frozen=True)
class DeadLetter:
    source: Source | None
    identity: tuple[UUID, str, int] | None


def read_db(database_url: str) -> tuple[dict[str, int], list[str], list[StoredEvent]]:
    with psycopg.connect(database_url, connect_timeout=KAFKA_TIMEOUT) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        states = connection.execute(
            "SELECT user_id, last_acked_seq, pending_event IS NOT NULL FROM producer_state ORDER BY user_id"
        ).fetchall()
        stored = connection.execute(
            "SELECT write_order, user_id, seq, duplicates FROM processed_events ORDER BY write_order"
        ).fetchall()
    high_watermarks = {user_id: last_seq for user_id, last_seq, _ in states}
    pending = [user_id for user_id, _, has_pending in states if has_pending]
    return high_watermarks, pending, [StoredEvent(*row) for row in stored]


def topic_bounds(reader: Consumer, topic: str) -> dict[int, tuple[int, int]]:
    metadata = reader.list_topics(topic, timeout=KAFKA_TIMEOUT).topics.get(topic)
    if metadata is None or metadata.error is not None or not metadata.partitions:
        raise VerificationUnavailable(f"cannot get partition metadata for {topic}")
    return {
        partition: reader.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=KAFKA_TIMEOUT
        )
        for partition in sorted(metadata.partitions)
    }


def parse_dlt(message: Message) -> DeadLetter:
    headers = dict(message.headers() or [])
    try:
        source = (
            headers["source-topic"].decode("utf-8"),
            int(headers["source-partition"]),
            int(headers["source-offset"]),
        )
    except (KeyError, TypeError, ValueError, AttributeError):
        source = None
    try:
        identity = event_identity(decode_event(message.value()))
    except InvalidEvent:
        identity = None
    return DeadLetter(source, identity)


def read_dlt(reader: Consumer, bounds: dict[int, tuple[int, int]]) -> list[DeadLetter]:
    remaining = {partition: end for partition, (start, end) in bounds.items() if start < end}
    if not remaining:
        return []
    reader.assign([
        TopicPartition(DLT_TOPIC, partition, bounds[partition][0]) for partition in remaining
    ])
    letters = []
    deadline = time.monotonic() + DLT_IDLE_TIMEOUT
    while remaining:
        if time.monotonic() >= deadline:
            raise VerificationUnavailable(f"DLT scan timed out before offsets {remaining}")
        message = reader.poll(1)
        if message is None:
            continue
        error = message.error()
        if error is not None and error.code() != KafkaError._PARTITION_EOF:
            raise KafkaException(error)
        partition = message.partition()
        if partition not in remaining:
            continue
        offset = message.offset()
        end = remaining[partition]
        if error is None and offset < end:
            letters.append(parse_dlt(message))
            deadline = time.monotonic() + DLT_IDLE_TIMEOUT
        if offset >= end or (error is None and offset + 1 == end):
            reader.pause([TopicPartition(DLT_TOPIC, partition)])
            del remaining[partition]
    return letters


def missing_ranges(observed: set[int], high_watermark: int) -> Iterator[tuple[int, int]]:
    next_seq = 1
    for seq in sorted(observed):
        if seq > high_watermark:
            break
        if seq > next_seq:
            yield next_seq, seq - 1
        next_seq = seq + 1
    if next_seq <= high_watermark:
        yield next_seq, high_watermark


def sort_and_count_inversions(sequences: list[int]) -> tuple[list[int], int]:
    if len(sequences) < 2:
        return sequences, 0
    middle = len(sequences) // 2
    left, left_count = sort_and_count_inversions(sequences[:middle])
    right, right_count = sort_and_count_inversions(sequences[middle:])
    count = left_count + right_count
    merged = []
    left_index = right_index = 0
    while left_index < len(left) and right_index < len(right):
        if left[left_index] <= right[right_index]:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
            count += len(left) - left_index
    merged.extend(left[left_index:])
    merged.extend(right[right_index:])
    return merged, count


def summarize(
    high_watermarks: dict[str, int],
    stored: list[StoredEvent],
    letters: list[DeadLetter],
) -> dict:
    observed: dict[str, set[int]] = defaultdict(set)
    write_order: dict[str, list[int]] = defaultdict(list)
    out_of_order = []
    late_events = 0
    highest: dict[str, int] = defaultdict(int)
    for row in stored:
        observed[row.user_id].add(row.seq)
        write_order[row.user_id].append(row.seq)
        if row.seq < highest[row.user_id]:
            late_events += 1
            if len(out_of_order) < SAMPLE_LIMIT:
                out_of_order.append({
                    "user_id": row.user_id,
                    "seq": row.seq,
                    "earlier_max_seq": highest[row.user_id],
                    "write_order": row.write_order,
                })
        else:
            highest[row.user_id] = row.seq

    sources = set()
    event_ids = set()
    dlt_duplicates = unidentifiable = 0
    for letter in letters:
        event_id = letter.identity[0] if letter.identity is not None else None
        duplicate = letter.source in sources or event_id in event_ids
        if letter.source is not None:
            sources.add(letter.source)
        if event_id is not None:
            event_ids.add(event_id)
        if duplicate:
            dlt_duplicates += 1
        elif letter.identity is None:
            unidentifiable += 1
        else:
            _, user_id, seq = letter.identity
            observed[user_id].add(seq)

    missing = []
    missing_count = 0
    for user_id, high in sorted(high_watermarks.items()):
        for start, end in missing_ranges(observed[user_id], high):
            missing_count += end - start + 1
            if len(missing) < SAMPLE_LIMIT:
                missing.append({"user_id": user_id, "start": start, "end": end})

    return {
        "acknowledged_events": sum(high_watermarks.values()),
        "processed_events": len(stored),
        "dlt_events": len(letters) - dlt_duplicates,
        "dlt_duplicates": dlt_duplicates,
        "dlt_unidentifiable": unidentifiable,
        "missing_sequences": missing_count,
        "missing_ranges": missing,
        "filtered_duplicates": sum(row.duplicates for row in stored),
        "write_order_violations": sum(
            sort_and_count_inversions(sequences)[1] for sequences in write_order.values()
        ),
        "late_events": late_events,
        "out_of_order": out_of_order,
    }


def check(reader: Consumer, database_url: str, final: bool) -> tuple[dict, int]:
    source_bounds = topic_bounds(reader, SOURCE_TOPIC)
    committed = reader.committed(
        [TopicPartition(SOURCE_TOPIC, partition) for partition in source_bounds],
        timeout=KAFKA_TIMEOUT,
    )
    offsets = {}
    for partition in committed:
        if partition.error is not None:
            raise KafkaException(partition.error)
        offsets[partition.partition] = max(0, partition.offset)
    ends = {partition: end for partition, (_, end) in source_bounds.items()}
    lag = {
        partition: end - offsets[partition]
        for partition, end in ends.items()
        if end != offsets[partition]
    }

    letters = read_dlt(reader, topic_bounds(reader, DLT_TOPIC))
    high_watermarks, pending, stored = read_db(database_url)
    source_changed = source_bounds != topic_bounds(reader, SOURCE_TOPIC)
    report = summarize(high_watermarks, stored, letters)
    report.update(
        final=final,
        pending_producers=pending,
        group_lag=lag,
        source_end_offsets=ends,
        group_committed_offsets=offsets,
        source_changed=source_changed,
    )
    if pending or lag or source_changed:
        report["status"] = "INCOMPLETE"
        return report, 2
    if report["missing_sequences"] or report["dlt_unidentifiable"]:
        report["status"] = "FAIL"
        return report, 1
    report["status"] = "PASS"
    return report, 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify sink and dead-letter coverage")
    parser.add_argument("--final", action="store_true", help="verify after stopping and draining producers")
    args = parser.parse_args()
    login = os.environ.get("GITHUB_LOGIN", "Len1viy")
    print(f"github={login} time={datetime.now(timezone.utc).isoformat()}", flush=True)

    reader = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "client.id": "verify",
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "error",
    })
    try:
        report, exit_code = check(reader, os.environ["DATABASE_URL"], args.final)
    except (KafkaException, psycopg.Error, VerificationUnavailable) as error:
        report = {"status": "INCOMPLETE", "final": args.final, "error": str(error)}
        exit_code = 2
    finally:
        reader.close()
    print(json.dumps(report, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
