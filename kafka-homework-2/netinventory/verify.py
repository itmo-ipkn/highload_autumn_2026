import argparse
from collections import defaultdict
from datetime import datetime
import json
import os
from time import monotonic, sleep
from uuid import uuid4

from confluent_kafka import Consumer, KafkaError, TopicPartition

from .settings import BOOTSTRAP, DEAD_LETTER_TOPIC, PARTITIONS, TOPIC, database


def dead_letters():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"verify-{uuid4()}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        starts, ends = [], {}
        for partition in range(PARTITIONS):
            position = TopicPartition(DEAD_LETTER_TOPIC, partition)
            low, high = consumer.get_watermark_offsets(position, timeout=5)
            if high > low:
                starts.append(TopicPartition(DEAD_LETTER_TOPIC, partition, low))
                ends[partition] = high
        if not starts:
            return []

        consumer.assign(starts)
        deadline = monotonic() + 10
        records = []
        while ends and monotonic() < deadline:
            message = consumer.poll(0.3)
            if message is None or message.partition() not in ends:
                continue
            partition = message.partition()
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF and message.offset() >= ends[partition]:
                    del ends[partition]
                elif message.error().code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(message.error())
                continue
            if message.offset() < ends[partition]:
                records.append((json.loads(message.value()), dict(message.headers() or [])))
            if message.offset() + 1 >= ends[partition]:
                del ends[partition]
        if ends:
            raise RuntimeError(f"could not reach DLT snapshot for partitions {sorted(ends)}")
        return records
    finally:
        consumer.close()


def expected_events(connection, mode):
    return dict(
        connection.execute(
            "SELECT user_id, confirmed_seq FROM producer_progress WHERE mode = %s OR %s = 'all' ORDER BY user_id",
            (mode, mode),
        )
    )


def check(connection, expected):
    invalid_headers = 0
    invalid_records = 0
    dlt_ids = set()
    covered = defaultdict(set)
    for event, headers in dead_letters():
        user_id, seq = event["user_id"], event["seq"]
        if user_id not in expected or seq > expected[user_id]:
            continue
        if (
            headers.get("source-topic") != TOPIC.encode()
            or headers.get("attempts") != b"3"
            or not (headers.get("error-reason") or b"").startswith(b"unknown device code:")
            or headers.get("source-partition") not in {str(i).encode() for i in range(PARTITIONS)}
            or not (headers.get("source-offset") or b"").isdigit()
        ):
            invalid_headers += 1
        if event["event_id"] not in dlt_ids and seq in covered[user_id]:
            invalid_records += 1
        dlt_ids.add(event["event_id"])
        covered[user_id].add(seq)

    previous_max = defaultdict(int)
    inversions = 0
    rows = 0
    duplicates = 0
    order_examples = []
    with connection.transaction(), connection.cursor(name="verify_sink") as cursor:
        cursor.execute(
            """
            SELECT p.user_id, p.seq, p.partition, COALESCE(d.count, 0)
            FROM processed_events p LEFT JOIN duplicate_deliveries d USING (event_id)
            WHERE p.user_id = ANY(%s) ORDER BY p.user_id, p.write_id
            """,
            (list(expected),),
        )
        for user_id, seq, partition, count in cursor:
            if seq > expected[user_id]:
                continue
            duplicates += count
            rows += 1
            if seq in covered[user_id]:
                invalid_records += 1
            covered[user_id].add(seq)

            if seq < previous_max[user_id]:
                inversions += 1
                if len(order_examples) < 5:
                    order_examples.append((user_id, seq, previous_max[user_id], partition))
            previous_max[user_id] = max(previous_max[user_id], seq)

    gaps = {}
    for user_id, end in expected.items():
        missing = [seq for seq in range(1, end + 1) if seq not in covered[user_id]]
        if missing:
            gaps[user_id] = missing
    return rows, len(dlt_ids), duplicates, inversions, gaps, invalid_headers, invalid_records, order_examples


def run():
    parser = argparse.ArgumentParser(description="Check sink and dead-letter topic against acknowledged publications.")
    parser.add_argument("--mode", choices=("all", "user_id", "none"), default="all")
    parser.add_argument("--wait", type=float, default=0, help="seconds to wait for the snapshot to be processed")
    args = parser.parse_args()
    print(f"{os.environ['GITHUB_LOGIN']}  {datetime.now().astimezone().isoformat(timespec='seconds')}", flush=True)

    with database() as connection:
        expected = expected_events(connection, args.mode)
        confirmed = sum(expected.values())
        if not confirmed:
            print(f"Mode: {args.mode} — no confirmed events", flush=True)
            return 1

        deadline = monotonic() + args.wait
        while True:
            result = check(connection, expected)
            rows, dlt, duplicates, inversions, gaps, bad_headers, bad_records, examples = result
            if not gaps or monotonic() >= deadline:
                break
            sleep(1)

    print(f"Mode: {args.mode} | users: {len(expected)} | Kafka confirmed: {confirmed}")
    print(f"PostgreSQL: {rows} | DLT (unique): {dlt}")
    print(f"Missing seq: {sum(map(len, gaps.values()))} | users with gaps: {len(gaps)}")
    print(f"Duplicates suppressed: {duplicates}")
    print(f"Out-of-order writes: {inversions}")
    print(f"Invalid DLT headers: {bad_headers} | conflicting records: {bad_records}")
    for user_id, seqs in list(gaps.items())[:5]:
        print(f"  {user_id}: missing {seqs[:10]}")
    for user_id, seq, previous, partition in examples:
        print(f"  {user_id}: seq={seq} after seq={previous} | partition={partition}")
    passed = not (gaps or bad_headers or bad_records) and (args.mode != "user_id" or not inversions)
    print("PASS" if passed else "CHECK FAILED")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(run())
