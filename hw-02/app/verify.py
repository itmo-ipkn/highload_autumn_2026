"""
Запуск: docker compose run --rm verify
"""
import json
import sys
from collections import defaultdict
from datetime import datetime

from confluent_kafka import Consumer, TopicPartition
from sqlalchemy import create_engine, func, select

from config import VerifySettings
from models import PodCounter, ProcessedEvent

settings = VerifySettings()


def read_dlt() -> tuple[dict[str, set[int]], int, int]:
    """Читает events.dlt с начала до текущего конца. Группа не используется, оффсеты не коммитятся."""
    consumer = Consumer({
        "bootstrap.servers": settings.BOOTSTRAP_SERVERS,
        "group.id": "verify",
        "enable.auto.commit": False,
    })
    try:
        meta = consumer.list_topics(settings.DLT_TOPIC, timeout=15)
        ends, assignment = {}, []
        for partition in meta.topics[settings.DLT_TOPIC].partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(settings.DLT_TOPIC, partition), timeout=15
            )
            if high > low:
                ends[partition] = high
                assignment.append(TopicPartition(settings.DLT_TOPIC, partition, low))

        by_user: dict[str, set[int]] = defaultdict(set)
        total = unparsed = 0
        consumer.assign(assignment)
        while ends:
            msg = consumer.poll(15.0)
            if msg is None:
                raise SystemExit(f"timeout reading {settings.DLT_TOPIC}, partitions left: {sorted(ends)}")
            if msg.error():
                raise SystemExit(f"error reading {settings.DLT_TOPIC}: {msg.error()}")
            total += 1
            try:
                event = json.loads(msg.value())
                by_user[event["user_id"]].add(int(event["seq"]))
            except (ValueError, KeyError, TypeError):
                unparsed += 1
            if msg.offset() >= ends.get(msg.partition(), 0) - 1:
                ends.pop(msg.partition(), None)
        return by_user, total, unparsed
    finally:
        consumer.close()


def read_sink():
    engine = create_engine(settings.SINK_DSN)
    with engine.connect() as conn:
        rows = conn.execute(
            select(ProcessedEvent.user_id, ProcessedEvent.seq, ProcessedEvent.pod)
            .order_by(ProcessedEvent.id)
        ).all()
        dup_rows = conn.execute(
            select(func.count() - func.count(ProcessedEvent.event_id.distinct()))
            .select_from(ProcessedEvent)
        ).scalar_one()
        counters = conn.execute(
            select(PodCounter.pod, PodCounter.dedup_dropped).order_by(PodCounter.pod)
        ).all()
    engine.dispose()
    return rows, dup_rows, {pod: count for pod, count in counters}


def main() -> int:
    print(f"{settings.GITHUB_LOGIN} {datetime.now().astimezone().isoformat(timespec='seconds')}")

    rows, dup_rows, dedup = read_sink()
    dlt, dlt_total, dlt_unparsed = read_dlt()

    sink_seqs: dict[str, set[int]] = defaultdict(set)
    per_pod: dict[str, int] = defaultdict(int)
    max_seen: dict[str, int] = {}
    order_violations = 0
    violating_users = set()
    for user_id, seq, pod in rows:  # в порядке записи в sink
        per_pod[pod] += 1
        sink_seqs[user_id].add(seq)
        if seq < max_seen.get(user_id, 0):
            order_violations += 1
            violating_users.add(user_id)
        else:
            max_seen[user_id] = seq

    users = sorted(set(sink_seqs) | set(dlt))
    lost: dict[str, list[int]] = {}
    in_dlt_only = in_both = 0
    for user_id in users:
        in_sink, in_dlt = sink_seqs.get(user_id, set()), dlt.get(user_id, set())
        # Верхняя граница — по sink: он читается раньше DLT, и события, попавшие в DLT уже после
        # снимка sink, не должны превращать ещё не записанные seq в «пропуски».
        last = max(in_sink, default=0)
        missing = [s for s in range(1, last + 1) if s not in in_sink]
        gaps = [s for s in missing if s not in in_dlt]
        in_dlt_only += len(missing) - len(gaps)
        in_both += len(in_sink & in_dlt)
        if gaps:
            lost[user_id] = gaps

    print("=== sink ===")
    print(f"rows: {len(rows)}, users: {len(sink_seqs)}")
    for pod in sorted(per_pod):
        print(f"  {pod}: {per_pod[pod]}")
    print(f"rows with duplicated event_id (must be 0): {dup_rows}")

    print(f"=== {settings.DLT_TOPIC} ===")
    print(f"messages: {dlt_total} (unparsed: {dlt_unparsed}), users: {len(dlt)}")
    print(f"seq missing in sink but present in DLT (ok): {in_dlt_only}")
    print(f"seq present both in sink and in DLT: {in_both}")

    print("=== checks ===")
    lost_total = sum(len(v) for v in lost.values())
    print(f"1. seq gaps (lost events): {lost_total} in {len(lost)} of {len(users)} users")
    for user_id, gaps in list(lost.items())[:10]:
        tail = " ..." if len(gaps) > 10 else ""
        print(f"     {user_id}: {gaps[:10]}{tail}")
    print(f"2. duplicates dropped by dedup: {sum(dedup.values())}")
    for pod, count in dedup.items():
        print(f"     {pod}: {count}")
    print(f"3. order violations: {order_violations} in {len(violating_users)} users")

    ok = lost_total == 0 and dup_rows == 0
    print("RESULT:", "OK" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
