from __future__ import annotations

import os
import sqlite3
import sys
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer


def read_sink(path: str) -> tuple[list[tuple], list[tuple]]:
    conn = sqlite3.connect(path)
    rows = conn.execute(
        "SELECT rowid, user_id, seq, event_id, consumer_pod, topic, partition, offset"
        " FROM sink ORDER BY rowid"
    ).fetchall()
    stats = conn.execute(
        "SELECT pod, processed, duplicates, dlt FROM stats ORDER BY pod"
    ).fetchall()
    conn.close()
    return rows, stats


def read_dlt(topic: str, hosts: str) -> list[tuple]:
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=hosts,
        auto_offset_reset="earliest",
        consumer_timeout_ms=15000,
    )
    out = []
    for msg in consumer:
        try:
            obj = json.loads(msg.value.decode("utf-8"))
        except ValueError:
            continue
        if (
            not isinstance(obj, dict)
            or obj.get("user_id") is None
            or obj.get("seq") is None
        ):
            continue
        headers = dict(msg.headers or [])
        out.append(
            (
                obj["user_id"],
                obj["seq"],
                obj.get("event_id"),
                {k: v.decode() for k, v in headers.items()},
            )
        )
    return out


def main() -> None:

    login = os.environ.get("GITHUB_LOGIN", "unknown")
    sink_path = os.environ.get("SINK", "/data/sink.sqlite")
    hosts = os.environ.get("KAFKA_HOSTS", "broker-1:9092,broker-2:9092,broker-3:9092")
    dlt_topic = os.environ.get("DLT_TOPIC", "events.dlt")

    rows, stats = read_sink(sink_path)
    dlt = read_dlt(dlt_topic, hosts)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    print(f"{login} - {now}")
    print(f"sink записей: {len(rows)}, dlt записей: {len(dlt)}")

    for pod, processed, duplicates, dlt_count in stats:
        print(
            f"pod {pod}: proc={processed} dupl={duplicates} dlt={dlt_count}"
        )

    dlt_seqs: dict[int, set[int]] = {}

    for user_id, seq, _, _ in dlt:
        dlt_seqs.setdefault(user_id, set()).add(seq)

    # Check1

    sink_seqs: dict[int, set[int]] = {}
    for _, user_id, seq, *_ in rows:
        sink_seqs.setdefault(user_id, set()).add(seq)
    gaps: list[str] = []
    for user_id in sorted(sink_seqs):
        written = sink_seqs[user_id]
        for seq in range(1, max(written) + 1):
            if seq not in written and seq not in dlt_seqs.get(user_id, set()):
                gaps.append(
                    f"{user_id} с seq {seq} нет в топиках"
                )
    print(f"Эксперимент 1: {len(gaps)} записей")
    for line in gaps[:10]:
        print(f"{line}")

    # Check1

    duplicates = sum(row[2] for row in stats)
    print(f"Эксперимент 2: {duplicates}")

    # Check3

    violations = 0
    last_seq: dict[int, int] = {}
    for _, user_id, seq, *_ in rows:
        if seq < last_seq.get(user_id, 0):
            violations += 1
        last_seq[user_id] = max(last_seq.get(user_id, 0), seq)
    print(f"Эксперимент 3: {violations}")

    ok = not gaps and not violations
    print("Итог: ", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
