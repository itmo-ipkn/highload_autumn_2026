"""verify: checks the sink (Postgres) + events.dlt.

  docker compose run --rm verify            # GITHUB_LOGIN is taken from the environment / .env

Prints:
  1. per user_id: seq has no gaps (a seq missing in the sink is OK only if it is in events.dlt)
  2. how many redeliveries were dropped by dedup
  3. order violations: how many times an event with a smaller seq was written to the sink
     AFTER an event with a bigger seq of the same user
"""
import os
import subprocess
import sys
import uuid
from collections import Counter, defaultdict

import psycopg
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from common import decode

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:29092,localhost:29093,localhost:29094")
PG_DSN = os.environ.get("PG_DSN", "postgresql://hw2:hw2@localhost:55432/hw2")
TOPIC = "events"
DLT = "events.dlt"
GROUP = "events-processor"
SHOW = 5


def now() -> str:
    try:
        return subprocess.check_output(["date"], text=True).strip()
    except Exception:  # noqa: BLE001
        from datetime import datetime
        return datetime.now().astimezone().isoformat()


def read_dlt():
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": f"verify-{uuid.uuid4()}",   # never commits, never joins the real group
        "enable.auto.commit": False,
    })
    md = c.list_topics(DLT, timeout=10)
    parts = list(md.topics[DLT].partitions)
    remaining, start = {}, {}
    for p in parts:
        lo, hi = c.get_watermark_offsets(TopicPartition(DLT, p), timeout=10)
        if hi > lo:
            remaining[p], start[p] = hi, lo
    c.assign([TopicPartition(DLT, p, start[p]) for p in remaining])
    out = []
    while remaining:
        msg = c.poll(5)
        if msg is None:
            break
        if msg.error():
            continue
        hdr = {k: v.decode() for k, v in (msg.headers() or [])}
        out.append((decode(msg.value()), hdr))
        if msg.offset() + 1 >= remaining.get(msg.partition(), 0):
            remaining.pop(msg.partition(), None)
    # group lag on the main topic, to warn if verify runs while consumers are behind
    lag = {}
    try:
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
        fut = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP)])[GROUP]
        committed = {tp.partition: tp.offset for tp in fut.result(10).topic_partitions
                     if tp.topic == TOPIC}
        for p in c.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions:
            _, hi = c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=10)
            lag[p] = hi - max(committed.get(p, 0), 0)
    except Exception as e:  # noqa: BLE001
        lag = {"error": str(e)}
    c.close()
    return out, lag


def main():
    login = os.environ.get("GITHUB_LOGIN", "unknown")
    print(f"{login} {now()}")
    print("=" * 72)

    with psycopg.connect(PG_DSN) as conn:
        rows = conn.execute(
            "SELECT user_id, seq, write_id, consumer_pod FROM processed_events ORDER BY write_id"
        ).fetchall()
        dups_by_pod = dict(conn.execute(
            "SELECT consumer_pod, count(*) FROM duplicate_deliveries GROUP BY 1 ORDER BY 1").fetchall())
        dup_total = sum(dups_by_pod.values())

    dlt, lag = read_dlt()

    by_pod = Counter(r[3] for r in rows)
    sink_seqs = defaultdict(set)
    written = defaultdict(list)          # user -> seqs in write order
    for user, seq, _wid, _pod in rows:
        sink_seqs[user].add(seq)
        written[user].append(seq)

    dlt_seqs = defaultdict(set)
    dlt_ids = set()
    dlt_errors = Counter()
    non_poison_in_dlt = 0
    for ev, hdr in dlt:
        dlt_seqs[ev["user_id"]].add(ev["seq"])
        dlt_ids.add(ev["event_id"])
        dlt_errors[hdr.get("dlt.error.class", "?")] += 1
        if not ev.get("payload", {}).get("poison"):
            non_poison_in_dlt += 1

    print(f"sink: {len(rows)} unique events, {len(sink_seqs)} users; by consumer pod: {dict(sorted(by_pod.items()))}")
    print(f"events.dlt: {len(dlt)} messages ({len(dlt_ids)} unique events, "
          f"{len(dlt) - len(dlt_ids)} re-sent to DLT after a crash), errors: {dict(dlt_errors)}, non-poison in DLT: {non_poison_in_dlt}")
    if dlt:
        ev, h = dlt[-1]
        print(f"  last DLT headers: orig={h.get('dlt.original.topic')}/{h.get('dlt.original.partition')}"
              f"@{h.get('dlt.original.offset')} attempts={h.get('dlt.attempts')} "
              f"error=\"{h.get('dlt.error.message')}\"")
    print(f"consumer group '{GROUP}' lag: {lag}")
    if isinstance(lag, dict) and any(isinstance(v, int) and v > 0 for v in lag.values()):
        print("  WARNING: consumers are still behind - stop producers and wait for lag=0 for a final check")

    # ---- 1. gaps
    users = set(sink_seqs) | set(dlt_seqs)
    gaps = {}
    in_both = 0
    for u in users:
        s, d = sink_seqs[u], dlt_seqs[u]
        in_both += len(s & d)
        top = max(s | d)
        missing = [x for x in range(1, top + 1) if x not in s and x not in d]
        if missing:
            gaps[u] = missing
    lost = sum(len(v) for v in gaps.values())
    produced = sum(max(sink_seqs[u] | dlt_seqs[u]) for u in users)
    covered_by_dlt = sum(len(d - sink_seqs[u]) for u, d in dlt_seqs.items())
    print("-" * 72)
    print(f"[1] seq gaps: events produced (sum of max seq)={produced}, users={len(users)}, users with gaps={len(gaps)}, LOST events={lost}; "
          f"seqs absent in sink but present in DLT={covered_by_dlt}; in both sink and DLT={in_both}")
    for u in sorted(gaps)[:SHOW]:
        print(f"    {u}: missing seq {gaps[u][:10]}{' ...' if len(gaps[u]) > 10 else ''}")

    # ---- 2. duplicates
    print(f"[2] duplicates dropped by dedup: {dup_total} (by pod: {dups_by_pod})")

    # ---- 3. order violations
    violations = 0
    users_viol = Counter()
    for u, seqs in written.items():
        hi = 0
        for s in seqs:
            if s < hi:
                violations += 1
                users_viol[u] += 1
            hi = max(hi, s)
    print(f"[3] order violations (smaller seq written after bigger seq): {violations} "
          f"in {len(users_viol)} users")
    for u, n in users_viol.most_common(SHOW):
        print(f"    {u}: {n}")

    print("=" * 72)
    ok = lost == 0 and violations == 0 and non_poison_in_dlt == 0
    print("RESULT:", "OK" if ok else "PROBLEMS FOUND", f"(lost={lost}, order_violations={violations}, "
          f"duplicates_dropped={dup_total})")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
