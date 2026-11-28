"""Verification CLI (HW part 4).

First output line is the GitHub login and the current time, as required.

Three checks only:
1. per-user seq continuity in `events`: a missing seq is acceptable only when the
   same user/seq event is found in the `events.dlt` topic (poison is routed, not lost);
2. how many duplicate deliveries were dropped by deduplication;
3. order violations per user_id: writes with a smaller seq recorded after a larger one
   (sink first-write order).

Exit code 0 = no unexplained gaps; 1 otherwise.
The DLT is a topic, not a table, so check 1 reads the actual topic from offset 0.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer

import logic


def conninfo() -> str:
    return (
        f"host={os.environ.get('PGHOST', 'postgres')} port={os.environ.get('PGPORT', '5432')} "
        f"dbname={os.environ.get('PGDATABASE', 'lab2')} user={os.environ.get('PGUSER', 'lab2')} "
        f"password={os.environ.get('PGPASSWORD', 'lab2-demo-password')}"
    )


def read_dlt() -> dict[str, tuple[str, int]]:
    """event_id -> (user_id, seq) for unique event_ids in events.dlt.

    A fresh unique group id per invocation guarantees a full read from offset 0
    on every run (earliest only applies when the group has no committed offsets).
    """
    # Inside a container the compose network names resolve; on the host the EXTERNAL
    # listener must be reached as 127.0.0.1, because 'localhost' resolves to ::1 first
    # on Windows while the broker ports are published on 127.0.0.1 only.
    bootstrap = os.environ.get('BOOTSTRAP_SERVERS', '127.0.0.1:9094,127.0.0.1:9095,127.0.0.1:9096')
    dlt_topic = os.environ.get('DLT_TOPIC', 'events.dlt')
    group = f"verify-cli-{int(time.time() * 1000) % 10**9}"
    errors: list[str] = []

    def on_error(ck, err) -> None:
        # A connection failure must be reported, not silently become an empty DLT read.
        errors.append(str(err))

    consumer = Consumer({
        'bootstrap.servers': bootstrap,
        'group.id': group,
        'client.id': group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'partition.assignment.strategy': 'cooperative-sticky',
        'socket.connection.setup.timeout.ms': 5000,
        'error_cb': on_error,
    })
    consumer.subscribe([dlt_topic])
    entries: dict[str, tuple[str, int]] = {}
    watermarks: dict[int, int] = {}  # partition -> end offset when the read started
    seen: dict[int, int] = {}        # partition -> next offset we expect to read
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            if not watermarks:
                parts = consumer.assignment()
                if not parts:
                    continue  # still connecting/rebalancing: empty polls are not an empty topic
                watermarks = {tp.partition: consumer.get_watermark_offsets(tp, 5)[1] for tp in parts}
                continue
            if all(seen.get(p, 0) >= high for p, high in watermarks.items()):
                break  # every partition read to its end offset: the DLT is drained
            continue
        if msg.error():
            errors.append(str(msg.error()))
            continue
        if msg.offset() is None:  # error/placeholder message, nothing to record
            continue
        seen[msg.partition()] = msg.offset() + 1
        obj = json.loads(msg.value())
        entries[obj['event_id']] = (obj['user_id'], obj['seq'])
    consumer.close()  # no commit: the group is read-only and unique per invocation
    if not entries and errors:
        # An unread DLT would otherwise look like "gaps not explained by DLT".
        print(f'WARNING: could not read {dlt_topic} through {bootstrap}: {errors[0]}')
        print('  host runs must reach the EXTERNAL listener as 127.0.0.1 (see '
              'scripts/verify_local.sh); inside the stand use the INTERNAL listener, '
              'e.g. docker compose run --rm -T verify.')
    return entries


def main() -> int:
    login = os.environ.get('GITHUB_LOGIN', 'tpl20000')
    print(f'{login} {datetime.now(timezone.utc).isoformat(timespec="seconds")}')

    conn = psycopg.connect(conninfo())
    events = conn.execute('SELECT user_id, seq FROM events ORDER BY user_id, seq').fetchall()
    deliveries = conn.execute(
        'SELECT user_id, seq, is_duplicate FROM deliveries ORDER BY attempt_seq').fetchall()
    dlt = read_dlt()

    dlt_by_user: dict[str, set[int]] = {}
    for user_id, seq in dlt.values():
        dlt_by_user.setdefault(user_id, set()).add(seq)

    # 1. per-user gaps explained by events.dlt
    by_user: dict[str, list[int]] = {}
    for user_id, seq in events:
        by_user.setdefault(user_id, []).append(seq)
    explained = unexplained = 0
    print('\n1. per-user seq continuity (gaps must be in events.dlt)')
    for user_id in sorted(set(by_user) | set(dlt_by_user)):
        present = sorted(by_user.get(user_id, []))
        end = max(max(present, default=0), max(dlt_by_user.get(user_id, set()), default=0))
        if end == 0:
            continue
        missing = logic.find_gaps(present, 1, end)
        ok, bad = logic.split_gaps(missing, dlt_by_user.get(user_id, set()))
        explained += len(ok)
        unexplained += len(bad)
        if bad:
            print(f'UNEXPLAINED {user_id}: missing seqs {bad}')
    print(f'users: {len(set(by_user) | set(dlt_by_user))}, '
          f'gaps explained by events.dlt: {explained}, unexplained: {unexplained}')

    # 2. duplicate deliveries dropped by dedup
    dropped = sum(1 for r in deliveries if r[2])
    print(f'\n2. deduplication: deliveries logged {len(deliveries)}, dropped duplicates {dropped}')

    # 3. order violations: smaller seq written after a larger one
    print('\n3. order violations (per user_id, sink first-write order)')
    order: dict[str, list[int]] = {}
    for user_id, seq, is_dup in deliveries:
        if not is_dup:
            order.setdefault(user_id, []).append(seq)
    total = 0
    for user_id in sorted(order):
        inv = logic.count_inversions(order[user_id])
        total += inv
        if inv:
            print(f'{user_id}: {inv}')
    print(f'total order violations: {total}')

    print(f'\nRESULT: {"PASS" if unexplained == 0 else "FAIL"}')
    return 0 if unexplained == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
