import json
import time
import uuid
from bisect import bisect_left, insort
from collections import defaultdict
from datetime import datetime

import psycopg2
from confluent_kafka import Consumer

from common.config import KAFKA_BOOTSTRAP, TOPIC_DLT, PG_DSN, GITHUB_LOGIN

EMPTY_POLLS_LIMIT = 3


def load_dlt() -> dict:
    """Читает events.dlt с самого начала, возвращает {user_id: set(seq)}."""
    c = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"verify-dlt-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([TOPIC_DLT])
    by_user = defaultdict(set)
    empty_polls = 0
    while empty_polls < EMPTY_POLLS_LIMIT:
        msgs = c.consume(num_messages=200, timeout=1.0)
        if not msgs:
            empty_polls += 1
            continue
        empty_polls = 0
        for m in msgs:
            if m.error():
                continue
            try:
                ev = json.loads(m.value().decode())
                by_user[ev["user_id"]].add(int(ev["seq"]))
            except Exception as e:
                print(f"  ! dlt record unreadable: {e}")
    c.close()
    return by_user


def main():
    # Первая строка — логин и текущее время (для проверки скриншотов)
    print(f"{GITHUB_LOGIN} {datetime.now().astimezone().isoformat(timespec='seconds')}")

    conn = psycopg2.connect(PG_DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, seq, event_id, inserted_at FROM events")
        sink_rows = cur.fetchall()
        cur.execute("SELECT duplicates FROM metrics WHERE id = 1")
        duplicates = cur.fetchone()[0]
    conn.close()

    sink_by_user = defaultdict(list)  # user -> [(inserted_at, seq)]
    for user_id, seq, _eid, inserted_at in sink_rows:
        sink_by_user[user_id].append((inserted_at, seq))

    dlt_by_user = load_dlt()

    # 1. Пропуски в seq: union(sink, dlt) должен быть 1..max без дырок
    users = set(sink_by_user) | set(dlt_by_user)
    missing_total = 0
    users_with_gaps = []
    for user in sorted(users):
        present = set(s for _, s in sink_by_user.get(user, [])) | dlt_by_user.get(user, set())
        if not present:
            continue
        max_seq = max(present)
        missing = [s for s in range(1, max_seq + 1) if s not in present]
        if missing:
            missing_total += len(missing)
            users_with_gaps.append((user, missing))

    # 2. Нарушения порядка: событие с меньшим seq записано позже события с большим
    inversions_total = 0
    for user in sorted(sink_by_user):
        events = sorted(sink_by_user[user], key=lambda x: (x[0], x[1]))
        seen = []
        for _, seq in events:
            idx = bisect_left(seen, seq)
            inversions_total += len(seen) - idx
            insort(seen, seq)

    print(f"sink events: {len(sink_rows)}")
    print(f"users observed: {len(users)}")
    print(f"duplicates dropped by deduplication: {duplicates}")
    print(f"ordering violations (inversions): {inversions_total}")
    print(f"missing seq (neither in sink nor in dlt): {missing_total}")
    if users_with_gaps:
        print("users with gaps:")
        for user, missing in users_with_gaps[:20]:
            print(f"  {user}: missing seq {missing[:20]}{'...' if len(missing) > 20 else ''}")
    else:
        print("no gaps: every seq of every user is either in sink or in events.dlt")
    if inversions_total > 0:
        print("ORDER VIOLATIONS DETECTED (expected with KEY_MODE=none)")
    else:
        print("order is consistent (expected with KEY_MODE=user_id)")


if __name__ == "__main__":
    main()
