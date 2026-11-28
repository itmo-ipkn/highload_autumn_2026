import collections
import datetime as dt
import json
import os
import time
import psycopg
from confluent_kafka import Consumer, TopicPartition
from app.common import BOOTSTRAP, DATABASE_URL

print(f"{os.getenv('GITHUB_LOGIN', 'CHANGE_ME')} {dt.datetime.now().astimezone().isoformat(timespec='seconds')}", flush=True)


def read_dlt():
    client = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': 'lab-verify-' + str(os.getpid()),
                       'enable.auto.commit': False, 'auto.offset.reset': 'earliest'})
    rows = []
    try:
        parts = [TopicPartition('events.dlt', p) for p in range(3)]
        end = {}
        for part in parts:
            low, high = client.get_watermark_offsets(part, timeout=15, cached=False)
            part.offset = low
            end[part.partition] = high
        client.assign(parts)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            positions = {p.partition: p.offset for p in client.position(parts)}
            if all(positions.get(p, -1) >= high for p, high in end.items()):
                break
            message = client.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            if message.offset() >= end[message.partition()]:
                continue
            headers = dict(message.headers() or [])
            required = ('error', 'original_topic', 'original_partition', 'original_offset')
            if any(not headers.get(k) for k in required):
                raise ValueError(f'Missing required DLT headers at {message.partition()}/{message.offset()}')
            event = json.loads(message.value())
            rows.append((event['user_id'], int(event['seq']), event['event_id']))
        else:
            raise TimeoutError('DLT scan did not reach captured high watermarks')
    finally:
        client.close()
    return rows


with psycopg.connect(DATABASE_URL) as conn:
    sink = conn.execute('SELECT user_id,seq,event_id,write_order FROM sink ORDER BY write_order').fetchall()
    duplicates = conn.execute("SELECT value FROM stats WHERE name='duplicates'").fetchone()[0]
    progress = dict(conn.execute('SELECT user_id,last_seq FROM producer_progress').fetchall())

dlt = read_dlt()
seen = collections.defaultdict(set)
ids = collections.defaultdict(set)
for user, seq, event_id, _ in sink:
    seen[user].add(seq)
    ids[(user, seq)].add(str(event_id))
for user, seq, event_id in dlt:
    seen[user].add(seq)
    ids[(user, seq)].add(str(event_id))


inversions = 0
order_by_user = collections.defaultdict(list)
for user, seq, _, _ in sink:
    order_by_user[user].append(seq)
for user, seqs in order_by_user.items():
    inversions += sum(1 for i, x in enumerate(seqs) for y in seqs[i+1:] if x > y)

missing_count = 0
bad_users = []
for user in sorted(set(progress) | set(seen)):
    maximum = max(progress.get(user, 0), max(seen[user], default=0))
    missing = sorted(set(range(1, maximum+1)) - seen[user])
    missing_count += len(missing)
    if missing:
        bad_users.append(user)
    print(f'user={user} max_seq={maximum} sink={len(order_by_user[user])} '
          f'dlt={sum(u == user for u, _, _ in dlt)} missing={len(missing)} '
          f'missing_examples={missing[:10]}')

collision = [(u, s) for (u, s), event_ids in ids.items() if len(event_ids) > 1]
print(f'RESULT sink_events={len(sink)} dlt_messages={len(dlt)} '
      f'deduplicated_redeliveries={duplicates} order_violations={inversions} '
      f'missing_seqs={missing_count} users_with_missing={len(bad_users)} '
      f'seq_event_id_conflicts={len(collision)}')
if missing_count or collision:
    print('FAIL: missing seq or contradictory event ids (stop producers and drain consumers before verification)')
    raise SystemExit(1)
print('PASS: no missing seq in sink + DLT; order violations may be nonzero in KEY_MODE=none')
