import datetime as dt
import json
import os
import random
import signal
import time
import uuid
import psycopg
from confluent_kafka import Producer
from app.common import DATABASE_URL, LOG, deliver_confirmed, producer_config

instance = int(os.getenv('INSTANCE_ID', '1'))
rate = float(os.getenv('EVENTS_PER_SECOND', '3'))
users_per = int(os.getenv('USERS_PER_PRODUCER', '12'))
mode = os.getenv('KEY_MODE', 'user_id')
if mode not in ('user_id', 'none') or rate <= 0:
    raise ValueError('KEY_MODE must be user_id or none; rate must be positive')
users = [f'user-{instance:02d}-{i:03d}' for i in range(1, users_per + 1)]
stop = False

def terminate(signum, _frame):
    global stop
    stop = True
    LOG.info('producer=%d received signal=%d; stopping', instance, signum)

signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)
producer = Producer(producer_config())
with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
    for user in users:
        conn.execute('INSERT INTO producer_progress (user_id) VALUES (%s) ON CONFLICT DO NOTHING', (user,))
    progress = dict(conn.execute('SELECT user_id, last_seq FROM producer_progress WHERE user_id = ANY(%s)', (users,)).fetchall())
    LOG.info('producer=%d started mode=%s users=%d rate=%s/s', instance, mode, len(users), rate)
    counter = 0
    next_at = time.monotonic()
    try:
        while not stop:
            now = time.monotonic()
            if now < next_at:
                time.sleep(min(next_at-now, 0.1))
                continue
            user = users[counter % len(users)]
            seq = progress[user] + 1
            event = {
                'event_id': str(uuid.uuid5(uuid.NAMESPACE_URL, f'kafka-lab/{user}/{seq}')),
                'user_id': user,
                'type': random.choice(['page_view', 'add_to_cart', 'purchase']),
                'seq': seq,
                'produced_at': dt.datetime.now(dt.timezone.utc).isoformat(),
                'payload': {'poison': random.random() < .02, 'producer': instance, 'sample': counter},
            }
            try:
                msg = deliver_confirmed(producer, 'events', json.dumps(event).encode(),
                                        key=user.encode() if mode == 'user_id' else None)
                conn.execute('UPDATE producer_progress SET last_seq=%s WHERE user_id=%s', (seq, user))
                progress[user] = seq
                counter += 1
                if counter % 30 == 0:
                    LOG.info('producer=%d sent=%d latest=%s/%d partition=%d', instance, counter, user, seq, msg.partition())
                next_at = max(next_at + 1/rate, time.monotonic())
            except Exception:
                LOG.exception('producer=%d send failed user=%s seq=%d (will retry)', instance, user, seq)
                time.sleep(1)
    finally:
        remaining = producer.flush(30)
        LOG.info('producer=%d shutdown flush_remaining=%d', instance, remaining)
        if remaining:
            raise RuntimeError(f'{remaining} buffered messages were not delivered')
