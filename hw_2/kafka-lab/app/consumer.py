import json
import os
import signal
import time
import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from app.common import DATABASE_URL, BOOTSTRAP, LOG, deliver_confirmed, producer_config

instance = os.getenv('INSTANCE_ID', '1')
delay = int(os.getenv('PROCESSING_DELAY_MS', '150')) / 1000
stop = False
pending = {}

def terminate(signum, _):
    global stop
    stop = True
    LOG.warning('consumer=%s signal=%d graceful shutdown requested', instance, signum)

signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)
consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP,
    'group.id': 'events-processor',
    'enable.auto.commit': False,
    'enable.auto.offset.store': False,
    'auto.offset.reset': 'earliest',
    'max.poll.interval.ms': 300000,
    'session.timeout.ms': 10000,
    'client.id': f'consumer-{instance}',
})
dlt_producer = Producer(producer_config())

def commit_processed(partitions=None):
    allowed = None if partitions is None else {(p.topic, p.partition) for p in partitions}
    offsets = [TopicPartition(t, p, offset) for (t, p), offset in pending.items()
               if allowed is None or (t, p) in allowed]
    if offsets:
        consumer.commit(offsets=offsets, asynchronous=False)
        LOG.info('consumer=%s committed=%s', instance, [(x.topic, x.partition, x.offset) for x in offsets])
        for x in offsets:
            pending.pop((x.topic, x.partition), None)

def on_assign(_consumer, partitions):
    LOG.info('consumer=%s ASSIGN %s', instance, [(x.topic, x.partition) for x in partitions])

def on_revoke(_consumer, partitions):
    LOG.warning('consumer=%s REVOKE %s', instance, [(x.topic, x.partition) for x in partitions])
    try:
        commit_processed(partitions)
    except Exception:
        LOG.exception('consumer=%s rebalance commit failed; replay will be deduplicated', instance)
    for p in partitions:
        pending.pop((p.topic, p.partition), None)

consumer.subscribe(['events'], on_assign=on_assign, on_revoke=on_revoke)
LOG.info('consumer=%s group=events-processor delay=%.3fs reset=earliest', instance, delay)
try:
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        while not stop:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                LOG.error('consumer=%s Kafka poll error=%s', instance, msg.error())
                continue
            key = (msg.topic(), msg.partition())
            try:
                event = json.loads(msg.value())
                for attempt in range(1, 4):
                    try:
                        time.sleep(delay)
                        if event.get('payload', {}).get('poison'):
                            raise ValueError('poison event (intentional)')
                        with conn.transaction():
                            inserted = conn.execute('''
                              INSERT INTO sink(event_id,user_id,seq,consumer_id,topic,partition,message_offset)
                              VALUES (%s,%s,%s,%s,%s,%s,%s)
                              ON CONFLICT (event_id) DO NOTHING RETURNING event_id
                            ''', (event['event_id'], event['user_id'], event['seq'], instance,
                                  msg.topic(), msg.partition(), msg.offset())).fetchone()
                            if inserted is None:
                                conn.execute("UPDATE stats SET value=value+1 WHERE name='duplicates'")
                                LOG.warning('consumer=%s DUPLICATE event=%s', instance, event['event_id'])
                        break
                    except (ValueError, KeyError, TypeError) as exc:
                        LOG.warning('consumer=%s failure attempt=%d/3 event=%s reason=%s',
                                    instance, attempt, event.get('event_id'), exc)
                        if attempt == 3:
                            headers = [
                                ('error', str(exc).encode()),
                                ('original_topic', msg.topic().encode()),
                                ('original_partition', str(msg.partition()).encode()),
                                ('original_offset', str(msg.offset()).encode()),
                            ]
                            deliver_confirmed(dlt_producer, 'events.dlt', msg.value(), msg.key(), headers)
                            LOG.warning('consumer=%s sent to DLT event=%s original=%s/%d/%d',
                                        instance, event.get('event_id'), msg.topic(), msg.partition(), msg.offset())
                        else:
                            time.sleep(0.15 * attempt)
                pending[key] = msg.offset() + 1
                commit_processed()
            except Exception:
                LOG.exception('consumer=%s processing failed; offset not committed, restarting', instance)
                raise
finally:
    try:
        commit_processed()
    except Exception:
        LOG.exception('consumer=%s shutdown commit unsuccessful', instance)
    consumer.close()
    remaining = dlt_producer.flush(20)
    LOG.info('consumer=%s shutdown dlt_flush_remaining=%d', instance, remaining)
