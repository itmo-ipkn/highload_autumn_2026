"""Consumer pod of group `events-processor`.

Semantics (also in README):
- enable.auto.commit=false; offsets are committed only after the sink write of a
  poll batch succeeds (at-least-once, duplicates suppressed by event_id dedup).
- Per-partition commit target = min(read position, smallest failed-but-not-yet-DLT
  offset). A poison message with < 3 attempts is NOT committed, so the next poll
  redelivers it — real redelivery attempts, while the partition advances as soon
  as the 3rd attempt routes the event to events.dlt and commits.
- on_revoke commits processed offsets for the partitions being released before
  they are handed to another member (commit-before-release).
- auto.offset.reset=earliest: deliberate. The group is created fresh for every
  experiment run; 'latest' would silently skip pre-run events and make
  "nothing is lost" impossible to prove.
- Cooperative-sticky assignment so commit(offsets=...) in on_revoke is valid.

Exit: 0 after RUN finishes + IDLE_EXIT_SECONDS empty polls, or on SIGTERM
(commit processed offsets before close). IDLE_EXIT_SECONDS=0 disables the idle exit,
so the pod keeps consuming until it is stopped — the stand stays live.
"""
import time
from datetime import datetime, timezone

import common
import event as event_model
import metrics
from common import env_int, env_str, log
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from sink import Sink

common.POD_ID = env_str('POD_ID', 'consumer1')
POD_ID = common.POD_ID
GROUP = env_str('CONSUMER_GROUP', 'events-processor')
BOOTSTRAP = env_str('BOOTSTRAP_SERVERS', 'broker1:9092,broker2:9092,broker3:9092')
TOPIC = env_str('TOPIC', 'events')
DLT_TOPIC = env_str('DLT_TOPIC', 'events.dlt')
DELAY_MS = env_int('PROCESSING_DELAY_MS', 20)
MAX_POLL_RECORDS = env_int('MAX_POLL_RECORDS', 50)
MAX_ATTEMPTS = env_int('MAX_ATTEMPTS', 3)
IDLE_EXIT_SECONDS = env_int('IDLE_EXIT_SECONDS', 10)  # 0 = never exit on idle
REDELIVERY_BACKOFF_MS = env_int('REDELIVERY_BACKOFF_MS', 200)

unresolved: dict[tuple[str, int], set[int]] = {}  # failed offsets not yet DLT-routed


def _commit_target(consumer, tp: TopicPartition) -> int:
    position = consumer.position([tp])[0].offset
    failed = unresolved.get((tp.topic, tp.partition))
    return min(position, min(failed)) if failed else position


def _seek_for_redelivery(consumer) -> bool:
    """Seek back to the smallest failed offset per partition.

    Kafka redelivery semantics: a member re-reads from the last COMMITTED offset
    (what a rebalance would do on re-assignment). We never commit past a failed
    offset, so seeking back here reproduces that redelivery inside the session
    and makes the 3-attempt policy real instead of theoretical.
    """
    sought = False
    for tp in consumer.assignment():
        failed = unresolved.get((tp.topic, tp.partition))
        if failed:
            consumer.seek(TopicPartition(tp.topic, tp.partition, min(failed)))
            sought = True
    return sought


def _commit(consumer) -> None:
    assigned = consumer.assignment()
    if not assigned:
        return
    offsets = [TopicPartition(tp.topic, tp.partition, _commit_target(consumer, tp))
               for tp in assigned]
    consumer.commit(offsets=offsets)


def on_assign(consumer, partitions):
    for tp in partitions:
        unresolved.setdefault((tp.topic, tp.partition), set())
    metrics.consumer_assigned_partitions.labels(pod=POD_ID).set(len(partitions))
    log('assign', partitions=[[tp.topic, tp.partition,
                               consumer.position([tp])[0].offset] for tp in partitions],
        note='position restored from committed offsets (or earliest on fresh group)')


def on_revoke(consumer, partitions):
    # Commit what this pod has already processed for the partitions it gives up.
    offsets = [TopicPartition(tp.topic, tp.partition, _commit_target(consumer, tp))
               for tp in partitions]
    try:
        consumer.commit(offsets=offsets)
        log('revoke_commit', committed=[[tp.topic, tp.partition, tp.offset] for tp in offsets],
            note='processed offsets committed before partitions are released')
    except KafkaException as exc:
        log('revoke_commit_failed', error=exc.str(), partitions=partitions)
    for tp in partitions:
        unresolved.pop((tp.topic, tp.partition), None)
    metrics.consumer_assigned_partitions.labels(pod=POD_ID).set(
        len(consumer.assignment()) - len(partitions))


def produce_dlt(dlt: Producer, msg, cause: str, attempts: int) -> None:
    """Produce to the DLT and BLOCK until the broker confirms delivery.

    Without a delivery callback, flush() can return after its timeout while the
    message is still queued or later fails silently — that is how experiment 2
    caught a real data-loss window (DLT partition leader down during an outage).
    Only a confirmed delivery may let the caller commit the consumer offset.
    """
    headers = [
        ('x-error', cause.encode('utf-8')),
        ('x-original-topic', msg.topic().encode('utf-8')),
        ('x-original-partition', str(msg.partition()).encode('utf-8')),
        ('x-original-offset', str(msg.offset()).encode('utf-8')),
        ('x-attempts', str(attempts).encode('utf-8')),
        ('x-pod-id', POD_ID.encode('utf-8')),
    ]
    state: dict = {}

    def cb(err, m):
        state['err'] = err

    dlt.produce(DLT_TOPIC, key=msg.key() or None, value=msg.value(), headers=headers, callback=cb)
    dlt.flush(10)
    if 'err' not in state:
        raise RuntimeError('dlt flush timeout without delivery report')
    if state['err'] is not None:
        raise RuntimeError(f'dlt delivery failed: {state["err"]}')


def handle_message(consumer, dlt: Producer, sink: Sink, msg) -> bool:
    """True when the offset for this message may be committed."""
    key = (msg.topic(), msg.partition(), msg.offset())
    try:
        obj = event_model.parse_envelope(msg.value())
        start = time.monotonic()
        event_model.handle(obj)  # simulated business logic, always fails on poison
        time.sleep(DELAY_MS / 1000)
        metrics.processing_delay_seconds.labels(pod=POD_ID).observe(time.monotonic() - start)
        duplicate = sink.record_delivery(obj['event_id'], obj['user_id'], obj['seq'], POD_ID,
                                         msg.topic(), msg.partition(), msg.offset(),
                                         datetime.now(timezone.utc).isoformat(timespec='milliseconds'))
        if duplicate:
            metrics.duplicates_dropped_total.labels(pod=POD_ID).inc()
            log('dedup_drop', event_id=obj['event_id'], user_id=obj['user_id'], seq=obj['seq'],
                partition=msg.partition(), offset=msg.offset(), attempt='duplicate dropped')
        else:
            metrics.events_processed_total.labels(pod=POD_ID).inc()
        unresolved[key[:2]].discard(key[2])
        return True
    except (ValueError, event_model.PoisonError) as cause:  # poison: bad payload/handler always fails
        attempts = sink.record_attempt(msg.topic(), msg.partition(), msg.offset(), POD_ID)
        metrics.poison_attempt_failures_total.labels(pod=POD_ID).inc()
        log('poison_attempt', attempts=attempts, error=str(cause),
            partition=msg.partition(), offset=msg.offset())
        if attempts >= MAX_ATTEMPTS:
            try:
                produce_dlt(dlt, msg, str(cause), attempts)
                metrics.dlt_produced_total.labels(pod=POD_ID).inc()
                unresolved[key[:2]].discard(key[2])
                log('poison_routed_to_dlt', attempts=attempts, error=str(cause),
                    partition=msg.partition(), offset=msg.offset())
                return True
            except Exception as exc:  # noqa: BLE001 - keep the attempt, do not commit
                log('dlt_produce_failed', error=str(exc), partition=msg.partition(),
                    offset=msg.offset(), note='offset not committed, will be retried')
                return False
        unresolved[key[:2]].add(key[2])
        return False


def main() -> int:
    metrics.expose()
    sink = Sink()
    conf = {
        'bootstrap.servers': BOOTSTRAP,
        'group.id': GROUP,
        'client.id': POD_ID,
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest',
        'max.poll.records': MAX_POLL_RECORDS,
        'session.timeout.ms': 6000,
        'partition.assignment.strategy': 'cooperative-sticky',
    }
    dlt = Producer({'bootstrap.servers': BOOTSTRAP, 'client.id': f'{POD_ID}-dlt'})
    consumer = Consumer(conf)
    common.install_sigterm_handler()
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    empty_polls = 0
    processed = 0
    while not common.TERM:
        msg = consumer.poll(1.0)
        if msg is None:
            empty_polls += 1
            if IDLE_EXIT_SECONDS and empty_polls >= IDLE_EXIT_SECONDS:
                log('consumer_exit', reason='idle', processed=processed)
                break
            continue
        empty_polls = 0
        batch = [msg]
        while len(batch) < MAX_POLL_RECORDS:
            m = consumer.poll(0.0)
            if m is None:
                break
            batch.append(m)
        for m in batch:
            if handle_message(consumer, dlt, sink, m):
                processed += 1
        _commit(consumer)
        # Re-read failed offsets (as a rebalance would): reprocessed non-poison
        # messages are suppressed by event_id dedup at the sink, poison messages
        # accumulate attempts until the 3rd failure routes them to events.dlt.
        if _seek_for_redelivery(consumer):
            time.sleep(REDELIVERY_BACKOFF_MS / 1000)

    _commit(consumer)  # commit anything processed but not yet committed
    consumer.close()
    log('consumer_closed', processed=processed, signal='SIGTERM' if common.TERM else 'exit')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
