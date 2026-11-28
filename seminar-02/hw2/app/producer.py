"""Producer pod: generates events for its own disjoint user range.

Design decisions (also in README):
- key = user_id (KEY_MODE=user_id|none); KEY_MODE=none uses round-robin so the
  experiment shows per-user order breaking across partitions.
- acks=all + enable.idempotence=true: no silent loss; delivery-report errors are
  logged, and librdkafka keeps retrying the batch (the event is not counted as
  produced until acknowledged).
- SIGTERM: finish the current round, flush() all buffered messages, persist
  per-user counters, then exit.
- RUN_DURATION=0 means unbounded: the pod keeps producing until SIGTERM, so the
  stand can be demonstrated live; exit happens only on stop/restart.
- Per-user seq counters are persisted in the sink keyed by pod_id, so a producer
  restart resumes counters instead of restarting at 1 (gapless seq survives
  restarts).

Exit code: 0 on normal end/flush, 1 if flush failed (buffered events dropped).
"""
import time

import common
import event as event_model
import metrics
from common import env_float, env_int, env_str, log
from confluent_kafka import KafkaException, Producer
from sink import Sink

POD_INDEX = env_int('POD_INDEX', 1)
POD_ID = f'producer{POD_INDEX}'
common.POD_ID = POD_ID
USERS_PER_POD = env_int('USERS_PER_POD', 10)
RATE = env_int('EVENTS_PER_SECOND', 10)
POISON_RATE = env_float('POISON_RATE', 0.02)
POISON_MODE = env_str('POISON_MODE', 'flag')
KEY_MODE = env_str('KEY_MODE', 'user_id')
RUN_DURATION = env_int('RUN_DURATION', 60)  # 0 = unbounded, run until SIGTERM
PERSIST_EVERY = env_int('PERSIST_EVERY', 100)
BOOTSTRAP = env_str('BOOTSTRAP_SERVERS', 'broker1:9092,broker2:9092,broker3:9092')
TOPIC = env_str('TOPIC', 'events')
FLUSH_TIMEOUT = env_int('FLUSH_TIMEOUT_SECONDS', 15)

USERS = sorted(f'user_{POD_INDEX}_{i:02d}' for i in range(1, USERS_PER_POD + 1))


def delivery_report(err, msg):
    if err is not None:
        # HW requirement: send errors must be logged, not swallowed.
        metrics.produce_errors_total.labels(pod=POD_ID, error=err.str()).inc()
        log('produce_error', error=err.str(), error_code=int(err.code()),
           key=msg.key().decode() if msg.key() else None)
    else:
        metrics.events_produced_total.labels(pod=POD_ID).inc()


def main() -> int:
    metrics.expose()
    common.install_sigterm_handler()
    sink = Sink()
    state = sink.load_state(POD_ID, USERS)
    counters = {u: state.get(u, 0) for u in USERS}
    if any(counters.values()):
        log('producer_state_resumed', counters=counters)

    conf = {
        'bootstrap.servers': BOOTSTRAP,
        'client.id': POD_ID,
        'acks': 'all',
        'enable.idempotence': True,
        'max.in.flight.requests.per.connection': 5,  # in-order within a connection
        # consistent = murmur2 on the key (per-user order when keyed); for KEY_MODE=none
        # we explicitly ask librdkafka for the `random` partitioner so same-user events
        # land on different partitions and can be consumed concurrently.
        'partitioner': 'consistent' if KEY_MODE == 'user_id' else 'random',
    }
    producer = Producer(conf)

    round_seconds = len(USERS) / RATE
    deadline = None if RUN_DURATION == 0 else time.monotonic() + RUN_DURATION
    produced = 0

    def active() -> bool:
        # deadline None = unbounded mode: only SIGTERM ends the run.
        return not common.TERM and (deadline is None or time.monotonic() < deadline)

    while active():
        round_end = time.monotonic() + round_seconds
        for user_id in USERS:
            if not active():
                break
            seq = counters[user_id] + 1
            counters[user_id] = seq
            ev = event_model.make_event(POD_INDEX, user_id, seq, POISON_RATE)
            key = user_id.encode() if KEY_MODE == 'user_id' else None
            try:
                producer.produce(TOPIC, key=key,
                                 value=ev.to_json(corrupt=(POISON_MODE == 'corrupt')),
                                 on_delivery=delivery_report)
                metrics.produce_attempts_total.labels(pod=POD_ID).inc()
                produced += 1
                if ev.payload.get('poison'):
                    log('poison_event_generated', event_id=ev.event_id, user_id=user_id,
                        seq=seq, key=key.decode() if key else None, partition_hint=None)
            except BufferError:
                # Local queue full: wait for callbacks instead of dropping (no silent loss).
                log('buffer_full', user_id=user_id, seq=seq)
                producer.poll(1.0)
            producer.poll(0)
        if produced % PERSIST_EVERY == 0 and produced:
            for user_id, last_seq in counters.items():
                sink.persist_state(POD_ID, user_id, last_seq)
        common.jitter_sleep(round_end)

    # SIGTERM / end-of-run: flush buffered messages before exit.
    try:
        producer.flush(FLUSH_TIMEOUT)
    except Exception as exc:
        log('flush_failed', error=str(exc), unflushed=produced)
        for user_id, last_seq in counters.items():
            sink.persist_state(POD_ID, user_id, last_seq)
        return 1

    for user_id, last_seq in counters.items():
        sink.persist_state(POD_ID, user_id, last_seq)
    log('producer_exit', produced=produced, flushed=True,
        signal='SIGTERM' if common.TERM else 'deadline', users=len(USERS))
    producer.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
