"""Event consumer pod, member of consumer group `events-processor`.

Delivery semantics: at-least-once + idempotent sink.
  * enable.auto.commit=false; an offset is committed only after the event has been
    written to Postgres (or parked in events.dlt);
  * on rebalance (partitions revoked) already processed offsets are committed synchronously;
  * redeliveries are suppressed by INSERT ... ON CONFLICT (event_id) DO NOTHING and counted.
Poison events: MAX_ATTEMPTS processing attempts, then the original message goes to
events.dlt with the error and the original topic/partition/offset in headers, and the offset
is committed so the partition is not blocked.
"""
import json
import signal
import time
from datetime import datetime, timezone

import psycopg
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from common import decode, env, setup_logging

POD_ID = env("POD_ID")
BOOTSTRAP = env("KAFKA_BOOTSTRAP")
TOPIC = env("TOPIC", "events")
DLT_TOPIC = env("DLT_TOPIC", "events.dlt")
GROUP_ID = env("GROUP_ID", "events-processor")
DELAY_S = env("PROCESSING_DELAY_MS", 5, int) / 1000
MAX_ATTEMPTS = env("MAX_ATTEMPTS", 3, int)
MAX_BATCH = env("MAX_BATCH", 50, int)
PG_DSN = env("PG_DSN")

log = setup_logging(POD_ID)

M_PROCESSED = Counter("consumer_events_written_total", "new events written to sink", ["pod", "partition"])
M_DUP = Counter("consumer_duplicates_total", "redeliveries dropped by dedup", ["pod", "partition"])
M_DLT = Counter("consumer_dlt_total", "events sent to DLT", ["pod"])
M_FAIL = Counter("consumer_processing_failures_total", "failed processing attempts", ["pod"])
M_REBAL = Counter("consumer_rebalances_total", "rebalance callbacks", ["pod", "kind"])
M_COMMIT_ERR = Counter("consumer_commit_errors_total", "failed offset commits", ["pod"])
M_LAG = Gauge("consumer_lag", "consumer lag per owned partition", ["pod", "partition"])
M_OWNED = Gauge("consumer_partition_owned", "1 if partition is assigned to this pod", ["pod", "partition"])
M_COMMITTED = Gauge("consumer_committed_offset", "last committed offset", ["pod", "partition"])


class PoisonEvent(Exception):
    pass


# ----------------------------------------------------------------------------- sink
class Sink:
    INSERT = """
        INSERT INTO processed_events
            (event_id, user_id, seq, type, produced_at, consumer_pod, topic, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
    """
    DUP = """
        INSERT INTO duplicate_deliveries
            (event_id, user_id, seq, consumer_pod, topic, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    def __init__(self):
        self.conn = None

    def _connect(self):
        self.conn = psycopg.connect(PG_DSN, autocommit=False)

    def write(self, ev: dict, msg) -> bool:
        """Idempotent write. Returns True if the event is new, False if it was a duplicate.
        Retries forever on sink errors: a broken sink is not a reason to skip an event."""
        backoff = 0.5
        while True:
            try:
                if self.conn is None or self.conn.closed:
                    self._connect()
                with self.conn.transaction():
                    cur = self.conn.execute(self.INSERT, (
                        ev["event_id"], ev["user_id"], ev["seq"], ev["type"], ev["produced_at"],
                        POD_ID, msg.topic(), msg.partition(), msg.offset()))
                    is_new = cur.rowcount == 1
                    if not is_new:
                        self.conn.execute(self.DUP, (
                            ev["event_id"], ev["user_id"], ev["seq"],
                            POD_ID, msg.topic(), msg.partition(), msg.offset()))
                return is_new
            except psycopg.OperationalError as e:
                log.error("sink unavailable (%s), retry in %.1fs", e, backoff)
                try:
                    self.conn and self.conn.close()
                except Exception:
                    pass
                self.conn = None
                if not running:
                    raise SystemExit("stopping while sink is down; offset not committed")
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)


# ----------------------------------------------------------------------------- kafka
dlt_producer = Producer({
    "bootstrap.servers": BOOTSTRAP,
    "client.id": f"{POD_ID}-dlt",
    "acks": "all",
    "enable.idempotence": True,
})

# next offset to commit per partition (offset of last fully handled message + 1)
processed: dict[int, int] = {}
committed: dict[int, int] = {}
first_after_assign: set[int] = set()


def commit(partitions=None) -> None:
    parts = [p for p in (partitions if partitions is not None else processed.keys())
             if p in processed and processed[p] != committed.get(p)]
    if not parts:
        return
    offsets = [TopicPartition(TOPIC, p, processed[p]) for p in parts]
    try:
        consumer.commit(offsets=offsets, asynchronous=False)
        for p in parts:
            committed[p] = processed[p]
            M_COMMITTED.labels(POD_ID, str(p)).set(processed[p])
    except KafkaException as e:
        # not fatal: the offsets stay in `processed` and are retried on the next commit;
        # if the partition moves to another pod meanwhile, it re-reads -> dedup drops dups
        M_COMMIT_ERR.labels(POD_ID).inc()
        log.error("commit failed for %s: %s", {p: processed[p] for p in parts}, e)


def on_assign(c, partitions):
    M_REBAL.labels(POD_ID, "assign").inc()
    try:
        comm = c.committed(partitions, timeout=10)
    except KafkaException as e:
        comm = partitions
        log.warning("cannot fetch committed offsets: %s", e)
    desc = []
    for tp in comm:
        M_OWNED.labels(POD_ID, str(tp.partition)).set(1)
        first_after_assign.add(tp.partition)
        processed.pop(tp.partition, None)
        committed.pop(tp.partition, None)
        desc.append(f"p{tp.partition}@{tp.offset if tp.offset >= 0 else 'none->earliest'}")
    log.info("REBALANCE assigned: %s (resume from committed offset)", ", ".join(desc) or "-")


def on_revoke(c, partitions):
    M_REBAL.labels(POD_ID, "revoke").inc()
    parts = [tp.partition for tp in partitions]
    # commit everything already written to the sink before giving partitions away
    commit(parts)
    log.info("REBALANCE revoked: %s, committed before handing over: %s",
             parts, {p: committed.get(p) for p in parts})
    for p in parts:
        M_OWNED.labels(POD_ID, str(p)).set(0)
        try:
            M_LAG.remove(POD_ID, str(p))
        except KeyError:
            pass
        processed.pop(p, None)
        committed.pop(p, None)


def on_lost(c, partitions):
    # partitions were taken away without a clean revoke (session timeout) - cannot commit
    M_REBAL.labels(POD_ID, "lost").inc()
    log.warning("REBALANCE lost: %s", [tp.partition for tp in partitions])
    for tp in partitions:
        M_OWNED.labels(POD_ID, str(tp.partition)).set(0)
        processed.pop(tp.partition, None)
        committed.pop(tp.partition, None)


def on_stats(raw: str):
    s = json.loads(raw)
    parts = s.get("topics", {}).get(TOPIC, {}).get("partitions", {})
    for p, info in parts.items():
        if p == "-1":
            continue
        if info.get("fetch_state") == "active" and info.get("consumer_lag", -1) >= 0:
            M_LAG.labels(POD_ID, p).set(info["consumer_lag"])


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "client.id": POD_ID,
    "enable.auto.commit": False,
    "enable.auto.offset.store": False,
    # a brand new group (or expired offsets) starts from the beginning of the log:
    # with "latest" every event produced before the first commit would be lost,
    # with "earliest" we may only re-read, and re-reads are absorbed by dedup.
    "auto.offset.reset": "earliest",
    # a killed pod is detected after session.timeout.ms -> rebalance
    "session.timeout.ms": 10_000,
    "heartbeat.interval.ms": 2_000,
    "max.poll.interval.ms": 120_000,
    "statistics.interval.ms": 2000,
    "stats_cb": on_stats,
})


# ----------------------------------------------------------------------------- processing
def process(raw: bytes) -> dict:
    """Emulated business logic."""
    ev = decode(raw)
    time.sleep(DELAY_S)
    if ev.get("payload", {}).get("poison"):
        raise PoisonEvent(f"cannot process poison event {ev['event_id']}")
    return ev


def send_to_dlt(msg, error: Exception, attempts: int):
    headers = list(msg.headers() or []) + [
        ("dlt.error.class", type(error).__name__.encode()),
        ("dlt.error.message", str(error).encode()),
        ("dlt.original.topic", msg.topic().encode()),
        ("dlt.original.partition", str(msg.partition()).encode()),
        ("dlt.original.offset", str(msg.offset()).encode()),
        ("dlt.attempts", str(attempts).encode()),
        ("dlt.consumer", POD_ID.encode()),
        ("dlt.failed_at", datetime.now(timezone.utc).isoformat().encode()),
    ]
    # The offset is committed only after the DLT write is acknowledged (acks=all).
    # While it is not acked (e.g. ISR < min.insync.replicas) we keep waiting for the SAME
    # message - re-producing it on a flush timeout would put a second copy into the queue.
    # A new copy is produced only when the client reports a final delivery error.
    while True:
        result = {}
        dlt_producer.produce(DLT_TOPIC, key=msg.key(), value=msg.value(), headers=headers,
                             on_delivery=lambda err, m: result.update(err=err))
        while "err" not in result:
            if dlt_producer.flush(10) > 0:
                log.error("DLT write p%d@%d not acknowledged yet, waiting",
                          msg.partition(), msg.offset())
        if result["err"] is None:
            return
        log.error("DLT write failed (%s), producing again", result["err"])
        time.sleep(1)


def handle(msg, sink: Sink):
    last_err = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            ev = process(msg.value())
            break
        except Exception as e:  # noqa: BLE001 - any processing error counts as a failed attempt
            last_err = e
            M_FAIL.labels(POD_ID).inc()
            log.warning("attempt %d/%d failed p%d@%d: %s", attempt, MAX_ATTEMPTS,
                        msg.partition(), msg.offset(), e)
            if attempt < MAX_ATTEMPTS:
                time.sleep(0.05 * attempt)
    else:
        send_to_dlt(msg, last_err, MAX_ATTEMPTS)
        M_DLT.labels(POD_ID).inc()
        log.warning("-> DLT p%d@%d: %s", msg.partition(), msg.offset(), last_err)
        return

    if sink.write(ev, msg):
        M_PROCESSED.labels(POD_ID, str(msg.partition())).inc()
    else:
        M_DUP.labels(POD_ID, str(msg.partition())).inc()
        log.info("duplicate dropped: user=%s seq=%s p%d@%d",
                 ev["user_id"], ev["seq"], msg.partition(), msg.offset())


running = True


def stop(signum, _frame):
    global running
    log.info("got signal %s, stopping", signal.Signals(signum).name)
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def main():
    start_http_server(8000)
    sink = Sink()
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)
    log.info("start: group=%s delay=%.0fms max_attempts=%d", GROUP_ID, DELAY_S * 1000, MAX_ATTEMPTS)
    try:
        while running:
            msgs = consumer.consume(num_messages=MAX_BATCH, timeout=1.0)
            for msg in msgs:
                if msg.error():
                    log.error("consume error: %s", msg.error())
                    continue
                p = msg.partition()
                if p in first_after_assign:
                    first_after_assign.discard(p)
                    log.info("first message after assign: p%d@%d", p, msg.offset())
                handle(msg, sink)
                processed[p] = msg.offset() + 1
            commit()
    finally:
        log.info("closing consumer (commits processed offsets and leaves the group)")
        commit()
        consumer.close()
        dlt_producer.flush(10)


if __name__ == "__main__":
    main()
