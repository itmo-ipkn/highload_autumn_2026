import json
import logging
import signal
import time

from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from config import ConsumerSettings
from sink import Sink

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("consumer")

settings = ConsumerSettings()

PROCESSED = Counter("consumer_events_processed_total", "Events written to sink", ["pod"])
REBALANCES = Counter("consumer_rebalance_events_total", "Rebalance callbacks", ["pod", "kind"])
ASSIGNED = Gauge("consumer_assigned_partitions", "Partitions assigned to this pod", ["pod"])
FAILURES = Counter("consumer_processing_failures_total", "Failed processing attempts", ["pod"])
DLT_SENT = Counter("consumer_dlt_sent_total", "Events sent to DLT", ["pod"])
DEDUP_DROPPED = Counter("consumer_dedup_dropped_total", "Duplicates dropped by sink", ["pod"])

running = True

# Обработанные, но ещё не закоммиченные оффсеты: (topic, partition) -> следующий оффсет к чтению.
pending: dict[tuple[str, int], int] = {}
uncommitted = 0
last_commit_at = time.monotonic()


class PoisonEventError(Exception):
    """Ядовитое событие: обработка падает всегда."""


class DltDeliveryError(Exception):
    """Событие не удалось доставить в DLT: оффсет коммитить нельзя."""


def stop(signum, _frame):
    global running
    log.info("signal %s received, stopping", signum)
    running = False


def process(sink: Sink, msg: Message) -> None:
    """Эмуляция работы: ждём, затем пишем результат в sink. Ядовитое событие падает всегда."""
    event = json.loads(msg.value())
    time.sleep(settings.PROCESSING_DELAY_MS / 1000)
    if event.get("payload", {}).get("poison"):
        raise PoisonEventError(f"poison event {event['event_id']}")
    if sink.write(event, msg.topic(), msg.partition(), msg.offset()):
        PROCESSED.labels(settings.POD_ID).inc()
    else:
        DEDUP_DROPPED.labels(settings.POD_ID).inc()


def send_to_dlt(dlt: Producer, msg: Message, reason: str) -> None:
    """Отправляет событие в DLT и ждёт подтверждения доставки (acks=all).

    value — исходный JSON, key — исходный ключ (user_id), в заголовках — причина и координаты
    исходного сообщения. Если доставка не подтверждена — исключение: оффсет не коммитим.
    """
    result: dict = {}

    def on_delivery(err, _msg):
        result["err"] = err

    dlt.produce(
        settings.DLT_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("error", reason.encode()),
            ("source_topic", msg.topic().encode()),
            ("source_partition", str(msg.partition()).encode()),
            ("source_offset", str(msg.offset()).encode()),
        ],
        on_delivery=on_delivery,
    )
    dlt.flush(settings.DLT_FLUSH_TIMEOUT_S)
    if "err" not in result:
        raise DltDeliveryError(f"no delivery report within {settings.DLT_FLUSH_TIMEOUT_S}s")
    if result["err"] is not None:
        raise DltDeliveryError(str(result["err"]))
    DLT_SENT.labels(settings.POD_ID).inc()


def on_commit(err, partitions):
    """Результат асинхронного коммита."""
    if err is not None:
        log.error("async commit failed: %s", err)


def commit_pending(consumer: Consumer, asynchronous: bool, only=None) -> None:
    """Коммитит накопленные оффсеты (уже offset + 1). only — ограничить набором партиций."""
    global uncommitted, last_commit_at
    keys = [k for k in pending if only is None or k in only]
    if not keys:
        return
    offsets = [TopicPartition(t, p, pending[(t, p)]) for t, p in keys]
    try:
        consumer.commit(offsets=offsets, asynchronous=asynchronous)
    except KafkaException as exc:
        # не страшно: сообщения перечитаются, дубли отбросит sink
        log.error("commit failed: %s", exc)
    for key in keys:
        del pending[key]
    if not pending:
        uncommitted = 0
    last_commit_at = time.monotonic()


def mark_processed(consumer: Consumer, msg: Message) -> None:
    """Запоминает оффсет как обработанный; коммит — пачкой, асинхронно."""
    global uncommitted
    pending[(msg.topic(), msg.partition())] = msg.offset() + 1
    uncommitted += 1
    if uncommitted >= settings.COMMIT_BATCH:
        commit_pending(consumer, asynchronous=True)


def handle_message(consumer: Consumer, dlt: Producer, sink: Sink, msg: Message) -> None:
    """До MAX_ATTEMPTS попыток с коротким backoff, затем DLT. Оффсет помечается обработанным
    только после успешной записи в sink или подтверждённой доставки в DLT."""
    error = None
    for attempt in range(1, settings.MAX_ATTEMPTS + 1):
        try:
            process(sink, msg)
            error = None
            break
        except Exception as exc:  # noqa: BLE001 — любая ошибка обработки считается попыткой
            error = exc
            FAILURES.labels(settings.POD_ID).inc()
            log.warning("processing failed (attempt %d/%d) %s[%d]@%d: %r",
                        attempt, settings.MAX_ATTEMPTS, msg.topic(), msg.partition(), msg.offset(), exc)
            if attempt < settings.MAX_ATTEMPTS:
                time.sleep(settings.RETRY_BACKOFF_MS * attempt / 1000)
    if error is not None:
        send_to_dlt(dlt, msg, repr(error))
        log.info("sent to DLT: %s[%d]@%d", msg.topic(), msg.partition(), msg.offset())
    mark_processed(consumer, msg)


def on_assign(consumer: Consumer, partitions):
    REBALANCES.labels(settings.POD_ID, "assign").inc()
    ASSIGNED.labels(settings.POD_ID).set(len(partitions))
    try:
        committed = consumer.committed(partitions, timeout=10)
        resume = {(p.topic, p.partition): p.offset for p in committed}
    except KafkaException as exc:
        resume = f"unknown ({exc})"
    # с какого оффсета продолжится обработка: последний закоммиченный группой
    log.info("assigned: %s, resume from committed offsets: %s",
             [(p.topic, p.partition) for p in partitions], resume)


def on_revoke(consumer: Consumer, partitions):
    """Перед отдачей партиций синхронно коммитим всё, что по ним уже обработано."""
    REBALANCES.labels(settings.POD_ID, "revoke").inc()
    ASSIGNED.labels(settings.POD_ID).set(0)
    revoked = {(p.topic, p.partition) for p in partitions}
    log.info("revoked: %s, committing %s", sorted(revoked),
             {k: v for k, v in pending.items() if k in revoked})
    commit_pending(consumer, asynchronous=False, only=revoked)


def main():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    start_http_server(8000)

    sink = Sink(settings.SINK_DSN, settings.POD_ID)
    consumer = Consumer({
        "bootstrap.servers": settings.BOOTSTRAP_SERVERS,
        "group.id": settings.GROUP_ID,
        "client.id": settings.POD_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": settings.AUTO_OFFSET_RESET,
        "on_commit": on_commit,
    })
    dlt = Producer({
        "bootstrap.servers": settings.BOOTSTRAP_SERVERS,
        "client.id": f"{settings.POD_ID}-dlt",
        "acks": "all",
        "enable.idempotence": True,
    })
    consumer.subscribe([settings.TOPIC], on_assign=on_assign, on_revoke=on_revoke)
    log.info("started: pod=%s group=%s topic=%s", settings.POD_ID, settings.GROUP_ID, settings.TOPIC)

    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is not None:
                if msg.error():
                    log.error("consumer error: %s", msg.error())
                else:
                    handle_message(consumer, dlt, sink, msg)
            if pending and time.monotonic() - last_commit_at >= settings.COMMIT_INTERVAL_S:
                commit_pending(consumer, asynchronous=True)
    finally:
        # обработанное коммитим синхронно, затем выходим из группы
        commit_pending(consumer, asynchronous=False)
        consumer.close()
        sink.close()
        log.info("stopped")


if __name__ == "__main__":
    main()
