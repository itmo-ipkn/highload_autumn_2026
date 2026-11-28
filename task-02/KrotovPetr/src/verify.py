"""Read-only CLI. The expected range comes from the persisted run manifest."""
import argparse
from datetime import datetime, timezone
import json
import os
import time
import uuid

from confluent_kafka import Consumer, KafkaError, TopicPartition
from psycopg.rows import dict_row

from src.config import Config
from src.events import expected_events
from src.sink import connect
from src.verification import analyze


def topic_snapshot(cfg, topic):
    reader = Consumer({
        "bootstrap.servers": cfg.brokers, "group.id": f"verify-{uuid.uuid4()}",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest", "enable.partition.eof": True,
        "allow.auto.create.topics": False,
    })
    records = []
    invalid_headers = 0
    try:
        meta = reader.list_topics(topic, timeout=10).topics[topic]
        if meta.error:
            raise RuntimeError(str(meta.error))
        limits = {}
        assigned = []
        for part in sorted(meta.partitions):
            low, high = reader.get_watermark_offsets(TopicPartition(topic, part), timeout=10)
            if low < high:
                limits[part] = high
                assigned.append(TopicPartition(topic, part, low))
        if not assigned:
            return records, invalid_headers
        reader.assign(assigned)
        pending = set(limits)
        deadline = time.monotonic() + 30
        while pending:
            if time.monotonic() > deadline:
                raise TimeoutError(f"Could not finish reading the {topic} snapshot")
            msg = reader.poll(0.5)
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        raise RuntimeError(str(msg.error()))
                elif msg.offset() < limits[msg.partition()]:
                    event = json.loads(msg.value())
                    if event.get("payload", {}).get("run_id") == cfg.run_id:
                        records.append(event)
                        if topic != "events.dlt":
                            continue
                        headers = dict(msg.headers() or [])
                        try:
                            valid = (
                                headers["source_topic"] == b"events"
                                and 0 <= int(headers["source_partition"]) < 3
                                and int(headers["source_offset"]) >= 0
                                and headers["attempts"] == b"3"
                                and bool(headers["error"])
                            )
                        except (KeyError, TypeError, ValueError):
                            valid = False
                        invalid_headers += not valid
            for position in reader.position(assigned):
                if position.offset >= limits[position.partition]:
                    pending.discard(position.partition)
        return records, invalid_headers
    finally:
        reader.close()


def snapshot(cfg):
    with connect() as conn:
        conn.row_factory = dict_row
        manifest = conn.execute("SELECT * FROM runs WHERE run_id=%s", (cfg.run_id,)).fetchone()
        if manifest is None:
            raise ValueError(f"No manifest for RUN_ID={cfg.run_id}")
        producers = conn.execute(
            "SELECT producer_id, status, acknowledged, errors FROM producer_runs "
            "WHERE run_id=%s ORDER BY producer_id", (cfg.run_id,),
        ).fetchall()
        sink = conn.execute(
            "SELECT event_id, user_id, seq, write_order, partition FROM processed_events WHERE run_id=%s",
            (cfg.run_id,),
        ).fetchall()
        duplicates = conn.execute(
            "SELECT COALESCE(sum(duplicates), 0) AS count FROM user_state WHERE run_id=%s", (cfg.run_id,),
        ).fetchone()["count"]
    dead, invalid_headers = topic_snapshot(cfg, "events.dlt")
    expected = expected_events(
        cfg.run_id, manifest["users_per_producer"], manifest["events_per_user"],
        manifest["seed"], manifest["poison_rate"],
    )
    result = analyze(expected, sink, dead, int(duplicates))
    per_producer = manifest["users_per_producer"] * manifest["events_per_user"]
    complete = len(producers) == 3 and all(
        p["status"] == "complete" and p["acknowledged"] == per_producer and not p["errors"]
        for p in producers
    )
    result.update({
        "run_id": cfg.run_id, "key_mode": manifest["key_mode"],
        "producers": producers, "producers_complete": complete,
        "invalid_dlt_headers": invalid_headers,
    })
    result["ok"] = result["delivery_ok"] and complete and not invalid_headers
    if manifest["key_mode"] == "user_id":
        result["ok"] = result["ok"] and result["order_violations"] == 0
    return result


def main():
    login = os.getenv("GITHUB_LOGIN", "not-set")
    print(f"{login} {datetime.now(timezone.utc).isoformat()}", flush=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait", type=float, default=120, help="Wait for a complete run, seconds")
    parser.add_argument("--summary", action="store_true", help="Omit individual user rows from the JSON output")
    args = parser.parse_args()
    if login in {"", "not-set", "replace-with-your-github-login"}:
        print("Set GITHUB_LOGIN in .env or the environment before taking screenshots.", flush=True)
        return 2
    cfg = Config.load()
    deadline = time.monotonic() + max(0, args.wait)
    while True:
        try:
            result = snapshot(cfg)
        except Exception as exc:
            if time.monotonic() >= deadline:
                print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
                return 2
            print(json.dumps({"waiting": True, "reason": str(exc)}, ensure_ascii=False), flush=True)
        else:
            # Wait for delivery, but report an order violation immediately once delivery is complete.
            finished = result["delivery_ok"] and result["producers_complete"]
            if finished or time.monotonic() >= deadline:
                if args.summary:
                    result.pop("per_user", None)
                print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
                return 0 if result["ok"] else 1
            print(json.dumps({
                "waiting": True, "run_id": cfg.run_id, "missing": result["missing_count"],
                "producers_complete": result["producers_complete"],
            }), flush=True)
        time.sleep(1)


if __name__ == "__main__":
    raise SystemExit(main())
