"""Audit the messages actually published during an intentionally interrupted run."""
from collections import defaultdict
from datetime import datetime, timezone
import json
import os
import time

from psycopg.rows import dict_row

from src.config import Config
from src.sink import connect
from src.verify import topic_snapshot
from src.verification import analyze


def main():
    print(f"{os.getenv('GITHUB_LOGIN', 'not-set')} {datetime.now(timezone.utc).isoformat()}", flush=True)
    cfg = Config.load()
    messages, _ = topic_snapshot(cfg, "events")
    with connect() as conn:
        producers = conn.execute(
            "SELECT producer_id, status, acknowledged, errors FROM producer_runs "
            "WHERE run_id=%s ORDER BY producer_id", (cfg.run_id,),
        ).fetchall()
    sequences = defaultdict(set)
    for message in messages:
        sequences[message["user_id"]].add(message["seq"])
    gaps = {user: sorted(set(range(1, max(seqs) + 1)) - seqs)
            for user, seqs in sequences.items()}
    acknowledged = sum(row[2] for row in producers)
    identities = {event["event_id"] for event in messages}
    expected = [{"event_id": event["event_id"], "user_id": event["user_id"], "seq": event["seq"],
                 "poison": event["payload"]["poison"]} for event in messages]
    deadline = time.monotonic() + 120
    while True:
        with connect() as conn:
            conn.row_factory = dict_row
            sink = conn.execute(
                "SELECT event_id, user_id, seq, write_order, partition FROM processed_events WHERE run_id=%s",
                (cfg.run_id,),
            ).fetchall()
            duplicates = conn.execute(
                "SELECT COALESCE(sum(duplicates), 0) AS count FROM user_state WHERE run_id=%s", (cfg.run_id,),
            ).fetchone()["count"]
        dead, invalid_headers = topic_snapshot(cfg, "events.dlt")
        delivery = analyze(expected, sink, dead, int(duplicates))
        if delivery["delivery_ok"] or time.monotonic() >= deadline:
            break
        print(json.dumps({"waiting_for_sink": True, "missing": delivery["missing_count"]}), flush=True)
        time.sleep(1)
    ok = (
        len(producers) == 3 and all(row[3] == 0 for row in producers)
        and len(messages) == len(identities) == acknowledged
        and not any(gaps.values())
        and delivery["delivery_ok"] and not invalid_headers
    )
    print(json.dumps({
        "run_id": cfg.run_id, "published": len(messages), "acknowledged": acknowledged,
        "source_duplicates": len(messages) - len(identities),
        "sink": delivery["sink"], "dlt": delivery["dlt_unique"],
        "missing_count": delivery["missing_count"], "invalid_dlt_headers": invalid_headers,
        "sequence_gaps": {user: missing for user, missing in gaps.items() if missing},
        "producers": [{"id": row[0], "status": row[1], "acknowledged": row[2], "errors": row[3]}
                      for row in producers],
        "ok": ok,
    }, indent=2), flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
