"""Pure verification rules, independent of Kafka and PostgreSQL clients."""
from collections import Counter, defaultdict


def analyze(expected, sink, dlt, duplicates):
    wanted = {e["event_id"]: e for e in expected}
    stored = {str(e["event_id"]): e for e in sink}
    dead = {e["event_id"]: e for e in dlt}
    seen = stored.keys() | dead.keys()
    missing = sorted(wanted.keys() - seen)
    unexpected = sorted(seen - wanted.keys())
    mismatches = []
    wrong_destination = []
    for destination, records in (("sink", stored), ("dlt", dead)):
        for identity, actual in records.items():
            if identity not in wanted:
                continue
            goal = wanted[identity]
            if (actual["user_id"], actual["seq"]) != (goal["user_id"], goal["seq"]):
                mismatches.append(identity)
            if goal["poison"] != (destination == "dlt"):
                wrong_destination.append(identity)
    by_user = defaultdict(list)
    for record in sink:
        by_user[record["user_id"]].append(record)
    violations = []
    violations_by_user = Counter()
    for user, records in by_user.items():
        maximum = 0
        for record in sorted(records, key=lambda e: e["write_order"]):
            if record["seq"] < maximum:
                violations.append({"user_id": user, "seq": record["seq"], "previous_max": maximum})
                violations_by_user[user] += 1
            maximum = max(maximum, record["seq"])
    per_user = Counter(wanted[e]["user_id"] for e in missing)
    overlap = sorted(stored.keys() & dead.keys())
    delivery_ok = not (missing or unexpected or mismatches or wrong_destination or overlap)
    expected_by_user = defaultdict(list)
    for identity, event in wanted.items():
        expected_by_user[event["user_id"]].append(identity)
    user_results = []
    for user, identities in sorted(expected_by_user.items()):
        user_results.append({
            "user_id": user,
            "expected": len(identities),
            "sink": sum(identity in stored for identity in identities),
            "dlt": sum(identity in dead for identity in identities),
            "missing_seq": sorted(wanted[identity]["seq"] for identity in identities if identity not in seen),
            "order_violations": violations_by_user[user],
            "sink_partitions": sorted({row["partition"] for row in by_user[user] if "partition" in row}),
        })
    return {
        "expected": len(wanted), "users_checked": len({e["user_id"] for e in wanted.values()}),
        "sink": len(sink), "dlt_unique": len(dead), "dlt_records": len(dlt),
        "dlt_duplicates": len(dlt) - len(dead), "duplicates_discarded": duplicates,
        "missing_count": len(missing), "missing_examples": [wanted[e] for e in missing[:10]],
        "missing_by_user": dict(per_user),
        "unexpected_count": len(unexpected), "unexpected_examples": unexpected[:10],
        "mismatched_count": len(mismatches), "wrong_destination_count": len(wrong_destination),
        "sink_dlt_overlap": len(overlap),
        "order_violations": len(violations), "order_examples": violations[:10],
        "delivery_ok": delivery_ok,
        "per_user": user_results,
    }
