"""Deterministic identities and poison selection make finite runs verifiable."""
from datetime import datetime, timezone
import hashlib
import uuid


class PoisonEvent(Exception):
    pass


def user_id(run_id, producer_id, user_number):
    return f"{run_id}:p{producer_id}:u{user_number:04d}"


def event_id(user, seq):
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ddia-hw-kafka/{user}/{seq}"))


def is_poison(seed, user, seq, rate):
    digest = hashlib.sha256(f"{seed}/{user}/{seq}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / (1 << 64) < rate


def make_event(cfg, producer_id, user_number, seq):
    user = user_id(cfg.run_id, producer_id, user_number)
    return {
        "event_id": event_id(user, seq),
        "user_id": user,
        "type": ("page_view", "add_to_cart", "purchase")[(seq - 1) % 3],
        "seq": seq,
        "produced_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "run_id": cfg.run_id,
            "producer_id": producer_id,
            "poison": is_poison(cfg.seed, user, seq, cfg.poison_rate),
        },
    }


def process_event(event):
    if event["payload"]["poison"]:
        raise PoisonEvent("deterministic poison event")


def expected_events(run_id, users, events, seed, poison_rate):
    for producer_id in range(1, 4):
        for user_number in range(1, users + 1):
            user = user_id(run_id, producer_id, user_number)
            for seq in range(1, events + 1):
                yield {
                    "event_id": event_id(user, seq), "user_id": user, "seq": seq,
                    "poison": is_poison(seed, user, seq, poison_rate),
                }
