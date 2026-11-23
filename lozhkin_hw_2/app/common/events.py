import json
from dataclasses import dataclass
from datetime import datetime, timezone

EVENT_TYPES = ("page_view", "add_to_cart", "purchase")


@dataclass
class Event:
    event_id: str
    user_id: str
    type: str
    seq: int
    produced_at: str
    payload: dict

    def to_json(self) -> str:
        return json.dumps(
            {
                "event_id": self.event_id,
                "user_id": self.user_id,
                "type": self.type,
                "seq": self.seq,
                "produced_at": self.produced_at,
                "payload": self.payload,
            },
            ensure_ascii=False,
        )

    @staticmethod
    def from_json(raw: str) -> "Event":
        d = json.loads(raw)
        return Event(
            event_id=d["event_id"],
            user_id=d["user_id"],
            type=d["type"],
            seq=int(d["seq"]),
            produced_at=d["produced_at"],
            payload=d.get("payload") or {},
        )

    def is_poison(self) -> bool:
        return bool(self.payload.get("poison"))


def new_event(user_id: str, seq: int, etype: str, poison: bool) -> "Event":
    payload = {"action": etype, "value": round(__import__("random").random(), 4)}
    if poison:
        payload = {"poison": True, "reason": "simulated bad record"}
    return Event(
        event_id=str(__import__("uuid").uuid4()),
        user_id=user_id,
        type=etype,
        seq=seq,
        produced_at=datetime.now(timezone.utc).isoformat(),
        payload=payload,
    )
