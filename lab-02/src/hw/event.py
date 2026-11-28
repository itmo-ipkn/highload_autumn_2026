from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

EVENT_TYPES = ("page_view", "add_to_cart", "purchase")

V1_KEYS = ("event_id", "user_id", "type", "seq", "produced_at")


class SchemaMismatch(ValueError):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds") #TODO: переписать на utc+3


@dataclass
class Event:
    user_id: int
    type: str
    seq: int
    payload: dict = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    produced_at: str = field(default_factory=_now_iso)

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "user_id": self.user_id,
            "type": self.type,
            "seq": self.seq,
            "produced_at": self.produced_at,
            "payload": self.payload,
        }

    def to_json(self) -> bytes:
        return json.dumps(self.to_dict()).encode("utf-8")

    def to_json_v2(self) -> bytes:
        return json.dumps(
            {
                "schema": "v2",
                "user_id": self.user_id,
                "event_type": self.type,
                "seq": self.seq,
                "event_id": self.event_id,
                "ts": self.produced_at,
                "payload": "plain text, not an object",
            }
        ).encode("utf-8")

    @classmethod
    def from_json(cls, raw: bytes) -> "Event":

        obj = json.loads(raw.decode("utf-8")) #TODO: переписать на JsonEncoder

        problems = [k for k in V1_KEYS if k not in obj]

        if not isinstance(obj.get("payload", {}), dict):
            problems.append("payload is not an object")
        if problems:
            raise SchemaMismatch("v1 schema mismatch: " + ", ".join(problems))
        return cls(
            user_id=int(obj["user_id"]),
            type=obj["type"],
            seq=int(obj["seq"]),
            payload=obj.get("payload") or {},
            event_id=obj["event_id"],
            produced_at=obj["produced_at"],
        )
