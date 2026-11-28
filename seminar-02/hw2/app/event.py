"""Event model (HW part "Подготовительная часть").

Fields: event_id (UUID), user_id, type in {page_view, add_to_cart, purchase},
seq (per user, from 1, gapless), produced_at, payload (arbitrary JSON).

Poison decision is deterministic via verify/logic.py (shared with `verify`):
hash(pod_index, user_id, seq) decides whether the event is poison (~POISON_RATE).
Determinism matters for two reasons:
1. `verify` recomputes the expected poison set for a single clean run and
   cross-checks every seq gap against events.dlt;
2. a producer restart resumes per-user counters from the sink, and the same
   (pod,user,seq) is still the same poison event.
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import logic

EVENT_TYPES = logic.EVENT_TYPES
REQUIRED_FIELDS = ('event_id', 'user_id', 'type', 'seq', 'produced_at', 'payload')


@dataclass(frozen=True)
class Event:
    event_id: str
    user_id: str
    type: str
    seq: int
    produced_at: str
    payload: dict

    def to_dict(self) -> dict:
        return {
            'event_id': self.event_id,
            'user_id': self.user_id,
            'type': self.type,
            'seq': self.seq,
            'produced_at': self.produced_at,
            'payload': self.payload,
        }

    def to_json(self, corrupt: bool = False) -> str:
        """corrupt=True emits a payload as a string: the consumer handler fails
        on payload validation (POISON_MODE=corrupt variant)."""
        d = self.to_dict()
        if corrupt:
            d['payload'] = f'corrupt-payload:{self.user_id}:{self.seq}'
        return json.dumps(d, ensure_ascii=False)


def make_event(pod_index: int, user_id: str, seq: int, poison_rate: float) -> Event:
    b = logic.bucket(pod_index, user_id, seq)
    etype = EVENT_TYPES[b % len(EVENT_TYPES)]
    payload = {
        'page_view': {'path': f'/products/{b % 1000:04d}', 'referrer': f'seq-{seq - 1 if seq > 1 else "entry"}'},
        'add_to_cart': {'item': f'sku-{b % 200:04d}', 'qty': (b % 3) + 1},
        'purchase': {'item': f'sku-{b % 200:04d}', 'amount_cents': (b % 10000) + 100},
    }[etype]
    if logic.poison_decision(pod_index, user_id, seq, poison_rate):
        payload['poison'] = True
    return Event(
        event_id=str(uuid.uuid4()),
        user_id=user_id,
        type=etype,
        seq=seq,
        produced_at=datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
        payload=payload,
    )


class PoisonError(Exception):
    """The simulated handler always fails for poison events."""


def parse_envelope(raw: bytes | str) -> dict:
    """Parse the message envelope; raises ValueError on malformed JSON/fields."""
    obj = json.loads(raw)  # json.JSONDecodeError is a ValueError subclass
    if not isinstance(obj, dict):
        raise ValueError('envelope is not a JSON object')
    for field in REQUIRED_FIELDS:
        if field not in obj:
            raise ValueError(f'missing field {field!r}')
    uuid.UUID(obj['event_id'])
    if not isinstance(obj['user_id'], str) or not obj['user_id']:
        raise ValueError('user_id must be a non-empty string')
    if obj['type'] not in EVENT_TYPES:
        raise ValueError(f'type must be one of {EVENT_TYPES}')
    if not isinstance(obj['seq'], int) or obj['seq'] < 1:
        raise ValueError('seq must be an int >= 1')
    if not isinstance(obj['produced_at'], str):
        raise ValueError('produced_at must be an ISO timestamp string')
    return obj


def handle(obj: dict) -> None:
    """Simulated business handler. Raises PoisonError for poison events."""
    if not isinstance(obj.get('payload'), dict):
        raise PoisonError('payload is not a JSON object')
    if obj['payload'].get('poison'):
        raise PoisonError('payload.poison=true: simulated handler failure')
