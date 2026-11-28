"""Shared pure logic (no I/O): deterministic event decisions + verify checks.

Used by app/event.py (generation side) and verify/verify.py (check side), so the
poison decision is computed by exactly one implementation on both sides.
"""
from __future__ import annotations

import bisect
import hashlib

EVENT_TYPES = ('page_view', 'add_to_cart', 'purchase')


def bucket(pod_index: int, user_id: str, seq: int) -> int:
    """Deterministic per (pod, user, seq): survives producer restarts."""
    digest = hashlib.sha256(f'{pod_index}|{user_id}|{seq}'.encode('utf-8')).hexdigest()
    return int(digest[:8], 16)


def poison_decision(pod_index: int, user_id: str, seq: int, rate: float) -> bool:
    return bucket(pod_index, user_id, seq) % 10000 < round(rate * 10000)


def event_type(pod_index: int, user_id: str, seq: int) -> str:
    return EVENT_TYPES[bucket(pod_index, user_id, seq) % len(EVENT_TYPES)]


def pod_index_from_user(user_id: str) -> int | None:
    """user_id format: user_<pod>_<nn>."""
    parts = user_id.split('_')
    if len(parts) == 3 and parts[0] == 'user':
        try:
            return int(parts[1])
        except ValueError:
            return None
    return None


def find_gaps(present: list[int] | set[int], start: int, end: int) -> list[int]:
    """Missing seq numbers in [start, end] for one user."""
    have = set(present)
    return sorted(s for s in range(start, end + 1) if s not in have)


def split_gaps(missing: list[int], explained_by_dlt: set[int]) -> tuple[list[int], list[int]]:
    """A missing seq is not 'lost' when the same user/seq event is in events.dlt."""
    explained = [s for s in missing if s in explained_by_dlt]
    unexplained = [s for s in missing if s not in explained_by_dlt]
    return explained, unexplained


def count_inversions(order: list[int]) -> int:
    """Pairs (i < j) where order[i] > order[j]: a smaller seq was written to the
    sink after a larger one for the same user (first-write order by arrival)."""
    seen: list[int] = []
    count = 0
    for s in order:
        pos = bisect.bisect_right(seen, s)
        count += len(seen) - pos
        seen.insert(pos, s)
    return count
