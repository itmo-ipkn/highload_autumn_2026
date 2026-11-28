"""Small load generator for HW-1.

Ramps to a steady background rate (default ~35 RPS total, within 20-50) over
RAMP_SECONDS, then holds it until the stack stops. Requests are spread across
both app instances and all three business handlers.

Error sources (so at least two codes show on the dashboard):
  * 404  -> a fraction of GET/PUT requests use a nonexistent id
  * 500  -> artificial faults in the app (ERROR_RATE env on the app service)
"""
from __future__ import annotations
import asyncio
import os
import random
import time

import aiohttp

TARGET_RPS = float(os.environ.get("TARGET_RPS", "35"))
RAMP_SECONDS = float(os.environ.get("RAMP_SECONDS", "120"))
RAMP_START_RPS = float(os.environ.get("RAMP_START_RPS", "5"))
LOAD_404_PROB = float(os.environ.get("LOAD_404_PROB", "0.10"))
URLS = [u for u in os.environ.get("INSTANCE_URLS", "").split(",") if u.strip()] or [
    "http://app1:8090",
    "http://app2:8090",
]
HANDLERS = ["POST", "GET", "PUT"]

KNOWN_IDS: dict[str, list[str]] = {url: [] for url in URLS}


def current_rate(elapsed: float) -> float:
    if RAMP_SECONDS <= 0:
        return TARGET_RPS
    frac = min(elapsed / RAMP_SECONDS, 1.0)
    start = min(RAMP_START_RPS, TARGET_RPS)
    return start + (TARGET_RPS - start) * frac


async def do_request(session: aiohttp.ClientSession, base: str, kind: str) -> int:
    timeout = aiohttp.ClientTimeout(total=5)
    if kind == "POST":
        resp = await session.post(f"{base}/items", json={"value": random.randint(1, 1000)},
                                  timeout=timeout, allow_redirects=False)
        if resp.status == 201:
            body = await resp.json()
            if body.get("id"):
                KNOWN_IDS[base].append(body["id"])
        return resp.status

    if random.random() < LOAD_404_PROB or not KNOWN_IDS[base]:
        item_id = f"missing-{random.randint(1, 1_000_000)}"
    else:
        item_id = random.choice(KNOWN_IDS[base])

    if kind == "GET":
        resp = await session.get(f"{base}/items/{item_id}", timeout=timeout, allow_redirects=False)
    else:
        resp = await session.put(f"{base}/items/{item_id}", json={"value": random.randint(1, 1000)},
                                 timeout=timeout, allow_redirects=False)
    return resp.status


async def main() -> None:
    started = time.monotonic()
    iteration = 0
    next_due = time.monotonic()
    print(f"load: target={TARGET_RPS} RPS, ramp={RAMP_SECONDS}s, instances={URLS}", flush=True)

    async with aiohttp.ClientSession() as session:
        while True:
            elapsed = time.monotonic() - started
            rate = current_rate(elapsed)
            interval = 1.0 / rate if rate > 0 else 1.0

            base = URLS[iteration % len(URLS)]
            kind = HANDLERS[iteration % len(HANDLERS)]
            try:
                status = await do_request(session, base, kind)
                if iteration % 50 == 0:
                    print(f"load: rate~{rate:.1f} RPS status={status} instance={base} handler={kind}", flush=True)
            except aiohttp.ClientError as exc:
                if iteration % 50 == 0:
                    print(f"load: error {exc}", flush=True)

            iteration += 1
            next_due += interval
            delay = next_due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)

if __name__ == "__main__":
    asyncio.run(main())
