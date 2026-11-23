import asyncio
import os
import random
import time
from collections import deque

import httpx

ACTIONS = ("POST", "GET", "PUT", "GET", "GET", "PUT", "POST", "GET", "PUT", "POST")
NOT_FOUND_INDICES = frozenset((11, 44))
ERROR_INDICES = frozenset((20, 39))


async def emit_one(client, urls, known_ids, sequence_number):
    replica = sequence_number % len(urls)
    base_url = urls[replica]
    active_ids = known_ids[replica]
    phase = sequence_number % 100
    action = ACTIONS[sequence_number % len(ACTIONS)]
    payload = {"name": f"item-{sequence_number}", "value": sequence_number}

    try:
        if phase in ERROR_INDICES:
            response = await client.post(base_url + "/items", json={"name": "__simulate_500__", "value": -1})
        elif phase in NOT_FOUND_INDICES:
            response = await client.get(base_url + "/items/nonexistent-item")
        elif action == "POST":
            response = await client.post(base_url + "/items", json=payload)
            if response.status_code == 201:
                active_ids.append(response.json()["id"])
        elif action == "GET":
            response = await client.get(base_url + "/items/" + random.choice(list(active_ids)))
        else:
            response = await client.put(base_url + "/items/" + random.choice(list(active_ids)), json=payload)

        if response.status_code not in (200, 201, 404, 500):
            print(f"Unexpected HTTP {response.status_code} from {base_url}", flush=True)
    except httpx.HTTPError as exc:
        print(f"Request failure on {base_url}: {exc}", flush=True)


async def main():
    urls = [url.strip().rstrip("/") for url in os.environ.get(
        "APP_URLS", "http://app-dc1:8000,http://app-dc2:8000"
    ).split(",") if url.strip()]
    if len(urls) != 2:
        raise ValueError("APP_URLS must contain exactly two URLs")
    target_rps = float(os.environ.get("TARGET_RPS", "36"))
    ramp_seconds = float(os.environ.get("RAMP_SECONDS", "120"))
    if target_rps <= 0 or ramp_seconds <= 0:
        raise ValueError("TARGET_RPS and RAMP_SECONDS must be positive")

    known_ids = [deque(maxlen=200) for _ in urls]
    limits = httpx.Limits(max_connections=100, max_keepalive_connections=40)
    async with httpx.AsyncClient(timeout=5.0, limits=limits) as client:
        for i, url in enumerate(urls):
            response = await client.post(url + "/items", json={"name": "seed", "value": i})
            response.raise_for_status()
            known_ids[i].append(response.json()["id"])

        print(f"Load ramp: 3 -> {target_rps:g} RPS over {ramp_seconds:g}s; URLs: {urls}", flush=True)
        begin = time.monotonic()
        deadline = begin
        sequence_number = 0
        pending = set()
        last_log = begin
        while True:
            now = time.monotonic()
            elapsed = now - begin
            rps = 3.0 + (target_rps - 3.0) * min(elapsed / ramp_seconds, 1.0)
            deadline = max(deadline + 1.0 / rps, now)
            await asyncio.sleep(max(0, deadline - time.monotonic()))
            task = asyncio.create_task(emit_one(client, urls, known_ids, sequence_number))
            pending.add(task)
            task.add_done_callback(pending.discard)
            sequence_number += 1
            if len(pending) >= 100:
                await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            now = time.monotonic()
            if now - last_log >= 10:
                print(f"elapsed={elapsed:.0f}s; target_current={rps:.1f} RPS; sent={sequence_number}; in_flight={len(pending)}", flush=True)
                last_log = now


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
