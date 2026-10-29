import asyncio
import json
import os
import random
import sys
import httpx

API_URLS = json.loads(os.getenv("API_URLS", "[]"))

RAMP_UP_SECONDS = 60
NUM_WORKERS = int(os.getenv("NUM_WORKERS", 20))


async def worker(client: httpx.AsyncClient, stop_event: asyncio.Event):
    while not stop_event.is_set():
        api_url = random.choice(API_URLS)
        action = random.choice(["create", "update", "get"])

        try:
            if action == "create":
                await client.post(f"{api_url}/items", json={"name": "test"})
            elif action == "update":
                await client.put(
                    f"{api_url}/items/{random.randint(50, 1500)}",
                    json={"name": "updated"},
                )
            elif action == "get":
                await client.get(f"{api_url}/items/{random.randint(50, 1500)}")
        except httpx.RequestError:
            pass

        await asyncio.sleep(random.uniform(0.05, 0.2))


async def main():
    stop_event = asyncio.Event()

    if not API_URLS:
        sys.exit(1)

    limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
    async with httpx.AsyncClient(limits=limits, timeout=5.0) as client:

        tasks = [
            asyncio.create_task(worker(client, stop_event)) for _ in range(NUM_WORKERS)
        ]

        await asyncio.sleep(RAMP_UP_SECONDS)

        try:
            while True:
                await asyncio.sleep(10)
        except KeyboardInterrupt:
            stop_event.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
