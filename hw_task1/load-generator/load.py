import asyncio
import os
import random
import time

import httpx


APP_URLS = [
    os.getenv("APP_1_URL", "http://localhost:8001"),
    os.getenv("APP_2_URL", "http://localhost:8002"),
]

TARGET_RPS = 30
RAMP_SECONDS = 120


async def send_request(
    client: httpx.AsyncClient,
    app_url: str,
) -> int:
    error_probability = random.random()

    if error_probability < 0.02:
        response = await client.get(f"{app_url}/items/999999")

    elif error_probability < 0.04:
        response = await client.get(f"{app_url}/items/-1")

    else:
        method = random.choice(["POST", "GET", "PUT"])

        item = {
            "name": "Dorian Gray",
            "description": "Load testing",
        }

        if method == "POST":
            response = await client.post(
                f"{app_url}/items",
                json=item,
            )

        elif method == "GET":
            response = await client.get(f"{app_url}/items/1")

        else:
            response = await client.put(
                f"{app_url}/items/1",
                json=item,
            )

    return response.status_code


async def main():
    async with httpx.AsyncClient(timeout=10.0) as client:
        for app_url in APP_URLS:
            response = await client.post(
                f"{app_url}/items",
                json={"name": "Initial item"},
            )
            response.raise_for_status()

        start_time = time.monotonic()
        iteration = 0

        while True:
            iteration_start = time.monotonic()
            elapsed = iteration_start - start_time

            progress = min(elapsed / RAMP_SECONDS, 1.0)
            current_rps = max(
                1,
                round(TARGET_RPS * progress),
            )

            tasks = [
                send_request(
                    client,
                    APP_URLS[(iteration + i) % len(APP_URLS)],
                )
                for i in range(current_rps)
            ]

            results = await asyncio.gather(
                *tasks,
                return_exceptions=True,
            )

            counts = {}

            for result in results:
                code = result if isinstance(result, int) else "ERR"
                counts[code] = counts.get(code, 0) + 1

            print(
                f"Target: {current_rps} RPS | "
                f"Responses: {counts}",
                flush=True,
            )

            iteration += current_rps

            duration = time.monotonic() - iteration_start
            await asyncio.sleep(max(0, 1.0 - duration))


if __name__ == "__main__":
    asyncio.run(main())
