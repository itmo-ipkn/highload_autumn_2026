"""Optional demo latency and failures; deliberately separate from telemetry."""

import asyncio

from fastapi import HTTPException, Request


async def simulate_backend(request: Request) -> None:
    settings = request.app.state.settings
    random = request.app.state.random
    delay = random.uniform(settings.latency_min_ms, settings.latency_max_ms) / 1000
    if delay:
        await asyncio.sleep(delay)
    if random.random() < settings.failure_rate:
        raise HTTPException(status_code=500, detail="Simulated backend failure")
