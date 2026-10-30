import time

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from app.metrics import (
    DATACENTER,
    POD_ID,
    REQUEST_LATENCY,
    REQUESTS_TOTAL,
)

app = FastAPI(title="Items Service")


@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    if request.url.path in (
        "/health",
        "/metrics",
        "/handlers",
        "/docs",
        "/redoc",
        "/openapi.json",
    ):
        return await call_next(request)

    start_time = time.perf_counter()
    status_code = 500

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        duration = time.perf_counter() - start_time

        route = request.scope.get("route")
        handler = route.path if route else "unmatched"

        labels = {
            "method": request.method,
            "handler": handler,
            "pod_id": POD_ID,
            "datacenter": DATACENTER,
        }

        REQUEST_LATENCY.labels(**labels).observe(duration)

        REQUESTS_TOTAL.labels(
            **labels,
            status_code=str(status_code),
        ).inc()


class Item(BaseModel):
    name: str
    description: str | None = None


items: dict[int, Item] = {}
next_id = 1


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/items", status_code=201)
async def create_item(item: Item):
    global next_id

    item_id = next_id
    items[item_id] = item
    next_id += 1

    return {"id": item_id, **item.model_dump()}


@app.get("/items/{id}")
async def get_item(id: int):
    if id == -1:
        raise HTTPException(
            status_code=500,
            detail="Simulated internal server error",
        )

    if id not in items:
        raise HTTPException(
            status_code=404,
            detail="Item not found",
        )

    return {"id": id, **items[id].model_dump()}


@app.put("/items/{id}")
async def update_item(id: int, item: Item):
    if id not in items:
        raise HTTPException(status_code=404, detail="Item not found")

    items[id] = item
    return {"id": id, **item.model_dump()}


@app.get("/metrics", include_in_schema=False)
async def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


@app.get("/handlers", include_in_schema=False)
async def get_handlers():
    handlers = []

    excluded_paths = {"/health", "/metrics", "/handlers"}

    for route in app.routes:
        if not isinstance(route, APIRoute):
            continue

        if route.path in excluded_paths:
            continue

        for method in sorted(route.methods):
            handlers.append({
                "method": method,
                "path": route.path,
            })

    return {"handlers": handlers}
