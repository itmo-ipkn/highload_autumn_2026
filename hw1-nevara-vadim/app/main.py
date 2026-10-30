import os
from threading import Lock
from time import perf_counter

from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field


class ItemInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)


class Item(ItemInput):
    id: int


app = FastAPI(
    title="DDIA Items",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)

POD_ID = os.getenv("POD_ID", "local")
DATACENTER = os.getenv("DATACENTER", "local")
EXCLUDED_ROUTES = frozenset({"/health", "/metrics"})

REQUESTS = Counter(
    "items_http_requests_total",
    "Completed HTTP requests to instrumented routes.",
    ("pod_id", "datacenter", "method", "route", "status_code"),
)
REQUEST_DURATION = Histogram(
    "items_http_request_duration_seconds",
    "Time to produce an HTTP response for an instrumented route, in seconds.",
    ("pod_id", "datacenter", "method", "route"),
    buckets=(
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
        0.1, 0.25, 0.5, 1, 2.5, 5, 10,
    ),
)

_items: dict[int, Item] = {}
_next_id = 1
_lock = Lock()


@app.middleware("http")
async def record_http_metrics(request: Request, call_next):
    started = perf_counter()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    finally:
        route = request.scope.get("route")
        if isinstance(route, APIRoute) and route.path not in EXCLUDED_ROUTES:
            labels = (POD_ID, DATACENTER, request.method, route.path)
            REQUESTS.labels(*labels, str(status_code)).inc()
            REQUEST_DURATION.labels(*labels).observe(perf_counter() - started)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/metrics")
def metrics() -> Response:
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/handlers")
def handlers() -> list[dict[str, str]]:
    return sorted(
        (
            {"method": method, "route": route.path}
            for route in app.routes
            if isinstance(route, APIRoute) and route.path not in EXCLUDED_ROUTES
            for method in route.methods
        ),
        key=lambda handler: (handler["route"], handler["method"]),
    )


@app.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
def create_item(payload: ItemInput) -> Item:
    global _next_id
    with _lock:
        item = Item(id=_next_id, **payload.model_dump())
        _items[_next_id] = item
        _next_id += 1
    return item


@app.get("/items/{id}", response_model=Item)
def get_item(id: int) -> Item:
    with _lock:
        item = _items.get(id)
    if item is None:
        raise HTTPException(status_code=404, detail="Item not found")
    return item


@app.put("/items/{id}", response_model=Item)
def update_item(id: int, payload: ItemInput) -> Item:
    with _lock:
        if id not in _items:
            raise HTTPException(status_code=404, detail="Item not found")
        item = Item(id=id, **payload.model_dump())
        _items[id] = item
    return item
