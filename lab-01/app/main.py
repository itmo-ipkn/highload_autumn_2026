import json
import logging
import os
import threading
import time

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

MAX_BODY_BYTES = 64 * 1024
POD_ID = os.getenv("POD_ID", "pod-1")
DATACENTER = os.getenv("DATACENTER", "unknown-loc")
METRIC_EXEMPT = ("/health", "/metrics", "/handlers")
FRAMEWORK_ROUTES = ("/docs", "/docs/oauth2-redirect", "/redoc", "/openapi.json")

REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Responses served, by method, route, exact status code, pod and datacenter.",
    ["method", "route", "code", "pod", "datacenter"],
)
REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "Latency in seconds, by method and route.",
    ["method", "route", "pod", "datacenter"],
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 10),
)


class Vault:

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._rows: dict[str, dict] = {}
        self._next_id = 0

    def create(self, value: dict) -> str:
        with self._lock:
            self._next_id += 1
            item_id = str(self._next_id)
            self._rows[item_id] = value
            return item_id

    def read(self, item_id: str) -> dict | None:
        with self._lock:
            value = self._rows.get(item_id)
            return None if value is None else {"id": item_id, "value": value}

    def update(self, item_id: str, value: dict) -> dict | None:
        with self._lock:
            if item_id not in self._rows:
                return None
            self._rows[item_id] = value
            return {"id": item_id, "value": value}

    def count(self) -> int:
        with self._lock:
            return len(self._rows)


vault = Vault()

READY = threading.Event()


@asynccontextmanager
async def lifespan(_: FastAPI):
    # Everything that has to happen before the first query is served goes here.
    READY.set()
    yield

app = FastAPI(
    title="crud-vault",
    description="In-memory CRUD vault for the highload lab.",
    lifespan=lifespan,
)


async def _read_json_row(request: Request) -> tuple[dict | None, str]:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        return None, f"body is larger than {MAX_BODY_BYTES} bytes"
    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        return None, f"body is larger than {MAX_BODY_BYTES} bytes"
    if not raw:
        return None, "expected a JSON object as the row"
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid JSON body: {exc}"
    if not isinstance(value, dict):
        return None, "expected a JSON object as the row"
    return value, ""


# Handlers

@app.post("/items", status_code=201, summary="Create a row")
async def create_item(request: Request):
    value, error = await _read_json_row(request)
    if value is None:
        return JSONResponse(status_code=400, content={"error": error})
    item_id = vault.create(value)
    return JSONResponse(status_code=201, content={"id": item_id, "value": value})


@app.get("/items/{item_id}", summary="Read a row")
async def read_item(item_id: str):
    row = vault.read(item_id)
    if row is None:
        return JSONResponse(status_code=404, content={"error": f"no item with id {item_id}"})
    return row


@app.put("/items/{item_id}", summary="Replace a row")
async def update_item(request: Request, item_id: str):
    value, error = await _read_json_row(request)
    if value is None:
        return JSONResponse(status_code=400, content={"error": error})
    row = vault.update(item_id, value)
    if row is None:
        return JSONResponse(status_code=404, content={"error": f"no item with id {item_id}"})
    return row


@app.get("/health", summary="Service readiness", include_in_schema=False)
async def health(request: Request):
    if not READY.is_set():
        return JSONResponse(status_code=503, content={"status": "starting"})
    return JSONResponse(status_code=200, content={"status": "ok", "items": vault.count()})


@app.get("/metrics", summary="Prometheus metrics", include_in_schema=False)
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/handlers", summary="List the handlers the app exposes", include_in_schema=False)
async def list_handlers():
    listing = []
    for route in app.routes:
        methods, path = getattr(route, "methods", None), getattr(route, "path", None)
        if not methods or not path or path in METRIC_EXEMPT + FRAMEWORK_ROUTES:
            continue
        if "GET" in methods:
            # Starlette registers an implicit HEAD companion for every GET.
            methods = methods - {"HEAD"}
        for method in methods:
            listing.append({"method": method, "path": path})
    listing.sort(key=lambda item: (item["path"], item["method"]))
    return JSONResponse(content={"handlers": listing})


@app.middleware("http")
async def record_signals(request: Request, call_next):
    started = time.perf_counter()
    response = await call_next(request)
    matched = request.scope.get("route")
    template = getattr(matched, "path", matched)
    if not isinstance(template, str) or not template.startswith("/"):
        return response
    if template in METRIC_EXEMPT:
        return response
    REQUESTS_TOTAL.labels(
        method=request.method, route=template,
        code=str(response.status_code),
        pod=POD_ID, datacenter=DATACENTER,
    ).inc()
    REQUEST_DURATION_SECONDS.labels(
        method=request.method, route=template,
        pod=POD_ID, datacenter=DATACENTER,
    ).observe(time.perf_counter() - started)
    return response


if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    uvicorn.run(app, host=host, port=port, log_level="info")
