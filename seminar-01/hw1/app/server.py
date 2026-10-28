"""Simple in-memory CRUD microservice with Prometheus golden signals.

Business handlers (instrumented by the metrics middleware):
  POST /items         create
  GET  /items/{id}    read   -> 404 when the id is unknown
  PUT  /items/{id}    update -> 404 when the id is unknown

Service endpoints (not instrumented): /health, /metrics, /handlers.

Runtime-only label values:
  POD_ID, DATACENTER  -> exposed as Prometheus labels by metrics.py
  ERROR_RATE          -> artificial 500 rate (used so the dashboard shows errors)
"""
from __future__ import annotations
import os
import random
import uuid
from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST
from metrics import Metrics

ERROR_RATE = float(os.environ.get("ERROR_RATE", "0.0"))
PORT = int(os.environ.get("PORT", "8090"))

# Canonical business handlers registered on the router, reported by /handlers.
BUSINESS_HANDLERS = [
    {"method": "POST", "path": "/items"},
    {"method": "GET", "path": "/items/{id}"},
    {"method": "PUT", "path": "/items/{id}"},
]

ITEMS: dict[str, dict] = {}
metrics = Metrics()


def _maybe_fault() -> web.Response | None:
    if ERROR_RATE > 0 and random.random() < ERROR_RATE:
        return web.json_response({"error": "injected fault"}, status=500)
    return None


async def create_item(request: web.Request) -> web.Response:
    fault = _maybe_fault()
    if fault is not None:
        return fault
    body = await request.json()
    item_id = uuid.uuid4().hex
    ITEMS[item_id] = body
    return web.json_response({"id": item_id, "data": body}, status=201)


async def get_item(request: web.Request) -> web.Response:
    fault = _maybe_fault()
    if fault is not None:
        return fault
    item_id = request.match_info["id"]
    if item_id not in ITEMS:
        return web.json_response({"error": "not found"}, status=404)
    return web.json_response({"id": item_id, "data": ITEMS[item_id]})


async def update_item(request: web.Request) -> web.Response:
    fault = _maybe_fault()
    if fault is not None:
        return fault
    item_id = request.match_info["id"]
    if item_id not in ITEMS:
        return web.json_response({"error": "not found"}, status=404)
    body = await request.json()
    ITEMS[item_id] = body
    return web.json_response({"id": item_id, "data": body})


async def health(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def handlers(request: web.Request) -> web.Response:
    return web.json_response({"handlers": BUSINESS_HANDLERS})


async def metrics_endpoint(request: web.Request) -> web.Response:
    # aiohttp rejects content_type values that carry a charset parameter, so the
    # Prometheus exposition content type is set through the header instead.
    response = web.Response(body=metrics.body(), content_type="text/plain", charset="utf-8")
    response.headers["Content-Type"] = CONTENT_TYPE_LATEST
    return response


def build_app() -> web.Application:
    app = web.Application(middlewares=[metrics.create_middleware()])
    app.router.add_post("/items", create_item)
    app.router.add_get("/items/{id}", get_item)
    app.router.add_put("/items/{id}", update_item)
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics_endpoint)
    app.router.add_get("/handlers", handlers)
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT)
