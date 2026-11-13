import os
import time

from fastapi import FastAPI, Request, Response
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

POD_ID = os.environ.get("POD_ID", "unknown")
DATACENTER = os.environ.get("DATACENTER", "unknown")

SERVICE_PATHS = {"/health", "/metrics", "/handlers"}

LATENCY_BUCKETS = (
    0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.00075, 0.001, 0.00125, 0.0015, 0.002, 0.0025, 0.003,
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
)

REQUEST_DURATION = Histogram(
    "http_request_duration_seconds",
    "Время обработки запроса, секунды",
    ["pod_id", "datacenter", "method", "route"],
    buckets=LATENCY_BUCKETS,
)
REQUESTS = Counter(
    "http_requests_total",
    "Число запросов по ручке и коду ответа",
    ["pod_id", "datacenter", "method", "route", "status_code"],
)

def route_template(request: Request) -> str | None:
    route = request.scope.get("route")
    return getattr(route, "path", None)

def business_routes(app: FastAPI) -> list[dict]:
    handlers = []
    for route in app.routes:
        if not isinstance(route, APIRoute) or route.path in SERVICE_PATHS:
            continue
        for method in sorted(route.methods - {"HEAD", "OPTIONS"}):
            handlers.append({"method": method, "path": route.path})
    return handlers

def setup_metrics(app: FastAPI) -> None:
    @app.middleware("http")
    async def golden_signals(request: Request, call_next):
        start = time.perf_counter()
        code = 500  # если ручка упала исключением, наружу уйдёт 500
        try:
            response = await call_next(request)
            code = response.status_code
            return response
        finally:
            route = route_template(request)
            # Неизвестные пути (404 роутера) не пишем: иначе каждый случайный URL — новый ряд.
            if route is not None and route not in SERVICE_PATHS:
                labels = dict(pod_id=POD_ID, datacenter=DATACENTER, method=request.method, route=route)
                REQUEST_DURATION.labels(**labels).observe(time.perf_counter() - start)
                REQUESTS.labels(**labels, status_code=str(code)).inc()

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/handlers", include_in_schema=False)
    async def handlers() -> list[dict]:
        return business_routes(app)
