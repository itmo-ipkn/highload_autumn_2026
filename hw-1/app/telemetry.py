"""Reusable ASGI instrumentation and runtime discovery of business routes.

The library knows the three service paths, but never enumerates business paths.
It observes the final wire status and finishes latency at the last response body.
Each application has its own registry, which also isolates multiple apps in tests.
"""

from collections.abc import Iterable
from time import perf_counter
from typing import Any

from fastapi import FastAPI
from fastapi.routing import APIRoute, iter_route_contexts
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest
from starlette.types import ASGIApp, Message, Receive, Scope, Send

SERVICE_PATHS = frozenset({"/health", "/metrics", "/handlers"})
LATENCY_BUCKETS = (
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075,
    0.1, 0.15, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
)


def business_handlers(application: FastAPI) -> list[dict[str, str]]:
    """Read the actual routing table, including routes added after creation."""
    handlers = {
        (method, context.path)
        for context in iter_route_contexts(application.routes)
        if isinstance(context.original_route, APIRoute)
        and context.path is not None and context.path not in SERVICE_PATHS
        for method in (context.methods or ())
    }
    return [
        {"method": method, "path": path}
        for method, path in sorted(handlers, key=lambda entry: (entry[1], entry[0]))
    ]


class Telemetry:
    def __init__(self, pod_id: str, datacenter: str) -> None:
        self.pod_id = pod_id
        self.datacenter = datacenter
        self.registry = CollectorRegistry()
        common = ("pod_id", "datacenter", "method", "route")
        self.duration = Histogram(
            "http_request_duration_seconds",
            "Time until the final HTTP response body is sent, in seconds.",
            common,
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        self.requests = Counter(
            "http_requests_total",
            "Number of completed HTTP requests, partitioned by response status.",
            (*common, "status_code"),
            registry=self.registry,
        )

    def register_handlers(self, handlers: Iterable[dict[str, str]]) -> None:
        # Publish zero-valued series before load starts so dashboard selectors
        # can discover both instances. Zero counters do not invent requests.
        for handler in handlers:
            labels = self._labels(handler["method"], handler["path"])
            self.duration.labels(**labels)
            for code in (200, 201, 400, 404, 422, 500):
                self.requests.labels(**labels, status_code=str(code))

    def _labels(self, method: str, route: str) -> dict[str, str]:
        return {
            "pod_id": self.pod_id,
            "datacenter": self.datacenter,
            "method": method,
            "route": route,
        }

    def observe(self, method: str, route: str, status_code: int, duration: float) -> None:
        labels = self._labels(method, route)
        self.requests.labels(**labels, status_code=str(status_code)).inc()
        self.duration.labels(**labels).observe(max(duration, 0.0))

    def render(self) -> bytes:
        return generate_latest(self.registry)


class HttpMetricsMiddleware:
    def __init__(self, app: ASGIApp, telemetry: Telemetry) -> None:
        self.app = app
        self.telemetry = telemetry

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started_at = perf_counter()
        finished_at: float | None = None
        status_code: int | None = None

        async def observe_send(message: Message) -> None:
            nonlocal status_code, finished_at
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)
            if message["type"] == "http.response.body" and not message.get("more_body", False):
                finished_at = perf_counter()

        try:
            await self.app(scope, receive, observe_send)
        except Exception:
            # Starlette's outer ServerErrorMiddleware sends an unhandled 500
            # after this middleware unwinds. Preserve an already sent status.
            if status_code is None:
                status_code = 500
            raise
        finally:
            route: Any = scope.get("route")
            # FastAPI 0.162 keeps included routers lazy. Its selected effective
            # context contains the complete path, including router prefixes.
            context = scope.get("fastapi", {}).get("effective_route_context")
            path = getattr(context, "path", getattr(route, "path", None))
            method = scope.get("method", "")
            if (
                isinstance(route, APIRoute)
                and path is not None and path not in SERVICE_PATHS
                and method in route.methods
                and status_code is not None
            ):
                elapsed = (finished_at if finished_at is not None else perf_counter()) - started_at
                self.telemetry.observe(method, path, status_code, elapsed)
