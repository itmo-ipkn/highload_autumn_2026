"""Golden-signals library for REST handlers (idiomatic aiohttp middleware).

Metrics chosen:
- http_requests_total          Counter    labels: method, handler, code, pod, datacenter
- http_request_duration_seconds Histogram  labels: method, handler, pod, datacenter

Histogram is chosen for latency because quantiles computed with
histogram_quantile(...) over summed *_bucket series remain correct when
aggregating across multiple app instances; a Summary's quantiles cannot be
aggregated across instances.

pod / datacenter are runtime-only values passed via POD_ID and DATACENTER.
"""
from __future__ import annotations
import os
import time

from aiohttp import web
from prometheus_client import CollectorRegistry, Counter, Histogram, CONTENT_TYPE_LATEST, generate_latest

# Latency buckets cover the fast common case up to slow tails so p50/p95/p99
# can be computed by histogram_quantile over the aggregated buckets.
BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.5, 0.75, 1.0, 1.5, 2.5, 5.0)

# Service endpoints are not business handlers and must not be instrumented.
EXCLUDED_PATHS = {"/health", "/metrics", "/handlers"}


class Metrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.pod = os.environ.get("POD_ID", "unknown")
        self.datacenter = os.environ.get("DATACENTER", "unknown")
        self.requests = Counter(
            "http_requests_total",
            "Total responses by handler, method, status code, pod and datacenter.",
            ["method", "handler", "code", "pod", "datacenter"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "http_request_duration_seconds",
            "Handler latency buckets by method, handler template, pod and datacenter.",
            ["method", "handler", "pod", "datacenter"],
            buckets=BUCKETS,
            registry=self.registry,
        )

    def handler_template(self, request) -> str:
        # Route template (e.g. /items/{id}), not the actual request path.
        # aiohttp resolves the route before the middleware runs, so the matched
        # resource is read through request.match_info (Request.resource no longer
        # exists in aiohttp 3.13); the fallback covers older versions.
        route = getattr(request.match_info, "route", None)
        resource = getattr(route, "resource", None) if route is not None else None
        if resource is None:
            resource = getattr(request, "resource", None)
        if resource is None:
            return "unmatched"
        # DynamicResource exposes the template as "formatter", PlainResource as "path".
        info = resource.get_info()
        template = info.get("formatter") or info.get("path") or resource.name
        return template or "unmatched"

    def _record(self, request, code: str, elapsed: float) -> None:
        template = self.handler_template(request)
        self.requests.labels(request.method, template, code, self.pod, self.datacenter).inc()
        self.duration.labels(request.method, template, self.pod, self.datacenter).observe(elapsed)

    def create_middleware(self):
        @web.middleware
        async def middleware(request: web.Request, handler):
            if request.path in EXCLUDED_PATHS:
                return await handler(request)
            start = time.perf_counter()
            try:
                response = await handler(request)
            except web.HTTPException as exc:
                # System routes (unmatched paths) and raised HTTP errors.
                self._record(request, str(exc.status_code), time.perf_counter() - start)
                raise
            except Exception:
                self._record(request, "500", time.perf_counter() - start)
                raise
            self._record(request, str(response.status), time.perf_counter() - start)
            return response
        return middleware

    def body(self) -> bytes:
        return generate_latest(self.registry)
