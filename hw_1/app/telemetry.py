import os
from time import perf_counter

from prometheus_client import Counter, Histogram

REQUESTS = Counter(
    "http_request_total",
    "HTTP responses by route, status and deployment location",
    ("pod_id", "datacenter", "method", "route", "status_code"),
)

DURATION = Histogram(
    "http_request_duration_seconds",
    "HTTP request handling duration in seconds",
    ("pod_id", "datacenter", "method", "route"),
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
             0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

EXCLUDED_PATHS = frozenset(("/health", "/metrics", "/handlers"))


class PrometheusMiddleware:

    def __init__(self, app):
        self.app = app
        self.pod_id = os.environ.get("POD_ID", "unknown")
        self.datacenter = os.environ.get("DATACENTER", "unknown")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope.get("path") in EXCLUDED_PATHS:
            await self.app(scope, receive, send)
            return

        started = perf_counter()
        status_code = 500

        async def send_with_status(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_with_status)
        finally:
            route = scope.get("route")
            template = getattr(route, "path_format", None)
            if template and template not in EXCLUDED_PATHS:
                method = scope.get("method", "UNKNOWN")
                elapsed = perf_counter() - started
                common = (self.pod_id, self.datacenter, method, template)
                REQUESTS.labels(*common, str(status_code)).inc()
                DURATION.labels(*common).observe(elapsed)
