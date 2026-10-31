import time
from collections.abc import Sequence

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    disable_created_metrics,
    generate_latest,
)
from starlette.requests import ClientDisconnect
from starlette.types import ASGIApp, Message, Receive, Scope, Send

LATENCY_BUCKETS = (0.001, 0.002, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0, 2.5)

INITIAL_STATUS_CODES = ("200", "400", "404", "405", "422", "500")

UNMATCHED_HANDLER = "unmatched"

SERVICE_PATHS = frozenset(
    {"/health", "/metrics", "/handlers", "/docs", "/docs/oauth2-redirect", "/openapi.json", "/redoc"}
)
disable_created_metrics()


class Metrics:
    def __init__(self, *, pod_id: str, datacenter: str, handlers: Sequence[str]) -> None:
        self._pod_id = pod_id
        self._datacenter = datacenter
        self._handlers = tuple(handlers)
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self.requests = Counter(
            "http_requests_total",
            "Total number of HTTP requests",
            ("handler", "status_code", "pod_id", "datacenter"),
            registry=self.registry,
        )
        self.latency = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            ("handler", "pod_id", "datacenter"),
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )

    def init_series(self) -> None:
        for handler in self._handlers:
            for status_code in INITIAL_STATUS_CODES:
                self._labels(handler, status_code)
            self.latency.labels(handler=handler, pod_id=self._pod_id, datacenter=self._datacenter)

    def observe(self, handler: str, status_code: int | str, seconds: float) -> None:
        self._labels(handler, str(status_code)).inc()
        self.latency.labels(handler=handler, pod_id=self._pod_id, datacenter=self._datacenter).observe(seconds)

    def render(self) -> bytes:
        return generate_latest(self.registry)

    def _labels(self, handler: str, status_code: str) -> Counter:
        return self.requests.labels(
            handler=handler,
            status_code=status_code,
            pod_id=self._pod_id,
            datacenter=self._datacenter,
        )


def resolve_handler(scope: Scope) -> str:
    route = scope.get("route")
    path_format = getattr(route, "path_format", None)
    methods = getattr(route, "methods", None) or set()
    method = scope.get("method", "")
    if not path_format or method not in methods:
        return UNMATCHED_HANDLER
    return f"{method} {path_format}"


class RequestMetricsMiddleware:
    def __init__(self, app: ASGIApp, metrics: Metrics) -> None:
        self._app = app
        self._metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in SERVICE_PATHS:
            await self._app(scope, receive, send)
            return

        started = time.perf_counter()
        status_code: int | None = None
        elapsed: float | None = None

        async def send_wrapper(message: Message) -> None:
            nonlocal status_code, elapsed
            if message["type"] == "http.response.start":
                status_code = message["status"]
                elapsed = time.perf_counter() - started
            await send(message)

        try:
            await self._app(scope, receive, send_wrapper)
        except ClientDisconnect:
            # Клиент ушёл, не дочитав тело: ответа сервиса не было.
            status_code = None
            raise
        except Exception:
            # Если ответ уже начат, клиент получил исходный статус — переписывать на 500 нельзя.
            if status_code is None:
                status_code = 500
                elapsed = time.perf_counter() - started
            raise
        except BaseException:
            # Отмена/обрыв соединения — не ответ сервиса, 500 писать нельзя.
            status_code = None
            raise
        finally:
            if status_code is not None:
                latency = elapsed if elapsed is not None else time.perf_counter() - started
                self._metrics.observe(resolve_handler(scope), status_code, latency)
