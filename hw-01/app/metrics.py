import time
from collections.abc import Awaitable, Callable

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import HTTPException, RequestValidationError
from fastapi.routing import APIRoute
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from config import settings

LABELS = ("pod_id", "datacenter", "method", "route")

LATENCY = Histogram(
    "latency_seconds",
    "latency for request",
    labelnames=LABELS,
    buckets=(.005, .01, .02, .03, .05, .075, .1, .15, .25, .5, 1, 2.5),
)

REQUESTS = Counter(
    "requests_amount",
    "requests amount",
    labelnames=LABELS + ("status_code",),
)


def _record(method: str, route: str, status_code: int, duration: float) -> None:
    LATENCY.labels(
        pod_id=settings.POD_ID,
        datacenter=settings.DATACENTER,
        method=method,
        route=route,
    ).observe(duration)

    REQUESTS.labels(
        pod_id=settings.POD_ID,
        datacenter=settings.DATACENTER,
        method=method,
        route=route,
        status_code=str(status_code),
    ).inc()


class MetricsRoute(APIRoute):
    """Класс роута, оборачивающий обработчик ручки сбором метрик.

    FastAPI создаёт по экземпляру на каждую ручку роутера и один раз вызывает
    get_route_handler(); обработчик, который он вернёт, вызывается на каждый запрос.
    """

    def get_route_handler(self) -> Callable[[Request], Awaitable[Response]]:
        original = super().get_route_handler()
        route = self.path

        async def handler(request: Request) -> Response:
            start = time.perf_counter()
            # Если ручка упала с необработанным исключением, клиент получит 500.
            status_code = 500
            try:
                response = await original(request)
                status_code = response.status_code
                return response
            except HTTPException as e:
                status_code = e.status_code
                raise
            except RequestValidationError:
                status_code = 422
                raise
            finally:
                _record(request.method, route, status_code, time.perf_counter() - start)

        return handler


def _metrics_endpoint() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def setup_metrics(app: FastAPI) -> None:
    app.add_api_route("/metrics", _metrics_endpoint, methods=["GET"], include_in_schema=False)
