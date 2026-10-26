"""Small reusable instrumentation library for FastAPI business routers."""
import os
from time import perf_counter

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from prometheus_client import Counter, Histogram
from starlette.exceptions import HTTPException

POD_ID = os.environ.get("POD_ID", "local-pod")
DATACENTER = os.environ.get("DATACENTER", "local-dc")
LABELS = ("pod_id", "datacenter", "method", "route")

REQUESTS = Counter(
    "netinventory_http_requests_total", "Completed registry HTTP requests",
    LABELS + ("status_code",),
)
DURATION = Histogram(
    "netinventory_request_duration_seconds", "Registry handler duration in seconds",
    LABELS,
    buckets=(.005, .01, .02, .03, .05, .075, .1, .15, .25, .5, 1, 2.5),
)


class MonitoredRoute(APIRoute):
    """Use on a business APIRouter, leaving service routes uninstrumented."""

    def get_route_handler(self):
        original = super().get_route_handler()
        # self.path is the route template, never /items/<actual UUID>.
        route = self.path
        for method in self.methods:
            labels = (POD_ID, DATACENTER, method, route)
            DURATION.labels(*labels)
            for code in (200, 201, 404, 409, 422, 500):
                REQUESTS.labels(*labels, str(code))

        async def measured(request: Request) -> Response:
            started = perf_counter()
            status = 500  # Unhandled exceptions must also count as errors.
            try:
                response = await original(request)
                status = response.status_code
                return response
            except HTTPException as error:
                status = error.status_code
                raise
            except RequestValidationError:
                status = 422
                raise
            finally:
                labels = (POD_ID, DATACENTER, request.method, route)
                REQUESTS.labels(*labels, str(status)).inc()
                DURATION.labels(*labels).observe(perf_counter() - started)

        return measured


def registered_handlers(app):
    """Same router registry drives /handlers and instrumentation."""
    return sorted(
        [
            {"method": method, "path": route.path}
            for route in app.routes if isinstance(route, MonitoredRoute)
            for method in route.methods
        ],
        key=lambda entry: (entry["path"], entry["method"]),
    )
