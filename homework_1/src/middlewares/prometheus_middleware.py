import time

from fastapi import Request

from core.config import settings
from core.prometheus_metrics import (
    http_errors_4xx_total,
    http_errors_5xx_total,
    http_request_duration_seconds,
    http_requests_total,
)
from middlewares.excluded_urls import PROMETHEUS_EXCLUDED_ENDPOINTS_URLS


class MetricsMiddleware:

    def __init__(self):
        self.excluded_urls: list[str] = PROMETHEUS_EXCLUDED_ENDPOINTS_URLS

    async def __call__(self, request: Request, call_next):
        start_time = time.time()

        response = await call_next(request)

        duration = time.time() - start_time
        route = request.scope.get("route")

        if route:
            path = route.path
        else:
            return response

        if path in self.excluded_urls:
            return response

        http_requests_total.labels(
            pod_id=settings.pod_id,
            datacenter=settings.datacenter,
            method=request.method,
            endpoint=path,
            status_code=str(response.status_code),
        ).inc()

        http_request_duration_seconds.labels(
            pod_id=settings.pod_id,
            datacenter=settings.datacenter,
            method=request.method,
            endpoint=path,
        ).observe(duration)

        status_code = response.status_code

        if 400 <= status_code < 500:
            http_errors_4xx_total.labels(
                pod_id=settings.pod_id,
                datacenter=settings.datacenter,
                method=request.method,
                endpoint=path,
                status_code=str(status_code),
            ).inc()
        elif status_code >= 500:
            http_errors_5xx_total.labels(
                pod_id=settings.pod_id,
                datacenter=settings.datacenter,
                method=request.method,
                endpoint=path,
                status_code=str(status_code),
            ).inc()

        return response
