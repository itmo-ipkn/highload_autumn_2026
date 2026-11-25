import random

from fastapi import HTTPException, Request, status

from core.config import settings
from core.prometheus_metrics import http_errors_5xx_total
from middlewares.excluded_urls import PROMETHEUS_EXCLUDED_ENDPOINTS_URLS


class DisorderMiddleware:

    def __init__(self, error_rate: float):
        self.excluded_urls: list[str] = PROMETHEUS_EXCLUDED_ENDPOINTS_URLS
        self.error_rate = error_rate

    async def __call__(self, request: Request, call_next):
        response = await call_next(request)
        route = request.scope.get("route")

        if route:
            path = route.path
        else:
            return response

        if path in self.excluded_urls:
            return response

        if random.random() < self.error_rate:
            http_errors_5xx_total.labels(
                pod_id=settings.pod_id,
                datacenter=settings.datacenter,
                method=request.method,
                endpoint=path,
                status_code=str(status.HTTP_500_INTERNAL_SERVER_ERROR),
            ).inc()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Disorder Internal Server Error.",
            )
        return response
