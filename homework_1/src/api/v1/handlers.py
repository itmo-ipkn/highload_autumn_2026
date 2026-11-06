from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from fastapi.routing import RouteContext, iter_route_contexts

from middlewares.prometheus_middleware import (
    PROMETHEUS_EXCLUDED_ENDPOINTS_URLS,
)

router = APIRouter(tags=["handlers"])


@router.get("/handlers", name="handlers_list")
async def endpoints(request: Request):
    raw_route_list: list[RouteContext] = [
        route
        for route in iter_route_contexts(request.app.routes)
        if route.path not in PROMETHEUS_EXCLUDED_ENDPOINTS_URLS
    ]
    route_list: set[str] = list({route.path for route in raw_route_list})
    return JSONResponse(status_code=status.HTTP_200_OK, content=route_list)
