import uuid
from collections.abc import Sequence
from random import random

from fastapi import APIRouter, HTTPException, Response
from prometheus_client import CONTENT_TYPE_LATEST

from metrics import SERVICE_PATHS, Metrics
from models import Item, ItemIn
from store import InMemoryStore


def build_service_router() -> APIRouter:
    router = APIRouter(tags=["service"])

    @router.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    return router


def build_observability_router(metrics: Metrics, handlers: Sequence[str]) -> APIRouter:
    router = APIRouter(tags=["service"])

    @router.get("/metrics")
    def metrics_endpoint() -> Response:
        return Response(content=metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @router.get("/handlers")
    def handlers_endpoint() -> dict[str, list[str]]:
        return {"handlers": list(handlers)}

    return router


def describe_handlers(*routers: APIRouter) -> list[str]:
    """Список бизнес-ручек для `/handlers`.

    Источник — сами бизнес-роутеры: `app.routes` в FastAPI 0.162 содержит ленивые
    обёртки включённых роутеров, у которых `path_format` не заполнен.
    """
    handlers: set[str] = set()
    for router in routers:
        for route in router.routes:
            path_format = getattr(route, "path_format", None)
            if not path_format or path_format in SERVICE_PATHS:
                continue
            for method in getattr(route, "methods", None) or set():
                if method != "HEAD":
                    handlers.add(f"{method} {path_format}")
    return sorted(handlers)


def build_router(store: InMemoryStore, failure_rate: float = 0.0) -> APIRouter:
    router = APIRouter(tags=["items"])

    def maybe_fail() -> None:
        if failure_rate > 0 and random() < failure_rate:
            raise HTTPException(status_code=500, detail="artificial failure")

    @router.post("/items", response_model=Item)
    def create_item(payload: ItemIn) -> Item:
        maybe_fail()
        return store.create(Item(id=uuid.uuid4().hex, name=payload.name, value=payload.value))

    @router.get("/items/{id}", response_model=Item)
    def get_item(id: str) -> Item:
        maybe_fail()
        item = store.get(id)
        if item is None:
            raise HTTPException(status_code=404, detail="item not found")
        return item

    @router.put("/items/{id}", response_model=Item)
    def update_item(id: str, payload: ItemIn) -> Item:
        maybe_fail()
        updated = store.update(id, Item(id=id, name=payload.name, value=payload.value))
        if updated is None:
            raise HTTPException(status_code=404, detail="item not found")
        return updated

    return router
