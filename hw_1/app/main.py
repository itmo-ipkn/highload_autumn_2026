from collections import OrderedDict
from threading import Lock
from uuid import uuid4

from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field

from .telemetry import PrometheusMiddleware

app = FastAPI(
    title="Items service",
    description="Лабораторная работа №1",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(PrometheusMiddleware)

MAX_ITEMS = 5000
_items = OrderedDict()
_lock = Lock()


class ItemData(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    value: int


class Item(ItemData):
    id: str


router = APIRouter()


@router.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
def create_item(data: ItemData):
    if data.name == "__simulate_500__":
        raise HTTPException(status_code=500, detail="Simulated internal error")

    item_id = str(uuid4())
    item = {"id": item_id, **data.model_dump()}
    with _lock:
        _items[item_id] = item
        if len(_items) > MAX_ITEMS:
            _items.popitem(last=False)
    return item


@router.get("/items/{id}", response_model=Item)
def get_item(id: str):
    with _lock:
        item = _items.get(id)
        if item is None:
            raise HTTPException(status_code=404, detail="Item not found")
        return item.copy()


@router.put("/items/{id}", response_model=Item)
def update_item(id: str, data: ItemData):
    with _lock:
        if id not in _items:
            raise HTTPException(status_code=404, detail="Item not found")
        item = {"id": id, **data.model_dump()}
        _items[id] = item
        return item.copy()


app.include_router(router)


@app.get("/health", include_in_schema=False)
def health():
    return {"status": "ok"}


@app.get("/metrics", include_in_schema=False)
def metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/handlers", include_in_schema=False)
def handlers():
    """Use registered routes: generator never hardcodes a list of handlers."""
    from fastapi.routing import APIRoute

    output = []
    for route in app.routes:
        if isinstance(route, APIRoute) and route.path_format not in {
            "/health", "/metrics", "/handlers"
        }:
            for method in sorted(route.methods):
                output.append({"method": method, "route": route.path_format})
    return sorted(output, key=lambda h: (h["route"], h["method"]))
