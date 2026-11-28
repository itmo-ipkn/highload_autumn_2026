from fastapi import APIRouter, HTTPException, Path, Response

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from models import Item


router = APIRouter()

items: dict[int, Item] = {}

@router.post("/items", status_code=201)
async def create_item(item: Item):
    item_id = item.id

    if item_id in items:
        raise HTTPException(
            status_code=409,
            detail=f"Item with id {item_id} already exists",
        )

    items[item_id] = item

    return item

@router.put("/items/{item_id}")
async def update_item(
    updated_item: Item,
    item_id: int = Path(ge=1),
):
    if item_id != updated_item.id:
        raise HTTPException(
            status_code=422,
            detail="Path ID must match body ID"
        )

    item = items.get(item_id)

    if item is None:
        raise HTTPException(
            status_code=404,
            detail="Item not found"
        )

    items[item_id] = updated_item

    return updated_item

@router.get("/items/{item_id}")
async def get_item(item_id: int = Path(ge=1)):
    item = items.get(item_id)

    if item is None:
        raise HTTPException(
            status_code=404,
            detail="Item not found"
        )
    
    if item_id == 42:
        raise HTTPException(
            status_code=500,
        )
    
    return item

@router.get("/health")
async def check_health():
    return {"status": "ok"}

@router.get("/metrics")
async def get_metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )

@router.get("/handlers")
async def get_handlers():
    handlers = []

    for route in router.routes:
        route_path = route.path

        if route_path not in (
            "/health",
            "/metrics",
            "/handlers",
        ):  
            handler = {
                "method": route.methods,
                "endpoint": route_path
            }
            handlers.append(handler)

    return handlers
