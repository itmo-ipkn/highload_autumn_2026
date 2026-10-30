import asyncio
import random

from fastapi import APIRouter, FastAPI, HTTPException

from config import settings
from metrics import MetricsRoute, setup_metrics
from storage import Item, ItemIn, Storage

app = FastAPI(title="items-service")
# Бизнес-ручки: на них вешаются метрики.
router = APIRouter(route_class=MetricsRoute)

setup_metrics(app)

storage = Storage()


async def simulate_work() -> None:
    """Имитация работы: задержка с экспоненциальным распределением,
    чтобы у latency был хвост и p50/p95/p99 различались."""
    if settings.DELAY_MEAN_MS > 0:
        await asyncio.sleep(random.expovariate(1000 / settings.DELAY_MEAN_MS))


def maybe_fail() -> None:
    if settings.FAIL_RATE > 0 and random.random() < settings.FAIL_RATE:
        raise HTTPException(status_code=500, detail="injected failure")


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "pod": settings.POD_ID, "dc": settings.DATACENTER}


@router.post("/items", status_code=201)
async def create_item(data: ItemIn) -> Item:
    await simulate_work()
    maybe_fail()
    return await storage.create(data)


@router.get("/items/{item_id}")
async def get_item(item_id: int) -> Item:
    await simulate_work()
    maybe_fail()
    item = await storage.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    return item


@router.put("/items/{item_id}")
async def update_item(item_id: int, data: ItemIn) -> Item:
    await simulate_work()
    maybe_fail()
    item = await storage.update(item_id, data)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    return item


# include_router копирует роуты в app, поэтому вызывается после объявления всех ручек.
app.include_router(router)
