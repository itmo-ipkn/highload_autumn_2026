import itertools
import os
import random
import threading

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

POD_ID = os.environ.get("POD_ID", "unknown")
DATACENTER = os.environ.get("DATACENTER", "unknown")
# Вероятность искусственного 500 на бизнес-ручках (0.0 — выключено).
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))

app = FastAPI(title="items-service")

# TODO(этап 2): подключить middleware с метриками и ручку GET /metrics.
# TODO(этап 3): ручка GET /handlers со списком бизнес-ручек.


class ItemIn(BaseModel):
    name: str
    value: int = 0


class Item(ItemIn):
    id: int


class Storage:
    """In-memory хранилище. Lock нужен, т.к. sync-ручки FastAPI выполняются в threadpool."""

    def __init__(self) -> None:
        self._items: dict[int, Item] = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()

    def create(self, data: ItemIn) -> Item:
        with self._lock:
            item = Item(id=next(self._ids), **data.model_dump())
            self._items[item.id] = item
            return item

    def get(self, item_id: int) -> Item | None:
        with self._lock:
            return self._items.get(item_id)

    def update(self, item_id: int, data: ItemIn) -> Item | None:
        with self._lock:
            if item_id not in self._items:
                return None
            item = Item(id=item_id, **data.model_dump())
            self._items[item_id] = item
            return item


storage = Storage()


def maybe_fail() -> None:
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        raise HTTPException(status_code=500, detail="injected failure")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "pod": POD_ID, "dc": DATACENTER}


@app.post("/items", status_code=201)
def create_item(data: ItemIn) -> Item:
    maybe_fail()
    return storage.create(data)


@app.get("/items/{item_id}")
def get_item(item_id: int) -> Item:
    maybe_fail()
    item = storage.get(item_id)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    return item


@app.put("/items/{item_id}")
def update_item(item_id: int, data: ItemIn) -> Item:
    maybe_fail()
    item = storage.update(item_id, data)
    if item is None:
        raise HTTPException(status_code=404, detail="item not found")
    return item
