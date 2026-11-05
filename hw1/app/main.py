import itertools
import os
import random

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from metrics import setup_metrics

ERROR_RATE = float(os.environ.get("ERROR_RATE", "0"))

class ItemIn(BaseModel):
    name: str
    price: float = 0


class Item(ItemIn):
    id: int

app = FastAPI(title="items-service")
setup_metrics(app)
items: dict[int, Item] = {}
ids = itertools.count(1)

def maybe_fail() -> None:
    if ERROR_RATE and random.random() < ERROR_RATE:
        raise HTTPException(status_code=500, detail="injected failure")

@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}

@app.post("/items", status_code=201)
async def create_item(data: ItemIn) -> Item:
    maybe_fail()
    item = Item(id=next(ids), **data.model_dump())
    items[item.id] = item
    return item

@app.get("/items/{id}")
async def get_item(id: int) -> Item:
    maybe_fail()
    if id not in items:
        raise HTTPException(status_code=404, detail="item not found")
    return items[id]

@app.put("/items/{id}")
async def update_item(id: int, data: ItemIn) -> Item:
    maybe_fail()
    if id not in items:
        raise HTTPException(status_code=404, detail="item not found")
    items[id] = Item(id=id, **data.model_dump())
    return items[id]
