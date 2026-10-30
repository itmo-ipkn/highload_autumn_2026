import asyncio
import itertools

from pydantic import BaseModel


class ItemIn(BaseModel):
    name: str
    value: int = 0


class Item(ItemIn):
    id: int


class Storage:
    def __init__(self) -> None:
        self._items: dict[int, Item] = {}
        self._ids = itertools.count(1)
        self._lock = asyncio.Lock()

    async def create(self, data: ItemIn) -> Item:
        async with self._lock:
            item = Item(id=next(self._ids), **data.model_dump())
            self._items[item.id] = item
            return item

    async def get(self, item_id: int) -> Item | None:
        async with self._lock:
            return self._items.get(item_id)

    async def update(self, item_id: int, data: ItemIn) -> Item | None:
        async with self._lock:
            if item_id not in self._items:
                return None
            item = Item(id=item_id, **data.model_dump())
            self._items[item_id] = item
            return item
