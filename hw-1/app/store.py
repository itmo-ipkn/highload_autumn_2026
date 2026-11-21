"""One independent, thread-safe store per application instance."""

from threading import Lock
from uuid import uuid4

from app.models import Item, ItemPayload


class InMemoryItemStore:
    def __init__(self) -> None:
        self._items: dict[str, Item] = {}
        self._lock = Lock()

    def create(self, payload: ItemPayload) -> Item:
        item = Item(id=str(uuid4()), **payload.model_dump())
        with self._lock:
            self._items[item.id] = item
        return item

    def get(self, item_id: str) -> Item | None:
        with self._lock:
            return self._items.get(item_id)

    def replace(self, item_id: str, payload: ItemPayload) -> Item | None:
        with self._lock:
            if item_id not in self._items:
                return None
            item = Item(id=item_id, **payload.model_dump())
            self._items[item_id] = item
            return item
