import threading
from collections import OrderedDict

from models import Item


class InMemoryStore:
    def __init__(self, max_items: int = 10_000) -> None:
        self._items: OrderedDict[str, Item] = OrderedDict()
        self._lock = threading.RLock()
        self._max_items = max_items

    def create(self, item: Item) -> Item:
        with self._lock:
            self._items[item.id] = item
            self._items.move_to_end(item.id)
            while len(self._items) > self._max_items:
                self._items.popitem(last=False)
        return item

    def get(self, item_id: str) -> Item | None:
        with self._lock:
            item = self._items.get(item_id)
            if item is not None:
                self._items.move_to_end(item_id)
            return item

    def update(self, item_id: str, item: Item) -> Item | None:
        with self._lock:
            if item_id not in self._items:
                return None
            self._items[item_id] = item
            self._items.move_to_end(item_id)
            return item

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
