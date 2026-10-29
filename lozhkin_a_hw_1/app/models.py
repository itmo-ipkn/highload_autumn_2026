"""In-memory хранилище записей."""
import threading


class NotFoundError(Exception):
    """Запрашиваемая запись не найдена."""


class InMemoryStore:
    def __init__(self):
        self._items = {}
        self._lock = threading.Lock()
        self._next_id = 1

    def create(self, data: dict) -> dict:
        with self._lock:
            item = {"id": self._next_id, **data}
            self._next_id += 1
            self._items[item["id"]] = item
            return dict(item)

    def get(self, item_id: int) -> dict:
        with self._lock:
            if item_id not in self._items:
                raise NotFoundError(f"item {item_id} not found")
            return dict(self._items[item_id])

    def update(self, item_id: int, data: dict) -> dict:
        with self._lock:
            if item_id not in self._items:
                raise NotFoundError(f"item {item_id} not found")
            self._items[item_id] = {"id": item_id, **data}
            return dict(self._items[item_id])

    def count(self) -> int:
        with self._lock:
            return len(self._items)
