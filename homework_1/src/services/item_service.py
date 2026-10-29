import uuid

from db.schemas import Item, ItemDB
from repositories.item_repository import RedisItemRepository


class ItemService:
    def __init__(self, repo: RedisItemRepository):
        self.repo = repo

    async def create(self, item_data: Item) -> ItemDB:
        return await self.repo.create(item_data)

    async def get_by_id(self, item_id: uuid.UUID) -> ItemDB | None:
        return await self.repo.get_by_id(item_id)

    async def update_by_id(
        self, item_id: uuid.UUID, item_data: Item
    ) -> ItemDB:
        return await self.repo.update_by_id(item_id, item_data)
