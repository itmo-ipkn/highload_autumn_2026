import uuid

from redis.asyncio import Redis

from db.schemas import Item, ItemDB


class ItemAlreadyExistsError(Exception):
    pass


class ItemDoesNotExistError(Exception):
    pass


class RedisItemRepository:
    def __init__(self, redis_client: Redis) -> None:
        self.db = redis_client

    def _get_key(self, item_id: uuid.UUID):
        return f"item:id:{item_id}"

    async def create(self, item: Item) -> ItemDB:
        item_db = ItemDB(**item.model_dump())
        key = self._get_key(item_db.id)

        item_data = await self.db.set(key, item_db.model_dump_json(), nx=True)

        if item_data is None:
            raise ItemAlreadyExistsError(
                f"Item ({item_db.id}) already exists."
            )

        return item_db

    async def get_by_id(self, item_id: uuid.UUID) -> ItemDB | None:
        key = self._get_key(item_id)
        item_data = await self.db.get(key)

        if item_data is None:
            return None

        return ItemDB.model_validate_json(item_data)

    async def update_by_id(
        self, item_id: uuid.UUID, item: Item
    ) -> ItemDB | None:
        key = self._get_key(item_id)
        item_db = ItemDB(id=item_id, **item.model_dump())
        item_data = await self.db.set(key, item_db.model_dump_json(), xx=True)

        if item_data is None:
            raise ItemDoesNotExistError(
                f"Update by id: Item ({item_id}) does not exist."
            )

        return item_db
