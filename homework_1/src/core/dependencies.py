from fastapi import Depends
from redis.asyncio import Redis

from core.redis import get_redis
from repositories.item_repository import RedisItemRepository
from services.item_service import ItemService


def get_item_repo(
    redis_client: Redis = Depends(get_redis),
) -> RedisItemRepository:
    return RedisItemRepository(redis_client)


def get_item_service(item_repo: RedisItemRepository = Depends(get_item_repo)):
    return ItemService(repo=item_repo)
