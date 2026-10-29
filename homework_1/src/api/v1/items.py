import uuid

from fastapi import APIRouter, Depends, HTTPException, status

from core.dependencies import get_item_service
from core.logger import get_logger
from db.schemas import Item, ItemDB
from repositories.item_repository import (
    ItemAlreadyExistsError,
    ItemDoesNotExistError,
)
from services.item_service import ItemService

logger = get_logger(__name__)
router = APIRouter(prefix="/api/v1", tags=["items"])


@router.post("/items", name="items_create")
async def create_item(
    item_data: Item, item_service: ItemService = Depends(get_item_service)
) -> ItemDB:
    try:
        return await item_service.create(item_data)
    except ItemAlreadyExistsError as err:
        logger.error(err)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Item already exists.",
        )


@router.get("/items/{item_id}", name="items_get_by_id")
async def get_item(
    item_id: uuid.UUID, item_service: ItemService = Depends(get_item_service)
) -> ItemDB:
    item = await item_service.get_by_id(item_id)
    if item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Item not found."
        )
    return item


@router.put("/items/{item_id}", name="items_update_by_id")
async def update_item(
    item_id: uuid.UUID,
    item_data: Item,
    item_service: ItemService = Depends(get_item_service),
) -> ItemDB:
    try:
        return await item_service.update_by_id(item_id, item_data)
    except ItemDoesNotExistError as err:
        logger.error(err)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Item not found."
        )
