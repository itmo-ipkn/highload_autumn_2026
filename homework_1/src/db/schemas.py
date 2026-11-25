import uuid

from pydantic import BaseModel, Field


class Item(BaseModel):
    value: str


class ItemDB(Item):
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
