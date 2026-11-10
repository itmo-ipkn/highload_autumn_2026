from pydantic import BaseModel, ConfigDict, Field


class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    value: int


class Item(ItemIn):
    model_config = ConfigDict(frozen=True)

    id: str
