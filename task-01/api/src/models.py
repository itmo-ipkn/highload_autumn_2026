from pydantic import BaseModel, Field


class Item(BaseModel):
    id: int = Field(
        ge=1,
        description="Unique item id, must be >= 1",
        examples=[1],
    )
    name: str
    description: str | None = None
    price: float
