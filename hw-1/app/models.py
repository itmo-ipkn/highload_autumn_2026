"""Request and response bodies for the three business endpoints."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class ItemPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    description: str = Field(default="", max_length=2000)


class Item(ItemPayload):
    # Stored values contain only immutable strings. Returning one cannot mutate
    # the shared store through a caller-held model reference.
    model_config = ConfigDict(extra="forbid", frozen=True)

    id: str


class Health(BaseModel):
    status: Literal["ok"] = "ok"
    pod_id: str
    datacenter: str
