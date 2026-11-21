from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel


class EventType(Enum):
    PAGE_VIEW = "page_view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class Event(BaseModel):
    event_id: UUID
    user_id: UUID
    event_type: EventType
    seq: int
    produced_at: datetime
    payload: dict
