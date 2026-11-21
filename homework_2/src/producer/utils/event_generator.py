import datetime
import random
import uuid

from models.events import Event, EventType


class EventGenerator:
    def __init__(self):
        self.user_seq: dict[uuid.UUID, int] = {}
        self.event_types: tuple[EventType] = [
            event_type for event_type in EventType
        ]

    def get_random_event_type(self):
        return random.choice(self.event_types)

    def generate_event(self, is_valid: bool = True) -> Event:
        return Event(
            event_id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            event_type=self.get_random_event_type(),
            seq=1,
            produced_at=datetime.datetime.now(datetime.UTC),
            payload={
                "message": "Valid event" if is_valid else "Invalid event",
            },
        )
