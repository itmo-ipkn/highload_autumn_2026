import datetime
import random
import uuid
from collections import defaultdict

from models.events import Event, EventType
from producer.utils.user_service import UserService


class EventGenerator:
    def __init__(self, poison_percent: int, user_service: UserService):
        self.poison_percent: int = poison_percent / 100
        self.user_service: UserService = user_service
        self.user_seq: defaultdict[uuid.UUID, int] = defaultdict(int)
        self.event_types: tuple[EventType] = [
            event_type for event_type in EventType
        ]

    def _get_random_event_type(self):
        return random.choice(self.event_types)

    def generate_event(self) -> Event:
        user_id = self.user_service.get_random_user_id()
        prev_seq = self.user_seq[user_id]
        seq = prev_seq + 1
        self.user_seq[user_id] = seq
        message = (
            "Valid event"
            if random.random() > self.poison_percent
            else "Invalid event"
        )
        return Event(
            event_id=uuid.uuid4(),
            user_id=user_id,
            event_type=self._get_random_event_type(),
            seq=seq,
            produced_at=datetime.datetime.now(datetime.UTC),
            payload={"message": message},
        )
