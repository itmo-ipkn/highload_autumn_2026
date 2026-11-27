import random
import uuid


class UserService:
    def __init__(self, count: int = 100):
        self.users: list[uuid.UUID] = [uuid.uuid4() for _ in range(count)]

    def get_random_user_id(self) -> uuid.UUID:
        return random.choice(self.users)
