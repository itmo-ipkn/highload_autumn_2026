from pydantic import Field
from pydantic_settings import BaseSettings

from core_common.config import ProjectBaseSettings


class KafkaProducerSerttings(BaseSettings):
    key_mode: str = Field(..., alias="PRODUCER_KEY_MODE")
    event_rate: int = Field(..., alias="PRODUCER_EVENT_RATE")
    poison_percent: int = Field(..., alias="PRODUCER_POISON_PERCENT")


class AppSettings(ProjectBaseSettings):
    producer_settings: KafkaProducerSerttings = KafkaProducerSerttings()


settings = AppSettings()
