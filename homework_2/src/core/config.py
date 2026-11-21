from pydantic import Field
from pydantic_settings import BaseSettings


class ProjectSettings(BaseSettings):
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    event_rate: int = Field(..., alias="EVENT_RATE")
    poison_percent: int = Field(..., alias="POISON_PERCENT")


class KafkaSettings(BaseSettings):
    bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
    topic: str = Field(..., alias="KAFKA_EVENT_TOPIC")


class AppSettings(BaseSettings):
    project: ProjectSettings = ProjectSettings()
    kafka: KafkaSettings = KafkaSettings()


settings = AppSettings()
