from pydantic import Field
from pydantic_settings import BaseSettings


class ProjectBaseSettings(BaseSettings):
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    topic: str = Field(..., alias="KAFKA_EVENT_TOPIC")
    dlt_topic: str = Field(..., alias="KAFKA_EVENT_DLT_TOPIC")
    bootstrap_servers: str = Field(..., alias="KAFKA_BOOTSTRAP_SERVERS")
