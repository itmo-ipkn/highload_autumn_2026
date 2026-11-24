from pydantic import Field
from pydantic_settings import BaseSettings

from consumer.core.config_pg import PostgresSettings
from core_common.config import ProjectBaseSettings


class KafkaConsumerSettings(BaseSettings):
    group_id: str = Field(..., alias="CONSUMER_GROUP_ID")
    pod_id: str = Field(..., alias="CONSUMER_POD_ID")
    delay: int = Field(..., alias="PROCESSING_DELAY_MS")
    error_threshold: int = Field(..., alias="CONSUMER_PROCESSING_THRESHOLD")


class AppSettings(ProjectBaseSettings):
    consumer_settings: KafkaConsumerSettings = KafkaConsumerSettings()
    postgres: PostgresSettings = PostgresSettings()


settings = AppSettings()
