from pydantic import Field
from pydantic_settings import BaseSettings

from core_common.config import ProjectBaseSettings


class KafkaConsumerSettings(BaseSettings):
    group_id: str = Field(..., alias="CONSUMER_GROUP_ID")
    pod_id: str = Field(..., alias="CONSUMER_POD_ID")
    delay: int = Field(..., alias="PROCESSING_DELAY_MS")
    error_threshold: int = Field(..., alias="CONSUMER_PROCESSING_THRESHOLD")


class PostgresSettings(BaseSettings):
    dialect: str = Field(..., alias="POSTGRES_DIALECT")
    user: str = Field(..., alias="POSTGRES_USER")
    password: str = Field(..., alias="POSTGRES_PASSWORD")
    host: str = Field(..., alias="POSTGRES_HOST")
    port: str = Field(..., alias="POSTGRES_PORT")
    database: str = Field(..., alias="POSTGRES_DB")


class AppSettings(ProjectBaseSettings):
    consumer_settings: KafkaConsumerSettings = KafkaConsumerSettings()
    postgres: PostgresSettings = PostgresSettings()


settings = AppSettings()
