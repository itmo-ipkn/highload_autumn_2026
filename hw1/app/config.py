from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False, extra="ignore")

    pod_id: str = Field(min_length=1)
    datacenter: str = Field(min_length=1)
    failure_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    max_items: int = Field(default=10_000, ge=1)
