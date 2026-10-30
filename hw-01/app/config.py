from pydantic import Field
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    POD_ID: str = "unknown"
    DATACENTER: str = "unknown"
    FAIL_RATE: float = Field(default=0.0, ge=0.0, le=1.0)
    # Средняя искусственная задержка бизнес-ручек, мс
    # (экспоненциальное распределение; 0 — выключено).
    DELAY_MEAN_MS: float = Field(default=0.0, ge=0.0)


settings = Settings()
