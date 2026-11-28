from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    BOOTSTRAP_SERVERS: str


class ProducerSettings(KafkaSettings):
    POD_ID: str
    TOPIC: str = "events"
    # Событий в секунду на один под
    EVENTS_PER_SEC: float = Field(default=20, gt=0)
    # Сколько пользователей ведёт один под
    USERS_PER_PRODUCER: int = Field(default=20, ge=1)
    # Доля ядовитых событий
    POISON_RATE: float = Field(default=0.02, ge=0.0, le=1.0)
    # none нужен для эксперимента 4.3
    KEY_MODE: Literal["user_id", "none"] = "user_id"
    # Сколько ждать досылки буфера по SIGTERM, с
    FLUSH_TIMEOUT_S: float = Field(default=25, gt=0)


class ConsumerSettings(KafkaSettings):
    POD_ID: str
    TOPIC: str = "events"
    DLT_TOPIC: str = "events.dlt"
    GROUP_ID: str = "events-processor"
    SINK_DSN: str
    # earliest: если у группы нет закоммиченного оффсета (первый запуск, оффсеты протухли),
    # читаем лог с начала — иначе всё, что продюсеры записали до старта группы, будет пропущено
    # и в seq появятся дыры.
    AUTO_OFFSET_RESET: Literal["earliest", "latest"] = "earliest"
    # Эмуляция работы, мс
    PROCESSING_DELAY_MS: int = Field(default=20, ge=0)
    # Попыток обработки до отправки в DLT
    MAX_ATTEMPTS: int = Field(default=3, ge=1)
    # Пауза между попытками = RETRY_BACKOFF_MS * номер попытки
    RETRY_BACKOFF_MS: int = Field(default=100, ge=0)
    # Коммит раз в COMMIT_BATCH сообщений или раз в COMMIT_INTERVAL_S секунд
    COMMIT_BATCH: int = Field(default=100, ge=1)
    COMMIT_INTERVAL_S: float = Field(default=1, gt=0)
    # Сколько ждать подтверждения доставки в DLT, с
    DLT_FLUSH_TIMEOUT_S: float = Field(default=30, gt=0)


class VerifySettings(KafkaSettings):
    DLT_TOPIC: str = "events.dlt"
    SINK_DSN: str
    GITHUB_LOGIN: str = "unknown"
