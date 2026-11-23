import logging

from core_common.logger import setup_logger
from producer.core.config import settings


def get_logger(
    name: str = "app", level: str = settings.log_level
) -> logging.Logger:
    return setup_logger(name, level)
