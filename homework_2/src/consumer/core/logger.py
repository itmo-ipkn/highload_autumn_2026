import logging

from consumer.core.config import settings
from core_common.logger import setup_logger


def get_logger(
    name: str = "app", level: str = settings.log_level
) -> logging.Logger:
    return setup_logger(name, level)
