import logging

from core.config_producer import settings
from core.logger import setup_logger


def get_logger(
    name: str = "app", level: str = settings.log_level
) -> logging.Logger:
    return setup_logger(name, level)
