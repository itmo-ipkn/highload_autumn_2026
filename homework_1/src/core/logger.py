import logging

from core.config import settings


def get_logger(name: str = "app") -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(settings.project.log_level)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(settings.project.log_level)
    stream_handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger
