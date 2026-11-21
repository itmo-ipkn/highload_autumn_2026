import asyncio

from core.logger import get_logger

logger = get_logger()


def sigterm_handler(event: asyncio.Event):
    logger.info("SIGTERM handler added.")

    def inner(signum, frame):
        logger.info("SIGTERM Received, stopping...")
        event.set()
        logger.info("Stopped by SIGTERM.")

    return inner


def sigint_handler(event: asyncio.Event):
    logger.info("SIGINT handler added.")

    def inner(signum, frame):
        logger.info("SIGINT Received, exiting...")
        event.set()
        logger.info("Stopped by SIGINT.")

    return inner
