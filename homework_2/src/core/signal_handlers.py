import asyncio
import logging


def sigterm_handler(event: asyncio.Event, logger: logging.Logger):
    logger.info("SIGTERM handler added.")

    def inner(signum, frame):
        logger.info("SIGTERM Received, stopping...")
        event.set()
        logger.info("Stopped by SIGTERM.")  ## delete?

    return inner


def sigint_handler(event: asyncio.Event, logger: logging.Logger):
    logger.info("SIGINT handler added.")

    def inner(signum, frame):
        logger.info("SIGINT Received, exiting...")
        event.set()
        logger.info("Stopped by SIGINT.")  ## delete?

    return inner
