"""Окружение alembic: URL берётся из SINK_DSN, метаданные — из models.py."""
import logging
import os

from alembic import context
from sqlalchemy import create_engine

from models import Base

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

target_metadata = Base.metadata


def run_migrations() -> None:
    engine = create_engine(os.environ["SINK_DSN"])
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations()
