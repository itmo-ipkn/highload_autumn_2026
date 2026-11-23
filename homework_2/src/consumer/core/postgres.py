from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from consumer.core.config import settings

DATABASE_URL_TEMPLATE = (
    "{dialect}://{user}:{password}@{host}:{port}/{database}"
)
DATABASE_URL = DATABASE_URL_TEMPLATE.format(
    dialect=settings.postgres.dialect,
    user=settings.postgres.user,
    password=settings.postgres.password,
    host=settings.postgres.host,
    port=settings.postgres.port,
    database=settings.postgres.database,
)

asyncio_engine = create_async_engine(DATABASE_URL, echo=False)
async_session_local: AsyncSession = async_sessionmaker(
    bind=asyncio_engine, expire_on_commit=False
)


class Base(DeclarativeBase):
    pass
