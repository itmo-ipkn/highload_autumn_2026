from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis
from starlette.middleware.base import BaseHTTPMiddleware

from api.v1 import handlers, health, items, metrics
from core import redis
from core.config import settings
from core.logger import get_logger
from middlewares.disorder_middleware import DisorderMiddleware
from middlewares.prometheus_middleware import MetricsMiddleware

logger = get_logger(settings.project.name)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Service starting...")
    redis.redis = Redis(
        host=settings.redis.host,
        port=settings.redis.port,
        password=settings.redis.password,
    )
    logger.info(
        f"Service started with DISORDER={settings.project.error_mode}."
    )

    yield

    logger.info("Service stopping...")
    await redis.redis.close()
    logger.info("Service stopped.")


app = FastAPI(
    title=settings.project.name,
    lifespan=lifespan,
)

app.add_middleware(BaseHTTPMiddleware, dispatch=MetricsMiddleware())

if settings.project.error_mode:
    app.add_middleware(
        BaseHTTPMiddleware,
        dispatch=DisorderMiddleware(error_rate=settings.project.error_rate),
    )

app.include_router(health.router)
app.include_router(metrics.router)
app.include_router(handlers.router)

app.include_router(items.router)
