from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import Settings
from metrics import Metrics, RequestMetricsMiddleware
from routes import build_observability_router, build_router, build_service_router, describe_handlers
from store import InMemoryStore


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or Settings()
    store = InMemoryStore(app_settings.max_items)

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        application.state.metrics.init_series()
        yield

    app = FastAPI(title="hw1-items-service", lifespan=lifespan)
    app.state.settings = app_settings
    app.state.store = store

    app.include_router(build_service_router())
    api_router = build_router(store, app_settings.failure_rate)
    app.include_router(api_router)

    handlers = describe_handlers(api_router)
    metrics = Metrics(pod_id=app_settings.pod_id, datacenter=app_settings.datacenter, handlers=handlers)
    app.state.metrics = metrics
    app.state.handlers = handlers

    app.include_router(build_observability_router(metrics, handlers))
    app.add_middleware(RequestMetricsMiddleware, metrics=metrics)
    return app
