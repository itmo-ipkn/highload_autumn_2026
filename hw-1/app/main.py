"""Three business endpoints plus readiness, metrics and route discovery."""

from random import Random

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Response, status
from prometheus_client import CONTENT_TYPE_LATEST

from app.config import Settings
from app.demo import simulate_backend
from app.models import Health, Item, ItemPayload
from app.store import InMemoryItemStore
from app.telemetry import HttpMetricsMiddleware, Telemetry, business_handlers


def create_app(settings: Settings | None = None) -> FastAPI:
    # Read runtime identity once. Each app owns its own memory and settings.
    settings = settings or Settings.from_environment()
    store = InMemoryItemStore()
    application = FastAPI(
        title="TASK1 Items",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        redirect_slashes=False,
    )
    application.state.settings = settings
    application.state.store = store
    application.state.random = Random()
    telemetry = Telemetry(settings.pod_id, settings.datacenter)
    application.state.telemetry = telemetry
    application.add_middleware(HttpMetricsMiddleware, telemetry=telemetry)
    business = APIRouter(dependencies=[Depends(simulate_backend)])

    @business.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
    def create_item(payload: ItemPayload) -> Item:
        return store.create(payload)

    @business.get("/items/{id}", response_model=Item)
    def get_item(id: str) -> Item:
        item = store.get(id)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
        return item

    @business.put("/items/{id}", response_model=Item)
    def replace_item(id: str, payload: ItemPayload) -> Item:
        item = store.replace(id, payload)
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Item not found")
        return item

    application.include_router(business)

    @application.get("/health", response_model=Health, include_in_schema=False)
    def health() -> Health:
        return Health(pod_id=settings.pod_id, datacenter=settings.datacenter)

    @application.get("/metrics", include_in_schema=False)
    def metrics() -> Response:
        return Response(content=telemetry.render(), media_type=CONTENT_TYPE_LATEST)

    @application.get("/handlers", include_in_schema=False)
    def handlers() -> dict:
        return {"service": "items-api", "handlers": business_handlers(application)}

    telemetry.register_handlers(business_handlers(application))
    return application


app = create_app()
