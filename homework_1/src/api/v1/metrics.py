from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from core.prometheus_metrics import registry

router = APIRouter(tags=["metrics"])


@router.get("/metrics", name="metrics_endpoint")
async def health_check():
    return PlainTextResponse(
        generate_latest(registry), media_type=CONTENT_TYPE_LATEST
    )
