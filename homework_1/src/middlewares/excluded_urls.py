# Reason: docs url mounted with Mount, not APIRouter
# this list is rarely changes
PROMETHEUS_EXCLUDED_ENDPOINTS_URLS: list[str] = [
    "/openapi.json",
    "/docs",
    "/redoc",
    "/docs/oauth2-redirect",
    "/health",
    "/metrics",
    "/handlers",
]
