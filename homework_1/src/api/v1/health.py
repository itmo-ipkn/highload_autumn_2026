from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health", name="health_endpoint")
async def health_check():
    return JSONResponse(
        status_code=status.HTTP_200_OK, content={"status": "ok"}
    )
