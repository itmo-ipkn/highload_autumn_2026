"""NetInventory device API, before metrics are introduced."""
from fastapi import APIRouter, FastAPI, HTTPException

from app.inventory import AddressConflict, DeviceInput, DeviceNotFound, DeviceRecord, DeviceRegistry

app = FastAPI(title="NetInventory", docs_url=None, redoc_url=None, openapi_url=None)
registry = DeviceRegistry()
api = APIRouter()


@api.post("/items", status_code=201, response_model=DeviceRecord)
async def register_device(body: DeviceInput):
    try:
        return registry.create(body)
    except AddressConflict as error:
        raise HTTPException(409, str(error)) from error


@api.get("/items/{id}", response_model=DeviceRecord)
async def read_device(id: str):
    try:
        return registry.get(id)
    except DeviceNotFound as error:
        raise HTTPException(404, str(error)) from error


@api.put("/items/{id}", response_model=DeviceRecord)
async def revise_device(id: str, body: DeviceInput):
    try:
        return registry.replace(id, body)
    except DeviceNotFound as error:
        raise HTTPException(404, str(error)) from error
    except AddressConflict as error:
        raise HTTPException(409, str(error)) from error


app.include_router(api)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "netinventory"}
