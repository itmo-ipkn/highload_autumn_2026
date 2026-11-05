"""NetInventory: a device registry with three monitored REST endpoints."""
import asyncio
import os
import random

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.inventory import AddressConflict, DeviceInput, DeviceNotFound, DeviceRecord, DeviceRegistry

from app.monitoring import DATACENTER, POD_ID, MonitoredRoute, registered_handlers

app = FastAPI(title="NetInventory", docs_url=None, redoc_url=None, openapi_url=None)
registry = DeviceRegistry()


async def demo_behavior(request: Request):
    # A small synthetic delay makes p50/p95/p99 visible in this teaching lab.
    await asyncio.sleep(random.uniform(.005, .05))
    if random.random() < .04:
        await asyncio.sleep(.1)
    if os.getenv("DEMO_FAULTS", "true").lower() == "true":
        if request.headers.get("x-demo-error") == "500":
            raise HTTPException(500, "Intentional demo fault")


api = APIRouter(route_class=MonitoredRoute, dependencies=[Depends(demo_behavior)])


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
    return {"status": "ok", "service": "netinventory", "pod_id": POD_ID, "datacenter": DATACENTER}


@app.get("/handlers")
async def handlers():
    return {"handlers": registered_handlers(app)}


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})
