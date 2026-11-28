import asyncio
import os
import random
from fastapi import FastAPI, HTTPException, status, Depends
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator, metrics

app = FastAPI()

POD_ID = os.getenv("POD_ID", "local")
DATACENTER = os.getenv("DATACENTER", "local")

instrumentator = Instrumentator(
    should_group_status_codes=False,
    excluded_handlers=["/health", "/metrics", "/handlers", ".*doc.*"],
)

instrumentator.add(
    metrics.default(custom_labels={"pod_id": POD_ID, "datacenter": DATACENTER})
)
instrumentator.instrument(app).expose(app, include_in_schema=False)

db: dict[int, dict] = {}
current_id = 1


class Item(BaseModel):
    name: str


async def simulate():
    if random.random() <= 0.1:
        await asyncio.sleep(random.random())
        raise HTTPException(status_code=random.randint(500, 504), detail="Server Error")
    if random.random() <= 0.1:
        await asyncio.sleep(random.random())
        raise HTTPException(status_code=random.randint(400, 404), detail="Client Error")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/handlers")
async def get_handlers():
    handlers = []
    for route in app.routes:
        if hasattr(route, "methods") and (
            route.path
            not in ["/health", "/metrics", "/handlers", "/openapi.json", "/redoc"]
            and not route.path.startswith("/docs")
        ):
            for method in route.methods:
                if method not in ["HEAD", "OPTIONS"]:
                    handlers.append({"method": method, "path": route.path})
    return {"handlers": handlers}


@app.post(
    "/items", status_code=status.HTTP_201_CREATED, dependencies=[Depends(simulate)]
)
async def create_item(item: Item):
    global current_id
    new_item = {"id": current_id, "name": item.name}
    db[current_id] = new_item
    current_id += 1
    return new_item


@app.get("/items/{id_}", dependencies=[Depends(simulate)])
async def get_item(id_: int):
    if id_ not in db:
        raise HTTPException(status_code=404, detail="Item not found")
    return db[id_]


@app.put("/items/{id_}", dependencies=[Depends(simulate)])
async def update_item(id_: int, item: Item):
    if id_ not in db:
        raise HTTPException(status_code=404, detail="Item not found")
    db[id_]["name"] = item.name
    return db[id_]
