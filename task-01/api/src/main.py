from fastapi import FastAPI

from prometheus import add_prometheus_middleware
from router import router


app = FastAPI()
app.include_router(router)

add_prometheus_middleware(app)
