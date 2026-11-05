import os
import time
from prometheus_client import Counter, Histogram


DATACENTER = os.environ["DATACENTER"]
POD_ID = os.environ["POD_ID"]

prometheus_label_names = [
    "method",
    "endpoint",
    "status_code",
    "datacenter",
    "pod_id",
]

prometheus_throughput = Counter(
    name="requests_total",
    documentation="Total requests",
    labelnames=prometheus_label_names,
)

prometheus_errors = Counter(
    name="errors_total",
    documentation="Total errors",
    labelnames=prometheus_label_names,
)

prometheus_latency = Histogram(
    name="request_duration_seconds",
    documentation="Request latency in seconds",
    labelnames=prometheus_label_names,
)

def add_prometheus_middleware(app):
    @app.middleware("http")
    async def write_prometheus_metrics(request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        route = request.get("route")

        if route is not None:
            endpoint = route.path

            if endpoint not in ("/metrics", "/health"):
                prometheus_labels = dict(
                    method=request.method,
                    endpoint=endpoint,
                    status_code=response.status_code,
                    datacenter=DATACENTER,
                    pod_id=POD_ID,
                )

                prometheus_throughput.labels(**prometheus_labels).inc()
                prometheus_latency.labels(**prometheus_labels).observe(duration)

                if response.status_code >= 400:
                    prometheus_errors.labels(**prometheus_labels).inc()

        return response
