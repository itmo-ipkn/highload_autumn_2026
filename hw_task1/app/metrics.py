import os

from prometheus_client import Counter, Histogram


POD_ID = os.getenv("POD_ID", "local")
DATACENTER = os.getenv("DATACENTER", "local")


REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "handler", "pod_id", "datacenter"],
    buckets=(
        0.005, 0.01, 0.025, 0.05, 0.1,
        0.25, 0.5, 1.0, 2.5, 5.0, 10.0
    ),
)

REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["method", "handler", "status_code", "pod_id", "datacenter"],
)
