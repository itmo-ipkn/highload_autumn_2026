"""
https://habr.com/ru/articles/959430/
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Histogram,
    multiprocess,
)

registry = CollectorRegistry()
multiprocess.MultiProcessCollector(registry)


http_requests_total = Counter(
    "http_requests_total",
    "Total number of HTTP requests",
    ["pod_id", "datacenter", "method", "endpoint", "status_code"],
    registry=registry,
)


http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["pod_id", "datacenter", "method", "endpoint"],
    registry=registry,
)


http_errors_4xx_total = Counter(
    "http_errors_4xx_total",
    "Total number of 4xx HTTP errors",
    ["pod_id", "datacenter", "method", "endpoint", "status_code"],
    registry=registry,
)


http_errors_5xx_total = Counter(
    "http_errors_5xx_total",
    "Total number of 5xx HTTP errors",
    ["pod_id", "datacenter", "method", "endpoint", "status_code"],
    registry=registry,
)
