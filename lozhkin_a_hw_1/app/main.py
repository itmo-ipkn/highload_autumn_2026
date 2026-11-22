"""Микросервис CRUD /items с экспортом метрик Prometheus.

Метрики:
  http_requests_total{pod_id, datacenter, handler, method, code}      — Counter (throughput, errors)
  http_request_duration_seconds{pod_id, datacenter, handler, method}  — Histogram (latency)
"""
import os
import random
import re
import time

from flask import Flask, Response, abort, jsonify, request
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from models import InMemoryStore, NotFoundError

app = Flask(__name__)
store = InMemoryStore()

POD_ID = os.environ.get("POD_ID", "unknown-pod")
DATACENTER = os.environ.get("DATACENTER", "unknown-dc")
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0.0"))

SERVICE_HANDLERS = {"/health", "/metrics", "/handlers"}

REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Количество HTTP-ответов по ручкам",
    ["pod_id", "datacenter", "handler", "method", "code"],
)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "Latency HTTP-запросов по ручкам",
    ["pod_id", "datacenter", "handler", "method"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


def handler_label(rule: str) -> str:
    """Превращает правило Flask в шаблон пути: /items/<item_id> -> /items/{id}."""
    return re.sub(r"<(.+?)>", r"{\1}", rule)


def maybe_fail():
    """Искусственные сбои: с вероятностью FAIL_RATE ответ 500 (для проверки errors)."""
    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        abort(500)


@app.before_request
def _start_timer():
    request._start_ts = time.perf_counter()


@app.after_request
def _record_metrics(response):
    rule = request.url_rule
    handler = handler_label(rule.rule) if rule is not None else "unmatched"
    if handler in SERVICE_HANDLERS:
        return response
    labels = {
        "pod_id": POD_ID,
        "datacenter": DATACENTER,
        "handler": handler,
        "method": request.method,
        "code": str(response.status_code),
    }
    REQUESTS_TOTAL.labels(**labels).inc()
    if rule is not None:
        LATENCY.labels(
            pod_id=POD_ID, datacenter=DATACENTER,
            handler=handler, method=request.method,
        ).observe(time.perf_counter() - request._start_ts)
    return response


@app.route("/items", methods=["POST"])
def create_item():
    maybe_fail()
    payload = request.get_json(silent=True) or {}
    return jsonify(store.create(payload)), 201


@app.route("/items/<int:item_id>", methods=["GET"])
def get_item(item_id):
    maybe_fail()
    try:
        return jsonify(store.get(item_id))
    except NotFoundError:
        abort(404)


@app.route("/items/<int:item_id>", methods=["PUT"])
def update_item(item_id):
    maybe_fail()
    payload = request.get_json(silent=True) or {}
    try:
        return jsonify(store.update(item_id, payload))
    except NotFoundError:
        abort(404)


@app.route("/health")
def health():
    return jsonify({"status": "ok", "pod_id": POD_ID, "datacenter": DATACENTER}), 200


@app.route("/handlers")
def handlers():
    """Список бизнес-ручек для генератора дашборда."""
    result = []
    for rule in app.url_map.iter_rules():
        if rule.endpoint == "static" or rule.rule in SERVICE_HANDLERS:
            continue
        for method in sorted(rule.methods - {"HEAD", "OPTIONS"}):
            result.append({"method": method, "path": handler_label(rule.rule)})
    result.sort(key=lambda h: (h["path"], h["method"]))
    return jsonify({"handlers": result})


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True, use_reloader=False)
