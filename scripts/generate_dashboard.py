#!/usr/bin/env python3
"""Generate one non-editable Grafana dashboard from the service's live handler catalog."""

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

APP_URL = os.environ.get("APP_URL", "http://localhost:8080").rstrip("/")
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://localhost:3000").rstrip("/")
GRAFANA_USER = os.environ.get("GRAFANA_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_PASSWORD", "")
DATASOURCE_UID = os.environ.get("PROMETHEUS_DS_UID", "prometheus")
DASHBOARD_UID = "items-service-java"


def request_json(url: str, *, method: str = "GET", payload=None, auth: bool = False):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {"Accept": "application/json"}
    if data is not None:
        headers["Content-Type"] = "application/json"
    if auth:
        token = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=10) as response:
        body = response.read()
        if not body:
            return {}
        return json.loads(body.decode("utf-8"))


def get_handlers():
    last_error = None
    for attempt in range(1, 13):
        try:
            response = request_json(f"{APP_URL}/handlers")
            handlers = response if isinstance(response, list) else response.get("handlers", [])
            if not handlers:
                raise RuntimeError("GET /handlers returned an empty handler list")
            return handlers
        except Exception as exc:  # healthcheck usually prevents this; retry transient DNS/startup errors
            last_error = exc
            print(f"Waiting for handler catalog ({attempt}/12): {exc}", flush=True)
            time.sleep(2)
    raise RuntimeError(f"Could not read handler catalog from {APP_URL}: {last_error}")


def datasource():
    return {"type": "prometheus", "uid": DATASOURCE_UID}


def prom_filter(handler):
    method = json.dumps(handler["method"])
    route = json.dumps(handler["route"])
    return (
        f'datacenter=~"$datacenter",pod=~"$pod",'
        f'method={method},route={route}'
    )


def target(ref_id, expression, legend):
    return {
        "refId": ref_id,
        "expr": expression,
        "legendFormat": legend,
        "range": True,
        "instant": False,
        "format": "time_series",
    }


def panel(panel_id, title, panel_type, targets, unit, x, y, width=8, height=8):
    return {
        "id": panel_id,
        "title": title,
        "type": panel_type,
        "datasource": datasource(),
        "gridPos": {"h": height, "w": width, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {"unit": unit, "min": 0, "color": {"mode": "palette-classic"}},
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def query_variables():
    return [
        {
            "name": "datacenter",
            "label": "Datacenter",
            "type": "query",
            "datasource": datasource(),
            "query": "label_values(http_requests_total, datacenter)",
            "definition": "label_values(http_requests_total, datacenter)",
            "refresh": 1,
            "multi": True,
            "includeAll": True,
            "allValue": ".*",
            "current": {"selected": True, "text": ["$__all"], "value": ["$__all"]},
            "options": [],
            "sort": 1,
        },
        {
            "name": "pod",
            "label": "Pod",
            "type": "query",
            "datasource": datasource(),
            "query": "label_values(http_requests_total, pod)",
            "definition": "label_values(http_requests_total, pod)",
            "refresh": 1,
            "multi": True,
            "includeAll": True,
            "allValue": ".*",
            "current": {"selected": True, "text": ["$__all"], "value": ["$__all"]},
            "options": [],
            "sort": 1,
        },
    ]


def build_dashboard(handlers):
    panels = []
    next_id = 1
    y = 0

    for handler in handlers:
        method = str(handler["method"]).upper()
        route = str(handler["route"])
        filters = prom_filter({"method": method, "route": route})
        selector = "{" + filters + "}"
        prefix = f"{method} {route}"

        latency_targets = [
            target("A", f"histogram_quantile(0.50, sum by (le) (rate(http_server_request_duration_seconds_bucket{selector}[$__rate_interval])))", "p50"),
            target("B", f"histogram_quantile(0.95, sum by (le) (rate(http_server_request_duration_seconds_bucket{selector}[$__rate_interval])))", "p95"),
            target("C", f"histogram_quantile(0.99, sum by (le) (rate(http_server_request_duration_seconds_bucket{selector}[$__rate_interval])))", "p99"),
        ]
        panels.append(panel(next_id, f"{prefix} — Latency", "timeseries", latency_targets, "s", 0, y, 8))
        next_id += 1

        rps_expr = f"sum(rate(http_requests_total{selector}[$__rate_interval]))"
        panels.append(panel(next_id, f"{prefix} — RPS", "timeseries", [target("A", rps_expr, "requests/sec")], "reqps", 8, y, 8))
        next_id += 1

        error_selector = "{" + filters + ',status=~"4..|5.."}'
        errors_expr = f"sum by (status) (rate(http_requests_total{error_selector}[$__rate_interval]))"
        panels.append(panel(next_id, f"{prefix} — Errors by HTTP status", "timeseries", [target("A", errors_expr, "{{status}}")], "reqps", 16, y, 8))
        next_id += 1
        y += 8

    return {
        "id": None,
        "uid": DASHBOARD_UID,
        "title": "Items Service — Golden Signals (Java)",
        "tags": ["highload", "items-service", "java", "generated"],
        "timezone": "browser",
        "schemaVersion": 41,
        "version": 0,
        "editable": False,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        "templating": {"list": query_variables()},
        "panels": panels,
    }


def main():
    handlers = get_handlers()
    print("Discovered handlers:", ", ".join(f"{h['method']} {h['route']}" for h in handlers), flush=True)
    dashboard = build_dashboard(handlers)
    payload = {"dashboard": dashboard, "overwrite": True, "message": "Generated from GET /handlers"}

    last_error = None
    for attempt in range(1, 13):
        try:
            result = request_json(f"{GRAFANA_URL}/api/dashboards/db", method="POST", payload=payload, auth=True)
            print(f"Dashboard saved: {GRAFANA_URL}{result.get('url', '/d/items-service-java')}", flush=True)
            return
        except urllib.error.HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            last_error = RuntimeError(f"Grafana API returned HTTP {exc.code}: {details}")
        except Exception as exc:
            last_error = exc
        print(f"Dashboard write attempt {attempt}/12 failed: {last_error}", flush=True)
        time.sleep(2)
    raise RuntimeError(f"Could not save dashboard: {last_error}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr, flush=True)
        sys.exit(1)
