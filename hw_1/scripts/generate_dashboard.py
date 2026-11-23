import json
import os
import sys
import time
from urllib.parse import urlsplit

import httpx

DATASOURCE_UID = "prometheus-main"
DASHBOARD_UID = "items-http-metrics"
EXCLUDED = frozenset(("/health", "/metrics", "/handlers"))


def prom_selector(method, route):
    """Безопасно кодируйте значения константных меток и интерполируйте переменные множественного выбора."""
    return (
        f'method={json.dumps(method)},route={json.dumps(route)},'
        'datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"'
    )


def prom_target(expr, legend, ref_id):
    return {
        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
        "expr": expr,
        "legendFormat": legend,
        "refId": ref_id,
        "range": True,
        "format": "time_series",
    }


def make_panel(panel_id, title, targets, unit, x, y):
    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
        "gridPos": {"h": 9, "w": 8, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min": 0,
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "linear",
                    "showPoints": "never",
                    "lineWidth": 2,
                    "fillOpacity": 10,
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def variable(name, label, query):
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
        "definition": query,
        "query": query,
        "regex": "",
        "refresh": 1,
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"text": "All", "value": "$__all", "selected": True},
        "options": [],
        "hide": 0,
    }


def build_dashboard(handlers):
    if not isinstance(handlers, list) or not handlers:
        raise ValueError("/handlers must return a non-empty JSON list")

    normalized = set()
    for handler in handlers:
        if not isinstance(handler, dict):
            raise ValueError("Invalid handler descriptor")
        method, route = handler.get("method"), handler.get("route")
        if not isinstance(method, str) or not method.isalpha():
            raise ValueError("Invalid handler HTTP method")
        if not isinstance(route, str) or not route.startswith("/"):
            raise ValueError("Invalid route template")
        if route not in EXCLUDED:
            normalized.add((method.upper(), route))

    if not normalized:
        raise ValueError("No business handlers returned")

    panels = []
    for index, (method, route) in enumerate(sorted(normalized, key=lambda x: (x[1], x[0]))):
        label_matchers = prom_selector(method, route)
        bucket_rate = (
            "sum by (le) (rate(http_request_duration_seconds_bucket"
            f"{{{label_matchers}}}[$__rate_interval]))"
        )
        latency_targets = [
            prom_target(f"histogram_quantile({p}, {bucket_rate})", legend, letter)
            for p, legend, letter in (
                (0.50, "p50", "A"),
                (0.95, "p95", "B"),
                (0.99, "p99", "C"),
            )
        ]
        rps_expr = f"sum(rate(http_requests_total{{{label_matchers}}}[$__rate_interval]))"
        errors_expr = (
            "sum by (status_code) (rate(http_requests_total"
            f'{{{label_matchers},status_code=~"4..|5.."}}[$__rate_interval]))'
        )
        y = index * 9
        title = f"{method} {route}"
        panels.extend(
            (
                make_panel(index * 3 + 1, f"{title} — Latency", latency_targets, "s", 0, y),
                make_panel(index * 3 + 2, f"{title} — RPS", [prom_target(rps_expr, "RPS", "A")], "reqps", 8, y),
                make_panel(index * 3 + 3, f"{title} — Errors by code", [prom_target(errors_expr, "{{status_code}}", "A")], "reqps", 16, y),
            )
        )

    return {
        "id": None,
        "uid": DASHBOARD_UID,
        "title": "Items service — Golden Signals",
        "tags": ["highload", "devops", "auto-generated"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 0,
        "editable": False,
        "graphTooltip": 1,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {
            "list": [
                variable("datacenter", "Datacenter", "label_values(http_requests_total, datacenter)"),
                variable("pod_id", "Pod", "label_values(http_requests_total, pod_id)"),
            ]
        },
        "panels": panels,
    }


def run():
    app_url = os.environ.get("APP_URL", "http://app-dc1:8000").rstrip("/")
    grafana_url = os.environ.get("GRAFANA_URL", "http://grafana:3000").rstrip("/")
    user = os.environ["GRAFANA_ADMIN_USER"]
    password = os.environ["GRAFANA_ADMIN_PASSWORD"]

    with httpx.Client(timeout=10, auth=(user, password)) as client:
        response = client.get(app_url + "/handlers")
        response.raise_for_status()
        dashboard = build_dashboard(response.json())

        payload = {"dashboard": dashboard, "overwrite": True, "message": "Auto-generated from /handlers"}
        for attempt in range(1, 11):
            try:
                result = client.post(grafana_url + "/api/dashboards/db", json=payload)
                result.raise_for_status()
                body = result.json()
                print("Dashboard created/updated:", body.get("url", "/d/" + DASHBOARD_UID), flush=True)
                print("Business handlers:", len(dashboard["panels"]) // 3, flush=True)
                return
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (400, 401, 403):
                    raise
                if attempt == 10:
                    raise
                print(f"Grafana API attempt {attempt}/10 failed: {exc}", file=sys.stderr, flush=True)
                time.sleep(2)


if __name__ == "__main__":
    run()
