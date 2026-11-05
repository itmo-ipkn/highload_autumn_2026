#!/usr/bin/env python3
"""Генерирует дашборд Grafana по списку ручек из GET /handlers."""

import base64
import json
import os
import sys
import urllib.error
import urllib.request

DASHBOARD_UID = "items-service"
DASHBOARD_TITLE = "Items service"
PANEL_HEIGHT = 8
PANEL_WIDTH = 8  # три панели в ряд на сетке из 24 колонок


def env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"fatal: environment variable {name} is not set", file=sys.stderr)
        raise SystemExit(1)
    return value


def http_json(url: str, method: str = "GET", body=None, auth=None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if auth is not None:
        token = base64.b64encode(f"{auth[0]}:{auth[1]}".encode()).decode()
        headers["Authorization"] = f"Basic {token}"

    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = response.read().decode()
    except urllib.error.HTTPError as error:
        print(f"fatal: {method} {url} -> {error.code}", file=sys.stderr)
        print(error.read().decode(), file=sys.stderr)
        raise SystemExit(1)
    except urllib.error.URLError as error:
        print(f"fatal: cannot reach {url}: {error.reason}", file=sys.stderr)
        raise SystemExit(1)

    return json.loads(payload) if payload else {}


def fetch_handlers(app_url: str):
    payload = http_json(f"{app_url}/handlers")
    handlers = payload.get("handlers") or []
    if not handlers:
        print("fatal: /handlers returned no handlers", file=sys.stderr)
        raise SystemExit(1)
    return handlers


def build_templating(datasource_uid: str):
    def label_variable(name: str):
        return {
            "name": name,
            "type": "query",
            "datasource": {"type": "prometheus", "uid": datasource_uid},
            "query": {
                "query": f"label_values(http_requests_total, {name})",
                "refId": name,
            },
            "multi": True,
            "includeAll": True,
            "allValue": ".*",
            "current": {"text": "All", "value": "$__all"},
            "refresh": 1,
        }

    return {"list": [label_variable("datacenter"), label_variable("pod")]}


def latency_panel(handler, datasource_uid, x, y):
    route = handler["route"]

    def expr(quantile: float) -> str:
        return (
            f"histogram_quantile({quantile}, sum by (le) ("
            f"rate(http_request_duration_seconds_bucket{{"
            f'route="{route}",datacenter=~"$datacenter",pod=~"$pod"}}'
            f"[$__rate_interval])))"
        )

    return {
        "type": "timeseries",
        "title": f'{handler["method"]} {route} — latency',
        "datasource": {"type": "prometheus", "uid": datasource_uid},
        "gridPos": {"h": PANEL_HEIGHT, "w": PANEL_WIDTH, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": "s", "min": 0}},
        "targets": [
            {"expr": expr(0.50), "legendFormat": "p50", "refId": "A"},
            {"expr": expr(0.95), "legendFormat": "p95", "refId": "B"},
            {"expr": expr(0.99), "legendFormat": "p99", "refId": "C"},
        ],
    }


def rps_panel(handler, datasource_uid, x, y):
    route = handler["route"]
    expression = (
        f"sum(rate(http_requests_total{{"
        f'route="{route}",datacenter=~"$datacenter",pod=~"$pod"}}'
        f"[$__rate_interval]))"
    )
    return {
        "type": "timeseries",
        "title": f'{handler["method"]} {route} — RPS',
        "datasource": {"type": "prometheus", "uid": datasource_uid},
        "gridPos": {"h": PANEL_HEIGHT, "w": PANEL_WIDTH, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": "reqps", "min": 0}},
        "targets": [
            {
                "expr": expression,
                "legendFormat": f'{handler["method"]} {route}',
                "refId": "A",
            }
        ],
    }


def errors_panel(handler, datasource_uid, x, y):
    route = handler["route"]
    expression = (
        f"sum by (status) (rate(http_requests_total{{"
        f'route="{route}",datacenter=~"$datacenter",pod=~"$pod",'
        f'status=~"4..|5.."}}[$__rate_interval]))'
    )
    return {
        "type": "timeseries",
        "title": f'{handler["method"]} {route} — errors',
        "datasource": {"type": "prometheus", "uid": datasource_uid},
        "gridPos": {"h": PANEL_HEIGHT, "w": PANEL_WIDTH, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": "reqps", "min": 0}},
        "targets": [
            {"expr": expression, "legendFormat": "{{status}}", "refId": "A"}
        ],
    }


def build_panels(handlers, datasource_uid):
    panels = []
    y = 0
    for handler in handlers:
        panels.append(latency_panel(handler, datasource_uid, 0, y))
        panels.append(rps_panel(handler, datasource_uid, PANEL_WIDTH, y))
        panels.append(errors_panel(handler, datasource_uid, PANEL_WIDTH * 2, y))
        y += PANEL_HEIGHT
    return panels


def main() -> int:
    app_url = env("APP_URL")
    grafana_url = env("GRAFANA_URL")
    grafana_user = env("GRAFANA_USER")
    grafana_password = env("GRAFANA_PASSWORD")
    datasource_uid = env("PROMETHEUS_DS_UID")

    handlers = fetch_handlers(app_url)
    print(f"discovered {len(handlers)} handler(s):")
    for handler in handlers:
        print(f'  {handler["method"]:<5} {handler["route"]}')

    panels = build_panels(handlers, datasource_uid)
    print(f"built {len(panels)} panel(s)")

    dashboard = {
        "uid": DASHBOARD_UID,
        "title": DASHBOARD_TITLE,
        "editable": False,
        "schemaVersion": 39,
        "version": 0,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "panels": panels,
        "templating": build_templating(datasource_uid),
    }

    result = http_json(
        f"{grafana_url}/api/dashboards/db",
        method="POST",
        body={"dashboard": dashboard, "overwrite": True},
        auth=(grafana_user, grafana_password),
    )
    print(f'dashboard saved: uid={result.get("uid")} url={result.get("url")}')
    return 0


if __name__ == "__main__":
    sys.exit(main())