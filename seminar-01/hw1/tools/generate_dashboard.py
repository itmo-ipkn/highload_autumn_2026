"""CLI dashboard generator (raw Grafana REST API, no SDK).

Reads the live handler list from the running app via GET /handlers, builds a
non-editable dashboard with one panel group per handler (latency percentiles,
RPS, errors by code), and publishes it through the Grafana HTTP API using admin
credentials from environment variables.

Run as a one-shot job in compose after app + Grafana health checks.
"""
from __future__ import annotations
import os
import time
import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
APP_URL = os.environ.get("APP_URL", "http://app1:8090")
ADMIN_USER = os.environ.get("GRAFANA_ADMIN_USER", "admin")
ADMIN_PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "")
DATASOURCE_UID = os.environ.get("DATASOURCE_UID", "hw1-prom")
DS = {"type": "prometheus", "uid": DATASOURCE_UID}
RATE = "5m"

def fetch_handlers() -> list[dict]:
    last = None
    for _ in range(20):
        try:
            resp = requests.get(f"{APP_URL}/handlers", timeout=5)
            if resp.status_code == 200:
                handlers = resp.json().get("handlers")
                if handlers:
                    return handlers
            last = resp.status_code
        except requests.RequestException as exc:
            last = str(exc)
        time.sleep(2)
    raise SystemExit(f"Could not read handler list from {APP_URL}/handlers: {last}")

def base_labels(method: str, path: str) -> str:
    # multi/All variables expand as regex, so =~ is used; "All" becomes .*
    return f'method="{method}", handler="{path}", datacenter=~"$datacenter", pod=~"$pod"'

def build_panels(handlers: list[dict]) -> list[dict]:
    panels: list[dict] = []
    y = 0
    panel_id = 1
    for handler in handlers:
        method, path = handler["method"], handler["path"]
        labels = base_labels(method, path)
        panels.append({
            "type": "row",
            "title": f"{method} {path}",
            "panelId": panel_id,
            "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
        })
        panel_id += 1
        y += 1

        latency = {
            "type": "timeseries",
            "title": f"{method} {path} - Latency p50 / p95 / p99",
            "datasource": DS,
            "panelId": panel_id,
            "gridPos": {"h": 8, "w": 8, "x": 0, "y": y},
            "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
            "targets": [
                {"refId": "A", "legendFormat": "p50",
                 "expr": f'histogram_quantile(0.50, sum by (le) (rate(http_request_duration_seconds_bucket{{{labels}}}[{RATE}])))'},
                {"refId": "B", "legendFormat": "p95",
                 "expr": f'histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{{{labels}}}[{RATE}])))'},
                {"refId": "C", "legendFormat": "p99",
                 "expr": f'histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{{{labels}}}[{RATE}])))'},
            ],
        }
        panel_id += 1

        rps = {
            "type": "timeseries",
            "title": f"{method} {path} - RPS",
            "datasource": DS,
            "panelId": panel_id,
            "gridPos": {"h": 8, "w": 8, "x": 8, "y": y},
            "fieldConfig": {"defaults": {"unit": "rps"}, "overrides": []},
            "targets": [
                {"refId": "A", "legendFormat": "RPS",
                 "expr": f'sum(rate(http_requests_total{{{labels}}}[{RATE}]))'},
            ],
        }
        panel_id += 1

        error_labels = base_labels(method, path) + ', code=~"4[0-9]{2}|5[0-9]{2}"'
        errors = {
            "type": "timeseries",
            "title": f"{method} {path} - Errors by response code",
            "datasource": DS,
            "panelId": panel_id,
            "gridPos": {"h": 8, "w": 8, "x": 16, "y": y},
            "fieldConfig": {"defaults": {"unit": "short"}, "overrides": []},
            "targets": [
                {"refId": "A", "legendFormat": "{{code}}",
                 "expr": f'sum by (code) (rate(http_requests_total{{{error_labels}}}[{RATE}]))'},
            ],
        }
        panel_id += 1

        panels.extend([latency, rps, errors])
        y += 9

    return panels

def build_dashboard(handlers: list[dict]) -> dict:
    return {
        "id": None,
        "uid": "hw1-app-dashboard",
        "title": "HW1 - Application handlers",
        "tags": ["hw1"],
        "timezone": "browser",
        "editable": False,
        "refresh": "10s",
        "templating": {
            "list": [
                {
                    "name": "datacenter",
                    "label": "Datacenter",
                    "type": "query",
                    "datasource": DS,
                    "query": "label_values(http_requests_total, datacenter)",
                    "includeAll": True,
                    "multi": True,
                    "current": {"text": ["All"], "value": ["$__all"]},
                },
                {
                    "name": "pod",
                    "label": "Pod",
                    "type": "query",
                    "datasource": DS,
                    "query": "label_values(http_requests_total, pod)",
                    "includeAll": True,
                    "multi": True,
                    "current": {"text": ["All"], "value": ["$__all"]},
                },
            ]
        },
        "panels": build_panels(handlers),
    }

def publish(dashboard: dict) -> None:
    last = None
    for attempt in range(1, 11):
        try:
            resp = requests.post(
                f"{GRAFANA_URL}/api/dashboards/db",
                json={"dashboard": dashboard, "overwrite": True, "message": "hw1 generator"},
                auth=(ADMIN_USER, ADMIN_PASSWORD),
                timeout=10,
            )
            if resp.status_code == 200:
                print(f"Dashboard published: {resp.json().get('url')}")
                return
            last = f"{resp.status_code}: {resp.text}"
        except requests.RequestException as exc:
            last = str(exc)
        time.sleep(3)
    raise SystemExit(f"Could not publish dashboard to Grafana: {last}")

def main() -> None:
    handlers = fetch_handlers()
    print(f"Fetched {len(handlers)} handlers from {APP_URL}/handlers")
    publish(build_dashboard(handlers))

if __name__ == "__main__":
    main()
