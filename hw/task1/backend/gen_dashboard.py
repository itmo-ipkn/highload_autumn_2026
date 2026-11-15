import json
import os
import requests

API_URLS = json.loads(os.getenv("API_URLS", "[]"))
GRAFANA_URL = os.getenv("GRAFANA_URL")
GRAFANA_USER = os.getenv("GRAFANA_ADMIN_USER")
GRAFANA_PASS = os.getenv("GRAFANA_ADMIN_PASSWORD")

DATASOURCE_UID = "prometheus-uid"


def build_dashboard(handlers):
    panels = []
    panel_id = 1
    y_pos = 0

    for h in handlers:
        method = h["method"]
        path = h["path"]
        handler_name = f"{method} {path}"

        selector = f'handler="{path}", method="{method}", datacenter=~"$datacenter", pod_id=~"$pod_id"'

        panels.append(
            {
                "id": panel_id,
                "type": "timeseries",
                "title": f"Latency p50, p95, p99 - {handler_name}",
                "gridPos": {"h": 8, "w": 8, "x": 0, "y": y_pos},
                "targets": [
                    {
                        "expr": f"histogram_quantile(0.50, sum(rate(http_request_duration_seconds_bucket{{{selector}}}[$__rate_interval])) by (le))",
                        "legendFormat": "p50",
                    },
                    {
                        "expr": f"histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{{{selector}}}[$__rate_interval])) by (le))",
                        "legendFormat": "p95",
                    },
                    {
                        "expr": f"histogram_quantile(0.99, sum(rate(http_request_duration_seconds_bucket{{{selector}}}[$__rate_interval])) by (le))",
                        "legendFormat": "p99",
                    },
                ],
                "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
                "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
            }
        )
        panel_id += 1

        panels.append(
            {
                "id": panel_id,
                "type": "timeseries",
                "title": f"RPS - {handler_name}",
                "gridPos": {"h": 8, "w": 8, "x": 8, "y": y_pos},
                "targets": [
                    {
                        "expr": f"sum(rate(http_requests_total{{{selector}}}[$__rate_interval]))",
                        "legendFormat": "RPS",
                    }
                ],
                "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
                "fieldConfig": {"defaults": {"unit": "reqps"}, "overrides": []},
            }
        )
        panel_id += 1

        panels.append(
            {
                "id": panel_id,
                "type": "timeseries",
                "title": f"Errors - {handler_name}",
                "gridPos": {"h": 8, "w": 8, "x": 16, "y": y_pos},
                "targets": [
                    {
                        "expr": f'sum(rate(http_requests_total{{{selector}, status=~"4..|5.."}}[$__rate_interval])) by (status)',
                        "legendFormat": "Status {{status}}",
                    }
                ],
                "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
                "fieldConfig": {"defaults": {"unit": "reqps"}, "overrides": []},
            }
        )
        panel_id += 1
        y_pos += 8

    dashboard = {
        "dashboard": {
            "title": "Monitoring",
            "tags": ["generated", "fastapi"],
            "timezone": "browser",
            "editable": False,  # Non-editable
            "schemaVersion": 38,
            "version": 1,
            "templating": {
                "list": [
                    {
                        "name": "datacenter",
                        "label": "Datacenter",
                        "type": "query",
                        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
                        "query": "label_values(http_requests_total, datacenter)",
                        "includeAll": True,
                        "multi": True,
                        "current": {"text": "All", "value": "$__all"},
                    },
                    {
                        "name": "pod_id",
                        "label": "Pod ID",
                        "type": "query",
                        "datasource": {"type": "prometheus", "uid": DATASOURCE_UID},
                        "query": "label_values(http_requests_total, pod_id)",
                        "includeAll": True,
                        "multi": True,
                        "current": {"text": "All", "value": "$__all"},
                    },
                ]
            },
            "panels": panels,
            "time": {"from": "now-15m", "to": "now"},
            "refresh": "10s",
        },
        "overwrite": True,
    }
    return dashboard


def main():

    resp = requests.get(f"{API_URLS[0]}/handlers")
    resp.raise_for_status()
    handlers = resp.json()["handlers"]

    dashboard = build_dashboard(handlers)

    requests.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        json=dashboard,
        auth=(GRAFANA_USER, GRAFANA_PASS),
    )


if __name__ == "__main__":
    main()
