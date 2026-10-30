import base64
import json
import os
import urllib.request

DATASOURCE = {"type": "prometheus", "uid": "prometheus-main"}
GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000").rstrip("/")
GRAFANA_ADMIN_USER = os.environ.get("GRAFANA_ADMIN_USER", "")
GRAFANA_ADMIN_PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "")
APP_URL = os.environ.get("APP_URL", "http://app-1:8080").rstrip("/")
DASHBOARD_UID = os.environ.get("DASHBOARD_UID", "items-service")
DASHBOARD_TITLE = os.environ.get("DASHBOARD_TITLE", "items-service")


def api(url, body=None):
    request = urllib.request.Request(url, data=body, method="GET" if body is None else "POST")
    if body is not None:
        token = base64.b64encode(f"{GRAFANA_ADMIN_USER}:{GRAFANA_ADMIN_PASSWORD}".encode()).decode()
        request.add_header("Content-Type", "application/json")
        request.add_header("Authorization", f"Basic {token}")
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read())


def variable(name, label, query):
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "definition": query,
        "query": {"query": query, "refId": "StandardVariableQuery"},
        "refresh": 1,
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "hide": 0,
    }


def panel(panel_id, title, unit, y, x, series):
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"h": 8, "w": 8, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom"},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [
            {
                "datasource": DATASOURCE,
                "expr": expr,
                "legendFormat": legend,
                "refId": chr(65 + index),
            }
            for index, (expr, legend) in enumerate(series)
        ],
    }


def build(handlers):
    panels = []
    panel_id = 1
    y = 0

    for handler in handlers:
        method = handler["method"]
        route = handler["route"]
        title = f"{method} {route}"
        selector = (
            f'route="{route}",method="{method}",'
            'datacenter=~"$datacenter",pod_id=~"$pod_id"'
        )

        panels.append(
            {
                "id": panel_id,
                "type": "row",
                "title": title,
                "collapsed": False,
                "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
                "panels": [],
            }
        )
        panel_id += 1
        y += 1

        latency = [
            (
                f"histogram_quantile({q}, sum by (le) "
                f"(rate(latency_seconds_bucket{{{selector}}}[$__rate_interval])))",
                f"p{int(q * 100)}",
            )
            for q in (0.5, 0.95, 0.99)
        ]
        rps = [
            (f"sum(rate(requests_amount_total{{{selector}}}[$__rate_interval]))", "RPS")
        ]
        errors = [
            (
                f'sum by (status_code) (rate(requests_amount_total{{{selector},'
                'status_code=~"[45][0-9][0-9]"}[$__rate_interval]))',
                "{{status_code}}",
            )
        ]

        for pid, kind, unit, x, series in (
            (panel_id, "latency", "s", 0, latency),
            (panel_id + 1, "RPS", "reqps", 8, rps),
            (panel_id + 2, "errors", "reqps", 16, errors),
        ):
            panels.append(panel(pid, f"{title} — {kind}", unit, y, x, series))
        panel_id += 3
        y += 8

    return {
        "uid": DASHBOARD_UID,
        "title": DASHBOARD_TITLE,
        "tags": ["items-service", "generated"],
        "editable": False,
        "graphTooltip": 1,
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 1,
        "refresh": "10s",
        "time": {"from": "now-15m", "to": "now"},
        "annotations": {"list": []},
        "templating": {
            "list": [
                variable(
                    "datacenter",
                    "Datacenter",
                    "label_values(requests_amount_total, datacenter)",
                ),
                variable(
                    "pod_id",
                    "Pod",
                    'label_values(requests_amount_total{datacenter=~"$datacenter"}, pod_id)',
                ),
            ]
        },
        "panels": panels,
    }


def main():
    handlers = api(f"{APP_URL}/handlers")["handlers"]
    body = json.dumps({"dashboard": build(handlers), "overwrite": True}).encode()
    api(f"{GRAFANA_URL}/api/dashboards/db", body)["url"]


if __name__ == "__main__":
    main()
