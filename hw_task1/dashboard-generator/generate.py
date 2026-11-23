import os

import httpx
import json


APP_URL = os.getenv("APP_URL", "http://localhost:8001")
GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3000")
DATASOURCE = {"type": "prometheus", "uid": "prometheus-main"}


def get_handlers() -> list[dict[str, str]]:
    response = httpx.get(
        f"{APP_URL}/handlers",
        timeout=10.0,
    )
    response.raise_for_status()

    return response.json()["handlers"]


def build_filters(method: str, path: str) -> str:
    labels = {
        "method": method,
        "handler": path,
    }

    filters = [
        f'{key}={json.dumps(value)}'
        for key, value in labels.items()
    ]

    filters.extend([
        'datacenter=~"${datacenter:regex}"',
        'pod_id=~"${pod_id:regex}"',
    ])

    return "{" + ",".join(filters) + "}"


def latency_query(
    method: str,
    path: str,
    quantile: float,
) -> str:
    filters = build_filters(method, path)

    return (
        f"histogram_quantile({quantile}, "
        f"sum by (le) ("
        f"rate(http_request_duration_seconds_bucket"
        f"{filters}[$__rate_interval])"
        f"))"
    )


def rps_query(method: str, path: str) -> str:
    filters = build_filters(method, path)

    return (
        f"sum(rate(http_requests_total"
        f"{filters}[$__rate_interval]))"
    )


def errors_query(method: str, path: str) -> str:
    filters = build_filters(method, path)
    filters = filters[:-1] + ',status_code=~"4..|5.."}'

    return (
        f"sum by (status_code) ("
        f"rate(http_requests_total"
        f"{filters}[$__rate_interval]))"
    )


def build_panel(
    panel_id: int,
    title: str,
    queries: list[tuple[str, str]],
    x: int,
    y: int,
    width: int = 8,
    unit: str = "short",
) -> dict:
    targets = []

    for index, (legend, expression) in enumerate(queries):
        targets.append({
            "refId": chr(65 + index),
            "expr": expression,
            "legendFormat": legend,
            "datasource": DATASOURCE,
        })

    return {
        "id": panel_id,
        "title": title,
        "type": "timeseries",
        "datasource": DATASOURCE,
        "gridPos": {
            "x": x,
            "y": y,
            "w": width,
            "h": 8,
        },
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "displayMode": "list",
                "placement": "bottom",
            },
            "tooltip": {"mode": "multi"},
        },
    }


def build_variable(name: str, label: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "query": {
            "query": f"label_values(http_requests_total, {name})",
            "refId": "PrometheusVariableQuery",
        },
        "refresh": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {
            "text": "All",
            "value": "$__all",
            "selected": True,
        },
    }


def build_dashboard(handlers: list[dict[str, str]]) -> dict:
    panels = []

    for index, handler in enumerate(handlers):
        method = handler["method"]
        path = handler["path"]
        title = f"{method} {path}"
        y = index * 8

        panels.append(build_panel(
            panel_id=index * 3 + 1,
            title=f"{title} | Latency",
            queries=[
                ("p50", latency_query(method, path, 0.50)),
                ("p95", latency_query(method, path, 0.95)),
                ("p99", latency_query(method, path, 0.99)),
            ],
            x=0,
            y=y,
            unit="s",
        ))

        panels.append(build_panel(
            panel_id=index * 3 + 2,
            title=f"{title} | RPS",
            queries=[("RPS", rps_query(method, path))],
            x=8,
            y=y,
            unit="reqps",
        ))

        panels.append(build_panel(
            panel_id=index * 3 + 3,
            title=f"{title} | Errors",
            queries=[("{{status_code}}", errors_query(method, path))],
            x=16,
            y=y,
            unit="reqps",
        ))

    return {
        "id": None,
        "uid": "items-service-monitoring",
        "title": "Items Service Monitoring",
        "tags": ["highload", "itmo"],
        "timezone": "browser",
        "schemaVersion": 41,
        "version": 0,
        "editable": False,
        "refresh": "5s",
        "time": {
            "from": "now-15m",
            "to": "now",
        },
        "templating": {
            "list": [
                build_variable("datacenter", "Datacenter"),
                build_variable("pod_id", "Pod"),
            ],
        },
        "panels": panels,
    }


def publish_dashboard(dashboard: dict) -> str:
    username = os.environ["GRAFANA_ADMIN_USER"]
    password = os.environ["GRAFANA_ADMIN_PASSWORD"]

    response = httpx.post(
        f"{GRAFANA_URL}/api/dashboards/db",
        auth=(username, password),
        json={
            "dashboard": dashboard,
            "overwrite": True,
            "message": "Automatically generated dashboard",
        },
        timeout=15.0,
    )

    response.raise_for_status()
    return response.json()["url"]


def main():
    handlers = get_handlers()
    dashboard = build_dashboard(handlers)

    print(f"Discovered handlers: {len(handlers)}")
    print(f"Generated panels: {len(dashboard['panels'])}")

    dashboard_url = publish_dashboard(dashboard)

    print("Dashboard successfully published!")
    print(f"Open: {GRAFANA_URL}{dashboard_url}")


if __name__ == "__main__":
    main()
