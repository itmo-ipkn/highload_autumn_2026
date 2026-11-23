import base64
import json
import os
import urllib.error
import urllib.request


DASHBOARD_UID = "ddia-hw1-items"
DATASOURCE_UID = "prometheus"
DATASOURCE = {"type": "prometheus", "uid": DATASOURCE_UID}
RATE_INTERVAL = "$__rate_interval"


def read_json(url: str, authorization: str | None = None) -> object:
    headers = {"Accept": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"GET request failed with HTTP {error.code}") from None


def discover_handlers(app_urls: list[str]) -> list[tuple[str, str]]:
    discovered = []
    for app_url in app_urls:
        payload = read_json(app_url.rstrip("/") + "/handlers")
        if not isinstance(payload, list):
            raise ValueError("/handlers must return a JSON list")
        handlers = []
        for entry in payload:
            if not isinstance(entry, dict):
                raise ValueError("/handlers contains a non-object entry")
            method, route = entry.get("method"), entry.get("route")
            if not isinstance(method, str) or not isinstance(route, str) or not route.startswith("/"):
                raise ValueError("/handlers contains an invalid method or route")
            handlers.append((method, route))
        if not handlers or len(set(handlers)) != len(handlers):
            raise ValueError("/handlers must contain unique routes")
        discovered.append(sorted(handlers))
    if any(handlers != discovered[0] for handlers in discovered[1:]):
        raise ValueError("Application instances reported different handlers")
    return discovered[0]


def variable(name: str, label: str) -> dict:
    query = f"label_values(items_http_requests_total, {name})"
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "definition": query,
        "query": query,
        "refresh": 1,
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"selected": True, "text": "All", "value": "$__all"},
        "options": [],
    }


def panel(panel_id: int, title: str, x: int, y: int, width: int, unit: str, targets: list[dict]) -> dict:
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"h": 9, "w": width, "x": x, "y": y},
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "min": 0}, "overrides": []},
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
    }


def target(expression: str, ref_id: str, legend: str) -> dict:
    return {
        "datasource": DATASOURCE,
        "expr": expression,
        "refId": ref_id,
        "legendFormat": legend,
        "range": True,
    }


def build_dashboard(handlers: list[tuple[str, str]]) -> dict:
    panels = []
    for index, (method, route) in enumerate(handlers):
        selector = (
            f'method={json.dumps(method)},route={json.dumps(route)},'
            'datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"'
        )
        latency_targets = [
            target(
                f"histogram_quantile({quantile}, "
                "sum by (le) ("
                f"rate(items_http_request_duration_seconds_bucket{{{selector}}}[{RATE_INTERVAL}])"
                "))",
                ref_id,
                legend,
            )
            for quantile, ref_id, legend in (
                (0.50, "A", "p50"),
                (0.95, "B", "p95"),
                (0.99, "C", "p99"),
            )
        ]
        rps = f"sum(rate(items_http_requests_total{{{selector}}}[{RATE_INTERVAL}]))"
        errors = (
            "sum by (status_code) ("
            f"rate(items_http_requests_total{{{selector},status_code=~\"4..|5..\"}}"
            f"[{RATE_INTERVAL}])"
            ")"
        )
        y = index * 9
        title = f"{method} {route}"
        panels.extend(
            [
                panel(index * 3 + 1, f"{title} · latency", 0, y, 12, "s", latency_targets),
                panel(index * 3 + 2, f"{title} · RPS", 12, y, 6, "reqps", [target(rps, "A", "requests/s")]),
                panel(index * 3 + 3, f"{title} · errors", 18, y, 6, "reqps", [target(errors, "A", "{{status_code}}")]),
            ]
        )
    return {
        "id": None,
        "uid": DASHBOARD_UID,
        "title": "DDIA HW1 · HTTP handlers",
        "tags": ["ddia", "generated"],
        "timezone": "browser",
        "editable": False,
        "schemaVersion": 41,
        "version": 0,
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {
            "list": [variable("datacenter", "Datacenter"), variable("pod_id", "Pod")]
        },
        "panels": panels,
    }


def publish_dashboard(grafana_url: str, username: str, password: str, dashboard: dict) -> str:
    authorization = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
    datasource = read_json(
        grafana_url.rstrip("/") + f"/api/datasources/uid/{DATASOURCE_UID}",
        authorization,
    )
    if not isinstance(datasource, dict) or datasource.get("uid") != DATASOURCE_UID:
        raise ValueError("Expected provisioned Prometheus datasource was not found")
    payload = json.dumps({"dashboard": dashboard, "overwrite": True}).encode()
    request = urllib.request.Request(
        grafana_url.rstrip("/") + "/api/dashboards/db",
        data=payload,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": authorization,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f"Grafana dashboard upload failed with HTTP {error.code}") from None
    if result.get("status") != "success" or result.get("uid") != DASHBOARD_UID:
        raise RuntimeError("Grafana did not confirm dashboard creation")
    return result["url"]


def main() -> None:
    app_urls = [url.strip() for url in os.environ["APP_URLS"].split(",") if url.strip()]
    if not app_urls:
        raise ValueError("APP_URLS contains no application URLs")
    handlers = discover_handlers(app_urls)
    dashboard = build_dashboard(handlers)
    dashboard_path = publish_dashboard(
        os.environ["GRAFANA_URL"],
        os.environ["GRAFANA_ADMIN_USER"],
        os.environ["GRAFANA_ADMIN_PASSWORD"],
        dashboard,
    )
    print(f"Generated {len(dashboard['panels'])} panels for {len(handlers)} handlers: {dashboard_path}")


if __name__ == "__main__":
    main()
