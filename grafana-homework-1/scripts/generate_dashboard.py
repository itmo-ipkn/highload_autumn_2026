"""Discover handlers at runtime, generate panels, upsert via Grafana HTTP API."""
import argparse
import base64
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DATASOURCE = {"type": "prometheus", "uid": "prometheus-lab"}
DASHBOARD_UID = "netinventory-api"


def request_json(url, payload=None, credentials=None):
    headers = {"Content-Type": "application/json"}
    if credentials:
        token = base64.b64encode(credentials.encode()).decode()
        headers["Authorization"] = "Basic " + token
    request = Request(url, data=json.dumps(payload).encode() if payload is not None else None, headers=headers)
    for attempt in range(15):
        try:
            with urlopen(request, timeout=10) as response:
                return json.load(response)
        except HTTPError as error:
            if error.code < 500:
                raise RuntimeError(f"{url}: HTTP {error.code}; check credentials/config") from error
        except (URLError, TimeoutError):
            pass
        if attempt == 14:
            raise RuntimeError(f"Service unavailable: {url}")
        time.sleep(2)


def discover_handlers(urls):
    handlers = set()
    for url in urls:
        for handler in request_json(url.rstrip("/") + "/handlers")["handlers"]:
            handlers.add((handler["method"], handler["path"]))
    if not handlers:
        raise RuntimeError("No business handlers discovered")
    return sorted(handlers, key=lambda handler: (handler[1], handler[0]))


def target(expression, legend, index=0):
    return {"refId": chr(65 + index), "expr": expression, "legendFormat": legend,
            "datasource": DATASOURCE, "range": True}


def panel(panel_id, title, targets, x, y, width=8, height=8, unit="short"):
    return {
        "id": panel_id, "title": title, "type": "timeseries", "datasource": DATASOURCE,
        "gridPos": {"x": x, "y": y, "w": width, "h": height}, "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "min": 0,
            "color": {"mode": "palette-classic"},
            "custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 8,
                       "showPoints": "never", "spanNulls": False}}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "calcs": ["lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def variable(name, label, query):
    return {"name": name, "label": label, "type": "query", "datasource": DATASOURCE,
            "query": query, "definition": query, "refresh": 1, "sort": 1,
            "multi": True, "includeAll": True, "allValue": ".*",
            "current": {"text": "All", "value": "$__all"}, "options": []}


def build_dashboard(handlers):
    scope = 'datacenter=~"${datacenter:regex}",pod_id=~"${pod_id:regex}"'
    total = 'netinventory_http_requests_total{' + scope + '}'
    panels = [
        panel(1, "Total RPS — selected pods", [target(f"sum(rate({total}[$__rate_interval]))", "All selected pods")], 0, 0, 12, 6, "reqps"),
        panel(2, "RPS by pod / datacenter", [target(f"sum by (pod_id, datacenter) (rate({total}[$__rate_interval]))", "{{pod_id}} / {{datacenter}}")], 12, 0, 12, 6, "reqps"),
    ]
    for row, (method, path) in enumerate(handlers):
        selector = scope + ',method=' + json.dumps(method) + ',route=' + json.dumps(path)
        counter = 'netinventory_http_requests_total{' + selector + '}'
        buckets = 'netinventory_request_duration_seconds_bucket{' + selector + '}'
        label = f"{method} {path}"
        y = 6 + row * 8
        panels.extend([
            panel(3 + row * 3, label + " • Latency", [
                target(f"histogram_quantile({q}, sum by (le) (rate({buckets}[$__rate_interval])))", legend, i)
                for i, (q, legend) in enumerate(((.5, "p50"), (.95, "p95"), (.99, "p99")))
            ], 0, y, unit="s"),
            panel(4 + row * 3, label + " • RPS", [target(f"sum(rate({counter}[$__rate_interval]))", "RPS")], 8, y, unit="reqps"),
            panel(5 + row * 3, label + " • Errors by HTTP code / s", [
                target('sum by (status_code) (rate(netinventory_http_requests_total{' + selector + ',status_code=~"4..|5.."}[$__rate_interval]))', "{{status_code}}")
            ], 16, y, unit="reqps"),
        ])
    return {
        "uid": DASHBOARD_UID, "title": "NetInventory / API health", "schemaVersion": 41,
        "editable": False, "tags": ["netinventory", "network", "generated"], "timezone": "browser",
        "description": "Generated from /handlers via HTTP API. Classic histograms are aggregated before computing percentiles. Errors include all 4xx and 5xx.",
        "refresh": "5s", "time": {"from": "now-5m", "to": "now"},
        "templating": {"list": [
            variable("datacenter", "Datacenter", "label_values(netinventory_http_requests_total, datacenter)"),
            variable("pod_id", "Pod", 'label_values(netinventory_http_requests_total{datacenter=~"${datacenter:regex}"}, pod_id)'),
        ]},
        "panels": panels,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-urls", default=os.getenv("APP_URLS", "http://app-1:8000,http://app-2:8000"))
    parser.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://grafana:3000"))
    parser.add_argument("--output", help="Optional path to also save the generated JSON")
    args = parser.parse_args()
    handlers = discover_handlers(args.app_urls.split(","))
    dashboard = build_dashboard(handlers)
    if args.output:
        Path(args.output).write_text(json.dumps(dashboard, ensure_ascii=False, indent=2))
    credentials = os.environ["GRAFANA_ADMIN_USER"] + ":" + os.environ["GRAFANA_ADMIN_PASSWORD"]
    request_json(args.grafana_url + "/api/datasources/uid/prometheus-lab", credentials=credentials)
    result = request_json(args.grafana_url + "/api/dashboards/db",
                          {"dashboard": dashboard, "overwrite": True}, credentials)
    print(f"Discovered {len(handlers)} handlers, generated {len(dashboard['panels'])} panels", flush=True)
    print(f"Dashboard ready: {result['url']} (HTTP API status: {result['status']})", flush=True)


if __name__ == "__main__":
    main()
