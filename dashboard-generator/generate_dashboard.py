
import base64
import json
import os
import time
import urllib.error
import urllib.request


APP_BASE_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "APP_BASE_URLS",
        "http://app1:8080,http://app2:8080",
    ).split(",")
    if url.strip()
]

GRAFANA_URL = os.getenv(
    "GRAFANA_URL", "http://grafana:3000"
).rstrip("/")

GRAFANA_USER = os.getenv("GRAFANA_ADMIN_USER", "admin")
GRAFANA_PASSWORD = os.environ["GRAFANA_ADMIN_PASSWORD"]

# Имя переменной должно совпадать с docker-compose.yaml.
DATASOURCE_UID = os.getenv(
    "GRAFANA_DATASOURCE_UID", "prometheus-ddia"
)

DASHBOARD_UID = os.getenv(
    "DASHBOARD_UID", "items-observability"
)


def get_json(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def post_json(url, payload):
    credentials = base64.b64encode(
        f"{GRAFANA_USER}:{GRAFANA_PASSWORD}".encode()
    ).decode()

    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {credentials}",
        },
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=15) as response:
        return json.load(response)


def get_handlers():
    last_error = None

    # Повторяем попытки на случай временной недоступности приложения.
    for attempt in range(60):
        try:
            unique_handlers = set()

            # Забираем маршруты с каждого экземпляра.
            for base_url in APP_BASE_URLS:
                handlers = get_json(f"{base_url}/handlers")

                if not isinstance(handlers, list):
                    raise ValueError(
                        f"{base_url}/handlers must return a JSON array"
                    )

                for item in handlers:
                    method = item.get("method")
                    path = item.get("path")

                    if not isinstance(method, str) or not isinstance(path, str):
                        raise ValueError(
                            f"Invalid handler descriptor: {item!r}"
                        )

                    unique_handlers.add((method, path))

            if not unique_handlers:
                raise ValueError("No handlers returned by applications")

            return sorted(
                unique_handlers,
                key=lambda item: (item[1], item[0]),
            )

        except Exception as exc:
            last_error = exc
            print(
                f"Attempt {attempt + 1}/60 failed: {exc}",
                flush=True,
            )
            time.sleep(2)

    raise RuntimeError(
        f"Cannot fetch handlers from {APP_BASE_URLS}"
    ) from last_error


def variable(name):
    return {
        "name": name,
        "label": name.capitalize(),
        "type": "query",
        "datasource": {
            "type": "prometheus",
            "uid": DATASOURCE_UID,
        },
        "definition": f"label_values(http_requests_total, {name})",
        "query": {
            "query": (
                f"label_values(http_requests_total, {name})"
            ),
            "refId": "PrometheusVariableQueryEditor-VariableQuery",
        },
        "refresh": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "sort": 1,
        "current": {
            "selected": True,
            "text": ["All"],
            "value": ["$__all"],
        },
        "options": [],
        "hide": 0,
    }


def make_panel(panel_id, title, expr, x, y, width, unit,
               legend="{{pod}} / {{datacenter}}"):
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "datasource": {
            "type": "prometheus",
            "uid": DATASOURCE_UID,
        },
        "gridPos": {
            "x": x, "y": y, "w": width, "h": 8
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "decimals": 3,
            },
            "overrides": [],
        },
        "options": {
            "legend": {
                "showLegend": True,
                "displayMode": "list",
                "placement": "bottom",
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [{
            "refId": "A",
            "expr": expr,
            "legendFormat": legend,
        }],
    }


def build_dashboard(handlers):
    panels = []
    panel_id = 1

    for route_index, (method, path) in enumerate(handlers):
        y = route_index * 9

        # JSON quoting safely creates PromQL string literals.
        selectors = [
            f"uri={json.dumps(path)}",
            'datacenter=~"$datacenter"',
            'pod=~"$pod"',
        ]
        if method != "*":
            selectors.append(f"method={json.dumps(method)}")

        selector = ",".join(selectors)
        heading = f"{method} {path}"

        # Aggregate buckets across instances before calculating quantiles.
        for quantile, x, width in [
            ("0.50", 0, 5),
            ("0.95", 5, 5),
            ("0.99", 10, 5),
        ]:
            expr = (
                f'histogram_quantile({quantile}, '
                f'sum by (le, pod, datacenter) '
                f'(rate(http_request_duration_seconds_bucket'
                f'{{{selector}}}[5m])))'
            )
            panels.append(make_panel(
                panel_id, f"{heading} — p{int(float(quantile)*100)}",
                expr, x, y, width, "s"
            ))
            panel_id += 1

        rps_expr = (
            "sum by (pod, datacenter) "
            f"(rate(http_requests_total{{{selector}}}[5m]))"
        )
        panels.append(make_panel(
            panel_id, f"{heading} — RPS",
            rps_expr, 15, y, 5, "reqps"
        ))
        panel_id += 1

        errors_expr = (
            "sum by (status) "
            f"(rate(http_errors_total{{{selector}}}[5m]))"
        )
        panels.append(make_panel(
            panel_id, f"{heading} — Errors by status",
            errors_expr, 20, y, 4, "reqps", "{{status}}"
        ))
        panel_id += 1

    return {
        "id": None,
        "uid": DASHBOARD_UID,
        "title": "Items API — HTTP observability",
        "tags": ["generated", "items-api"],
        "timezone": "browser",
        "schemaVersion": 39,
        "version": 0,
        "refresh": "10s",
        "editable": False,
        "panels": panels,
        "templating": {
            "list": [
                variable("datacenter"),
                variable("pod"),
            ]
        },
    }


def main():
    handlers = get_handlers()
    dashboard = build_dashboard(handlers)

    result = post_json(
        f"{GRAFANA_URL}/api/dashboards/db",
        {
            "dashboard": dashboard,
            "folderId": 0,
            "overwrite": True,
            "message": "Generated from /handlers",
        },
    )

    print(json.dumps(result, indent=2))
    print(f"Dashboard: {GRAFANA_URL}{result.get('url', '')}")


if __name__ == "__main__":
    main()
