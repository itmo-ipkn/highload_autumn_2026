"""Генератор дашборда Grafana.

Динамически получает список ручек от GET /handlers обоих экземпляров приложения
и создаёт дашборд: на каждую ручку — панели latency (p50/p95/p99), RPS и ошибки
по кодам. Селекторы датацентра и пода — multi + includeAll. Дашборд non-editable.
"""
import os
import sys
import time

import requests

GRAFANA_URL = os.environ.get("GRAFANA_URL", "http://grafana:3000")
GRAFANA_USER = os.environ.get("GRAFANA_ADMIN_USER", "admin")
GRAFANA_PASSWORD = os.environ.get("GRAFANA_ADMIN_PASSWORD", "")
APP_URLS = [
    u.strip() for u in os.environ.get("APP_URLS", "http://app1:8000,http://app2:8000").split(",")
    if u.strip()
]
DS_UID = os.environ.get("DATASOURCE_UID", "prometheus")
DASHBOARD_TITLE = os.environ.get("DASHBOARD_TITLE", "Items service — Golden Signals")
DS = {"type": "prometheus", "uid": DS_UID}

_next_panel_id = 0


def _panel_id():
    global _next_panel_id
    _next_panel_id += 1
    return _next_panel_id


def wait_grafana(timeout_s: int = 180):
    print(f"Ждём готовности Grafana на {GRAFANA_URL} ...", flush=True)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            if requests.get(f"{GRAFANA_URL}/api/health", timeout=3).ok:
                print("Grafana готова.", flush=True)
                return
        except requests.RequestException:
            pass
        time.sleep(2)
    raise RuntimeError("Grafana не отвечает на /api/health")


def fetch_handlers() -> list:
    """Опрашивает все экземпляры приложения и объединяет списки ручек."""
    handlers = set()
    for url in APP_URLS:
        print(f"Получаем /handlers от {url} ...", flush=True)
        for attempt in range(30):
            try:
                resp = requests.get(f"{url}/handlers", timeout=5)
                resp.raise_for_status()
                for h in resp.json().get("handlers", []):
                    handlers.add((h["method"], h["path"]))
                break
            except requests.RequestException as exc:
                print(f"  попытка {attempt + 1}: {exc}", flush=True)
                time.sleep(2)
        else:
            raise RuntimeError(f"Не удалось получить /handlers от {url}")
    return sorted(handlers)


def build_variables() -> dict:
    return {"list": [
        {
            "name": "datacenter",
            "label": "Датацентр",
            "type": "query",
            "datasource": DS,
            "query": "label_values(http_requests_total, datacenter)",
            "refresh": 1,
            "includeAll": True,
            "multi": True,
            "sort": 1,
            "current": {"text": "All", "value": "$__all"},
        },
        {
            "name": "pod_id",
            "label": "Под",
            "type": "query",
            "datasource": DS,
            "query": 'label_values(http_requests_total{datacenter=~"$datacenter"}, pod_id)',
            "refresh": 1,
            "includeAll": True,
            "multi": True,
            "sort": 1,
            "current": {"text": "All", "value": "$__all"},
        },
    ]}


def build_row(method: str, path: str, y: int) -> list:
    """Строка дашборда для одной ручки + три панели под ней."""
    label_filter = f'handler="{path}",method="{method}",datacenter=~"$datacenter",pod_id=~"$pod_id"'

    row = {
        "id": _panel_id(),
        "title": f"{method} {path}",
        "type": "row",
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }

    latency_panel = {
        "id": _panel_id(),
        "title": "Latency percentiles",
        "type": "timeseries",
        "datasource": DS,
        "gridPos": {"x": 0, "y": y + 1, "w": 8, "h": 8},
        "fieldConfig": {"defaults": {"unit": "s"}, "overrides": []},
        "targets": [
            {
                "refId": "A",
                "expr": f"histogram_quantile(0.50, sum by (le) (rate(http_request_duration_seconds_bucket{{{label_filter}}}[1m])))",
                "legendFormat": "p50",
            },
            {
                "refId": "B",
                "expr": f"histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{{{label_filter}}}[1m])))",
                "legendFormat": "p95",
            },
            {
                "refId": "C",
                "expr": f"histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{{{label_filter}}}[1m])))",
                "legendFormat": "p99",
            },
        ],
    }

    rps_panel = {
        "id": _panel_id(),
        "title": "RPS",
        "type": "timeseries",
        "datasource": DS,
        "gridPos": {"x": 8, "y": y + 1, "w": 8, "h": 8},
        "fieldConfig": {"defaults": {"unit": "reqps"}, "overrides": []},
        "targets": [
            {
                "refId": "A",
                "expr": f"sum(rate(http_requests_total{{{label_filter}}}[1m]))",
                "legendFormat": "rps",
            },
        ],
    }

    errors_panel = {
        "id": _panel_id(),
        "title": "Errors by code",
        "type": "timeseries",
        "datasource": DS,
        "gridPos": {"x": 16, "y": y + 1, "w": 8, "h": 8},
        "fieldConfig": {"defaults": {"unit": "reqps"}, "overrides": []},
        "targets": [
            {
                "refId": "A",
                "expr": f'sum by (code) (rate(http_requests_total{{{label_filter},code=~"4..|5.."}}[1m]))',
                "legendFormat": "{{code}}",
            },
        ],
    }

    return [row, latency_panel, rps_panel, errors_panel]


def build_dashboard(handlers: list) -> dict:
    panels = []
    y = 0
    for method, path in handlers:
        panels.extend(build_row(method, path, y))
        y += 9  # 1 на строку-заголовок + 8 на панели

    return {
        "annotations": {
            "list": []
        },
        "editable": False,
        "gnetId": None,
        "graphTooltip": 0,
        "id": None,
        "iteration": 1,
        "links": [],
        "panels": panels,
        "schemaVersion": 39,
        "style": "dark",
        "tags": ["ddia", "homework-1"],
        "templating": build_variables(),
        "time": {
            "from": "now-15m",
            "to": "now"
        },
        "timepicker": {
            "refresh_intervals": [
                "5s",
                "10s",
                "30s",
                "1m",
                "5m",
                "15m",
                "30m",
                "1h",
                "2h",
                "1d"
            ],
            "time_options": [
                "5m",
                "15m",
                "1h",
                "6h",
                "12h",
                "24h",
                "2d",
                "7d",
                "30d"
            ]
        },
        "timezone": "browser",
        "title": DASHBOARD_TITLE,
        "uid": "items-golden-signals",
        "version": 1
    }


def push_dashboard(dashboard: dict):
    """Публикует дашборд напрямую через REST API Grafana.

    Grafana ожидает тело вида {"dashboard": {...}, "overwrite": true}.
    Ответ сервера печатаем целиком — чтобы любая ошибка была видна в логах.
    """
    payload = {"dashboard": dashboard, "overwrite": True}
    for attempt in range(10):
        try:
            resp = requests.post(
                f"{GRAFANA_URL}/api/dashboards/db",
                json=payload,
                auth=(GRAFANA_USER, GRAFANA_PASSWORD),
                timeout=10,
            )
            if resp.ok:
                print(f"Дашборд создан/обновлён: {resp.json()}", flush=True)
                return
            print(
                f"Попытка {attempt + 1}: HTTP {resp.status_code}, ответ: {resp.text}",
                flush=True,
            )
        except requests.RequestException as exc:
            print(f"Попытка {attempt + 1} не удалась: {exc}", flush=True)
        time.sleep(5)
    raise RuntimeError("Не удалось создать дашборд в Grafana")


def main():
    if not GRAFANA_PASSWORD:
        print("GRAFANA_ADMIN_PASSWORD не задана", file=sys.stderr)
        sys.exit(1)
    wait_grafana()
    handlers = fetch_handlers()
    print("Обнаруженные ручки:", flush=True)
    for method, path in handlers:
        print(f"  {method} {path}", flush=True)
    dashboard = build_dashboard(handlers)
    push_dashboard(dashboard)
    print("Готово.", flush=True)


if __name__ == "__main__":
    main()
