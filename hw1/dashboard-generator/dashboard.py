"""Сборка JSON дашборда Grafana из списка ручек.

Чистые функции без сети и файлов: вход — список `handler` из `/handlers`,
выход — словарь дашборда. Все параметры зафиксированы в `docs/dashboard-contract.md`.
"""

from collections.abc import Sequence

DATASOURCE_UID = "prometheus-main"
DATASOURCE_TYPE = "prometheus"

DASHBOARD_UID = "hw1-items-overview"
DASHBOARD_TITLE = "HW-1 Items Service"
DASHBOARD_TAGS = ("hw1", "devops")
SCHEMA_VERSION = 39
REFRESH = "10s"
TIME_RANGE = {"from": "now-15m", "to": "now"}

WINDOW = "1m"
SELECTOR = 'datacenter=~"$datacenter", pod_id=~"$pod"'
QUANTILES = (("p50", "0.5"), ("p95", "0.95"), ("p99", "0.99"))
REF_IDS = ("A", "B", "C")

PANEL_WIDTH = 8
PANEL_HEIGHT = 8


class DashboardError(ValueError):
    """Список ручек не годится для сборки дашборда."""


def _datasource() -> dict:
    return {"type": DATASOURCE_TYPE, "uid": DATASOURCE_UID}


def latency_expr(handler: str, quantile: str) -> str:
    return (
        f"histogram_quantile({quantile}, sum by (le, handler) "
        f'(rate(http_request_duration_seconds_bucket{{{SELECTOR}, handler="{handler}"}}[{WINDOW}])))'
    )


def rps_expr(handler: str) -> str:
    return (
        f'sum by (handler) (rate(http_requests_total{{{SELECTOR}, handler="{handler}"}}[{WINDOW}]))'
    )


def errors_expr(handler: str) -> str:
    return (
        f"sum by (handler, status_code) "
        f'(rate(http_requests_total{{{SELECTOR}, handler="{handler}", status_code=~"4..|5.."}}[{WINDOW}]))'
    )


def _target(ref_id: str, expr: str, legend: str) -> dict:
    return {
        "refId": ref_id,
        "expr": expr,
        "legendFormat": legend,
        "datasource": _datasource(),
        "editorMode": "code",
        "range": True,
    }


def _panel(panel_id: int, handler: str, kind: str, title_suffix: str, grid: dict) -> dict:
    if kind == "latency":
        targets = [
            _target(ref_id, latency_expr(handler, quantile), legend)
            for ref_id, (legend, quantile) in zip(REF_IDS, QUANTILES)
        ]
        unit, description = "s", f"Латентность {handler}: p50/p95/p99 по агрегированным бакетам"
    elif kind == "rps":
        targets = [_target("A", rps_expr(handler), "{{handler}}")]
        unit, description = "reqps", f"Запросы в секунду на {handler}"
    else:
        targets = [_target("A", errors_expr(handler), "{{status_code}}")]
        unit, description = "reqps", f"Ответы 4xx/5xx на {handler} с разбивкой по коду"

    return {
        "id": panel_id,
        "type": "timeseries",
        "title": f"{handler} — {title_suffix}",
        "description": description,
        "datasource": _datasource(),
        "gridPos": grid,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "custom": {
                    "drawStyle": "line",
                    "lineWidth": 1,
                    "fillOpacity": 10,
                    "showPoints": "never",
                    "spanNulls": False,
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": targets,
    }


def _variable(name: str, query: str) -> dict:
    return {
        "name": name,
        "label": name,
        "type": "query",
        "datasource": _datasource(),
        "definition": query,
        "query": query,
        "refresh": 2,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "options": [],
        "sort": 1,
    }


def validate_handlers(handlers: Sequence[str]) -> list[str]:
    if not handlers:
        raise DashboardError("список ручек пуст: дашборд без панелей не собирается")

    cleaned = []
    for handler in handlers:
        if not isinstance(handler, str) or not handler.strip():
            raise DashboardError(f"некорректная ручка: {handler!r}")
        if " " not in handler.strip():
            raise DashboardError(f"ручка должна быть в формате '<METHOD> <path>': {handler!r}")
        cleaned.append(handler.strip())

    if len(set(cleaned)) != len(cleaned):
        raise DashboardError(f"список ручек содержит дубликаты: {sorted(cleaned)}")

    return sorted(cleaned)


def build_dashboard(handlers: Sequence[str]) -> dict:
    names = validate_handlers(handlers)

    panels = []
    panel_id = 1
    for row, handler in enumerate(names):
        y = row * PANEL_HEIGHT
        for column, (kind, suffix) in enumerate(
            (("latency", "latency"), ("rps", "rps"), ("errors", "errors"))
        ):
            grid = {"h": PANEL_HEIGHT, "w": PANEL_WIDTH, "x": column * PANEL_WIDTH, "y": y}
            panels.append(_panel(panel_id, handler, kind, suffix, grid))
            panel_id += 1

    return {
        "uid": DASHBOARD_UID,
        "title": DASHBOARD_TITLE,
        "tags": list(DASHBOARD_TAGS),
        "timezone": "browser",
        "editable": False,
        "schemaVersion": SCHEMA_VERSION,
        "refresh": REFRESH,
        "time": dict(TIME_RANGE),
        "graphTooltip": 1,
        "templating": {
            "list": [
                _variable("datacenter", "label_values(http_requests_total, datacenter)"),
                _variable(
                    "pod",
                    'label_values(http_requests_total{datacenter=~"$datacenter"}, pod_id)',
                ),
            ]
        },
        "panels": panels,
    }
