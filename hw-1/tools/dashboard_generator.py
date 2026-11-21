"""Discover HTTP handlers and create/update their Grafana dashboard.

No endpoint catalogue is embedded here: the running application owns /handlers.
The stable legacy dashboard API remains supported by the pinned Grafana 13.2.4.
Run with ``python -m tools.dashboard_generator``; only the standard library is used.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib import error, parse, request


DEFAULT_DATASOURCE_UID = "task1-prometheus"
DEFAULT_DASHBOARD_UID = "items-golden-signals"
HTTP_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE", "CONNECT"})
RETRYABLE_HTTP_CODES = frozenset({404, 408, 425, 429, 500, 502, 503, 504})
MAX_RESPONSE_BYTES = 2 * 1024 * 1024


class GeneratorError(RuntimeError):
    """A failed discovery, configuration check, or dashboard update."""


@dataclass(frozen=True, order=True)
class Handler:
    method: str
    path: str


def _safe_text(value: object, name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise GeneratorError(f"{name} must be a nonempty string of at most {maximum} characters")
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise GeneratorError(f"{name} contains a control character")
    return value


def parse_handlers(payload: object) -> tuple[str, list[Handler]]:
    if not isinstance(payload, dict):
        raise GeneratorError("/handlers must return a JSON object")
    service = _safe_text(payload.get("service", "items-api"), "service", 120)
    entries = payload.get("handlers")
    if not isinstance(entries, list) or not entries:
        raise GeneratorError("/handlers must contain a nonempty handlers array")
    result: set[Handler] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise GeneratorError("Each handler must be an object with method and path")
        method = _safe_text(entry.get("method"), "handler.method", 16).upper()
        path = _safe_text(entry.get("path"), "handler.path", 1024)
        if method not in HTTP_METHODS:
            raise GeneratorError("A handler has an unsupported HTTP method")
        if not path.startswith("/"):
            raise GeneratorError("A handler path must start with /")
        result.add(Handler(method=method, path=path))
    return service, sorted(result)


def promql_string(value: str) -> str:
    """Quote a literal for PromQL and shield it from Grafana interpolation.

    PromQL accepts Unicode escapes in quoted strings. Escaping literal dollar
    signs and opening brackets also prevents route text such as $pod_id or
    [[datacenter]] from being interpreted as a Grafana template variable.
    """
    return json.dumps(value, ensure_ascii=False).replace("$", "\\u0024").replace("[", "\\u005b")


def _uid(value: str, name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", value):
        raise GeneratorError(f"{name} must contain 1–40 letters, digits, underscores or hyphens")
    return value


def _selector(handler: Handler, *, errors_only: bool = False) -> str:
    labels = [
        f"method={promql_string(handler.method)}",
        f"route={promql_string(handler.path)}",
        'datacenter=~"${datacenter:regex}"',
        'pod_id=~"${pod_id:regex}"',
    ]
    if errors_only:
        labels.append('status_code=~"4..|5.."')
    return "{" + ",".join(labels) + "}"


def _variable(name: str, label: str, query: str, datasource: dict[str, str]) -> dict[str, Any]:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": datasource,
        "query": {"query": query, "refId": "PrometheusVariableQueryEditor-VariableQuery"},
        "definition": query,
        "refresh": 2,
        "sort": 1,
        "regex": "",
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
        "options": [],
        "hide": 0,
        "skipUrlSync": False,
    }


def _target(ref_id: str, expression: str, legend: str, datasource: dict[str, str]) -> dict[str, Any]:
    return {
        "refId": ref_id,
        "datasource": datasource,
        "editorMode": "code",
        "expr": expression,
        "legendFormat": legend,
        "range": True,
        "instant": False,
    }


def _color_override(series: str, color: str) -> dict[str, Any]:
    return {
        "matcher": {"id": "byName", "options": series},
        "properties": [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}],
    }


def _panel(
    panel_id: int,
    title: str,
    description: str,
    unit: str,
    column: int,
    row: int,
    targets: list[dict[str, Any]],
    datasource: dict[str, str],
    overrides: list[dict[str, Any]] | None = None,
    legend_mode: str = "table",
) -> dict[str, Any]:
    return {
        "id": panel_id,
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": datasource,
        "gridPos": {"x": column * 8, "y": row * 8, "w": 8, "h": 8},
        "targets": targets,
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "min": 0,
                "color": {"mode": "palette-classic"},
                "custom": {
                    "drawStyle": "line",
                    "lineInterpolation": "linear",
                    "lineWidth": 2,
                    "fillOpacity": 8,
                    "showPoints": "never",
                    "spanNulls": False,
                    "axisPlacement": "auto",
                    "axisBorderShow": False,
                    "scaleDistribution": {"type": "linear"},
                    "stacking": {"mode": "none", "group": "A"},
                    "thresholdsStyle": {"mode": "off"},
                },
                "thresholds": {"mode": "absolute", "steps": [{"color": "green", "value": None}]},
                "mappings": [],
            },
            "overrides": overrides or [],
        },
        "options": {
            "legend": {"showLegend": True, "displayMode": legend_mode, "placement": "bottom", "calcs": ["lastNotNull"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }


def build_dashboard(
    handlers: Sequence[Handler],
    *,
    service_name: str = "items-api",
    datasource_uid: str = DEFAULT_DATASOURCE_UID,
    dashboard_uid: str = DEFAULT_DASHBOARD_UID,
) -> dict[str, Any]:
    """Build one row of three panels for each discovered method/path pair."""
    if not handlers:
        raise GeneratorError("Cannot generate a dashboard without handlers")
    datasource = {"type": "prometheus", "uid": _uid(datasource_uid, "PROMETHEUS_UID")}
    panels: list[dict[str, Any]] = []
    for row, handler in enumerate(sorted(set(handlers))):
        identity = f"{handler.method} {handler.path}"
        selected = _selector(handler)
        latency_targets = [
            _target(
                ref_id,
                f"histogram_quantile({quantile}, sum by (le) (rate(http_request_duration_seconds_bucket{selected}[$__rate_interval])))",
                legend,
                datasource,
            )
            for ref_id, quantile, legend in [("A", "0.50", "p50"), ("B", "0.95", "p95"), ("C", "0.99", "p99")]
        ]
        panels.append(_panel(
            row * 3 + 1, f"{identity} · Latency", "Percentiles of the combined selected pods; histogram buckets are aggregated before quantiles.",
            "s", 0, row, latency_targets, datasource,
            [_color_override("p50", "green"), _color_override("p95", "yellow"), _color_override("p99", "red")],
        ))
        panels.append(_panel(
            row * 3 + 2, f"{identity} · Requests", "Total requests per second across the selected datacenters and pods, including unsuccessful requests.",
            "reqps", 1, row,
            [_target("A", f"sum(rate(http_requests_total{selected}[$__rate_interval]))", "Requests / s", datasource)],
            datasource, [_color_override("Requests / s", "blue")],
        ))
        panels.append(_panel(
            row * 3 + 3, f"{identity} · Errors", "Failed requests per second, split by exact HTTP status code; 4xx and 5xx only.",
            "ops", 2, row,
            [_target("A", f"sum by (status_code) (rate(http_requests_total{_selector(handler, errors_only=True)}[$__rate_interval]))", "HTTP {{status_code}}", datasource)],
            datasource, [_color_override("HTTP 404", "yellow"), _color_override("HTTP 422", "orange"), _color_override("HTTP 500", "red")],
            legend_mode="list",
        ))
    return {
        "id": None,
        "uid": _uid(dashboard_uid, "DASHBOARD_UID"),
        "title": f"{_safe_text(service_name, 'service', 120)} · Golden signals",
        "description": "Generated from the running application's /handlers endpoint. Select one or several datacenters and pods, or All.",
        "tags": ["ddia", "task1", "generated"],
        "schemaVersion": 41,
        "version": 0,
        "editable": False,
        "graphTooltip": 1,
        "timezone": "browser",
        "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
        "annotations": {"list": []},
        "links": [],
        "panels": panels,
        "templating": {"list": [
            _variable("datacenter", "Datacenter", "label_values(http_requests_total, datacenter)", datasource),
            _variable("pod_id", "Pod", 'label_values(http_requests_total{datacenter=~"${datacenter:regex}"}, pod_id)', datasource),
        ]},
    }


def _base_url(value: str, name: str) -> str:
    parsed = parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise GeneratorError(f"{name} must be an HTTP(S) base URL without credentials, query or fragment")
    return value.rstrip("/")


def request_json(
    method: str,
    url: str,
    *,
    payload: object | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 10,
    attempts: int = 15,
    retry_delay: float = 2,
) -> Any:
    """Bounded retries for startup races; permanent auth/validation errors fail fast."""
    if attempts < 1 or not math.isfinite(timeout) or timeout <= 0 or not math.isfinite(retry_delay) or retry_delay < 0:
        raise GeneratorError("Invalid HTTP timeout or retry configuration")
    body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request_headers = {"Accept": "application/json", **(headers or {})}
    if body is not None:
        request_headers["Content-Type"] = "application/json"
    # Report only endpoint paths: headers, credentials and response bodies never
    # enter logs, including on Grafana authentication failures.
    endpoint = parse.urlsplit(url).path
    failure = "request failed"
    for attempt in range(attempts):
        outgoing = request.Request(url, data=body, headers=request_headers, method=method)
        try:
            with request.urlopen(outgoing, timeout=timeout) as response:
                raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                raise GeneratorError(f"Response from {endpoint} is too large")
            try:
                return json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                raise GeneratorError(f"Response from {endpoint} is not valid JSON") from exc
        except error.HTTPError as exc:
            code = exc.code
            exc.close()
            failure = f"HTTP {code} from {endpoint}"
            if code not in RETRYABLE_HTTP_CODES:
                raise GeneratorError(failure) from None
        except (error.URLError, TimeoutError, ConnectionError, OSError):
            failure = f"Unable to connect to {endpoint}"
        if attempt + 1 < attempts:
            time.sleep(retry_delay)
    raise GeneratorError(f"{failure} after {attempts} attempts")


def publish_dashboard(
    dashboard: dict[str, Any],
    *,
    grafana_url: str,
    username: str,
    password: str,
    timeout: float = 10,
    attempts: int = 15,
) -> str:
    if not username or not password or ":" in username:
        raise GeneratorError("Set nonempty GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD; username must not contain ':'")
    base_url = _base_url(grafana_url, "GRAFANA_URL")
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    options = {"headers": {"Authorization": f"Basic {auth}"}, "timeout": timeout, "attempts": attempts}
    datasource_uid = dashboard["panels"][0]["datasource"]["uid"]
    datasource = request_json("GET", f"{base_url}/api/datasources/uid/{parse.quote(datasource_uid, safe='')}", **options)
    if not isinstance(datasource, dict) or datasource.get("type") != "prometheus" or datasource.get("uid") != datasource_uid:
        raise GeneratorError("Provisioned Prometheus datasource does not match the expected UID/type")
    response = request_json(
        "POST", f"{base_url}/api/dashboards/db",
        payload={"dashboard": dashboard, "overwrite": True, "message": "Regenerated from the application's runtime handler catalogue"},
        **options,
    )
    if not isinstance(response, dict) or response.get("status") != "success" or response.get("uid") != dashboard["uid"]:
        raise GeneratorError("Grafana did not confirm successful dashboard creation")
    saved = request_json("GET", f"{base_url}/api/dashboards/uid/{parse.quote(dashboard['uid'], safe='')}", **options)
    saved_dashboard = saved.get("dashboard") if isinstance(saved, dict) else None
    if not isinstance(saved_dashboard, dict) or saved_dashboard.get("uid") != dashboard["uid"]:
        raise GeneratorError("Saved dashboard could not be verified")
    if saved_dashboard.get("editable") is not False or len(saved_dashboard.get("panels", [])) != len(dashboard["panels"]):
        raise GeneratorError("Saved dashboard does not have the expected panels/editability")
    return f"{base_url}/d/{dashboard['uid']}"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-url", default=os.getenv("APP_URL", "http://app1:8000"))
    parser.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://grafana:3000"))
    parser.add_argument("--datasource-uid", default=os.getenv("PROMETHEUS_UID", DEFAULT_DATASOURCE_UID))
    parser.add_argument("--dashboard-uid", default=os.getenv("DASHBOARD_UID", DEFAULT_DASHBOARD_UID))
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--attempts", type=int, default=15)
    parser.add_argument("--output", type=Path, help="Write the generated dashboard JSON to this file")
    parser.add_argument("--dry-run", action="store_true", help="Discover handlers and generate JSON without accessing Grafana")
    args = parser.parse_args(argv)
    try:
        app_url = _base_url(args.app_url, "APP_URL")
        service_name, handlers = parse_handlers(request_json(
            "GET", f"{app_url}/handlers", timeout=args.timeout, attempts=args.attempts,
        ))
        dashboard = build_dashboard(handlers, service_name=service_name, datasource_uid=args.datasource_uid, dashboard_uid=args.dashboard_uid)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        if args.dry_run:
            if not args.output:
                print(json.dumps(dashboard, ensure_ascii=False, indent=2))
            print(f"Generated {len(dashboard['panels'])} panels from {len(handlers)} discovered handlers", file=sys.stderr)
            return 0
        dashboard_url = publish_dashboard(
            dashboard, grafana_url=args.grafana_url,
            username=os.getenv("GRAFANA_ADMIN_USER", ""), password=os.getenv("GRAFANA_ADMIN_PASSWORD", ""),
            timeout=args.timeout, attempts=args.attempts,
        )
        print(f"Dashboard ready: {dashboard_url}; {len(handlers)} handlers, {len(dashboard['panels'])} panels")
        return 0
    except (GeneratorError, OSError) as exc:
        print(f"Dashboard generation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
