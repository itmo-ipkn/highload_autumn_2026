"""Discover /handlers at runtime and publish a Grafana 11 dashboard.

Requires Python 3.9+ and the standard library only. Administrator credentials
are read from GRAFANA_ADMIN_USER and GRAFANA_ADMIN_PASSWORD (or its _FILE).
"""
from __future__ import annotations

import argparse
import base64
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

DATASOURCE_UID = "practice1-prometheus"
DASHBOARD_UID = "practice1-handlers"
DASHBOARD_TITLE = "Practice 1 · HTTP golden signals"
PROMETHEUS_JOB = "my_service"


def request_json(url: str, *, payload: dict | None = None, auth: str | None = None) -> dict:
    headers = {"Accept": "application/json"}
    if auth is not None:
        headers["Authorization"] = "Basic " + auth
    data = None
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    with urlopen(Request(url, data=data, headers=headers), timeout=5) as response:
        document = json.load(response)
    if not isinstance(document, dict):
        raise ValueError("Expected a JSON object from " + url)
    return document


def discover(app_urls: list[str]) -> list[tuple[str, str]]:
    if not app_urls:
        raise ValueError("At least one application URL is required")
    expected = None
    for app_url in app_urls:
        document = request_json(app_url.rstrip("/") + "/handlers")
        handlers = document.get("handlers")
        if not isinstance(handlers, list) or not handlers:
            raise ValueError("/handlers must contain a non-empty handlers list")
        routes = set()
        for handler in handlers:
            if not isinstance(handler, dict):
                raise ValueError("Each discovered handler must be an object")
            method, path = handler.get("method"), handler.get("path")
            if not isinstance(method, str) or not re.fullmatch(r"[A-Z]+", method):
                raise ValueError("A discovered handler has an invalid HTTP method")
            if not isinstance(path, str) or not path.startswith("/"):
                raise ValueError("A discovered handler has an invalid route template")
            routes.add((method, path))
        if expected is not None and expected != routes:
            raise ValueError("Application replicas expose different monitored routes")
        expected = routes
    return sorted(expected)


def selector(method: str, path: str, job: str = PROMETHEUS_JOB) -> str:
    # JSON quoting provides the required escapes for PromQL string literals.
    return (
        f'job={json.dumps(job)},method={json.dumps(method)},route={json.dumps(path)},'
        'datacenter=~"${datacenter:regex}",pod_id=~"${pod:regex}"'
    )


def make_panel(panel_id: int, title: str, x: int, y: int, unit: str,
               expressions: list[tuple[str, str]], datasource: dict, description: str) -> dict:
    return {
        "id": panel_id, "type": "timeseries", "title": title,
        "description": description, "datasource": datasource,
        "gridPos": {"h": 8, "w": 8, "x": x, "y": y},
        "fieldConfig": {
            "defaults": {"unit": unit, "min": 0,
                         "custom": {"lineWidth": 2, "fillOpacity": 8}},
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull"]},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [
            {"refId": chr(65 + index), "expr": expression, "legendFormat": legend,
             "range": True, "datasource": datasource}
            for index, (expression, legend) in enumerate(expressions)
        ],
    }


def build_dashboard(handlers: list[tuple[str, str]], *, datasource_uid: str = DATASOURCE_UID,
                    dashboard_uid: str = DASHBOARD_UID, title: str = DASHBOARD_TITLE,
                    job: str = PROMETHEUS_JOB) -> dict:
    datasource = {"type": "prometheus", "uid": datasource_uid}
    panels = []
    for row, (method, path) in enumerate(sorted(set(handlers))):
        labels = selector(method, path, job)
        name = f"{method} {path}"
        latency = [
            (f'histogram_quantile({quantile}, sum by (le) '
             f'(rate(http_request_duration_seconds_bucket{{{labels}}}[$__rate_interval])))', legend)
            for quantile, legend in ((0.5, "p50"), (0.95, "p95"), (0.99, "p99"))
        ]
        rps = [(f'sum(rate(http_requests_total{{{labels}}}[$__rate_interval]))', "RPS")]
        errors = [(f'sum by (status) (rate(http_requests_total{{{labels},status=~"4..|5.."}}'
                   '[$__rate_interval]))', "{{status}}")]
        for column, suffix, unit, expressions, description in (
            (0, "latency", "s", latency, "p50/p95/p99 of the merged histogram across selected replicas."),
            (1, "RPS", "reqps", rps, "Requests per second summed across selected replicas, including errors."),
            (2, "errors", "reqps", errors, "4xx/5xx responses per second grouped by exact HTTP status."),
        ):
            panels.append(make_panel(len(panels) + 1, name + " · " + suffix,
                                     column * 8, row * 8, unit, expressions, datasource, description))

    job_label = "job=" + json.dumps(job)
    variables = []
    for name, label, query in (
        ("datacenter", "Datacenter", f'label_values(http_requests_total{{{job_label}}}, datacenter)'),
        ("pod", "Pod", 'label_values(http_requests_total{' + job_label +
         ',datacenter=~"${datacenter:regex}"}, pod_id)'),
    ):
        variables.append({
            "name": name, "label": label, "type": "query", "datasource": datasource,
            "definition": query, "query": {"query": query, "refId": "VariableQuery"},
            "multi": True, "includeAll": True, "allValue": ".*", "refresh": 1, "sort": 1,
            "current": {"selected": True, "text": ["All"], "value": ["$__all"]}, "options": [],
        })
    return {
        "id": None, "uid": dashboard_uid, "title": title,
        "description": "Generated from the running application's /handlers endpoint.",
        "tags": ["practice1", "userver", "generated"], "schemaVersion": 40,
        "version": 0, "editable": False, "timezone": "browser", "refresh": "5s",
        "time": {"from": "now-15m", "to": "now"},
        "templating": {"list": variables}, "panels": panels, "annotations": {"list": []},
    }


def save_dashboard(dashboard: dict, output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=output.parent,
                                         prefix=".dashboard-", delete=False) as stream:
            temporary = Path(stream.name)
            json.dump(dashboard, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
        temporary.chmod(0o644)
        temporary.replace(output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def base_url(value: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("Application and Grafana URLs must use http:// or https://")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("Pass Grafana credentials through environment variables, not URLs")
    if parsed.query or parsed.fragment:
        raise ValueError("Base URLs must not contain query parameters or fragments")
    return value.rstrip("/")


def read_password() -> str:
    filename = os.getenv("GRAFANA_ADMIN_PASSWORD_FILE")
    if filename:
        for attempt in range(60):
            try:
                password = Path(filename).read_text().strip()
                if password:
                    return password
            except FileNotFoundError:
                pass
            except OSError as error:
                raise ValueError("Cannot read GRAFANA_ADMIN_PASSWORD_FILE") from error
            if attempt < 59:
                time.sleep(0.5)
        raise ValueError("Credentials initializer has not provided a password within 30 seconds")
    password = os.getenv("GRAFANA_ADMIN_PASSWORD", "")
    if not password:
        raise ValueError("Set GRAFANA_ADMIN_PASSWORD or GRAFANA_ADMIN_PASSWORD_FILE")
    return password


def main(argv: list[str] | None = None) -> int:
    # A failed rerun must invalidate a previous successful run for load startup.
    ready_file = Path(os.environ["DASHBOARD_READY_FILE"]) if os.getenv("DASHBOARD_READY_FILE") else None
    if ready_file is not None:
        ready_file.unlink(missing_ok=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app-url", action="append", help="Application URL; repeat for each replica")
    parser.add_argument("--grafana-url", default=os.getenv("GRAFANA_URL", "http://localhost:3000"))
    parser.add_argument("--output", type=Path, default=Path(os.getenv("DASHBOARD_OUTPUT", "generated/dashboard.json")))
    parser.add_argument("--datasource-uid", default=os.getenv("DATASOURCE_UID", DATASOURCE_UID))
    parser.add_argument("--dashboard-uid", default=os.getenv("DASHBOARD_UID", DASHBOARD_UID))
    parser.add_argument("--title", default=os.getenv("DASHBOARD_TITLE", DASHBOARD_TITLE))
    parser.add_argument("--job", default=os.getenv("PROMETHEUS_JOB", PROMETHEUS_JOB))
    parser.add_argument("--attempts", type=int, default=15, help="Number of attempts for temporary network/API errors")
    args = parser.parse_args(argv)

    try:
        apps = args.app_url or [url.strip() for url in os.getenv(
            "APP_URLS", "http://localhost:18081,http://localhost:18082").split(",") if url.strip()]
        apps = [base_url(url) for url in apps]
        grafana = base_url(args.grafana_url)
        if args.attempts < 1:
            raise ValueError("--attempts must be positive")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,40}", args.dashboard_uid):
            raise ValueError("Dashboard UID must contain 1..40 letters, digits, underscores or hyphens")
        password = read_password()
        username = os.getenv("GRAFANA_ADMIN_USER") or "admin"
        auth = base64.b64encode(f"{username}:{password}".encode()).decode()
    except ValueError as error:
        parser.error(str(error))

    for attempt in range(1, args.attempts + 1):
        try:
            # Podman can start dependencies natively before Compose finishes
            # waiting for their health status. Verify the health APIs as well.
            for app in apps:
                with urlopen(app + "/health", timeout=5) as response:
                    if response.status != 200:
                        raise URLError("Application healthcheck is not ready")
            if request_json(grafana + "/api/health").get("database") != "ok":
                raise URLError("Grafana database is not ready")
            handlers = discover(apps)
            source = request_json(grafana + "/api/datasources/uid/" + args.datasource_uid, auth=auth)
            if source.get("type") != "prometheus":
                raise ValueError("The configured datasource must have type prometheus")
            dashboard = build_dashboard(handlers, datasource_uid=args.datasource_uid,
                                        dashboard_uid=args.dashboard_uid, title=args.title, job=args.job)
            save_dashboard(dashboard, args.output)
            result = request_json(grafana + "/api/dashboards/db", auth=auth,
                                  payload={"dashboard": dashboard, "overwrite": True,
                                           "message": "Generated from live /handlers"})
            if result.get("status") != "success":
                raise ValueError("Grafana did not confirm dashboard creation")
            if ready_file is not None:
                save_dashboard({"status": "success", "uid": args.dashboard_uid}, ready_file)
            print(f"Dashboard ready: {len(handlers)} operations, {len(dashboard['panels'])} panels", flush=True)
            print("JSON: " + str(args.output), flush=True)
            print("Grafana: " + grafana + result.get("url", "/d/" + args.dashboard_uid), flush=True)
            return 0
        except HTTPError as error:
            if error.code not in (408, 429) and error.code < 500:
                print(f"Generation failed: HTTP {error.code}; check URLs, credentials and datasource UID", file=sys.stderr)
                return 1
            failure = f"HTTP {error.code}"
        except (URLError, TimeoutError, ConnectionError) as error:
            failure = type(error).__name__
        except (ValueError, OSError) as error:
            print("Generation failed: " + str(error), file=sys.stderr)
            return 1
        if attempt == args.attempts:
            print("Generation failed after retries: " + failure, file=sys.stderr)
            return 1
        print(f"Waiting for application/Grafana API ({attempt}/{args.attempts}): {failure}", flush=True)
        time.sleep(2)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
