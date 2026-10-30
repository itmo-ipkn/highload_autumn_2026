import argparse
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

DATASOURCE = {"type": "prometheus", "uid": os.environ.get("DATASOURCE_UID", "prometheus")}
DASHBOARD_UID = "items"
SELECTOR = 'datacenter=~"$datacenter", pod_id=~"$pod"'

def fetch_handlers(app_urls: list[str]) -> list[dict]:
    for url in app_urls:
        try:
            with urllib.request.urlopen(f"{url}/handlers", timeout=5) as resp:
                return json.load(resp)
        except (urllib.error.URLError, OSError) as e:
            print(f"{url}/handlers: {e}", file=sys.stderr)
    raise SystemExit("ни один экземпляр не отдал /handlers")

def promql_str(s: str) -> str:
    return s.replace("\\", "\\\\").replace('"', '\\"')

def variable(name: str, label: str, query: str) -> dict:
    return {
        "name": name,
        "label": label,
        "type": "query",
        "datasource": DATASOURCE,
        "query": query,
        "definition": query,
        "refresh": 2,
        "sort": 1,
        "multi": True,
        "includeAll": True,
        "allValue": ".*",
        "current": {"selected": True, "text": ["All"], "value": ["$__all"]},
    }

def timeseries(title: str, targets: list[tuple[str, str]], unit: str, x: int, y: int) -> dict:
    return {
        "type": "timeseries",
        "title": title,
        "datasource": DATASOURCE,
        "gridPos": {"h": 8, "w": 8, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "min": 0}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [
            {"refId": chr(ord("A") + i), "datasource": DATASOURCE, "expr": expr, "legendFormat": legend}
            for i, (expr, legend) in enumerate(targets)
        ],
    }

def handler_panels(h: dict, y: int) -> list[dict]:
    sel = f'{SELECTOR}, method="{promql_str(h["method"])}", route="{promql_str(h["path"])}"'
    title = f'{h["method"]} {h["path"]}'
    latency = [
        (
            f"histogram_quantile({q}, sum by (le) "
            f"(rate(http_request_duration_seconds_bucket{{{sel}}}[$__rate_interval])))",
            f"p{int(q * 100)}",
        )
        for q in (0.5, 0.95, 0.99)
    ]
    rps = [(f"sum(rate(http_requests_total{{{sel}}}[$__rate_interval]))", "RPS")]
    errors = [
        (
            f'sum by (status_code) (rate(http_requests_total{{{sel}, status_code=~"4..|5.."}}[$__rate_interval]))',
            "{{status_code}}",
        )
    ]
    return [
        {"type": "row", "title": title, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
         "panels": []},
        timeseries(f"{title} — latency", latency, "s", 0, y + 1),
        timeseries(f"{title} — RPS", rps, "reqps", 8, y + 1),
        timeseries(f"{title} — ошибки по кодам", errors, "reqps", 16, y + 1),
    ]

def build_dashboard(handlers: list[dict]) -> dict:
    panels = []
    for i, h in enumerate(handlers):
        panels += handler_panels(h, y=i * 9)
    for pid, p in enumerate(panels, start=1):
        p["id"] = pid
    return {
        "uid": DASHBOARD_UID,
        "title": "Items service",
        "tags": ["generated"],
        "editable": False,
        "timezone": "browser",
        "refresh": "10s",
        "time": {"from": "now-15m", "to": "now"},
        "schemaVersion": 39,
        "templating": {
            "list": [
                variable("datacenter", "Датацентр", "label_values(http_requests_total, datacenter)"),
                variable("pod", "Под", 'label_values(http_requests_total{datacenter=~"$datacenter"}, pod_id)'),
            ]
        },
        "panels": panels,
    }

def upload(grafana_url: str, user: str, password: str, dashboard: dict, attempts: int = 5) -> None:
    body = json.dumps({"dashboard": dashboard, "overwrite": True, "message": "generated"}).encode()
    auth = base64.b64encode(f"{user}:{password}".encode()).decode()
    req = urllib.request.Request(
        f"{grafana_url}/api/dashboards/db",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Basic {auth}"},
    )
    for attempt in range(1, attempts + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                print(f"дашборд загружен: {json.load(resp).get('url')}")
                return
        except urllib.error.HTTPError as e:
            # 4xx — ошибка в запросе или учётке: повтор не поможет.
            if e.code < 500:
                raise SystemExit(f"Grafana ответила {e.code}: {e.read().decode(errors='replace')}")
            print(f"попытка {attempt}: Grafana ответила {e.code}", file=sys.stderr)
        except (urllib.error.URLError, OSError) as e:
            print(f"попытка {attempt}: {e}", file=sys.stderr)
        time.sleep(2)
    raise SystemExit("не удалось загрузить дашборд")

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dry-run", action="store_true", help="напечатать JSON дашборда и не отправлять")
    args = parser.parse_args()

    app_urls = os.environ.get("APP_URLS", "http://app-1:8000").split(",")
    handlers = fetch_handlers(app_urls)
    print("ручки: " + ", ".join(f'{h["method"]} {h["path"]}' for h in handlers), file=sys.stderr)
    dashboard = build_dashboard(handlers)
    if args.dry_run:
        print(json.dumps(dashboard, ensure_ascii=False, indent=2))
        return
    upload(
        os.environ.get("GRAFANA_URL", "http://grafana:3000"),
        os.environ["GRAFANA_USER"],
        os.environ["GRAFANA_PASSWORD"],
        dashboard,
    )

if __name__ == "__main__":
    main()
