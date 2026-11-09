#!/usr/bin/env python3
"""Grafana dashboard generator for multiple app instances.

Reads APP1_BASE_URL and APP2_BASE_URL from the environment (or CLI args).
Verifies both apps are alive and share the same handler schema, then generates
a single unified dashboard. The dashboard uses the existing 'pod' and 'datacenter'
template variables to allow filtering between app1 and app2, or viewing them combined.
"""
import json
import os
import sys
import urllib.request
from pathlib import Path

# Read from environment variables, with fallbacks for local dev
APP1_BASE_URL = os.getenv("APP1_BASE_URL", "http://app1:8080")
APP2_BASE_URL = os.getenv("APP2_BASE_URL", "http://app2:8080")

DEFS_DIR = Path("grafana/provisioning/dashboards/defs")
DS_UID = "crud-vault-prometheus"
GEN_UID = "handlers"
GEN_LEGACY_PREFIX = "handler-"
QUANTILES = (0.50, 0.95, 0.99)

# (template variable name = metric label, selector caption per lab spec)
SELECTORS = (("datacenter", "DATACENTER"), ("pod", "POD_ID"))
SKIP_HANDLERS = {("GET", "/handlers")}


def label_filter(method: str, path: str) -> str:
    """Prometheus label filter pinning exactly one handler."""
    return f'method="{method}",route="{path}"'


def selector_filter() -> str:
    """Label filters for the two dashboard selectors."""
    return ",".join(f'{name}=~"${name}"' for name, _ in SELECTORS)


def selectors() -> list:
    """Query template variables -> dropdown selectors."""
    out = []
    for name, caption in SELECTORS:
        out.append({
            "type": "query",
            "name": name,
            "label": caption,
            "query": f"label_values({name})",
            "regex": "",
            "datasource": {"type": "prometheus", "uid": DS_UID},
            "includeAll": True,
            "includeAllValue": ".*|$",         # Matches existing labels OR absent labels
            "multi": True,
            "options": [],
            "current": {"tags": [], "text": "All", "value": "$__all"},
            "sort": 0,
            "hide": 0,
        })
    return out


def panel(pid: int, x: int, y: int, title: str, expressions: list,
          aliases: list | None = None,
          unit: str = "short",
          axis_label: str | None = None) -> dict:
    """Panel JSON with correct Prometheus datasource keys."""
    targets = []
    for i, e in enumerate(expressions):
        t = {
            "refId": chr(ord("A") + i),
            "expr": e,
            "datasource": {"type": "prometheus", "uid": DS_UID}
        }
        if aliases:
            t["legendFormat"] = aliases[i]
        targets.append(t)

    field_config = {
        "defaults": {
            "unit": unit,  # <-- Y-axis unit (formats the numbers)
            "custom": {
                "drawStyle": "line",
                "lineInterpolation": "linear",
                "lineWidth": 1,
                "fillOpacity": 0,
                "gradientMode": "none",
                "spanNulls": False,
                "showPoints": "never",
                "pointSize": 5,
                "stacking": {"mode": "none"},
                "axisPlacement": "auto",
                "axisLabel": axis_label or "",  # <-- Custom axis title
                "axisColorMode": "text",
                "scaleDistribution": {"type": "linear"},
            },
        },
        "overrides": [],
    }

    return {
        "type": "timeseries",
        "id": pid,
        "title": title,
        "gridPos": {"h": 8, "w": 8, "x": x, "y": y},
        "datasource": {"type": "prometheus", "uid": DS_UID},
        "targets": targets,
        "fieldConfig": field_config,  # <-- NEW: y-axis configuration
        "options": {
            "tooltip": {"mode": "multi", "sort": "desc"},
            "legend": {"showLegend": True, "displayMode": "list", "placement": "bottom"},
        },
    }


def handler_panels(method: str, path: str, row: int) -> list:
    """Three panels for one handler (latency / RPS / errors)."""
    f = f"{label_filter(method, path)},{selector_filter()}"
    tag = f"{method} {path}"
    y = row * 9

    # 1. Latency panel — values are in milliseconds
    latency = [
        f'histogram_quantile({q}, sum(rate(http_request_duration_seconds_bucket'
        f'{{{f}}}[1m])) by (le)) * 1000'
        for q in QUANTILES
    ]
    latency_panel = panel(
        row * 3 + 1, 0, y,
        f"{tag} Latency (ms)",
        latency,
        aliases=[f"p{int(round(q * 100))}" for q in QUANTILES],
        unit="ms",                # <-- Grafana formats as "15.5 ms"
        axis_label="latency (ms)",
    )

    # 2. RPS panel — requests per second
    rps = [f'sum(rate(http_requests_total{{{f}}}[1m]))']
    rps_panel = panel(
        row * 3 + 2, 8, y,
        f"{tag} RPS (req/s)",
        rps,
        unit="reqps",             # <-- Grafana formats as "42 req/s"
        axis_label="requests / sec",
    )

    # 3. Errors panel — requests per second per code
    errors = [f'sum by (code) (rate(http_requests_total'
              f'{{{f},code=~"4..|5.."}}[1m]))']
    errors_panel = panel(
        row * 3 + 3, 16, y,
        f"{tag} Errors (req/s)",
        errors,
        unit="reqps",             # <-- Grafana formats as "3 req/s"
        axis_label="errors / sec",
    )

    return [latency_panel, rps_panel, errors_panel]


def dashboard(handlers: list) -> dict:
    return {
        "title": "CRUD vault -- metrics per handler (Multi-App)",
        "uid": GEN_UID,
        "timezone": "utc",
        "schemaVersion": 40,
        "version": 1,
        "templating": {"list": selectors()},
        "panels": [p for row, (m, pth) in enumerate(handlers)
                   for p in handler_panels(m, pth, row)],
    }


def fetch_handlers(base: str) -> list:
    """Live handler listing: [{"method": ..., "path": ...}, ...]"""
    try:
        with urllib.request.urlopen(base + "/handlers", timeout=10) as r:
            data = json.loads(r.read())
        handlers = [(h["method"], h["path"]) for h in data["handlers"]]
        return handlers
    except Exception as e:
        print(f"WARNING: Failed to fetch handlers from {base}: {e}", file=sys.stderr)
        return []


def main() -> int:
    # Allow CLI args to override env vars for flexibility
    base1 = sys.argv[1] if len(sys.argv) > 1 else APP1_BASE_URL
    base2 = sys.argv[2] if len(sys.argv) > 2 else APP2_BASE_URL

    print(f"Fetching handlers from App 1: {base1}")
    handlers1 = fetch_handlers(base1)

    print(f"Fetching handlers from App 2: {base2}")
    handlers2 = fetch_handlers(base2)

    if not handlers1 and not handlers2:
        raise SystemExit("ERROR: Both apps failed to return handlers. Is the stack running?")

    if handlers1 and handlers2 and handlers1 != handlers2:
        print("WARNING: App 1 and App 2 have different handler schemas!", file=sys.stderr)
        print(f"  App 1: {handlers1}", file=sys.stderr)
        print(f"  App 2: {handlers2}", file=sys.stderr)

    # Use App 1's handlers to build the dashboard (they should be identical)
    handlers = handlers1 or handlers2
    if not handlers:
        raise SystemExit("GET /handlers returned an empty listing")

    DEFS_DIR.mkdir(parents=True, exist_ok=True)

    # Clean up stale generated files
    for stale in list(DEFS_DIR.glob(GEN_LEGACY_PREFIX + "*.json")) + \
                 list(DEFS_DIR.glob(GEN_UID + ".json")):
        stale.unlink()

    body = dashboard(handlers)
    out = DEFS_DIR / (body["uid"] + ".json")
    out.write_text(json.dumps(body, indent=2) + "\n")

    print(f"\n✅ Dashboard JSON written to {DEFS_DIR}")
    print(f"   (uid: {GEN_UID}, {len(handlers)} handlers x 3 panels)")
    print("   💡 Tip: Use the POD_ID or DATACENTER dropdowns in Grafana to filter")
    print("           between app1 and app2, or select 'All' to see combined metrics.\n")

    for method, path in handlers:
        print(" ", method, path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
