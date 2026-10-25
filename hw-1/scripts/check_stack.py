#!/usr/bin/env python3
"""Read-only acceptance check for a running stack (Python standard library)."""

import argparse
import base64
import json
import math
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def local_settings() -> dict[str, str]:
    """Read the simple KEY=VALUE example format without executing shell code."""
    values = {}
    path = Path(__file__).resolve().parents[1] / ".env"
    if path.exists():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
                value = value[1:-1]
            values[key.strip()] = value
    values.update(os.environ)
    return values


def read_json(url: str, authorization: str | None = None) -> dict:
    headers = {"Accept": "application/json"}
    if authorization:
        headers["Authorization"] = authorization
    with urlopen(Request(url, headers=headers), timeout=5) as response:
        return json.load(response)


def query(prometheus: str, expression: str, evaluation_time: float) -> list[dict]:
    parameters = {"query": expression, "time": f"{evaluation_time:.6f}"}
    result = read_json(f"{prometheus}/api/v1/query?{urlencode(parameters)}")
    if result.get("status") != "success":
        raise ValueError("Prometheus query did not succeed")
    return result["data"]["result"]


def sample_value(sample: dict) -> float:
    number = float(sample["value"][1])
    if not math.isfinite(number):
        raise ValueError("Prometheus returned a non-finite sample")
    return number


def inspect(args: argparse.Namespace, settings: dict[str, str]) -> dict:
    # All comparisons use exactly the same sample window, even if a scrape
    # happens while this check is running.
    evaluation_time = time.time()
    credentials = f"{settings['GRAFANA_ADMIN_USER']}:{settings['GRAFANA_ADMIN_PASSWORD']}"
    authorization = "Basic " + base64.b64encode(credentials.encode()).decode()
    identities = []
    handlers = None
    for app_url in args.apps:
        health = read_json(f"{app_url}/health")
        identities.append((health["pod_id"], health["datacenter"]))
        current = read_json(f"{app_url}/handlers")["handlers"]
        current_set = {(item["method"], item["path"]) for item in current}
        if handlers is not None and handlers != current_set:
            raise ValueError("Application instances report different handlers")
        handlers = current_set
    if not handlers or len(set(identities)) != len(args.apps):
        raise ValueError("Missing handlers or duplicate instance identities")

    targets = read_json(f"{args.prometheus}/api/v1/targets")["data"]["activeTargets"]
    application_targets = [target for target in targets if target.get("labels", {}).get("job") == "items-service"]
    if len(application_targets) != len(args.apps) or any(target["health"] != "up" for target in application_targets):
        raise ValueError("Not all application targets are UP in Prometheus")

    datasource = read_json(f"{args.grafana}/api/datasources/uid/{args.datasource_uid}", authorization)
    if datasource.get("type") != "prometheus":
        raise ValueError("Provisioned datasource is not Prometheus")
    stored = read_json(f"{args.grafana}/api/dashboards/uid/{args.dashboard_uid}", authorization)
    dashboard = stored["dashboard"]
    if dashboard.get("editable") is not False:
        raise ValueError("Dashboard is editable")
    variables = {item["name"]: item for item in dashboard["templating"]["list"]}
    for name in ("datacenter", "pod_id"):
        variable = variables[name]
        if not variable.get("multi") or not variable.get("includeAll"):
            raise ValueError(f"Variable {name} does not support multi-selection and All")

    def flatten(panels: list[dict]):
        for panel in panels:
            yield panel
            yield from flatten(panel.get("panels", []))

    panels = [panel for panel in flatten(dashboard["panels"]) if panel.get("type") == "timeseries"]
    if len(panels) != 3 * len(handlers):
        raise ValueError("Dashboard must contain three time-series panels per handler")
    datacenters = sorted({dc for _, dc in identities})
    pod_ids = sorted({pod for pod, _ in identities})
    selections = {"all": (None, None)}
    selections.update({f"dc:{dc}": ([dc], None) for dc in datacenters})
    selections.update({f"pod:{pod}@{dc}": ([dc], [pod]) for pod, dc in identities})
    selections["multi"] = (datacenters, pod_ids)
    rps_by_selection = {name: {} for name in selections}
    panel_query_checks = 0

    def matcher(values: list[str] | None) -> str:
        pattern = ".*" if values is None else "(" + "|".join(re.escape(value) for value in values) + ")"
        # The substitution goes inside an existing PromQL quoted string.
        return json.dumps(pattern)[1:-1]

    for selection, (selected_dcs, selected_pods) in selections.items():
        for panel in panels:
            quantiles = {}
            for target in panel.get("targets", []):
                expression = target["expr"]
                expression = expression.replace("${datacenter:regex}", matcher(selected_dcs))
                expression = expression.replace("${pod_id:regex}", matcher(selected_pods))
                expression = expression.replace("$__rate_interval", "1m")
                samples = query(args.prometheus, expression, evaluation_time)
                panel_query_checks += 1
                if not samples:
                    raise ValueError(f"Panel has no data for {selection}: {panel['title']}")
                values = [sample_value(sample) for sample in samples]
                if any(value < 0 for value in values):
                    raise ValueError("A panel contains a negative rate or latency")
                unit = panel.get("fieldConfig", {}).get("defaults", {}).get("unit")
                if unit == "reqps":
                    rps_by_selection[selection][panel["title"]] = sum(values)
                if target.get("legendFormat") in {"p50", "p95", "p99"}:
                    if len(values) != 1:
                        raise ValueError("Latency must be aggregated over the selected instances")
                    quantiles[target["legendFormat"]] = values[0]
            if quantiles and not quantiles["p50"] <= quantiles["p95"] <= quantiles["p99"]:
                raise ValueError("Latency percentiles are not ordered")

    for title, all_value in rps_by_selection["all"].items():
        dc_sum = sum(rps_by_selection[f"dc:{dc}"][title] for dc in datacenters)
        pod_sum = sum(rps_by_selection[f"pod:{pod}@{dc}"][title] for pod, dc in identities)
        multi_value = rps_by_selection["multi"][title]
        if not all(math.isclose(value, all_value, rel_tol=1e-6, abs_tol=1e-8) for value in (dc_sum, pod_sum, multi_value)):
            raise ValueError(f"RPS aggregation mismatch for {title}")

    coverage_samples = query(args.prometheus, "sum by (pod_id, datacenter, method, route) (http_requests_total)", evaluation_time)
    coverage = {
        (sample["metric"]["pod_id"], sample["metric"]["datacenter"], sample["metric"]["method"], sample["metric"]["route"])
        for sample in coverage_samples if sample_value(sample) > 0
    }
    expected = {(pod, dc, method, route) for pod, dc in identities for method, route in handlers}
    if not expected.issubset(coverage):
        raise ValueError("Load has not reached every handler on every instance")

    rps_samples = query(args.prometheus, "sum(rate(http_requests_total[1m]))", evaluation_time)
    rps = sample_value(rps_samples[0]) if rps_samples else 0.0
    if not args.min_rps <= rps <= args.max_rps:
        raise ValueError(f"Total RPS {rps:.2f} outside {args.min_rps:g}–{args.max_rps:g}; wait for ramp-up")
    errors = query(args.prometheus, 'sum by (status_code) (rate(http_requests_total{status_code=~"4..|5.."}[1m]))', evaluation_time)
    error_rates = {sample["metric"]["status_code"]: sample_value(sample) for sample in errors if sample_value(sample) > 0}
    if len(error_rates) < 2:
        raise ValueError("Fewer than two error status codes are visible")
    return {
        "result": "PASS",
        "healthy_instances": len(identities),
        "prometheus_targets_up": len(application_targets),
        "handlers": len(handlers),
        "panels": len(panels),
        "total_rps_1m": round(rps, 2),
        "error_rps_by_code": {code: round(value, 3) for code, value in sorted(error_rates.items())},
        "error_fraction_1m": round(sum(error_rates.values()) / rps, 4),
        "dashboard_editable": False,
        "multi_and_all": True,
        "evaluation_timestamp": round(evaluation_time, 6),
        "panel_query_checks": panel_query_checks,
        "selection_rps_1m": {name: round(sum(values.values()), 6) for name, values in rps_by_selection.items()},
        "rps_additivity_by_handler": True,
        "ordered_latency_percentiles": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apps", nargs="+", default=["http://localhost:8001", "http://localhost:8002"])
    parser.add_argument("--prometheus", default="http://localhost:9090")
    parser.add_argument("--grafana", default="http://localhost:3000")
    parser.add_argument("--datasource-uid", default="task1-prometheus")
    parser.add_argument("--dashboard-uid", default="items-golden-signals")
    parser.add_argument("--wait", type=float, default=180, help="Seconds to wait for data and ramp-up")
    parser.add_argument("--min-rps", type=float, default=20)
    parser.add_argument("--max-rps", type=float, default=50)
    args = parser.parse_args()
    if not all(math.isfinite(value) for value in (args.wait, args.min_rps, args.max_rps)) or args.wait < 0 or not 0 < args.min_rps <= args.max_rps:
        parser.error("wait must be nonnegative; RPS limits must be finite and positive")
    settings = local_settings()
    if not settings.get("GRAFANA_ADMIN_USER") or not settings.get("GRAFANA_ADMIN_PASSWORD"):
        parser.error("Set Grafana credentials in .env or the environment")
    deadline = time.monotonic() + args.wait
    while True:
        try:
            result = inspect(args, settings)
        except (HTTPError, URLError, TimeoutError, ValueError, KeyError, TypeError, IndexError) as error:
            if time.monotonic() >= deadline:
                print(f"FAIL: {error}", file=sys.stderr)
                return 1
            time.sleep(min(3, max(0, deadline - time.monotonic())))
        else:
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
