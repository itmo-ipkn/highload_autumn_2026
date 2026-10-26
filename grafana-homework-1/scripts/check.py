"""End-to-end lab check. Run after 2-3 minutes of background traffic."""
import json
import math
import os
import sys
import time
from uuid import uuid4
from urllib.parse import urlencode
from urllib.request import urlopen

from generate_dashboard import DASHBOARD_UID, discover_handlers, request_json
from load import send


def check(condition, message):
    if not condition:
        raise AssertionError(message)
    print("PASS:", message, flush=True)


def main():
    urls = os.getenv("APP_URLS", "http://app-1:8000,http://app-2:8000").split(",")
    grafana = os.getenv("GRAFANA_URL", "http://grafana:3000")
    credentials = os.environ["GRAFANA_ADMIN_USER"] + ":" + os.environ["GRAFANA_ADMIN_PASSWORD"]
    expected = {("POST", "/items"), ("GET", "/items/{id}"), ("PUT", "/items/{id}")}
    identities = []
    for base in urls:
        health = request_json(base + "/health")
        identities.append((health["pod_id"], health["datacenter"]))
        actual = {(h["method"], h["path"]) for h in request_json(base + "/handlers")["handlers"]}
        check(actual == expected, f"{base}: exactly three required business handlers")
        address = "fd20:" + ":".join(uuid4().hex[i:i+4] for i in range(0, 28, 4))
        body = {"name": "edge-switch", "ip_address": address, "kind": "switch", "state": "active"}
        code, item = send(base, "POST", "/items", body)
        check(code == 201, f"{base}: POST registers a device")
        item_id = item["id"]
        code, found = send(base, "GET", "/items/" + item_id)
        check(code == 200 and found["state"] == "active", f"{base}: GET reads its device record")
        check(send(base, "POST", "/items", body)[0] == 409, f"{base}: duplicate IP gives 409")
        body["state"] = "maintenance"
        code, changed = send(base, "PUT", "/items/" + item_id, body)
        check(code == 200 and changed["state"] == "maintenance", f"{base}: PUT changes maintenance state")
        check(send(base, "GET", "/items/missing-check-id")[0] == 404, f"{base}: missing id gives 404")
        check(send(base, "POST", "/items", {**body, "ip_address": "not-an-ip"})[0] == 422, f"{base}: invalid IP gives 422")
        check(send(base, "GET", "/items/" + item_id, fault=True)[0] == 500, f"{base}: controlled fault gives 500")
        with urlopen(base + "/metrics", timeout=5) as response:
            metrics = response.read().decode()
        check('route="/items/{id}"' in metrics and f'route="/items/{item_id}"' not in metrics,
              f"{base}: labels contain path templates, never item IDs")
        check(all(f'route="{path}"' not in metrics for path in ("/health", "/metrics", "/handlers")),
              f"{base}: service endpoints excluded from business metrics")
    check(len(set(p for p, _ in identities)) == 2 and len(set(dc for _, dc in identities)) == 2,
          "Two different pods AND datacenters")
    dashboard = request_json(grafana + "/api/dashboards/uid/" + DASHBOARD_UID, credentials=credentials)["dashboard"]
    check(dashboard["editable"] is False, "Dashboard is non-editable")
    check(len(dashboard["panels"]) == 2 + 3 * len(discover_handlers(urls)), "Three panels per discovered handler plus two overview panels")
    check(all(v["multi"] and v["includeAll"] for v in dashboard["templating"]["list"]), "Both selectors support multiple values and All")
    datasource = request_json(grafana + "/api/datasources/uid/prometheus-lab", credentials=credentials)
    check(datasource["url"] == "http://prometheus:9090" and datasource["readOnly"], "Provisioned datasource with fixed UID")
    targets = request_json("http://prometheus:9090/api/v1/targets")["data"]["activeTargets"]
    check(len(targets) == 2 and all(t["health"] == "up" for t in targets), "Prometheus scrapes BOTH backends")
    evaluated_at = time.time()

    def query(expression):
        url = "http://prometheus:9090/api/v1/query?" + urlencode({"query": expression, "time": evaluated_at})
        result = request_json(url)
        check(result["status"] == "success", "PromQL parsed successfully")
        return result["data"]["result"]

    for p in dashboard["panels"]:
        for t in p["targets"]:
            expression = t["expr"].replace("${datacenter:regex}", ".*").replace("${pod_id:regex}", ".*").replace("$__rate_interval", "1m")
            result = query(expression)
            check(bool(result) and all(math.isfinite(float(r["value"][1])) for r in result), "Panel has finite data: " + p["title"])
    total = query("sum(rate(netinventory_http_requests_total[1m]))")
    pods = query("sum by (pod_id, datacenter) (rate(netinventory_http_requests_total[1m]))")
    errors = query('sum by (status_code) (increase(netinventory_http_requests_total{status_code=~"4..|5.."}[5m]))')
    observed_codes = {r["metric"]["status_code"] for r in errors if float(r["value"][1]) > 0}
    check({"404", "409", "500"}.issubset(observed_codes), "Real collected metrics contain 404, 409 and 500 errors")
    total_rps = float(total[0]["value"][1])
    pod_rps = sum(float(p["value"][1]) for p in pods)
    check(len(pods) == 2 and math.isclose(total_rps, pod_rps, rel_tol=1e-8), "Total RPS equals sum of two pods at the same timestamp")
    for pod_id, datacenter in identities:
        filtered = query('sum(rate(netinventory_http_requests_total{pod_id="' + pod_id + '",datacenter="' + datacenter + '"}[1m]))')
        expected_rps = next(float(p["value"][1]) for p in pods if p["metric"]["pod_id"] == pod_id)
        check(math.isclose(float(filtered[0]["value"][1]), expected_rps, rel_tol=1e-8),
              f"Datacenter/pod selector isolates {pod_id} / {datacenter}")
    check(20 <= total_rps <= 50, f"Steady load is in 20-50 RPS range (actual {total_rps:.2f})")
    print("\nALL CHECKS PASSED")
    print(json.dumps({"total_rps": total_rps, "pods": [{**p["metric"], "rps": float(p["value"][1])} for p in pods]}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("FAIL:", error, file=sys.stderr)
        sys.exit(1)
