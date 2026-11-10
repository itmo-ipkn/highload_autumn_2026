"""Тесты чистой сборки дашборда: контракт `docs/dashboard-contract.md` без сети."""

import json
from pathlib import Path

import pytest

from dashboard import DashboardError, build_dashboard

THREE = ["GET /items/{id}", "POST /items", "PUT /items/{id}"]
FOUR = [*THREE, "DELETE /items/{id}"]

ARTIFACT = Path(__file__).resolve().parents[2] / "docs/examples/dashboard.json"

HANDLER = "POST /items"
EXPECTED_LATENCY = (
    'histogram_quantile(0.95, sum by (le, handler) '
    '(rate(http_request_duration_seconds_bucket{datacenter=~"$datacenter", pod_id=~"$pod", '
    'handler="POST /items"}[1m])))'
)
EXPECTED_RPS = (
    'sum by (handler) (rate(http_requests_total{datacenter=~"$datacenter", pod_id=~"$pod", '
    'handler="POST /items"}[1m]))'
)
EXPECTED_ERRORS = (
    'sum by (handler, status_code) (rate(http_requests_total{datacenter=~"$datacenter", '
    'pod_id=~"$pod", handler="POST /items", status_code=~"4..|5.."}[1m]))'
)


def panels_of(dashboard, handler):
    return [p for p in dashboard["panels"] if p["title"].startswith(f"{handler} — ")]


def targets(dashboard, handler, suffix):
    (panel,) = [p for p in panels_of(dashboard, handler) if p["title"].endswith(suffix)]
    return panel, panel["targets"]


def test_three_handlers_build_nine_panels():
    dashboard = build_dashboard(THREE)

    assert len(dashboard["panels"]) == 9
    assert [p["id"] for p in dashboard["panels"]] == list(range(1, 10))


def test_four_handlers_build_twelve_panels():
    dashboard = build_dashboard(FOUR)

    assert len(dashboard["panels"]) == 12
    assert len({p["title"] for p in dashboard["panels"]}) == 12


def test_each_handler_gets_latency_rps_errors():
    dashboard = build_dashboard(THREE)

    for handler in THREE:
        suffixes = sorted(p["title"].split(" — ")[1] for p in panels_of(dashboard, handler))
        assert suffixes == ["errors", "latency", "rps"]


def test_latency_panel_has_three_quantile_targets():
    _, latency = targets(build_dashboard(THREE), HANDLER, "latency")

    assert [t["refId"] for t in latency] == ["A", "B", "C"]
    assert [t["legendFormat"] for t in latency] == ["p50", "p95", "p99"]
    assert "histogram_quantile(0.5," in latency[0]["expr"]
    assert latency[1]["expr"] == EXPECTED_LATENCY
    assert "histogram_quantile(0.99," in latency[2]["expr"]


def test_expression_matches_contract_rps():
    _, rps = targets(build_dashboard(THREE), HANDLER, "rps")

    assert len(rps) == 1
    assert rps[0]["expr"] == EXPECTED_RPS
    assert rps[0]["legendFormat"] == "{{handler}}"


def test_expression_matches_contract_errors():
    _, errors = targets(build_dashboard(THREE), HANDLER, "errors")

    assert len(errors) == 1
    assert errors[0]["expr"] == EXPECTED_ERRORS
    assert errors[0]["legendFormat"] == "{{status_code}}"


def test_no_or_vector_zero_fallback_in_errors():
    dashboard = build_dashboard(THREE)

    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            assert "or vector(0)" not in target["expr"]


def test_expression_bound_to_its_own_handler():
    dashboard = build_dashboard(THREE)

    for handler in THREE:
        _, rps = targets(dashboard, handler, "rps")
        assert f'handler="{handler}"' in rps[0]["expr"]
        others = [h for h in THREE if h != handler]
        for other in others:
            assert f'handler="{other}"' not in rps[0]["expr"]


def test_datasource_uid_on_panels_targets_and_variables():
    dashboard = build_dashboard(THREE)

    for panel in dashboard["panels"]:
        assert panel["datasource"]["uid"] == "prometheus-main"
        for target in panel["targets"]:
            assert target["datasource"]["uid"] == "prometheus-main"
    for variable in dashboard["templating"]["list"]:
        assert variable["datasource"]["uid"] == "prometheus-main"


def test_panels_layout_is_three_columns_of_three_rows():
    dashboard = build_dashboard(THREE)

    grid = {(p["gridPos"]["x"], p["gridPos"]["y"]) for p in dashboard["panels"]}
    assert grid == {(0, 0), (8, 0), (16, 0), (0, 8), (8, 8), (16, 8), (0, 16), (8, 16), (16, 16)}
    assert all(p["gridPos"]["w"] == 8 and p["gridPos"]["h"] == 8 for p in dashboard["panels"])


def test_panel_units_follow_contract():
    dashboard = build_dashboard(THREE)

    for panel in dashboard["panels"]:
        is_latency = any("histogram_quantile" in target["expr"] for target in panel["targets"])
        assert panel["fieldConfig"]["defaults"]["unit"] == ("s" if is_latency else "reqps")


def test_panels_are_timeseries():
    for panel in build_dashboard(THREE)["panels"]:
        assert panel["type"] == "timeseries"


def test_dashboard_metadata_follows_contract():
    dashboard = build_dashboard(THREE)

    assert dashboard["uid"] == "hw1-items-overview"
    assert dashboard["title"] == "HW-1 Items Service"
    assert dashboard["tags"] == ["hw1", "devops"]
    assert dashboard["timezone"] == "browser"
    assert dashboard["graphTooltip"] == 1
    assert dashboard["editable"] is False
    assert dashboard["schemaVersion"] == 39
    assert dashboard["refresh"] == "10s"
    assert dashboard["time"] == {"from": "now-15m", "to": "now"}
    assert "version" not in dashboard


def test_built_dashboard_equals_committed_artifact():
    artifact = json.loads(ARTIFACT.read_text(encoding="utf-8"))

    assert build_dashboard(THREE) == artifact, (
        "docs/examples/dashboard.json разошёлся с кодом: перегенерируйте артефакт "
        "(generator.py --dry-run --out docs/examples/dashboard.json)"
    )


def test_variables_are_datacenter_then_pod():
    variables = build_dashboard(THREE)["templating"]["list"]

    assert [v["name"] for v in variables] == ["datacenter", "pod"]
    assert variables[0]["query"] == "label_values(http_requests_total, datacenter)"
    assert variables[1]["query"] == (
        'label_values(http_requests_total{datacenter=~"$datacenter"}, pod_id)'
    )
    for variable in variables:
        assert variable["multi"] is True
        assert variable["includeAll"] is True
        assert variable["allValue"] == ".*"


def test_handler_variable_is_not_defined():
    names = {v["name"] for v in build_dashboard(THREE)["templating"]["list"]}

    assert "handler" not in names


def test_build_is_deterministic_and_order_insensitive():
    first = build_dashboard(THREE)
    second = build_dashboard(list(reversed(THREE)))

    assert first == second


def test_input_list_is_not_mutated():
    handlers = list(THREE)
    build_dashboard(handlers)

    assert handlers == THREE


@pytest.mark.parametrize("handlers", [[], ()])
def test_empty_handlers_rejected(handlers):
    with pytest.raises(DashboardError):
        build_dashboard(handlers)


@pytest.mark.parametrize(
    "handlers",
    [
        ["POST /items", "POST /items"],
        ["POST /items", " POST /items "],
        [""],
        ["   "],
        ["POST"],
        [None],
        [123],
    ],
)
def test_invalid_handlers_rejected(handlers):
    with pytest.raises(DashboardError):
        build_dashboard(handlers)
