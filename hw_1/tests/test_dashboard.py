from scripts.generate_dashboard import build_dashboard


def test_dynamic_handlers_add_three_panels_per_handler():
    handlers = [
        {"method": "POST", "route": "/items"},
        {"method": "GET", "route": "/items/{id}"},
        {"method": "PUT", "route": "/items/{id}"},
    ]
    dashboard = build_dashboard(handlers)
    assert dashboard["editable"] is False
    assert len(dashboard["panels"]) == 9
    assert {v["name"] for v in dashboard["templating"]["list"]} == {"datacenter", "pod_id"}
    panel = dashboard["panels"][0]
    expr = panel["targets"][0]["expr"]
    assert "histogram_quantile(0.5, sum by (le) (rate(" in expr
    assert 'datacenter=~"${datacenter:regex}"' in expr
    assert 'pod_id=~"${pod_id:regex}"' in expr

    handlers.append({"method": "DELETE", "route": "/items/{id}"})
    expanded = build_dashboard(handlers)
    assert len(expanded["panels"]) == 12


def test_ignore_technical_routes():
    dashboard = build_dashboard([
        {"method": "GET", "route": "/health"},
        {"method": "GET", "route": "/handlers"},
        {"method": "POST", "route": "/items"},
    ])
    assert len(dashboard["panels"]) == 3
    errors = dashboard["panels"][2]["targets"][0]["expr"]
    assert 'status_code=~"4..|5.."' in errors
    assert "sum by (status_code) (rate(" in errors
