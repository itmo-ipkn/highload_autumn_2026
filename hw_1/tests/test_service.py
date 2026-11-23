import re

from fastapi.testclient import TestClient

from app.main import app


def test_crud_and_expected_statuses():
    with TestClient(app) as client:
        created = client.post("/items", json={"name": "hello", "value": 3})
        assert created.status_code == 201
        item_id = created.json()["id"]
        assert client.get("/items/" + item_id).json()["name"] == "hello"
        updated = client.put("/items/" + item_id, json={"name": "new", "value": 5})
        assert updated.status_code == 200
        assert updated.json()["value"] == 5
        assert client.get("/items/not-found").status_code == 404
        assert client.post("/items", json={"name": "__simulate_500__", "value": 0}).status_code == 500


def test_metrics_use_templates_and_have_expected_codes():
    with TestClient(app) as client:
        client.get("/items/unpredictable-1234567890")
        metrics = client.get("/metrics")
        assert metrics.status_code == 200
        body = metrics.text
        assert 'route="/items/{id}"' in body
        assert 'status_code="404"' in body
        assert 'status_code="500"' in body
        assert "http_request_duration_seconds_bucket" in body
        assert "http_requests_total" in body
        assert 'route="/metrics"' not in body
        assert 'route="/health"' not in body
        assert 'route="/handlers"' not in body
        assert 'route="/items/unpredictable-1234567890"' not in body


def test_handlers_are_discovered_and_not_manually_listed():
    with TestClient(app) as client:
        result = client.get("/handlers")
        assert result.status_code == 200
        assert result.json() == [
            {"method": "POST", "route": "/items"},
            {"method": "GET", "route": "/items/{id}"},
            {"method": "PUT", "route": "/items/{id}"},
        ]


def test_metrics_capture_validation_errors():
    with TestClient(app) as client:
        result = client.post("/items", json={"name": "", "value": 1})
        assert result.status_code == 422
        assert 'status_code="422"' in client.get("/metrics").text
