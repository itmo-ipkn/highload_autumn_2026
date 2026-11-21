"""Behavioral coverage for metrics, route discovery and both error paths."""

import unittest

from fastapi import APIRouter
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.telemetry import LATENCY_BUCKETS, Telemetry


class TelemetryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = create_app(Settings(pod_id="test-pod", datacenter="test-dc"))
        self.client = self.enterContext(TestClient(self.app, raise_server_exceptions=False))

    def sample(self, name, method, route, **extra):
        labels = {"pod_id": "test-pod", "datacenter": "test-dc", "method": method, "route": route}
        return self.app.state.telemetry.registry.get_sample_value(name, {**labels, **extra})

    def test_route_templates_and_real_statuses_are_recorded_once(self) -> None:
        item = self.client.post("/items", json={"name": "demo"}).json()
        self.client.get(f"/items/{item['id']}")
        self.client.get("/items/another-id")
        self.client.get("/items/yet-another-id")
        self.client.put(f"/items/{item['id']}", json={"name": "new"})
        self.client.post("/items", json={})

        self.assertEqual(self.sample("http_requests_total", "POST", "/items", status_code="201"), 1)
        self.assertEqual(self.sample("http_requests_total", "POST", "/items", status_code="422"), 1)
        self.assertEqual(self.sample("http_requests_total", "GET", "/items/{id}", status_code="200"), 1)
        self.assertEqual(self.sample("http_requests_total", "GET", "/items/{id}", status_code="404"), 2)
        self.assertEqual(self.sample("http_request_duration_seconds_count", "GET", "/items/{id}"), 3)
        exposition = self.client.get("/metrics").text
        self.assertNotIn(item["id"], exposition)
        self.assertNotIn("another-id", exposition)
        self.assertGreater(self.sample("http_request_duration_seconds_sum", "GET", "/items/{id}"), 0)

    def test_service_paths_unknown_paths_and_unsupported_methods_are_not_business_requests(self) -> None:
        before = self.app.state.telemetry.render()
        for path in ("/health", "/metrics", "/handlers", "/does-not-exist"):
            self.client.get(path)
        self.client.delete("/items/missing")
        self.assertEqual(self.app.state.telemetry.render(), before)

    def test_unhandled_exception_is_a_single_500_observation(self) -> None:
        @self.app.get("/broken")
        def broken():
            raise RuntimeError("deliberate test failure")

        self.assertEqual(self.client.get("/broken").status_code, 500)
        self.assertEqual(self.sample("http_requests_total", "GET", "/broken", status_code="500"), 1)
        self.assertEqual(self.sample("http_request_duration_seconds_count", "GET", "/broken"), 1)

    def test_configured_failures_do_not_affect_service_endpoints(self) -> None:
        failing = create_app(Settings("fault-pod", "fault-dc", failure_rate=1.0))
        with TestClient(failing) as client:
            self.assertEqual(client.post("/items", json={"name": "demo"}).status_code, 500)
            self.assertEqual(client.get("/items/missing").status_code, 500)
            self.assertEqual(client.put("/items/missing", json={"name": "demo"}).status_code, 500)
            for path in ("/health", "/metrics", "/handlers"):
                self.assertEqual(client.get(path).status_code, 200)
        total = sum(
            sample.value
            for family in failing.state.telemetry.registry.collect()
            for sample in family.samples
            if sample.name == "http_requests_total" and sample.labels["status_code"] == "500"
        )
        self.assertEqual(total, 3)

    def test_discovery_changes_when_a_business_route_is_added(self) -> None:
        initial = self.client.get("/handlers").json()["handlers"]
        self.assertEqual(len(initial), 3)

        @self.app.get("/widgets/{widget_id}")
        def widget(widget_id: str):
            return {"id": widget_id}

        handlers = self.client.get("/handlers").json()["handlers"]
        self.assertEqual(len(handlers), 4)
        self.assertIn({"method": "GET", "path": "/widgets/{widget_id}"}, handlers)
        self.assertEqual(self.client.get("/widgets/123").status_code, 200)
        self.assertEqual(self.sample("http_requests_total", "GET", "/widgets/{widget_id}", status_code="200"), 1)

    def test_independent_registries_have_compatible_cumulative_buckets(self) -> None:
        first = Telemetry("one", "east")
        second = Telemetry("two", "west")
        for duration in (0.01, 0.1):
            first.observe("GET", "/demo", 200, duration)
        for duration in (0.05, 0.2, 0.3):
            second.observe("GET", "/demo", 200, duration)

        def buckets(telemetry):
            return {
                sample.labels["le"]: sample.value
                for family in telemetry.registry.collect()
                for sample in family.samples
                if sample.name == "http_request_duration_seconds_bucket"
            }

        left, right = buckets(first), buckets(second)
        self.assertEqual(left.keys(), right.keys())
        self.assertEqual(len(left), len(LATENCY_BUCKETS) + 1)
        self.assertEqual(left["0.1"] + right["0.1"], 3)
        self.assertEqual(left["+Inf"] + right["+Inf"], 5)

    def test_router_prefix_is_the_same_in_discovery_and_metrics(self) -> None:
        router = APIRouter()

        @router.get("/widgets/{id}")
        def widgets(id: str):
            return {"id": id}

        self.app.include_router(router, prefix="/v2")
        self.assertIn(
            {"method": "GET", "path": "/v2/widgets/{id}"},
            self.client.get("/handlers").json()["handlers"],
        )
        self.assertEqual(self.client.get("/v2/widgets/42").status_code, 200)
        self.assertEqual(self.sample("http_requests_total", "GET", "/v2/widgets/{id}", status_code="200"), 1)

    def test_invalid_runtime_settings_fail_fast(self) -> None:
        for kwargs in ({"failure_rate": -0.1}, {"failure_rate": 1.1},
                       {"latency_min_ms": 10, "latency_max_ms": 1},
                       {"failure_rate": float("nan")}, {"latency_max_ms": float("inf")}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Settings("pod", "dc", **kwargs)


if __name__ == "__main__":
    unittest.main()
