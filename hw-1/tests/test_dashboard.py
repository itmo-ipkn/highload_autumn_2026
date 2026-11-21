"""Contract and failure-mode checks for runtime Grafana dashboard generation."""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from urllib.error import HTTPError, URLError
from unittest.mock import patch

from tools.dashboard_generator import (
    GeneratorError,
    Handler,
    build_dashboard,
    main,
    parse_handlers,
    promql_string,
    publish_dashboard,
    request_json,
)


CATALOGUE = {
    "service": "test-service",
    "handlers": [
        {"method": "POST", "path": "/items"},
        {"method": "GET", "path": "/items/{id}"},
        {"method": "PUT", "path": "/items/{id}"},
    ],
}


class DashboardTests(unittest.TestCase):
    def dashboard(self):
        service, handlers = parse_handlers(CATALOGUE)
        return build_dashboard(handlers, service_name=service)

    def test_new_runtime_handler_adds_three_panels_without_generator_changes(self):
        existing = self.dashboard()
        extended = {**CATALOGUE, "handlers": CATALOGUE["handlers"] + [{"method": "PATCH", "path": "/new/{key}"}]}
        service, handlers = parse_handlers(extended)
        generated = build_dashboard(handlers, service_name=service)
        self.assertEqual(len(existing["panels"]), 9)
        self.assertEqual(len(generated["panels"]), 12)
        new_panels = [p for p in generated["panels"] if p["title"].startswith("PATCH /new/{key}")]
        self.assertEqual(len(new_panels), 3)
        for panel in new_panels:
            self.assertIn('method="PATCH"', panel["targets"][0]["expr"])
            self.assertIn('route="/new/{key}"', panel["targets"][0]["expr"])

    def test_queries_aggregate_selected_pods_in_the_required_order(self):
        panels = self.dashboard()["panels"]
        for index in range(0, len(panels), 3):
            latency, traffic, errors = panels[index:index + 3]
            self.assertEqual([t["legendFormat"] for t in latency["targets"]], ["p50", "p95", "p99"])
            for target, quantile in zip(latency["targets"], ["0.50", "0.95", "0.99"]):
                self.assertTrue(target["expr"].startswith(f"histogram_quantile({quantile}, sum by (le) (rate("))
                self.assertIn("http_request_duration_seconds_bucket", target["expr"])
            self.assertTrue(traffic["targets"][0]["expr"].startswith("sum(rate(http_requests_total"))
            self.assertTrue(errors["targets"][0]["expr"].startswith("sum by (status_code) (rate("))
            self.assertIn('status_code=~"4..|5.."', errors["targets"][0]["expr"])
            for panel in [latency, traffic, errors]:
                for target in panel["targets"]:
                    self.assertIn('datacenter=~"${datacenter:regex}"', target["expr"])
                    self.assertIn('pod_id=~"${pod_id:regex}"', target["expr"])
                    self.assertIn("[$__rate_interval]", target["expr"])

    def test_variables_support_multiple_values_all_and_dependent_pods(self):
        dashboard = self.dashboard()
        self.assertFalse(dashboard["editable"])
        for variable in dashboard["templating"]["list"]:
            self.assertTrue(variable["multi"])
            self.assertTrue(variable["includeAll"])
            self.assertEqual(variable["allValue"], ".*")
            self.assertEqual(variable["current"]["value"], ["$__all"])
            self.assertEqual(variable["datasource"]["uid"], "task1-prometheus")
        pod = dashboard["templating"]["list"][1]
        self.assertIn('datacenter=~"${datacenter:regex}"', pod["query"]["query"])
        for panel in dashboard["panels"]:
            self.assertEqual(panel["datasource"]["uid"], "task1-prometheus")
            for target in panel["targets"]:
                self.assertEqual(target["datasource"]["uid"], "task1-prometheus")

    def test_route_literals_cannot_break_promql_or_expand_grafana_variables(self):
        path = '/odd/"quoted"/\\backslash/$pod_id/[[datacenter]]/юникод'
        quoted = promql_string(path)
        self.assertEqual(json.loads(quoted), path)
        self.assertNotIn("$pod_id", quoted)
        self.assertNotIn("[[datacenter]]", quoted)
        dashboard = build_dashboard([Handler("GET", path)])
        self.assertIn(f"route={quoted}", dashboard["panels"][0]["targets"][0]["expr"])

    def test_catalogue_normalizes_duplicates_and_rejects_invalid_data(self):
        _, handlers = parse_handlers({"handlers": [{"method": "get", "path": "/x"}, {"method": "GET", "path": "/x"}]})
        self.assertEqual(handlers, [Handler("GET", "/x")])
        invalid = [None, {}, {"handlers": []}, {"handlers": ["GET /x"]},
                   {"handlers": [{"method": "BAD", "path": "/x"}]},
                   {"handlers": [{"method": "GET", "path": "relative"}]},
                   {"handlers": [{"method": "GET", "path": "/line\nbreak"}]}]
        for payload in invalid:
            with self.subTest(payload=payload), self.assertRaises(GeneratorError):
                parse_handlers(payload)

    def test_permanent_http_error_fails_immediately_without_leaking_response(self):
        failure = HTTPError("http://grafana/api/dashboards/db", 401, "Unauthorized", {}, io.BytesIO(b"sensitive response"))
        with patch("tools.dashboard_generator.request.urlopen", side_effect=failure) as opening, patch("tools.dashboard_generator.time.sleep") as sleep:
            with self.assertRaises(GeneratorError) as caught:
                request_json("POST", "http://grafana/api/dashboards/db", attempts=3)
        self.assertEqual(opening.call_count, 1)
        sleep.assert_not_called()
        self.assertIn("HTTP 401", str(caught.exception))
        self.assertNotIn("sensitive", str(caught.exception))

    def test_transient_startup_failure_is_retried(self):
        with patch("tools.dashboard_generator.request.urlopen", side_effect=[URLError("not ready"), io.BytesIO(b'{"ok": true}')]) as opening, patch("tools.dashboard_generator.time.sleep") as sleep:
            value = request_json("GET", "http://grafana/api/health", attempts=2)
        self.assertEqual(value, {"ok": True})
        self.assertEqual(opening.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_retry_exhaustion_and_invalid_json_fail(self):
        with patch("tools.dashboard_generator.request.urlopen", side_effect=URLError("not ready")) as opening, patch("tools.dashboard_generator.time.sleep"):
            with self.assertRaisesRegex(GeneratorError, "after 3 attempts"):
                request_json("GET", "http://grafana/api/health", attempts=3)
        self.assertEqual(opening.call_count, 3)
        with patch("tools.dashboard_generator.request.urlopen", return_value=io.BytesIO(b"not json")):
            with self.assertRaisesRegex(GeneratorError, "not valid JSON"):
                request_json("GET", "http://grafana/api/health")

    def test_publish_uses_fixed_uid_overwrite_and_verifies_saved_dashboard(self):
        dashboard = self.dashboard()
        replies = [
            {"uid": "task1-prometheus", "type": "prometheus"},
            {"uid": "items-golden-signals", "status": "success"},
            {"dashboard": dashboard},
        ]
        with patch("tools.dashboard_generator.request_json", side_effect=replies) as sending:
            url = publish_dashboard(dashboard, grafana_url="http://grafana:3000", username="admin", password="example")
        self.assertEqual(url, "http://grafana:3000/d/items-golden-signals")
        self.assertEqual(sending.call_count, 3)
        post = sending.call_args_list[1]
        self.assertEqual(post.args, ("POST", "http://grafana:3000/api/dashboards/db"))
        self.assertTrue(post.kwargs["payload"]["overwrite"])
        self.assertEqual(post.kwargs["payload"]["dashboard"]["uid"], "items-golden-signals")
        self.assertTrue(post.kwargs["headers"]["Authorization"].startswith("Basic "))

    def test_publish_cannot_report_success_when_verification_fails(self):
        dashboard = self.dashboard()
        replies = [
            {"uid": "task1-prometheus", "type": "prometheus"},
            {"uid": "items-golden-signals", "status": "success"},
            {"dashboard": {**dashboard, "editable": True}},
        ]
        with patch("tools.dashboard_generator.request_json", side_effect=replies):
            with self.assertRaisesRegex(GeneratorError, "editability"):
                publish_dashboard(dashboard, grafana_url="http://grafana:3000", username="admin", password="example")

    def test_dry_run_needs_only_the_application_and_no_credentials(self):
        output = io.StringIO()
        with patch.dict("os.environ", {}, clear=True), patch("tools.dashboard_generator.request_json", return_value=CATALOGUE) as sending, redirect_stdout(output), redirect_stderr(io.StringIO()):
            status = main(["--app-url", "http://example:8000", "--dry-run"])
        self.assertEqual(status, 0)
        self.assertEqual(len(json.loads(output.getvalue())["panels"]), 9)
        self.assertEqual(sending.call_count, 1)
        self.assertEqual(sending.call_args.args, ("GET", "http://example:8000/handlers"))

    def test_cli_returns_nonzero_when_grafana_rejects_creation(self):
        errors = io.StringIO()
        with patch("tools.dashboard_generator.request_json", return_value=CATALOGUE), patch("tools.dashboard_generator.publish_dashboard", side_effect=GeneratorError("HTTP 403")), redirect_stderr(errors):
            status = main(["--app-url", "http://example:8000"])
        self.assertEqual(status, 1)
        self.assertIn("HTTP 403", errors.getvalue())


if __name__ == "__main__":
    unittest.main()
