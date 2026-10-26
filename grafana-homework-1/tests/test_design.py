"""Check properties that are easy to accidentally break when extending the lab."""
import unittest

from fastapi import APIRouter, FastAPI

from app.monitoring import MonitoredRoute, registered_handlers
from scripts.generate_dashboard import build_dashboard


class DesignTests(unittest.TestCase):
    def test_new_registered_handler_appears_without_generator_changes(self):
        app = FastAPI()
        router = APIRouter(route_class=MonitoredRoute)

        @router.get("/arbitrary/{key}")
        async def arbitrary(key: str):
            return {"key": key}

        app.include_router(router)

        @app.get("/health")
        async def health():
            return {"ok": True}

        handlers = registered_handlers(app)
        self.assertEqual(handlers, [{"method": "GET", "path": "/arbitrary/{key}"}])
        dashboard = build_dashboard([(h["method"], h["path"]) for h in handlers])
        self.assertEqual(len(dashboard["panels"]), 5)
        self.assertIn("GET /arbitrary/{key}", dashboard["panels"][2]["title"])
        expr = dashboard["panels"][2]["targets"][0]["expr"]
        self.assertIn("sum by (le)", expr)
        self.assertNotIn("avg(", expr)

    def test_no_fixed_route_list_or_division_by_number_of_pods(self):
        handlers = [("PATCH", "/new"), ("GET", "/other")]
        dashboard = build_dashboard(handlers)
        self.assertFalse(dashboard["editable"])
        self.assertEqual(len(dashboard["panels"]), 8)
        rps = dashboard["panels"][3]["targets"][0]["expr"]
        self.assertTrue(rps.startswith("sum(rate("))
        errors = dashboard["panels"][4]["targets"][0]["expr"]
        self.assertIn('status_code=~"4..|5.."', errors)


if __name__ == "__main__":
    unittest.main()
