"""Run from the project root: python -m unittest discover -v."""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from uuid import UUID

from fastapi.testclient import TestClient
from fastapi.routing import iter_route_contexts

from app.main import create_app


class ItemsApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = self.enterContext(TestClient(create_app()))

    def test_create_read_and_replace(self) -> None:
        created = self.client.post("/items", json={"name": "first", "description": "old"})
        self.assertEqual(created.status_code, 201)
        original = created.json()
        UUID(original["id"])
        path = f"/items/{original['id']}"
        self.assertEqual(self.client.get(path).json(), original)

        # PUT replaces the representation: an omitted description gets its
        # default instead of retaining the old value.
        updated = self.client.put(path, json={"name": "second"})
        self.assertEqual(updated.status_code, 200)
        expected = {"id": original["id"], "name": "second", "description": ""}
        self.assertEqual(updated.json(), expected)
        self.assertEqual(self.client.get(path).json(), expected)

    def test_unknown_ids_are_404_and_put_does_not_insert(self) -> None:
        for item_id in ["missing", "00000000-0000-0000-0000-000000000000"]:
            with self.subTest(item_id=item_id):
                path = f"/items/{item_id}"
                self.assertEqual(self.client.get(path).status_code, 404)
                self.assertEqual(self.client.put(path, json={"name": "new"}).status_code, 404)
                self.assertEqual(self.client.get(path).status_code, 404)

    def test_invalid_payloads_are_rejected_without_changing_the_item(self) -> None:
        original = self.client.post("/items", json={"name": "unchanged"}).json()
        path = f"/items/{original['id']}"
        invalid_payloads = [
            {},
            {"name": ""},
            {"name": "x" * 201},
            {"name": "valid", "description": "x" * 2001},
            {"name": "valid", "unexpected": True},
        ]
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.assertEqual(self.client.post("/items", json=payload).status_code, 422)
                self.assertEqual(self.client.put(path, json=payload).status_code, 422)
                self.assertEqual(self.client.get(path).json(), original)

    def test_application_instances_have_independent_stores(self) -> None:
        original = self.client.post("/items", json={"name": "only in first"}).json()
        with TestClient(create_app()) as another:
            self.assertEqual(another.get(f"/items/{original['id']}").status_code, 404)
        self.assertEqual(self.client.get(f"/items/{original['id']}").status_code, 200)

    def test_environment_is_read_once_per_application(self) -> None:
        with patch.dict(os.environ, {"POD_ID": "pod-a", "DATACENTER": "dc-a"}):
            first = create_app()
        with patch.dict(os.environ, {"POD_ID": "pod-b", "DATACENTER": "dc-b"}):
            second = create_app()
            with TestClient(first) as first_client, TestClient(second) as second_client:
                self.assertEqual(
                    first_client.get("/health").json(),
                    {"status": "ok", "pod_id": "pod-a", "datacenter": "dc-a"},
                )
                self.assertEqual(
                    second_client.get("/health").json(),
                    {"status": "ok", "pod_id": "pod-b", "datacenter": "dc-b"},
                )

    def test_exactly_three_business_and_three_service_routes_are_registered(self) -> None:
        registered = {
            (method, route.path)
            for route in iter_route_contexts(self.client.app.routes)
            for method in (route.methods or ())
        }
        self.assertEqual(
            registered,
            {
                ("POST", "/items"),
                ("GET", "/items/{id}"),
                ("PUT", "/items/{id}"),
                ("GET", "/health"),
                ("GET", "/metrics"),
                ("GET", "/handlers"),
            },
        )
        for path in ["/docs", "/redoc", "/openapi.json"]:
            self.assertEqual(self.client.get(path).status_code, 404)

    def test_concurrent_creates_remain_readable(self) -> None:
        def create(index: int) -> dict:
            response = self.client.post("/items", json={"name": f"item-{index}"})
            self.assertEqual(response.status_code, 201)
            return response.json()

        with ThreadPoolExecutor(max_workers=8) as pool:
            items = list(pool.map(create, range(24)))
        self.assertEqual(len({item["id"] for item in items}), 24)
        for item in items:
            self.assertEqual(self.client.get(f"/items/{item['id']}").json(), item)


if __name__ == "__main__":
    unittest.main()
