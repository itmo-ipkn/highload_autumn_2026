"""Behaviour checks for the bounded, independently routed background workload."""

import asyncio
from collections import Counter
import unittest

import httpx

from tools.load_generator import LoadConfig, LoadGenerator, RampSchedule, RequestPlanner


class WorkloadPlanningTests(unittest.TestCase):
    def test_ramp_meets_endpoints_and_integrated_request_budget(self) -> None:
        schedule = RampSchedule(3, 36, 120)
        self.assertEqual(schedule.rate_at(0), 3)
        self.assertEqual(schedule.rate_at(60), 19.5)
        self.assertEqual(schedule.rate_at(120), 36)
        self.assertEqual(schedule.rate_at(300), 36)
        # Trapezoid area: 120 s * (3 + 36) / 2 = 2340 offered requests.
        self.assertEqual(schedule.cumulative(120), 2340)
        self.assertEqual(schedule.cumulative(130), 2700)
        self.assertEqual(schedule.offset_for(2340), 120)
        self.assertEqual(schedule.offset_for(2700), 130)
        previous = -1.0
        for number in range(3000):
            offset = schedule.offset_for(number)
            self.assertGreater(offset, previous)
            self.assertAlmostEqual(schedule.cumulative(offset), number, places=8)
            previous = offset

    def test_routing_method_mix_and_missing_share_apply_to_all_requests(self) -> None:
        planner = RequestPlanner(LoadConfig(urls=("http://one", "http://two")))
        planner.remember(0, "owned-by-one")
        planner.remember(1, "owned-by-two")
        plans = [planner.next_request() for _ in range(1000)]
        self.assertEqual(Counter(plan.target for plan in plans), {0: 500, 1: 500})
        self.assertEqual(Counter(plan.method for plan in plans), {"POST": 200, "GET": 400, "PUT": 400})
        self.assertEqual(sum(plan.intentional_missing for plan in plans), 30)
        for target in (0, 1):
            self.assertEqual({plan.method for plan in plans if plan.target == target}, {"GET", "POST", "PUT"})
            self.assertTrue(any(plan.intentional_missing for plan in plans if plan.target == target))
        for plan in plans:
            if plan.method == "POST" or plan.intentional_missing:
                continue
            self.assertEqual(plan.item_id, "owned-by-one" if plan.target == 0 else "owned-by-two")

    def test_id_memory_is_bounded_and_empty_pool_is_refilled(self) -> None:
        planner = RequestPlanner(LoadConfig(urls=("http://one", "http://two"), pool_size=2, missing_rate=0))
        for item_id in ("old", "middle", "new"):
            planner.remember(1, item_id)
        self.assertEqual(list(planner.ids[1]), ["middle", "new"])
        planner.forget(1, "middle")
        planner.forget(1, "new")
        planner.next_request()  # The first method is POST on target zero.
        plan = planner.next_request()  # GET on target one becomes a recovery POST.
        self.assertEqual((plan.target, plan.method, plan.path), (1, "POST", "/items"))

    def test_invalid_configuration_is_rejected(self) -> None:
        for override in (
            {"target_rps": float("nan")},
            {"ramp_seconds": -1},
            {"missing_rate": 0.9},
            {"start_rps": 40, "target_rps": 36},
            {"max_in_flight": 0},
            {"urls": ("http://admin:secret@host",)},
            {"urls": ("http://one", "http://one")},
        ):
            with self.subTest(override=override), self.assertRaises(ValueError):
                LoadConfig(**override)


class WorkloadRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_responses_drop_offers_instead_of_growing_concurrency(self) -> None:
        in_flight = 0
        peak = 0
        identifiers = 0
        seen: list[tuple[str, str]] = []

        async def transport(request: httpx.Request) -> httpx.Response:
            nonlocal in_flight, peak, identifiers
            in_flight += 1
            peak = max(peak, in_flight)
            seen.append((request.url.host, request.method))
            try:
                await asyncio.sleep(0.03)
                if request.method == "POST":
                    identifiers += 1
                    return httpx.Response(201, json={"id": f"{request.url.host}-{identifiers}"})
                return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1]})
            finally:
                in_flight -= 1

        config = LoadConfig(
            urls=("http://one", "http://two"),
            start_rps=300,
            target_rps=300,
            ramp_seconds=0,
            max_in_flight=2,
            missing_rate=0,
            stats_interval=0.05,
        )
        messages: list[dict] = []
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            workload = LoadGenerator(config, client, messages.append)
            await workload.run(asyncio.Event(), duration=0.25)
        self.assertLessEqual(peak, 2)
        self.assertGreater(workload.counts["skipped_capacity"], 0)
        self.assertEqual(workload.counts["sent"], workload.counts["completed"])
        self.assertEqual(in_flight, 0)
        self.assertEqual(len(workload.tasks), 0)
        self.assertEqual(messages[-1]["event"], "load_finished")
        self.assertEqual({host for host, _ in seen}, {"one", "two"})

    async def test_seed_failure_is_bounded_and_stops_before_workload(self) -> None:
        calls = 0

        async def transport(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(500)

        config = LoadConfig(urls=("http://one", "http://two"), seed_attempts=1)
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            workload = LoadGenerator(config, client, lambda event: None)
            with self.assertRaisesRegex(RuntimeError, "Cannot seed"):
                await workload.run(asyncio.Event(), duration=0.1)
        self.assertEqual(calls, 1)
        self.assertEqual(workload.counts["sent"], 0)

    async def test_network_failures_are_separate_from_http_statuses(self) -> None:
        async def transport(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("offline", request=request)

        config = LoadConfig(urls=("http://one", "http://two"))
        async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
            workload = LoadGenerator(config, client, lambda event: None)
            plan = workload.planner.next_request()
            await workload._send(plan, asyncio.Event())
        self.assertEqual(workload.counts["network_errors"], 1)
        self.assertEqual(workload.counts["completed"], 1)
        self.assertFalse(workload.status_codes)


if __name__ == "__main__":
    unittest.main()
