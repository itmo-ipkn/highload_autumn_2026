"""Small open-loop HTTP workload for the two independent Items instances.

Run with ``python -m tools.load_generator``. Rates are TOTAL across all targets.
The process runs until SIGTERM/SIGINT unless --duration is supplied. The normal
method cycle is POST, GET, PUT, GET, PUT (20/40/40 percent); missing requests are
selected by an accumulator against ALL admitted requests, not just the reads.
HTTP 500 responses are produced by the application's configured FAILURE_RATE.
"""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter, deque
from dataclasses import dataclass
import json
import math
import os
import signal
import sys
from typing import Callable, Mapping
from urllib.parse import quote, urlsplit

import httpx


METHOD_CYCLE = ("POST", "GET", "PUT", "GET", "PUT")
LogWriter = Callable[[dict[str, object]], None]


def emit_json(event: dict[str, object]) -> None:
    print(json.dumps(event, ensure_ascii=False, sort_keys=True), flush=True)


@dataclass(frozen=True, slots=True)
class LoadConfig:
    urls: tuple[str, ...] = ("http://app1:8000", "http://app2:8000")
    start_rps: float = 3.0
    target_rps: float = 36.0
    ramp_seconds: float = 120.0
    missing_rate: float = 0.03
    max_in_flight: int = 64
    request_timeout: float = 5.0
    stats_interval: float = 10.0
    pool_size: int = 256
    seed_attempts: int = 5

    def __post_init__(self) -> None:
        if not self.urls or len(set(self.urls)) != len(self.urls):
            raise ValueError("APP_URLS must contain distinct application URLs")
        for url in self.urls:
            parsed = urlsplit(url)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
                or url.endswith("/")
            ):
                raise ValueError("APP_URLS must contain HTTP(S) URLs without credentials, queries or trailing slashes")
            try:
                parsed.port
            except ValueError as exc:
                raise ValueError("APP_URLS contains an invalid port") from exc
        for name in ("start_rps", "target_rps", "request_timeout", "stats_interval"):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")
        if self.start_rps > self.target_rps:
            raise ValueError("start_rps cannot exceed target_rps")
        if not math.isfinite(self.ramp_seconds) or self.ramp_seconds < 0:
            raise ValueError("ramp_seconds must be finite and nonnegative")
        if not math.isfinite(self.missing_rate) or not 0 <= self.missing_rate <= 0.8:
            raise ValueError("missing_rate must be between 0 and 0.8 (the GET/PUT share)")
        for name in ("max_in_flight", "pool_size", "seed_attempts"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")

    @classmethod
    def from_environment(cls, environ: Mapping[str, str] | None = None) -> LoadConfig:
        env = os.environ if environ is None else environ
        try:
            return cls(
                urls=tuple(
                    part.strip().rstrip("/")
                    for part in env.get("APP_URLS", "http://app1:8000,http://app2:8000").split(",")
                    if part.strip()
                ),
                start_rps=float(env.get("LOAD_START_RPS", "3")),
                target_rps=float(env.get("LOAD_TARGET_RPS", "36")),
                ramp_seconds=float(env.get("LOAD_RAMP_SECONDS", "120")),
                missing_rate=float(env.get("LOAD_MISSING_RATE", "0.03")),
                max_in_flight=int(env.get("LOAD_MAX_IN_FLIGHT", "64")),
                request_timeout=float(env.get("LOAD_REQUEST_TIMEOUT", "5")),
                stats_interval=float(env.get("LOAD_STATS_INTERVAL", "10")),
                pool_size=int(env.get("LOAD_ID_POOL_SIZE", "256")),
                seed_attempts=int(env.get("LOAD_SEED_ATTEMPTS", "5")),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid load configuration: {exc}") from exc


@dataclass(frozen=True, slots=True)
class RampSchedule:
    start_rps: float
    target_rps: float
    ramp_seconds: float

    def rate_at(self, elapsed: float) -> float:
        if self.ramp_seconds == 0:
            return self.target_rps
        fraction = min(1.0, max(0.0, elapsed / self.ramp_seconds))
        return self.start_rps + (self.target_rps - self.start_rps) * fraction

    def cumulative(self, elapsed: float) -> float:
        """Integrated offered requests, useful for skipping missed deadlines."""
        elapsed = max(0.0, elapsed)
        if self.ramp_seconds == 0:
            return elapsed * self.target_rps
        during_ramp = min(elapsed, self.ramp_seconds)
        slope = (self.target_rps - self.start_rps) / self.ramp_seconds
        return (
            self.start_rps * during_ramp
            + 0.5 * slope * during_ramp**2
            + max(0.0, elapsed - self.ramp_seconds) * self.target_rps
        )

    def offset_for(self, request_number: int) -> float:
        """Invert the rate integral; request zero starts immediately."""
        if request_number < 0:
            raise ValueError("request_number must be nonnegative")
        if self.ramp_seconds == 0 or self.start_rps == self.target_rps:
            return request_number / self.target_rps
        ramp_requests = (self.start_rps + self.target_rps) * self.ramp_seconds / 2
        if request_number >= ramp_requests:
            return self.ramp_seconds + (request_number - ramp_requests) / self.target_rps
        slope = (self.target_rps - self.start_rps) / self.ramp_seconds
        # This equivalent quadratic-root form avoids cancellation near t=0.
        return 2 * request_number / (
            self.start_rps + math.sqrt(self.start_rps**2 + 2 * slope * request_number)
        )


@dataclass(frozen=True, slots=True)
class RequestPlan:
    target: int
    method: str
    path: str
    payload: dict[str, str] | None = None
    item_id: str | None = None
    intentional_missing: bool = False


class RequestPlanner:
    """Per-target IDs prevent accidental cross-instance 404 responses."""

    def __init__(self, config: LoadConfig) -> None:
        self.config = config
        self.ids: list[deque[str]] = [deque(maxlen=config.pool_size) for _ in config.urls]
        self.sequence = 0
        self._missing_budget = 0.0

    def remember(self, target: int, item_id: str) -> None:
        if item_id not in self.ids[target]:
            self.ids[target].append(item_id)

    def forget(self, target: int, item_id: str) -> None:
        try:
            self.ids[target].remove(item_id)
        except ValueError:
            pass

    def next_request(self) -> RequestPlan:
        number = self.sequence
        self.sequence += 1
        target = number % len(self.config.urls)
        method = METHOD_CYCLE[number % len(METHOD_CYCLE)]
        self._missing_budget += self.config.missing_rate
        missing = method != "POST" and self._missing_budget >= 1 - 1e-12
        if missing:
            self._missing_budget = max(0.0, self._missing_budget - 1)
        if method != "POST" and not missing and not self.ids[target]:
            # A restart can invalidate all remembered IDs. Refill via the same
            # business POST instead of spinning on known-stale reads.
            method = "POST"
        payload = {"name": f"load-{target}-{number}", "description": "background workload"}
        if method == "POST":
            return RequestPlan(target, method, "/items", payload)
        if missing:
            item_id = f"load-missing-{number}"
        else:
            self.ids[target].rotate(-1)
            item_id = self.ids[target][0]
        return RequestPlan(
            target=target,
            method=method,
            path=f"/items/{quote(item_id, safe='')}",
            payload=payload if method == "PUT" else None,
            item_id=item_id,
            intentional_missing=missing,
        )


class LoadGenerator:
    def __init__(self, config: LoadConfig, client: httpx.AsyncClient, log: LogWriter = emit_json) -> None:
        self.config = config
        self.client = client
        self.log = log
        self.planner = RequestPlanner(config)
        self.schedule = RampSchedule(config.start_rps, config.target_rps, config.ramp_seconds)
        self.tasks: set[asyncio.Task[None]] = set()
        self.counts: Counter[str] = Counter()
        self.status_codes: Counter[str] = Counter()
        self.by_target: list[Counter[str]] = [Counter() for _ in config.urls]
        self.fatal_error: Exception | None = None
        self._origin = 0.0
        self._last_report = 0.0
        self._previous_counts: Counter[str] = Counter()

    async def seed(self, stop: asyncio.Event) -> None:
        for target, url in enumerate(self.config.urls):
            last_result = "not attempted"
            for attempt in range(self.config.seed_attempts):
                if stop.is_set():
                    return
                try:
                    async with asyncio.timeout(self.config.request_timeout):
                        response = await self.client.post(
                            f"{url}/items",
                            json={"name": f"load-seed-{target}", "description": "initial item"},
                        )
                    last_result = f"HTTP {response.status_code}"
                    if response.status_code == 201:
                        item_id = self._read_id(response)
                        if item_id is not None:
                            self.planner.remember(target, item_id)
                            break
                        last_result = "201 response without a string id"
                except (httpx.HTTPError, TimeoutError) as exc:
                    last_result = type(exc).__name__
                if attempt + 1 < self.config.seed_attempts:
                    await self._wait_or_stop(stop, 0.2)
            else:
                raise RuntimeError(f"Cannot seed {url} after {self.config.seed_attempts} attempts ({last_result})")
        self.log({"event": "load_seeded", "targets": len(self.config.urls), "items_per_target": 1})

    @staticmethod
    def _read_id(response: httpx.Response) -> str | None:
        try:
            payload = response.json()
        except ValueError:
            return None
        value = payload.get("id") if isinstance(payload, dict) else None
        return value if isinstance(value, str) and value else None

    @staticmethod
    async def _wait_or_stop(stop: asyncio.Event, timeout: float) -> None:
        if timeout <= 0:
            return
        try:
            await asyncio.wait_for(stop.wait(), timeout=timeout)
        except TimeoutError:
            pass

    async def _send(self, plan: RequestPlan, stop: asyncio.Event) -> None:
        target_stats = self.by_target[plan.target]
        try:
            async with asyncio.timeout(self.config.request_timeout):
                response = await self.client.request(
                    plan.method, self.config.urls[plan.target] + plan.path, json=plan.payload
                )
            code = str(response.status_code)
            self.status_codes[code] += 1
            target_stats[code] += 1
            self.counts["completed"] += 1
            if plan.method == "POST" and response.status_code == 201:
                item_id = self._read_id(response)
                if item_id is not None:
                    self.planner.remember(plan.target, item_id)
                else:
                    self.counts["invalid_create_response"] += 1
            if response.status_code == 404 and not plan.intentional_missing and plan.item_id:
                self.planner.forget(plan.target, plan.item_id)
                self.counts["unexpected_404"] += 1
        except (httpx.HTTPError, TimeoutError):
            self.counts["network_errors"] += 1
            self.counts["completed"] += 1
            target_stats["network_errors"] += 1
        except asyncio.CancelledError:
            self.counts["cancelled"] += 1
            raise
        except Exception as exc:
            self.fatal_error = exc
            self.counts["internal_errors"] += 1
            stop.set()

    def _report(self, *, final: bool = False) -> None:
        now = asyncio.get_running_loop().time()
        elapsed = now - self._origin
        interval = max(now - self._last_report, 1e-9)
        rates = {
            f"{key}_rps": round((self.counts[key] - self._previous_counts[key]) / interval, 3)
            for key in ("offered", "sent", "completed")
        }
        self.log(
            {
                "event": "load_finished" if final else "load_stats",
                "elapsed_seconds": round(elapsed, 3),
                "interval_seconds": round(interval, 3),
                "target_rps": round(self.schedule.rate_at(elapsed), 3),
                **rates,
                "totals": dict(self.counts),
                "status_codes": dict(self.status_codes),
                "in_flight": len(self.tasks),
                "targets": {
                    url: dict(stats) for url, stats in zip(self.config.urls, self.by_target, strict=True)
                },
            }
        )
        self._last_report = now
        self._previous_counts = self.counts.copy()

    async def _reporter(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self._wait_or_stop(stop, self.config.stats_interval)
            if not stop.is_set():
                self._report()

    async def run(self, stop: asyncio.Event, duration: float | None = None) -> None:
        if duration is not None and (not math.isfinite(duration) or duration <= 0):
            raise ValueError("duration must be a finite positive number")
        await self.seed(stop)
        if stop.is_set():
            return
        loop = asyncio.get_running_loop()
        self._origin = self._last_report = loop.time()
        deadline = self._origin + duration if duration is not None else math.inf
        reporter = asyncio.create_task(self._reporter(stop))
        cursor = 0
        self.log(
            {
                "event": "load_started",
                "urls": list(self.config.urls),
                "start_rps": self.config.start_rps,
                "target_rps": self.config.target_rps,
                "ramp_seconds": self.config.ramp_seconds,
                "missing_rate": self.config.missing_rate,
                "max_in_flight": self.config.max_in_flight,
                "duration_seconds": duration,
            }
        )
        try:
            while not stop.is_set():
                due = self._origin + self.schedule.offset_for(cursor)
                await self._wait_or_stop(stop, min(due, deadline) - loop.time())
                now = loop.time()
                if stop.is_set() or now >= deadline:
                    break
                elapsed = now - self._origin
                # Do not replay an accumulated backlog after a process stall.
                # Skip missed slots and wait for the next future deadline.
                if now - due > 1 / self.schedule.rate_at(elapsed):
                    next_cursor = max(cursor + 1, math.ceil(self.schedule.cumulative(elapsed)))
                    skipped = next_cursor - cursor
                    self.counts["offered"] += skipped
                    self.counts["skipped_late"] += skipped
                    cursor = next_cursor
                    continue
                self.counts["offered"] += 1
                cursor += 1
                if len(self.tasks) >= self.config.max_in_flight:
                    self.counts["skipped_capacity"] += 1
                    continue
                plan = self.planner.next_request()
                self.counts["sent"] += 1
                self.by_target[plan.target]["sent"] += 1
                if plan.intentional_missing:
                    self.counts["intentional_missing"] += 1
                task = asyncio.create_task(self._send(plan, stop))
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
        finally:
            stop.set()
            await reporter
            if self.tasks:
                await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
            self._report(final=True)
        if self.fatal_error is not None:
            raise RuntimeError("Unexpected error inside a load worker") from self.fatal_error


async def async_main(config: LoadConfig, duration: float | None) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    installed_signals: list[signal.Signals] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, stop.set)
            installed_signals.append(signum)
        except (NotImplementedError, RuntimeError):
            # asyncio.run still handles Ctrl+C on platforms without this API.
            pass
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(config.request_timeout),
            limits=httpx.Limits(
                max_connections=config.max_in_flight,
                max_keepalive_connections=config.max_in_flight,
            ),
            trust_env=False,
        ) as client:
            await LoadGenerator(config, client).run(stop, duration)
    finally:
        for signum in installed_signals:
            loop.remove_signal_handler(signum)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, help="Stop after this many workload seconds (after seeding)")
    parser.add_argument("--ramp-seconds", type=float, help="Override LOAD_RAMP_SECONDS; 0 starts at target rate")
    parser.add_argument("--target-rps", type=float, help="Override total LOAD_TARGET_RPS")
    parser.add_argument("--start-rps", type=float, help="Override total LOAD_START_RPS")
    parser.add_argument("--urls", help="Override comma-separated APP_URLS")
    args = parser.parse_args(argv)
    env = dict(os.environ)
    for key, value in (
        ("APP_URLS", args.urls),
        ("LOAD_RAMP_SECONDS", args.ramp_seconds),
        ("LOAD_TARGET_RPS", args.target_rps),
        ("LOAD_START_RPS", args.start_rps),
    ):
        if value is not None:
            env[key] = str(value)
    try:
        config = LoadConfig.from_environment(env)
        if args.duration is not None and (not math.isfinite(args.duration) or args.duration <= 0):
            raise ValueError("duration must be a finite positive number")
    except ValueError as exc:
        parser.error(str(exc))
    try:
        asyncio.run(async_main(config, args.duration))
    except KeyboardInterrupt:
        return 0
    except Exception as exc:
        emit_json({"event": "load_failed", "error": str(exc), "type": type(exc).__name__})
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
