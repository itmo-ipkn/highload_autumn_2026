"""Run bounded background HTTP load until SIGTERM/SIGINT, using stdlib only."""
from __future__ import annotations

from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import math
import os
from pathlib import Path
import random
import signal
import sys
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

MISSING_ID = "18446744073709551614"


@dataclass(frozen=True)
class LoadConfig:
    apps: tuple[str, ...]
    target_rps: float = 30.0
    ramp_seconds: float = 120.0
    error_percent: float = 4.0
    workers: int = 16

    @classmethod
    def from_env(cls, environment=None):
        environment = os.environ if environment is None else environment
        apps = tuple(dict.fromkeys(url.strip().rstrip("/") for url in environment.get(
            "APP_URLS", "http://app-1:8080,http://app-2:8080").split(",") if url.strip()))
        if not apps:
            raise ValueError("APP_URLS must contain at least one application URL")
        for app in apps:
            parsed = urlsplit(app)
            if parsed.scheme not in ("http", "https") or not parsed.hostname:
                raise ValueError("APP_URLS must contain HTTP or HTTPS base URLs")
            if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
                raise ValueError("Application base URLs must not contain credentials, query parameters or fragments")
        result = cls(apps, float(environment.get("LOAD_RPS", "30")),
                     float(environment.get("LOAD_RAMP_SECONDS", "120")),
                     float(environment.get("LOAD_ERROR_PERCENT", "4")),
                     int(environment.get("LOAD_WORKERS", "16")))
        if not math.isfinite(result.target_rps) or result.target_rps <= 0:
            raise ValueError("LOAD_RPS must be finite and positive")
        if not math.isfinite(result.ramp_seconds) or result.ramp_seconds < 0:
            raise ValueError("LOAD_RAMP_SECONDS must be finite and non-negative")
        if not math.isfinite(result.error_percent) or not 0 <= result.error_percent < 100:
            raise ValueError("LOAD_ERROR_PERCENT must be in [0, 100)")
        if result.workers < 1:
            raise ValueError("LOAD_WORKERS must be positive")
        return result


def rate_at(config: LoadConfig, elapsed: float) -> float:
    initial = min(2.0, config.target_rps)
    fraction = min(1.0, max(0.0, elapsed) / config.ramp_seconds) if config.ramp_seconds else 1.0
    return initial + (config.target_rps - initial) * fraction


@dataclass(frozen=True)
class RequestSpec:
    app: str
    method: str
    path: str
    data: dict | None = None
    fault: bool = False
    expected_status: int = 200


def send(spec: RequestSpec) -> tuple[int, dict]:
    headers = {"Content-Type": "application/json"}
    if spec.fault:
        headers["X-Demo-Fault"] = "500"
    request = Request(spec.app + spec.path, method=spec.method, headers=headers,
                      data=None if spec.data is None else json.dumps(spec.data).encode())
    try:
        with urlopen(request, timeout=3) as response:
            body = response.read()
            document = json.loads(body) if body else {}
            return (response.status, document) if isinstance(document, dict) else (0, {})
    except HTTPError as error:
        error.close()
        return error.code, {}
    except (URLError, TimeoutError, ConnectionError, OSError, ValueError):
        # Status 0 is a client/network/protocol failure, not an HTTP response.
        return 0, {}


class Workload:
    def __init__(self, config: LoadConfig):
        self.config = config
        self.ids = {app: deque(maxlen=128) for app in config.apps}
        self.lock = threading.Lock()
        self.rng = random.Random(2026)

    def remember(self, app: str, document: dict) -> bool:
        item_id = document.get("id")
        if not isinstance(item_id, (str, int)) or not str(item_id).isdigit():
            return False
        with self.lock:
            self.ids[app].append(str(item_id))
        return True

    def seed(self, stop: threading.Event) -> bool:
        for app in self.config.apps:
            if stop.is_set():
                return False
            status, document = send(RequestSpec(app, "POST", "/items", {"name": "load-seed", "value": 0},
                                               expected_status=201))
            if status != 201 or not self.remember(app, document):
                raise RuntimeError(f"Cannot seed {app}: HTTP {status} or invalid item ID")
        return True

    def choose(self, index: int) -> RequestSpec:
        app = self.config.apps[index % len(self.config.apps)]
        method = ("POST", "GET", "PUT")[(index // len(self.config.apps)) % 3]
        with self.lock:
            item_id = self.rng.choice(tuple(self.ids[app]))
        path = "/items" if method == "POST" else "/items/" + item_id
        data = {"name": "background-load", "value": index} if method != "GET" else None
        roll = self.rng.random()
        fraction = self.config.error_percent / 100.0
        if roll < fraction / 2:
            return RequestSpec(app, method, path, data, fault=True, expected_status=500)
        if roll < fraction:
            return RequestSpec(app, "GET", "/items/" + MISSING_ID, expected_status=404)
        if index % 97 == 96:
            return RequestSpec(app, "GET", "/handlers")
        return RequestSpec(app, method, path, data, expected_status=201 if method == "POST" else 200)


def log(event: str, **values) -> None:
    print(json.dumps({"event": event, **values}, sort_keys=True), flush=True)


def wait_for_dashboard(stop: threading.Event) -> bool:
    filename = os.getenv("DASHBOARD_READY_FILE")
    if not filename:
        return True
    log("waiting_for_dashboard")
    deadline = time.monotonic() + 120
    observed = None
    unchanged_since = time.monotonic()
    while not stop.is_set():
        try:
            path = Path(filename)
            stamp = path.stat().st_mtime_ns
            ready = json.loads(path.read_text()).get("status") == "success"
        except (OSError, ValueError, AttributeError):
            stamp, ready = None, False
        now = time.monotonic()
        if stamp != observed or not ready:
            observed, unchanged_since = stamp, now
        # Podman may start the completed dependency again through --requires.
        # Require a stable success marker before making any application requests.
        if ready and now - unchanged_since >= 1:
            return True
        if now >= deadline:
            raise RuntimeError("Dashboard generator has not confirmed success within 120 seconds")
        stop.wait(0.2)
    return False


def run(config: LoadConfig, stop: threading.Event) -> int:
    if not wait_for_dashboard(stop):
        return 0
    workload = Workload(config)
    if not workload.seed(stop):
        return 0
    lock = threading.Lock()
    capacity = threading.BoundedSemaphore(config.workers)
    failed = threading.Event()
    codes = Counter()
    by_instance = Counter()
    planned_errors = Counter()
    unexpected = 0

    def work(spec: RequestSpec) -> None:
        nonlocal unexpected
        try:
            status, document = send(spec)
            valid_id = True
            if spec.method == "POST" and status == 201:
                valid_id = workload.remember(spec.app, document)
            with lock:
                codes[str(status)] += 1
                by_instance[spec.app] += 1
                unexpected += int(status != spec.expected_status or not valid_id)
        finally:
            capacity.release()

    def check_worker(future) -> None:
        error = future.exception()
        if error is not None:
            log("worker_error", error=type(error).__name__)
            failed.set()
            stop.set()

    started = next_request = last_report = time.monotonic()
    submitted = index = skipped = previous_completed = 0
    log("started", target_rps=config.target_rps, ramp_seconds=config.ramp_seconds,
        error_percent=config.error_percent, instances=len(config.apps), workers=config.workers)
    with ThreadPoolExecutor(max_workers=config.workers) as pool:
        while not stop.is_set():
            if stop.wait(max(0.0, next_request - time.monotonic())):
                break
            now = time.monotonic()
            rate = rate_at(config, now - started)
            if capacity.acquire(blocking=False):
                spec = workload.choose(index)
                pool.submit(work, spec).add_done_callback(check_worker)
                submitted += 1
                if spec.expected_status in (404, 500):
                    planned_errors[str(spec.expected_status)] += 1
            else:
                skipped += 1
            index += 1
            next_request += 1.0 / rate
            if next_request < now:
                missed = int((now - next_request) * rate) + 1
                skipped += missed
                index += missed
                next_request = now + 1.0 / rate
            if now - last_report >= 10:
                with lock:
                    counts, instances = dict(codes), dict(by_instance)
                    completed, unexpected_count = sum(codes.values()), unexpected
                log("progress", elapsed=round(now - started, 1), scheduled_rps=round(rate, 2),
                    completed_rps=round((completed - previous_completed) / (now - last_report), 2),
                    submitted=submitted, completed=completed, codes=counts,
                    by_instance=instances, planned_errors=dict(planned_errors),
                    unexpected=unexpected_count, skipped=skipped)
                previous_completed, last_report = completed, now
    log("stopped", submitted=submitted, completed=sum(codes.values()), codes=dict(codes),
        by_instance=dict(by_instance), unexpected=unexpected, skipped=skipped)
    return 1 if failed.is_set() else 0


def main() -> int:
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    try:
        return run(LoadConfig.from_env(), stop)
    except (ValueError, RuntimeError) as error:
        print("Load failed: " + str(error), file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
