import json
import math
import os
import queue
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True)
class Job:
    pod: int
    method: str
    path: str
    payload: dict[str, str] | None
    expected_status: int


def request(base_url: str, method: str, path: str, payload: dict[str, str] | None = None) -> tuple[int, bytes]:
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if payload is not None else {}
    http_request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(http_request, timeout=3) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def plan_job(sequence: int, item_ids: list[int]) -> Job:
    pod = (sequence // 2) % len(item_ids)
    slot = sequence % 50
    if slot == 0:
        return Job(pod, "POST", "/items", {"name": f"load-{sequence}"}, 201)
    if slot == 11:
        return Job(pod, "GET", "/items/2147483647", None, 404)
    if slot == 23:
        return Job(pod, "GET", "/handlers", None, 200)
    if slot == 37:
        return Job(pod, "POST", "/items", {"name": ""}, 422)
    if slot % 2 == 0:
        return Job(pod, "GET", f"/items/{item_ids[pod]}", None, 200)
    return Job(pod, "PUT", f"/items/{item_ids[pod]}", {"name": f"updated-{sequence}"}, 200)


def ramp_rate(elapsed: float, initial: float, target: float, ramp_seconds: float) -> float:
    return initial + (target - initial) * min(elapsed / ramp_seconds, 1.0)


def worker(jobs: queue.Queue[Job], app_urls: list[str], stats: dict, lock: threading.Lock) -> None:
    while True:
        job = jobs.get()
        try:
            try:
                status, _ = request(app_urls[job.pod], job.method, job.path, job.payload)
            except (urllib.error.URLError, OSError, TimeoutError):
                status = 0
            with lock:
                stats["completed"] += 1
                stats["statuses"][status] += 1
                stats["pods"][f"pod-{job.pod + 1}"] += 1
                if status != job.expected_status:
                    stats["unexpected"] += 1
        finally:
            jobs.task_done()


def read_positive_number(name: str, default: str) -> float:
    value = float(os.getenv(name, default))
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


def main() -> None:
    app_urls = [url.strip() for url in os.environ["APP_URLS"].split(",") if url.strip()]
    if len(app_urls) != 2:
        raise ValueError("APP_URLS must contain exactly two application URLs")
    initial_rps = read_positive_number("LOAD_INITIAL_RPS", "2")
    target_rps = read_positive_number("LOAD_TARGET_RPS", "30")
    ramp_seconds = read_positive_number("LOAD_RAMP_SECONDS", "150")
    if target_rps < initial_rps:
        raise ValueError("LOAD_TARGET_RPS must be at least LOAD_INITIAL_RPS")

    item_ids = []
    for index, app_url in enumerate(app_urls):
        status, body = request(app_url, "POST", "/items", {"name": f"load-anchor-{index}"})
        if status != 201:
            raise RuntimeError(f"Could not create the load anchor on pod {index + 1}: HTTP {status}")
        item_ids.append(json.loads(body)["id"])

    jobs: queue.Queue[Job] = queue.Queue(maxsize=128)
    lock = threading.Lock()
    stats = {"completed": 0, "statuses": Counter(), "pods": Counter(), "unexpected": 0, "dropped": 0}
    for index in range(8):
        threading.Thread(
            target=worker,
            args=(jobs, app_urls, stats, lock),
            name=f"load-worker-{index}",
            daemon=True,
        ).start()

    print(
        f"Load started: {initial_rps:g} -> {target_rps:g} RPS over {ramp_seconds:g}s, "
        "then steady until stopped; expected 404 and 422 errors: 4%",
        flush=True,
    )
    started = time.monotonic()
    next_request = started
    last_report = started
    previous_completed = 0
    sequence = 0
    while True:
        delay = next_request - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        now = time.monotonic()
        rate = ramp_rate(now - started, initial_rps, target_rps, ramp_seconds)
        job = plan_job(sequence, item_ids)
        try:
            jobs.put(job, timeout=1)
        except queue.Full:
            with lock:
                stats["dropped"] += 1
        sequence += 1
        next_request = max(next_request, time.monotonic()) + 1 / rate

        now = time.monotonic()
        if now - last_report >= 30:
            with lock:
                completed = stats["completed"]
                statuses = dict(sorted(stats["statuses"].items()))
                pods = dict(sorted(stats["pods"].items()))
                unexpected = stats["unexpected"]
                dropped = stats["dropped"]
            observed_rps = (completed - previous_completed) / (now - last_report)
            print(
                f"elapsed={now - started:.0f}s target={rate:.1f} RPS "
                f"observed={observed_rps:.1f} RPS completed={completed} "
                f"statuses={statuses} pods={pods} unexpected={unexpected} dropped={dropped}",
                flush=True,
            )
            last_report = now
            previous_completed = completed


if __name__ == "__main__":
    main()
