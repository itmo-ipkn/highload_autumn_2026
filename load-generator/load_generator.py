import json
import os
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor


TARGET_URLS = [
    url.strip().rstrip("/")
    for url in os.getenv(
        "TARGET_URLS",
        "http://app1:8080,http://app2:8080",
    ).split(",")
    if url.strip()
]
TARGET_RPS = float(os.getenv("TARGET_RPS", "40"))
START_RPS = float(os.getenv("START_RPS", "20"))
RAMP_SECONDS = float(os.getenv("RAMP_SECONDS", "180"))
ERROR_RATE = float(os.getenv("ERROR_RATE", "0.05"))
TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "5"))
DURATION_SECONDS = float(os.getenv("DURATION_SECONDS", "0"))

if not TARGET_URLS:
    raise ValueError("TARGET_URLS must contain at least one URL")
if TARGET_RPS <= 0 or START_RPS <= 0:
    raise ValueError("TARGET_RPS and START_RPS must be positive")
if not 0 <= ERROR_RATE <= 1:
    raise ValueError("ERROR_RATE must be between 0 and 1")
if RAMP_SECONDS < 0 or TIMEOUT <= 0 or DURATION_SECONDS < 0:
    raise ValueError("Invalid time configuration")

START_RPS = min(START_RPS, TARGET_RPS)

# Per-instance item IDs, populated by successful POST requests.
item_ids = {url: [] for url in TARGET_URLS}
item_ids_lock = threading.Lock()

stats = Counter()
stats_lock = threading.Lock()


def http_request(base_url, method, path, body=None):
    data = None
    headers = {}

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        base_url + path,
        data=data,
        headers=headers,
        method=method,
        )

    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()
    except Exception as exc:
        print(f"Request failed: {method} {base_url}{path}: {exc}", flush=True)
        return 0, b""


def extract_id(body):
    try:
        value = json.loads(body)
    except (ValueError, TypeError):
        return None

    if isinstance(value, dict):
        if value.get("id") is not None:
            return str(value["id"])

        nested = value.get("item")
        if isinstance(nested, dict) and nested.get("id") is not None:
            return str(nested["id"])

    return None


def remember_created_item(base_url, body):
    item_id = extract_id(body)
    if item_id is not None:
        with item_ids_lock:
            if item_id not in item_ids[base_url]:
                item_ids[base_url].append(item_id)


def seed_instance(base_url):
    # Create an item so that normal GET and PUT requests can succeed.
    status, body = http_request(
        base_url,
        "POST",
        "/items",
        {"value": "load-generator-seed"},
    )

    if 200 <= status < 300:
        remember_created_item(base_url, body)

    print(
        f"Seed {base_url}: HTTP {status}, "
        f"known items={len(item_ids[base_url])}",
        flush=True,
    )


def valid_item_id(base_url):
    with item_ids_lock:
        known = list(item_ids[base_url])

    return random.choice(known) if known else None


def choose_request(base_url):
    # ERROR_RATE is the total probability of a synthetic HTTP error.
    if random.random() < ERROR_RATE:
        if random.random() < 0.5:
            missing_id = "__loadgen_missing_" + uuid.uuid4().hex
            return "GET", f"/items/{missing_id}", None

        return "GET", "/__loadtest__/500", None

    # Normal workload: 15% POST, 60% GET, 25% PUT.
    operation = random.random()

    if operation < 0.15:
        return "POST", "/items", {
            "value": f"load-{uuid.uuid4().hex}"
        }

    item_id = valid_item_id(base_url)

    # If seeding did not return an ID, create an item instead.
    if item_id is None:
        return "POST", "/items", {
            "value": f"load-{uuid.uuid4().hex}"
        }

    escaped_id = urllib.parse.quote(item_id, safe="")

    if operation < 0.75:
        return "GET", f"/items/{escaped_id}", None

    return "PUT", f"/items/{escaped_id}", {
        "value": f"updated-{uuid.uuid4().hex}"
    }


def execute_one(base_url):
    method, path, body = choose_request(base_url)
    status, response_body = http_request(
        base_url, method, path, body
    )

    if method == "POST" and 200 <= status < 300:
        remember_created_item(base_url, response_body)

    with stats_lock:
        stats[str(status)] += 1
        stats["total"] += 1


def report_stats():
    with stats_lock:
        current = Counter(stats)
        stats.clear()

    total = current.pop("total", 0)
    return total, current


def main():
    print(
        f"Starting load generator: targets={TARGET_URLS}, "
        f"start_rps={START_RPS}, target_rps={TARGET_RPS}, "
        f"ramp={RAMP_SECONDS}s, error_rate={ERROR_RATE}",
        flush=True,
    )

    for base_url in TARGET_URLS:
        seed_instance(base_url)

    started = time.monotonic()
    next_launch = started
    last_report = started
    sequence = 0
    max_workers = max(32, int(TARGET_RPS * 2))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        try:
            while True:
                now = time.monotonic()
                elapsed = now - started

                if DURATION_SECONDS and elapsed >= DURATION_SECONDS:
                    break

                if RAMP_SECONDS > 0:
                    fraction = min(elapsed / RAMP_SECONDS, 1.0)
                else:
                    fraction = 1.0

                current_rps = (
                        START_RPS
                        + (TARGET_RPS - START_RPS) * fraction
                )
                interval = 1.0 / current_rps

                if now < next_launch:
                    time.sleep(next_launch - now)

                # Round-robin keeps the offered rate balanced between apps.
                base_url = TARGET_URLS[sequence % len(TARGET_URLS)]
                sequence += 1
                pool.submit(execute_one, base_url)

                now = time.monotonic()
                next_launch = max(next_launch + interval, now)

                if now - last_report >= 10:
                    total, codes = report_stats()
                    window = now - last_report
                    print(
                        f"last {window:.1f}s: "
                        f"actual_rps={total / window:.1f}, "
                        f"status_counts={dict(codes)}",
                        flush=True,
                    )
                    last_report = now

        except KeyboardInterrupt:
            print("Stopping load generator...", flush=True)

    total, codes = report_stats()
    print(
        f"Final outstanding-window stats: "
        f"requests={total}, status_counts={dict(codes)}",
        flush=True,
    )


if __name__ == "__main__":
    main()