"""Register network devices, read their cards and rotate maintenance states."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
from ipaddress import IPv6Address
from secrets import randbits
import os
import signal
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

STOP = threading.Event()
COUNTS = Counter()
LOCK = threading.Lock()


def send(base, method, path, body=None, fault=False):
    headers = {"Content-Type": "application/json"}
    if fault:
        headers["X-Demo-Error"] = "500"
    request = Request(base + path, method=method, headers=headers,
                      data=json.dumps(body).encode() if body is not None else None)
    try:
        with urlopen(request, timeout=3) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, None
    except (URLError, TimeoutError):
        return "network_error", None


def new_address():
    return str(IPv6Address(int(IPv6Address("fd10::")) | randbits(112)))


def exercise(seq, urls, ids, seed_addresses):
    # Each group of 6 hits POST/GET/PUT on each pod exactly once.
    pod = seq % len(urls)
    operation = (seq // len(urls)) % 3
    cycle = (seq // (len(urls) * 3)) % 100
    fault = cycle < 2            # 2%: controlled 500 on each handler
    client_error = 2 <= cycle < 5  # 3%: missing devices, duplicate or invalid IPs
    item_id = "unregistered-device" if client_error else ids[pod]
    state = ("active", "maintenance", "disabled")[cycle % 3]
    body = {"name": f"campus-switch-{pod}-{seq}", "ip_address": seed_addresses[pod],
            "kind": "switch", "state": state}
    if operation == 0:
        body["ip_address"] = new_address()
        if cycle in (2, 3):
            body["ip_address"] = seed_addresses[pod]
        elif cycle == 4:
            body["ip_address"] = "invalid-address"
        code, _ = send(urls[pod], "POST", "/items", body, fault)
    elif operation == 1:
        code, _ = send(urls[pod], "GET", "/items/" + item_id, fault=fault)
    else:
        code, _ = send(urls[pod], "PUT", "/items/" + item_id, body, fault)
    with LOCK:
        COUNTS[str(code)] += 1


def main():
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: STOP.set())
    urls = os.getenv("APP_URLS", "http://app-1:8000,http://app-2:8000").split(",")
    target = float(os.getenv("TARGET_RPS", "30"))
    ramp = float(os.getenv("RAMP_SECONDS", "120"))
    if target <= 0 or ramp < 0:
        raise ValueError("TARGET_RPS must be positive, RAMP_SECONDS non-negative")
    ids, seed_addresses = [], []
    for url in urls:
        address = new_address()
        code, item = send(url, "POST", "/items", {
            "name": "campus-core", "ip_address": address, "kind": "switch", "state": "active"})
        if code != 201:
            raise RuntimeError(f"Cannot seed {url}: HTTP {code}")
        ids.append(item["id"])
        seed_addresses.append(address)
    started = next_request = last_report = time.monotonic()
    seq = previous_count = 0
    print(f"Load started: {len(urls)} pods, ramp={ramp}s, target={target} RPS TOTAL", flush=True)
    with ThreadPoolExecutor(max_workers=16) as pool:
        while not STOP.is_set():
            now = time.monotonic()
            elapsed = now - started
            progress = min(1.0, elapsed / ramp) if ramp else 1.0
            rps = min(target, 3.0) + (target - min(target, 3.0)) * progress
            pool.submit(exercise, seq, urls, ids, seed_addresses)
            seq += 1
            if now - last_report >= 10:
                with LOCK:
                    counts = dict(COUNTS)
                completed = sum(counts.values())
                actual = (completed - previous_count) / (now - last_report)
                print(f"t={elapsed:.0f}s scheduled_rps={rps:.1f} completed_rps={actual:.1f} codes={counts}", flush=True)
                last_report, previous_count = now, completed
            next_request = max(next_request + 1 / rps, now)
            STOP.wait(max(0, next_request - time.monotonic()))


if __name__ == "__main__":
    main()
