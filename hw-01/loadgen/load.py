import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

URLS = [
    url.strip().rstrip("/")
    for url in os.environ.get("APP_URLS", "http://app-1:8080,http://app-2:8080").split(",")
    if url.strip()
]
TARGET_RPS = float(os.environ.get("TARGET_RPS", "40"))
RAMP_SECONDS = float(os.environ.get("RAMP_SECONDS", "180"))
NOT_FOUND_RATE = float(os.environ.get("NOT_FOUND_RATE", "0.05"))
MISSING_ID = 10**9

# У каждого пода свое собственное хранилище поэтому, чтобы контролировать число 404 запросов
# надо на клиенте запоминать, где какое лежит
ids = {}
ids_lock = threading.Lock()


def call(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, response.read()
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        return 0, b""


def create(base):
    status, body = call(
        f"{base}/items", "POST", {"name": "load", "value": random.randint(1, 1000)}
    )
    if status == 201:
        item_id = json.loads(body)["id"]
        with ids_lock:
            bucket = ids.setdefault(base, [])
            if len(bucket) < 1000:
                bucket.append(item_id)


def pick_id(base):
    with ids_lock:
        bucket = ids.get(base, [])
        if bucket and random.random() >= NOT_FOUND_RATE:
            return random.choice(bucket)
    return MISSING_ID + random.randint(0, 10**6)


def hit():
    base = random.choice(URLS)
    action = random.choice(("create", "get", "update"))
    if action == "create":
        create(base)
    elif action == "get":
        call(f"{base}/items/{pick_id(base)}")
    else:
        call(
            f"{base}/items/{pick_id(base)}",
            "PUT",
            {"name": "load", "value": random.randint(1, 1000)},
        )


def main():
    for base in URLS:
        for _ in range(3):
            create(base)

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=32) as pool:
        while True:
            elapsed = time.monotonic() - start
            rps = TARGET_RPS * min(1, elapsed / RAMP_SECONDS) if RAMP_SECONDS else TARGET_RPS
            tick = time.monotonic()
            pool.submit(hit)
            time.sleep(max(0, tick + 1 / max(rps, 1) - time.monotonic()))


if __name__ == "__main__":
    main()
