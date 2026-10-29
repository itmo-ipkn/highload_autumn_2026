"""Нагрузочный генератор.

Плавно наращивает RPS (ramp-up) до TARGET_RPS суммарно по обоим экземплярам,
равномерно распределяет запросы между подами, затрагивает все ручки и создаёт
ошибки: часть GET идёт на несуществующий id (404), часть запросов падает 500
из-за FAIL_RATE на стороне сервиса. Работает, пока стек не остановлен.
"""
import os
import random
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

INSTANCES = [
    u.strip() for u in os.environ.get("TARGET_URLS", "http://app1:8000,http://app2:8000").split(",")
    if u.strip()
]
TARGET_RPS = float(os.environ.get("TARGET_RPS", "30"))
RAMP_UP_SECONDS = float(os.environ.get("RAMP_UP_SECONDS", "120"))
TICK_SECONDS = 0.5          # шаг планировщика
REQUEST_TIMEOUT = 5.0
NOT_FOUND_RATE = 0.10       # доля GET на несуществующий id -> 404

STOP = threading.Event()
_ids_lock = threading.Lock()
_created_ids = []
_stats_lock = threading.Lock()
_stats = {"total": 0, "errors": 0}


def remember_item(item_id):
    if item_id is None:
        return
    with _ids_lock:
        _created_ids.append(item_id)
        if len(_created_ids) > 10000:
            _created_ids.pop(0)


def random_existing_id():
    with _ids_lock:
        if not _created_ids:
            return None
        return random.choice(_created_ids)


def note_result(status_code):
    with _stats_lock:
        _stats["total"] += 1
        if status_code is None or status_code >= 400:
            _stats["errors"] += 1


def do_post(base):
    r = None
    try:
        r = requests.post(
            f"{base}/items",
            json={"name": f"item-{random.randint(1, 10**6)}"},
            timeout=REQUEST_TIMEOUT,
        )
        if r.status_code == 201:
            remember_item(r.json().get("id"))
    except requests.RequestException:
        pass
    finally:
        note_result(r.status_code if r is not None else None)


def do_get(base):
    item_id = None
    if random.random() >= NOT_FOUND_RATE:
        item_id = random_existing_id()
    if item_id is None:
        # создадим запись, чтобы следующий GET мог попасть по существующему id
        do_post(base)
        return
    r = None
    try:
        r = requests.get(f"{base}/items/{item_id}", timeout=REQUEST_TIMEOUT)
    except requests.RequestException:
        pass
    finally:
        note_result(r.status_code if r is not None else None)


def do_put(base):
    item_id = random_existing_id()
    if item_id is None:
        do_post(base)
        return
    r = None
    try:
        r = requests.put(
            f"{base}/items/{item_id}",
            json={"name": f"item-{random.randint(1, 10**6)}-updated"},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException:
        pass
    finally:
        note_result(r.status_code if r is not None else None)


def do_request():
    base = random.choice(INSTANCES)  # равномерное распределение по подам
    roll = random.random()
    if roll < 0.40:
        do_post(base)
    elif roll < 0.70:
        do_get(base)
    else:
        do_put(base)


def wait_for_apps():
    deadline = time.monotonic() + 120
    for url in INSTANCES:
        while time.monotonic() < deadline:
            try:
                if requests.get(f"{url}/health", timeout=3).ok:
                    print(f"{url} готов.", flush=True)
                    break
            except requests.RequestException:
                pass
            time.sleep(2)
        else:
            raise RuntimeError(f"{url} не прошёл health-check")


def report_periodically(last_report):
    now = time.monotonic()
    if now - last_report >= 10:
        with _stats_lock:
            print(
                f"[stats] total={_stats['total']} errors={_stats['errors']}",
                flush=True,
            )
        return now
    return last_report


def main():
    signal.signal(signal.SIGTERM, lambda *_: STOP.set())
    signal.signal(signal.SIGINT, lambda *_: STOP.set())
    wait_for_apps()
    print(
        f"Старт: TARGET_RPS={TARGET_RPS}, RAMP_UP_SECONDS={RAMP_UP_SECONDS}, "
        f"экземпляры={INSTANCES}",
        flush=True,
    )
    start = time.monotonic()
    last_report = start
    with ThreadPoolExecutor(max_workers=32) as pool:
        while not STOP.is_set():
            elapsed = time.monotonic() - start
            rate = (
                TARGET_RPS if RAMP_UP_SECONDS <= 0
                else min(TARGET_RPS, TARGET_RPS * elapsed / RAMP_UP_SECONDS)
            )
            expected = rate * TICK_SECONDS
            n = int(expected)
            if random.random() < expected - n:
                n += 1
            for _ in range(n):
                pool.submit(do_request)
            last_report = report_periodically(last_report)
            STOP.wait(TICK_SECONDS)
    print("Нагрузочный генератор остановлен.", flush=True)


if __name__ == "__main__":
    main()
