"""Pauseable demonstrations; each selected step establishes its own preconditions."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
import os
from pathlib import Path
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "app"))
import grpc
import catalog_pb2 as pb
import catalog_pb2_grpc as rpc

DOCKER = os.environ.get("LAB_NETWORK") == "docker"
ONE = "http://backend-a:8081" if DOCKER else "http://127.0.0.1:8081"
TWO = "http://backend-b:8081" if DOCKER else "http://127.0.0.1:8082"
API = "http://gateway:8080" if DOCKER else "http://127.0.0.1:8080"
ORIGIN = "http://origin:8090" if DOCKER else "http://127.0.0.1:8090"
GRPC = "backend-a:50051" if DOCKER else "127.0.0.1:50051"
L4 = "tcp-lb:50050" if DOCKER else "127.0.0.1:50050"


def request(base, path, method="GET", value=None, headers=None):
    body = None if value is None else json.dumps(value).encode()
    req = urllib.request.Request(base + path, data=body, method=method,
                                  headers={"Content-Type": "application/json", **(headers or {})})
    try:
        response = urllib.request.urlopen(req, timeout=8)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read()
        return response.status, dict(response.headers), json.loads(body) if body else None


def show_http(result):
    status, headers, item = result
    print(f"HTTP {status}")
    for name in ("ETag", "X-Backend", "X-Cache", "Cache-Control"):
        if name in headers:
            print(f"{name}: {headers[name]}")
    print(json.dumps(item, ensure_ascii=False) if item is not None else "<empty body>")


def reset():
    # Run steps serially, with no other clients modifying this teaching fixture.
    status, _, _ = request(ORIGIN, "/reset", "POST")
    assert status == 200
    invalidate()


def invalidate():
    for base in (ONE, TWO):
        assert request(base, "/admin/invalidate", "POST")[0] == 200


def update():
    result = request(ORIGIN, "/items/book-1", "PUT", {"price_rub": 1500})
    assert result[0] == 200
    return result


def reads():
    return request(ORIGIN, "/stats")[2]["origin_reads"]


def channel(target, options=()):
    value = grpc.insecure_channel(target, options=options)
    grpc.channel_ready_future(value).result(timeout=5)
    return value


def contracts(step):
    if step == "http":
        reset()
        print("GET /items/book-1")
        show_http(request(ONE, "/items/book-1"))
    elif step == "grpc":
        reset()
        with channel(GRPC) as conn:
            response = rpc.CatalogServiceStub(conn).GetItem(pb.GetItemRequest(id="book-1"), timeout=2)
            print("CatalogService.GetItem ->", response)
            item = request(ONE, "/items/book-1")[2]
            assert (response.item.id, response.item.price_rub, response.item.version) == (
                item["id"], item["price_rub"], item["version"])
            print("Same id, price_rub and version via both transports: PASS")
    elif step == "conditional":
        reset()
        first = request(ONE, "/items/book-1")
        print("Initial representation:")
        show_http(first)
        print("GET with If-None-Match:", first[1]["ETag"])
        result = request(ONE, "/items/book-1", headers={"If-None-Match": first[1]["ETag"]})
        show_http(result)
        assert result[0] == 304 and result[2] is None
        print("origin_reads:", reads(), "(304 saved body bytes; this implementation still read origin)")
    elif step == "errors":
        show_http(request(ONE, "/items/missing"))
        with channel(GRPC) as conn:
            try:
                rpc.CatalogServiceStub(conn).GetItem(pb.GetItemRequest(id="missing"), timeout=2)
            except grpc.RpcError as error:
                print("gRPC status:", error.code().name, "details:", error.details())
                assert error.code() == grpc.StatusCode.NOT_FOUND
            else:
                raise AssertionError("missing item unexpectedly exists")
    elif step == "deadline":
        with channel(GRPC) as conn:
            start = time.monotonic()
            try:
                rpc.CatalogServiceStub(conn).GetItem(
                    pb.GetItemRequest(id="book-1", delay_ms=500), timeout=0.05)
            except grpc.RpcError as error:
                print("gRPC status:", error.code().name,
                      f"elapsed about {(time.monotonic() - start) * 1000:.0f} ms")
                assert error.code() == grpc.StatusCode.DEADLINE_EXCEEDED
            else:
                raise AssertionError("deadline unexpectedly survived")
        # Origin still finishes its blocking read: cancellation is not rollback.
        time.sleep(0.6)
        print("The client stopped waiting; blocking origin work could still complete.")


def balance(step):
    if step == "http":
        # One persistent downstream HTTP/1.1 connection, six separate requests.
        target = API.removeprefix("http://")
        conn = http.client.HTTPConnection(target, timeout=5)
        names = []
        try:
            for _ in range(6):
                conn.request("GET", "/items/book-1")
                response = conn.getresponse()
                response.read()
                assert response.status == 200
                names.append(response.getheader("X-Backend"))
        finally:
            conn.close()
        print("HTTP requests over one connection:", names)
        assert set(names) == {"backend-a", "backend-b"}
    elif step == "grpc-l4":
        with channel(L4) as conn:
            stub = rpc.CatalogServiceStub(conn)
            names = [stub.GetItem(pb.GetItemRequest(id="book-1"), timeout=2).served_by for _ in range(6)]
        print("gRPC RPCs over one L4-balanced connection:", names)
        assert len(set(names)) == 1
        print("Connection balancing cannot redistribute RPCs inside this HTTP/2 connection.")
    elif step == "grpc-rr":
        # Explicit static endpoint list avoids dependence on local DNS resolver behavior.
        if DOCKER:
            import socket
            endpoints = [f"{socket.gethostbyname(name)}:50051" for name in ("backend-a", "backend-b")]
        else:
            endpoints = ["127.0.0.1:50051", "127.0.0.1:50052"]
        target = "ipv4:///" + ",".join(endpoints)
        with channel(target, (("grpc.lb_policy_name", "round_robin"),)) as conn:
            stub = rpc.CatalogServiceStub(conn)
            # First READY subchannel is sufficient for channel readiness. Warm both.
            deadline = time.monotonic() + 5
            seen = set()
            while len(seen) < 2 and time.monotonic() < deadline:
                seen.add(stub.GetItem(pb.GetItemRequest(id="book-1"), timeout=2).served_by)
            assert len(seen) == 2, "both endpoints must be reachable"
            names = [stub.GetItem(pb.GetItemRequest(id="book-1"), timeout=2).served_by for _ in range(8)]
        print("Client-side round_robin over discovered endpoints:", names)
        assert set(names) == {"backend-a", "backend-b"}
        print("Endpoint list is static in this lab; production needs service discovery.")


def burst(mode):
    gate = threading.Barrier(12)

    def worker(_):
        gate.wait(timeout=10)
        return request(ONE, f"/items/book-1?cache={mode}&delay_ms=500")

    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=12) as pool:
        responses = list(pool.map(worker, range(12)))
    assert all(row[0] == 200 for row in responses)
    print(mode, "origin_reads:", reads(), "cache results:",
          dict(Counter(row[1]["X-Cache"] for row in responses)),
          f"elapsed about {(time.monotonic() - start) * 1000:.0f} ms")
    return reads()


def cache(step):
    reset()
    path = "/items/book-1?cache=singleflight"
    if step == "cold":
        for _ in range(2):
            show_http(request(ONE, path))
        print("origin_reads:", reads())
        assert reads() == 1
    elif step in ("stale", "invalidate", "ttl"):
        print("Fill local backend-a cache:")
        show_http(request(ONE, path))
        print("Update origin to price_rub=1500, version=2:")
        show_http(update())
        if step == "stale":
            print("Cached response (still 1200):")
            result = request(ONE, path)
            show_http(result)
            assert result[2]["price_rub"] == 1200
            print("Bypass response (1500):")
            show_http(request(ONE, "/items/book-1?cache=none"))
        elif step == "invalidate":
            invalidate()
            print("Invalidate both local caches, then read:")
            result = request(ONE, path)
            show_http(result)
            assert result[2]["price_rub"] == 1500 and result[1]["X-Cache"] == "MISS"
        else:
            print("Wait 3.2 seconds, longer than this lab's 3-second TTL:")
            time.sleep(3.2)
            result = request(ONE, path)
            show_http(result)
            assert result[2]["price_rub"] == 1500 and result[1]["X-Cache"] == "MISS"
    elif step == "stampede":
        naive = burst("naive")
        reset()
        protected = burst("singleflight")
        assert naive > 1 and protected == 1
        print("PASS: concurrent misses were coalesced per key in one replica.")


STEPS = {
    "contracts": ["http", "grpc", "conditional", "errors", "deadline"],
    "balance": ["http", "grpc-l4", "grpc-rr"],
    "cache": ["cold", "stale", "invalidate", "ttl", "stampede"],
}


def check():
    for base in (ONE, TWO, API, ORIGIN):
        assert request(base, "/health")[0] == 200, base
    for target in (GRPC, L4):
        with channel(target) as conn:
            reply = rpc.CatalogServiceStub(conn).GetItem(pb.GetItemRequest(id="book-1"), timeout=2)
            assert reply.item.id == "book-1"
    print("READY: origin, two HTTP replicas, HTTP gateway, gRPC, TCP gateway")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("demo", choices=[*STEPS, "all", "check", "reset"])
    parser.add_argument("--step")
    args = parser.parse_args()
    if args.demo == "check":
        check()
        return
    if args.demo == "reset":
        reset()
        print("RESET: book-1=1200, version=1, caches empty, origin_reads=0")
        return
    if args.step and (args.demo == "all" or args.step not in STEPS[args.demo]):
        parser.error("--step must belong to the chosen demo")
    for name in STEPS if args.demo == "all" else [args.demo]:
        for step in [args.step] if args.step else STEPS[name]:
            print(f"\n=== {name}/{step} ===", flush=True)
            globals()[name](step)


if __name__ == "__main__":
    main()
