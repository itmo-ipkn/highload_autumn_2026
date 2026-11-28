"""Small teaching service. Plaintext HTTP/gRPC is for loopback lab use only."""
import argparse
import concurrent.futures
import http.client
import json
import os
import select
import socket
import socketserver
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import grpc
import catalog_pb2 as pb
import catalog_pb2_grpc as rpc


def json_bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")


class HTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 128


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def reply(self, status, value=None, headers=None):
        body = b"" if value is None else json_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if status != 304:
            self.send_header("Content-Length", str(len(body)))
        for key, val in (headers or {}).items():
            self.send_header(key, val)
        self.end_headers()
        if body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def read_json(self):
        size = int(self.headers.get("Content-Length", "0"))
        if size > 4096:
            raise ValueError("body too large")
        return json.loads(self.rfile.read(size) or b"{}")


class Origin:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()

    def reset(self):
        with self.lock:
            self.item = {"id": "book-1", "title": "Distributed Systems Notebook",
                         "price_rub": 1200, "version": 1}
            self.reads = 0

    def handler(self):
        origin = self

        class OriginHandler(Handler):
            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                if url.path in ("/health", "/stats"):
                    with origin.lock:
                        self.reply(200, {"origin_reads": origin.reads, "healthy": True})
                    return
                if url.path != "/items/book-1":
                    self.reply(404, {"error": "item not found"})
                    return
                try:
                    delay = int(urllib.parse.parse_qs(url.query).get("delay_ms", ["120"])[0])
                    if not 0 <= delay <= 2000:
                        raise ValueError("invalid delay")
                except ValueError:
                    self.reply(400, {"error": "delay_ms must be 0..2000"})
                    return
                with origin.lock:
                    origin.reads += 1
                time.sleep(delay / 1000)
                with origin.lock:
                    item = origin.item.copy()
                self.reply(200, item)

            def do_PUT(self):
                if self.path != "/items/book-1":
                    self.reply(404, {"error": "item not found"})
                    return
                try:
                    price = self.read_json()["price_rub"]
                    if type(price) is not int or not 0 <= price <= 1_000_000:
                        raise ValueError("invalid price")
                except (KeyError, ValueError, TypeError):
                    self.reply(400, {"error": "price_rub must be integer 0..1000000"})
                    return
                with origin.lock:
                    origin.item["price_rub"] = price
                    origin.item["version"] += 1
                    item = origin.item.copy()
                self.reply(200, item)

            def do_POST(self):
                if self.path == "/reset":
                    origin.reset()
                    self.reply(200, {"reset": True})
                else:
                    self.reply(404, {"error": "unknown control"})

        return OriginHandler


class Catalog:
    def __init__(self, origin_url, name, ttl=3.0):
        self.origin_url, self.name, self.ttl = origin_url, name, ttl
        self.cache, self.key_locks = {}, {}
        self.lock = threading.Lock()
        self.generation = 0

    def invalidate(self):
        with self.lock:
            self.cache.clear()
            self.generation += 1

    def cached(self, item_id):
        with self.lock:
            entry = self.cache.get(item_id)
            if entry and entry[0] > time.monotonic():
                return entry[1].copy()
        return None

    def source(self, item_id, delay):
        if item_id != "book-1":
            raise KeyError(item_id)
        with urllib.request.urlopen(
                f"{self.origin_url}/items/{item_id}?delay_ms={delay}", timeout=5) as response:
            return json.load(response)

    def get(self, item_id, mode="none", delay=120):
        if mode not in ("none", "naive", "singleflight") or not 0 <= delay <= 2000:
            raise ValueError("cache mode or delay_ms is invalid")
        if item_id != "book-1":
            raise KeyError(item_id)
        if mode == "none":
            return self.source(item_id, delay), "BYPASS"
        item = self.cached(item_id)
        if item is not None:
            return item, "HIT"
        with self.lock:
            key_lock = self.key_locks.setdefault(item_id, threading.Lock())

        def fill():
            with self.lock:
                generation = self.generation
            item = self.source(item_id, delay)
            with self.lock:
                # Do not resurrect a fill that started before explicit invalidation.
                if generation == self.generation:
                    self.cache[item_id] = (time.monotonic() + self.ttl, item.copy())
            return item, "MISS"

        if mode == "naive":
            return fill()
        with key_lock:
            item = self.cached(item_id)
            if item is not None:
                return item, "COALESCED"
            return fill()

    def handler(self):
        catalog = self

        class CatalogHandler(Handler):
            def do_GET(self):
                url = urllib.parse.urlsplit(self.path)
                if url.path == "/health":
                    self.reply(200, {"healthy": True, "served_by": catalog.name})
                    return
                if not url.path.startswith("/items/"):
                    self.reply(404, {"error": "unknown route"})
                    return
                params = urllib.parse.parse_qs(url.query)
                try:
                    item, cache = catalog.get(url.path[len("/items/"):],
                        params.get("cache", ["none"])[0],
                        int(params.get("delay_ms", ["120"])[0]))
                except KeyError:
                    self.reply(404, {"error": "item not found"})
                    return
                except ValueError as error:
                    self.reply(400, {"error": str(error)})
                    return
                except (OSError, urllib.error.URLError):
                    self.reply(503, {"error": "origin unavailable"})
                    return
                etag = f'"{item["id"]}-v{item["version"]}"'
                headers = {"ETag": etag, "X-Backend": catalog.name, "X-Cache": cache,
                           "Cache-Control": "public, max-age=0, must-revalidate"}
                supplied = [v.strip().removeprefix("W/") for v in
                            self.headers.get("If-None-Match", "").split(",")]
                if etag in supplied or "*" in supplied:
                    self.reply(304, headers=headers)
                else:
                    self.reply(200, item, headers)

            def do_POST(self):
                if self.path == "/admin/invalidate":
                    catalog.invalidate()
                    self.reply(200, {"invalidated": True, "served_by": catalog.name})
                else:
                    self.reply(404, {"error": "unknown control"})

        return CatalogHandler


class CatalogRPC(rpc.CatalogServiceServicer):
    def __init__(self, catalog):
        self.catalog = catalog

    def GetItem(self, request, context):
        try:
            item, cache = self.catalog.get(request.id, request.cache_mode or "none",
                                           request.delay_ms or 120)
        except KeyError:
            context.abort(grpc.StatusCode.NOT_FOUND, "item not found")
        except ValueError as error:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except (OSError, urllib.error.URLError):
            context.abort(grpc.StatusCode.UNAVAILABLE, "origin unavailable")
        if not context.is_active():
            context.abort(grpc.StatusCode.CANCELLED, "client no longer waits")
        return pb.GetItemResponse(item=pb.Item(**item), served_by=self.catalog.name, cache=cache)


def start_http(host, port, handler):
    server = HTTPServer((host, port), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def start_backend(host, http_port, grpc_port, origin_url, name):
    catalog = Catalog(origin_url, name)
    http = start_http(host, http_port, catalog.handler())
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=32))
    rpc.add_CatalogServiceServicer_to_server(CatalogRPC(catalog), server)
    if server.add_insecure_port(f"{host}:{grpc_port}") == 0:
        raise RuntimeError(f"cannot listen on gRPC port {grpc_port}")
    server.start()
    return http, server


def gateway_handler(targets):
    lock, index = threading.Lock(), 0

    class Gateway(Handler):
        def do_GET(self):
            nonlocal index
            with lock:
                target = targets[index % len(targets)]
                index += 1
            conn = http.client.HTTPConnection(*target, timeout=6)
            try:
                forwarded = {"If-None-Match": self.headers["If-None-Match"]} if "If-None-Match" in self.headers else {}
                conn.request("GET", self.path, headers=forwarded)
                response = conn.getresponse()
                body = response.read()
                self.send_response(response.status)
                for key in ("Content-Type", "ETag", "X-Backend", "X-Cache", "Cache-Control"):
                    if response.getheader(key):
                        self.send_header(key, response.getheader(key))
                if response.status != 304:
                    self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except OSError:
                self.reply(502, {"error": "upstream unavailable"})
            finally:
                conn.close()

    return Gateway


def start_tcp(host, port, targets):
    lock, index = threading.Lock(), 0

    class Forward(socketserver.BaseRequestHandler):
        def handle(self):
            nonlocal index
            with lock:
                target = targets[index % len(targets)]
                index += 1
            try:
                with socket.create_connection(target, timeout=3) as upstream:
                    upstream.settimeout(None)
                    peers = [self.request, upstream]
                    while True:
                        ready, _, _ = select.select(peers, [], [], 60)
                        if not ready:
                            continue
                        for incoming in ready:
                            data = incoming.recv(65536)
                            if not data:
                                return
                            outgoing = upstream if incoming is self.request else self.request
                            outgoing.sendall(data)
            except OSError:
                return

    class TCPServer(socketserver.ThreadingTCPServer):
        allow_reuse_address = True
        daemon_threads = True

    server = TCPServer((host, port), Forward)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("origin", "backend", "tcp", "native"))
    args = parser.parse_args()
    host = "127.0.0.1" if args.mode == "native" else "0.0.0.0"
    services = []  # Keep gRPC server objects alive for this process's lifetime.
    if args.mode == "origin":
        services.append(start_http(host, 8090, Origin().handler()))
    elif args.mode == "backend":
        services.append(start_backend(host, 8081, 50051, os.environ["ORIGIN_URL"], os.environ["BACKEND_NAME"]))
    elif args.mode == "tcp":
        services.append(start_tcp(host, 50050, [("backend-a", 50051), ("backend-b", 50051)]))
    else:
        # Check every port before starting any listener: avoid a partly started lab.
        for port in (8090, 8080, 8081, 8082, 50050, 50051, 50052):
            with socket.socket() as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind((host, port))
        services.append(start_http(host, 8090, Origin().handler()))
        services.append(start_backend(host, 8081, 50051, "http://127.0.0.1:8090", "backend-a"))
        services.append(start_backend(host, 8082, 50052, "http://127.0.0.1:8090", "backend-b"))
        services.append(start_http(host, 8080, gateway_handler([(host, 8081), (host, 8082)])))
        services.append(start_tcp(host, 50050, [(host, 50051), (host, 50052)]))
    print(f"READY {args.mode}; Ctrl-C stops this process", flush=True)
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
