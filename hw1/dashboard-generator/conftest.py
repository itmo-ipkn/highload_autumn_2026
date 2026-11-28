"""Фикстуры генератора: фейковый HTTP-сервер (stdlib), без внешних зависимостей."""

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

HANDLERS = ["GET /items/{id}", "POST /items", "PUT /items/{id}"]
FOUR_HANDLERS = [*HANDLERS, "DELETE /items/{id}"]


class FakeStack:
    """Состояние фейкового стека: `/handlers` эмулирует приложение, `/api/*` — Grafana."""

    def __init__(self) -> None:
        self.handlers = list(HANDLERS)
        self.handlers_status = 200
        self.health_failures = 0
        self.upload_failures = 0
        self.upload_status = 500
        self.datasource_missing = False
        self.datasource_status = 200
        self.drop_last_panel = False
        self.saved_editable = None
        self.saved_uid = None
        self.can_save = False
        self.uploads = []
        self.auth_headers = []
        self.saved = None
        self.url = ""

    @property
    def handlers_url(self) -> str:
        return self.url + "/handlers"


def make_handler(stack: FakeStack):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):  # noqa: A002 - подавляем шум сервера в тестах
            pass

        def _reply(self, status, payload):
            body = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            stack.auth_headers.append(self.headers.get("Authorization"))
            if self.path == "/handlers":
                if stack.handlers_status != 200:
                    return self._reply(stack.handlers_status, {"detail": "unavailable"})
                return self._reply(200, {"handlers": stack.handlers})
            if self.path == "/api/health":
                if stack.health_failures > 0:
                    stack.health_failures -= 1
                    return self._reply(503, {"database": "failing"})
                return self._reply(200, {"database": "ok"})
            if self.path == "/api/datasources/uid/prometheus-main":
                if stack.datasource_status != 200:
                    return self._reply(stack.datasource_status, {"message": "Unauthorized"})
                if stack.datasource_missing:
                    return self._reply(404, {"message": "Data source not found"})
                return self._reply(200, {"uid": "prometheus-main", "type": "prometheus"})
            if self.path.startswith("/api/dashboards/uid/"):
                if stack.saved is None:
                    return self._reply(404, {"message": "not found"})
                return self._reply(200, {"dashboard": stack.saved, "meta": {"canSave": stack.can_save}})
            return self._reply(404, {"message": "no route"})

        def do_POST(self):
            stack.auth_headers.append(self.headers.get("Authorization"))
            length = int(self.headers.get("Content-Length") or 0)
            payload = json.loads(self.rfile.read(length) or b"{}")
            if self.path != "/api/dashboards/db":
                return self._reply(404, {"message": "no route"})
            if stack.upload_failures > 0:
                stack.upload_failures -= 1
                return self._reply(stack.upload_status, {"message": "boom"})

            stack.uploads.append(payload)
            dashboard = dict(payload["dashboard"])
            if stack.drop_last_panel:
                dashboard["panels"] = dashboard["panels"][:-1]
            if stack.saved_editable is not None:
                dashboard["editable"] = stack.saved_editable
            if stack.saved_uid is not None:
                dashboard["uid"] = stack.saved_uid
            stack.saved = dashboard
            return self._reply(200, {"status": "success", "uid": dashboard["uid"], "version": 1})

    return Handler


@pytest.fixture
def fake_stack():
    stack = FakeStack()
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(stack))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    stack.url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        yield stack
    finally:
        server.shutdown()
        server.server_close()
