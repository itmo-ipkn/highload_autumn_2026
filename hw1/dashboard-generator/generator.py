"""CLI: собрать дашборд из `/handlers` и залить его в Grafana.

Порядок: получить список ручек в рантайме → собрать JSON → дождаться Grafana →
убедиться, что datasource на месте → `POST /api/dashboards/db` с `overwrite` →
проверить результат через API. `/handlers` читается первым: если приложение
лежит, незачем ждать Grafana.

`--dry-run` останавливается после сборки JSON и ничего не заливает: этим
путём проверяется контракт дашборда без запущенной Grafana.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

from dashboard import DASHBOARD_UID, DashboardError, build_dashboard, validate_handlers

BACKOFF_BASE = 1.0
BACKOFF_MAX = 8.0
REQUEST_TIMEOUT = 5.0

HEALTH_PATH = "/api/health"
UPLOAD_PATH = "/api/dashboards/db"
DASHBOARD_PATH = "/api/dashboards/uid/{uid}"
DATASOURCE_PATH = "/api/datasources/uid/{uid}"
DATASOURCE_UID = "prometheus-main"

HANDLERS_KEY = "handlers"


class GeneratorError(RuntimeError):
    """Штатная ошибка генератора: печатается без трейсбека, exit code 1."""


def log(message: str) -> None:
    print(message, flush=True)


def backoff(attempt: int) -> float:
    return min(BACKOFF_BASE * 2**attempt, BACKOFF_MAX)


def call_with_retry(session, method, url, deadline, **kwargs):
    started = time.monotonic()
    attempt = 0
    while True:
        try:
            response = session.request(method, url, timeout=REQUEST_TIMEOUT, **kwargs)
            if response.status_code < 500:
                return response
            reason = f"HTTP {response.status_code}"
        except requests.RequestException as exc:
            reason = str(exc)

        if time.monotonic() - started >= deadline:
            raise GeneratorError(f"{method} {url}: не удалось за {deadline:.0f} с ({reason})")
        delay = backoff(attempt)
        log(f"  повтор через {delay:.1f} с: {reason}")
        time.sleep(delay)
        attempt += 1


def fetch_handlers(session, url: str, deadline: float) -> list[str]:
    response = call_with_retry(session, "GET", url, deadline)
    if response.status_code != 200:
        raise GeneratorError(f"GET {url}: HTTP {response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise GeneratorError(f"GET {url}: ответ не является JSON") from exc

    handlers = payload.get(HANDLERS_KEY) if isinstance(payload, dict) else None
    if not isinstance(handlers, list):
        raise GeneratorError(f"GET {url}: в ответе нет списка '{HANDLERS_KEY}'")

    try:
        return validate_handlers(handlers)
    except DashboardError as exc:
        raise GeneratorError(f"GET {url}: {exc}") from exc


def wait_for_grafana(session, base_url: str, deadline: float) -> None:
    response = call_with_retry(session, "GET", base_url + HEALTH_PATH, deadline)
    if response.status_code != 200:
        raise GeneratorError(
            f"Grafana {base_url} не готова: HTTP {response.status_code} "
            "(проверьте GF_SECURITY_ADMIN_* и статус контейнера)"
        )


def upload_dashboard(session, base_url: str, dashboard: dict, deadline: float) -> None:
    response = call_with_retry(
        session,
        "POST",
        base_url + UPLOAD_PATH,
        deadline,
        json={"dashboard": dashboard, "overwrite": True},
    )
    if response.status_code == 401:
        raise GeneratorError("Grafana отклонила креды (401): проверьте GF_SECURITY_ADMIN_USER/PASSWORD")
    if response.status_code != 200:
        raise GeneratorError(f"POST {UPLOAD_PATH}: HTTP {response.status_code} {response.text[:200]}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise GeneratorError(f"POST {UPLOAD_PATH}: ответ не является JSON") from exc

    if payload.get("status") != "success":
        raise GeneratorError(f"POST {UPLOAD_PATH}: неожиданный ответ {payload}")


def check_datasource(session, base_url: str, deadline: float) -> None:
    url = base_url + DATASOURCE_PATH.format(uid=DATASOURCE_UID)
    response = call_with_retry(session, "GET", url, deadline)
    if response.status_code == 401:
        raise GeneratorError("Grafana отклонила креды (401): проверьте GF_SECURITY_ADMIN_USER/PASSWORD")
    if response.status_code == 404:
        raise GeneratorError(
            f"Grafana не знает datasource {DATASOURCE_UID}: provisioning не применился — "
            "панели откроются с ошибкой 'Datasource not found'"
        )
    if response.status_code != 200:
        raise GeneratorError(f"GET {url}: HTTP {response.status_code}")


def verify_dashboard(session, base_url: str, expected: dict, deadline: float) -> None:
    url = base_url + DASHBOARD_PATH.format(uid=DASHBOARD_UID)
    response = call_with_retry(session, "GET", url, deadline)
    if response.status_code != 200:
        raise GeneratorError(f"GET {url}: HTTP {response.status_code}")

    try:
        payload = response.json()
    except ValueError as exc:
        raise GeneratorError(f"GET {url}: ответ не является JSON") from exc
    if not isinstance(payload, dict):
        raise GeneratorError(f"GET {url}: ожидался объект, получено {type(payload).__name__}")

    dashboard = payload.get("dashboard") or {}
    if dashboard.get("uid") != DASHBOARD_UID:
        raise GeneratorError(f"после заливки uid = {dashboard.get('uid')!r}")
    if dashboard.get("editable") is not False:
        raise GeneratorError("дашборд редактируемый: editable != false")

    panels = dashboard.get("panels") or []
    if len(panels) != len(expected["panels"]):
        raise GeneratorError(f"панелей {len(panels)}, ожидалось {len(expected['panels'])}")
    got_titles = [panel.get("title") for panel in panels]
    want_titles = [panel["title"] for panel in expected["panels"]]
    if got_titles != want_titles:
        raise GeneratorError("набор панелей после заливки не совпал с отправленным")

    got_vars = [variable.get("name") for variable in (dashboard.get("templating") or {}).get("list") or []]
    want_vars = [variable["name"] for variable in expected["templating"]["list"]]
    if got_vars != want_vars:
        raise GeneratorError(f"переменные после заливки {got_vars}, ожидались {want_vars}")

    can_save = (payload.get("meta") or {}).get("canSave")
    if can_save:
        log("  предупреждение: meta.canSave=true (у admin-кредов бывает даже при editable=false)")


def write_json(dashboard: dict, out: str | None) -> None:
    text = json.dumps(dashboard, ensure_ascii=False, indent=2)
    if out:
        path = Path(out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
        log(f"JSON записан: {path}")
    else:
        print(text)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Генератор дашборда Grafana для HW-1")
    parser.add_argument(
        "--grafana-url",
        default=os.getenv("GRAFANA_URL", "http://grafana:3000"),
        help="базовый URL Grafana (в compose — имя сервиса, не localhost)",
    )
    parser.add_argument(
        "--handlers-url",
        default=os.getenv("HANDLERS_URL", "http://app-1:8000/handlers"),
        help="URL со списком бизнес-ручек",
    )
    parser.add_argument("--user", default=os.getenv("GF_SECURITY_ADMIN_USER"))
    parser.add_argument("--password", default=os.getenv("GF_SECURITY_ADMIN_PASSWORD"))
    parser.add_argument("--timeout", type=float, default=60.0, help="бюджет ретраев, секунды")
    parser.add_argument("--dry-run", action="store_true", help="собрать JSON и выйти, без Grafana")
    parser.add_argument("--out", default=None, help="куда записать JSON при --dry-run")
    return parser.parse_args(argv)


def run(args) -> int:
    session = requests.Session()
    base_url = args.grafana_url.rstrip("/")

    log(f"Ручки: {args.handlers_url}")
    handlers = fetch_handlers(session, args.handlers_url, args.timeout)
    dashboard = build_dashboard(handlers)
    log(f"Собрано панелей: {len(dashboard['panels'])} для ручек: {', '.join(handlers)}")

    if args.dry_run:
        write_json(dashboard, args.out)
        return 0

    if not args.user or not args.password:
        raise GeneratorError("не заданы GF_SECURITY_ADMIN_USER/GF_SECURITY_ADMIN_PASSWORD")
    session.auth = (args.user, args.password)

    log(f"Grafana: {base_url}")
    wait_for_grafana(session, base_url, args.timeout)
    check_datasource(session, base_url, args.timeout)
    upload_dashboard(session, base_url, dashboard, args.timeout)
    log(f"  дашборд залит: uid={DASHBOARD_UID}")
    verify_dashboard(session, base_url, dashboard, args.timeout)
    log("  проверка пройдена: editable=false, панели и переменные совпадают с отправленными")
    log(f"  открыть: {base_url}/d/{DASHBOARD_UID}")
    log(
        "  предупреждение: пока Prometheus не собрал данные, панели будут пустыми ('No data'); "
        "латентность появляется только после первого трафика"
    )
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    try:
        return run(args)
    except GeneratorError as exc:
        log(f"ОШИБКА: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
