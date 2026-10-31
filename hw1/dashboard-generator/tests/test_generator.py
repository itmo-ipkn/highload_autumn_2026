"""Тесты CLI генератора на фейковом HTTP-стеке (фикстура `fake_stack` из корневого conftest)."""

import base64
import json
import re
import socket
from pathlib import Path

import pytest

import generator
from conftest import FOUR_HANDLERS, HANDLERS

USER = "admin"
PASSWORD = "s3cret-pass"


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    monkeypatch.setattr(generator, "BACKOFF_BASE", 0.01)
    monkeypatch.setattr(generator, "BACKOFF_MAX", 0.02)


def run_generator(stack, *extra, user=USER, password=PASSWORD, timeout=2.0):
    argv = ["--handlers-url", stack.handlers_url, "--grafana-url", stack.url, "--timeout", str(timeout)]
    argv += list(extra)
    if user is not None:
        argv += ["--user", user]
    if password is not None:
        argv += ["--password", password]
    return generator.main(argv)


def test_dry_run_writes_json_without_touching_grafana(fake_stack, tmp_path):
    out = tmp_path / "nested" / "dashboard.json"

    assert generator.main(
        ["--handlers-url", fake_stack.handlers_url, "--dry-run", "--out", str(out)]
    ) == 0

    dashboard = json.loads(out.read_text(encoding="utf-8"))
    assert len(dashboard["panels"]) == 9
    assert dashboard["uid"] == "hw1-items-overview"
    assert fake_stack.uploads == []


def test_dry_run_without_out_prints_json(fake_stack, capsys):
    assert generator.main(["--handlers-url", fake_stack.handlers_url, "--dry-run"]) == 0

    out = capsys.readouterr().out
    assert '"uid": "hw1-items-overview"' in out
    assert '"schemaVersion": 39' in out


def test_success_uploads_once_and_verifies(fake_stack, capsys):
    assert run_generator(fake_stack) == 0

    assert len(fake_stack.uploads) == 1
    payload = fake_stack.uploads[0]
    assert payload["overwrite"] is True
    assert "version" not in payload["dashboard"]
    assert "folderUid" not in payload
    assert len(fake_stack.saved["panels"]) == 9
    assert "проверка пройдена" in capsys.readouterr().out


def test_two_runs_send_identical_dashboard(fake_stack):
    assert run_generator(fake_stack) == 0
    assert run_generator(fake_stack) == 0

    assert len(fake_stack.uploads) == 2
    assert fake_stack.uploads[0]["dashboard"] == fake_stack.uploads[1]["dashboard"]


def test_credentials_are_sent_as_basic_auth(fake_stack):
    assert run_generator(fake_stack) == 0

    expected = "Basic " + base64.b64encode(f"{USER}:{PASSWORD}".encode()).decode()
    assert expected in fake_stack.auth_headers
    assert PASSWORD not in json.dumps(fake_stack.uploads)


def test_missing_credentials_fail_before_upload(fake_stack):
    assert run_generator(fake_stack, user=None, password=None) == 1

    assert fake_stack.uploads == []
    assert not any(h for h in fake_stack.auth_headers)


def test_empty_handlers_fail_without_upload(fake_stack):
    fake_stack.handlers = []

    assert run_generator(fake_stack) == 1

    assert fake_stack.uploads == []


def test_handlers_not_a_list_fail(fake_stack):
    fake_stack.handlers = "GET /items/{id}"

    assert run_generator(fake_stack) == 1

    assert fake_stack.uploads == []


def test_unavailable_handlers_fail_without_upload(fake_stack):
    fake_stack.handlers_status = 500

    assert run_generator(fake_stack, timeout=0.2) == 1

    assert fake_stack.uploads == []


def test_grafana_health_retry_then_success(fake_stack):
    fake_stack.health_failures = 2

    assert run_generator(fake_stack) == 0

    assert len(fake_stack.uploads) == 1


def test_grafana_never_healthy_fails(fake_stack, capsys):
    fake_stack.health_failures = 10_000

    assert run_generator(fake_stack, timeout=0.2) == 1

    assert fake_stack.uploads == []
    out = capsys.readouterr().out
    assert "/api/health" in out
    assert "ОШИБКА" in out


def test_upload_retries_on_5xx_then_succeeds(fake_stack):
    fake_stack.upload_failures = 2

    assert run_generator(fake_stack) == 0

    assert len(fake_stack.uploads) == 1


def test_upload_never_succeeds_fails(fake_stack, capsys):
    fake_stack.upload_failures = 10_000

    assert run_generator(fake_stack, timeout=0.2) == 1

    assert "ОШИБКА" in capsys.readouterr().out


def test_panel_count_mismatch_fails_verification(fake_stack, capsys):
    fake_stack.drop_last_panel = True

    assert run_generator(fake_stack) == 1

    assert "панелей 8, ожидалось 9" in capsys.readouterr().out


def test_can_save_true_only_warns(fake_stack, capsys):
    fake_stack.can_save = True

    assert run_generator(fake_stack) == 0

    assert "canSave" in capsys.readouterr().out


def test_generator_sources_contain_no_handler_literals():
    root = Path(generator.__file__).parent
    forbidden = re.compile(r"[\"'](GET|POST|PUT|DELETE|PATCH|HEAD) /")

    for name in ("dashboard.py", "generator.py"):
        source = (root / name).read_text(encoding="utf-8")
        assert not forbidden.search(source), f"{name} содержит литерал ручки"


def test_dry_run_does_not_require_credentials(fake_stack, tmp_path, monkeypatch):
    monkeypatch.delenv("GF_SECURITY_ADMIN_USER", raising=False)
    monkeypatch.delenv("GF_SECURITY_ADMIN_PASSWORD", raising=False)
    out = tmp_path / "dashboard.json"

    code = generator.main(
        ["--handlers-url", fake_stack.handlers_url, "--dry-run", "--out", str(out)]
    )

    assert code == 0
    assert out.exists()


def test_panel_count_follows_handlers_from_handlers_endpoint(fake_stack):
    fake_stack.handlers = list(FOUR_HANDLERS)

    assert run_generator(fake_stack) == 0

    assert len(fake_stack.uploads[0]["dashboard"]["panels"]) == 12
    assert len(fake_stack.saved["panels"]) == 12


def test_handler_disappears_between_runs(fake_stack):
    fake_stack.handlers = list(FOUR_HANDLERS)
    assert run_generator(fake_stack) == 0
    assert len(fake_stack.saved["panels"]) == 12

    fake_stack.handlers = list(HANDLERS)
    assert run_generator(fake_stack) == 0

    assert len(fake_stack.saved["panels"]) == 9
    titles = [panel["title"] for panel in fake_stack.saved["panels"]]
    assert not any(title.startswith("DELETE /items") for title in titles)


def test_missing_datasource_fails_before_upload(fake_stack, capsys):
    fake_stack.datasource_missing = True

    assert run_generator(fake_stack) == 1

    assert fake_stack.uploads == []
    assert "prometheus-main" in capsys.readouterr().out


def test_unauthorized_datasource_reports_credentials(fake_stack, capsys):
    """Неверный пароль ловится на первом же аутентифицированном запросе.

    Grafana отдаёт `/api/health` без авторизации, поэтому `401` приходит не из
    health-проверки, а из datasource. В логе должна быть причина (креды), а не
    голый `HTTP 401` по URL — иначе сценарий §5(б) из `docs/acceptance.md`
    проверял бы не то сообщение, которое печатается.
    """
    fake_stack.datasource_status = 401

    assert run_generator(fake_stack) == 1

    assert fake_stack.uploads == []
    out = capsys.readouterr().out
    assert "Grafana отклонила креды (401)" in out
    assert "GF_SECURITY_ADMIN_USER/PASSWORD" in out


def test_4xx_is_not_retried(fake_stack):
    fake_stack.upload_failures = 5
    fake_stack.upload_status = 403

    assert run_generator(fake_stack) == 1

    assert fake_stack.upload_failures == 4, "на 4xx должна быть ровно одна попытка"


def test_connection_error_is_retried_then_fails(fake_stack, capsys):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]

    code = generator.main(
        [
            "--handlers-url",
            fake_stack.handlers_url,
            "--grafana-url",
            f"http://127.0.0.1:{closed_port}",
            "--timeout",
            "0.2",
            "--user",
            USER,
            "--password",
            PASSWORD,
        ]
    )

    out = capsys.readouterr().out
    assert code == 1
    assert "повтор через" in out
    assert "не удалось за" in out
    assert fake_stack.uploads == []


def test_editable_dashboard_is_detected(fake_stack, capsys):
    fake_stack.saved_editable = True

    assert run_generator(fake_stack) == 1

    assert "editable" in capsys.readouterr().out


def test_foreign_uid_is_detected(fake_stack, capsys):
    fake_stack.saved_uid = "someone-else"

    assert run_generator(fake_stack) == 1

    assert "someone-else" in capsys.readouterr().out
