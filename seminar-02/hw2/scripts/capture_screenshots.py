#!/usr/bin/env python
"""Screenshot helpers for docs/screenshots/ (headless Chromium through Playwright).

Two kinds of images, both made from real stand data:

1. `grafana`  - a real dashboard view for a given panel and time range. The chart is
   the actual Prometheus series for the experiment window, so the screenshot shows what
   the dashboard showed while the experiment was running.
2. `terminal` - a saved command output (kafka-consumer-groups.sh describe, verify CLI
   output) rendered as a terminal page and screenshotted. These are console captures of
   the evidence files in results/.

Setup (host venv): pip install playwright && playwright install chromium

Examples:
  python scripts/capture_screenshots.py grafana --panel 5 \
      --from 2026-11-28T09:45:00Z --to 2026-11-28T09:49:00Z \
      --out docs/screenshots/e1-1.png
  python scripts/capture_screenshots.py terminal \
      --out docs/screenshots/e1-2.png \
      --title "Experiment 1 - consumer group after the run" \
      --file results/e1-.../consumer-group.txt --file results/e1-.../verify.txt

Grafana accepts only epoch milliseconds or relative times in the URL, so --from/--to
(ISO UTC) are converted to epoch ms.
"""
from __future__ import annotations

import argparse
import html
import os
import time
from datetime import datetime, timezone
from pathlib import Path

BASE = os.environ.get('GRAFANA_URL', 'http://127.0.0.1:3000')
DASHBOARD = '/d/lab2-kafka/hw2-kafka-queue'
USER = os.environ.get('GRAFANA_ADMIN_USER', 'admin')
PASSWORD = os.environ.get('GRAFANA_ADMIN_PASSWORD', '1qa2ws3ed')

TERM_CSS = """
body { background: #111417; color: #e6e6e6; font-family: Consolas, 'DejaVu Sans Mono', monospace;
       margin: 0; padding: 18px 22px; }
h2 { color: #8ab4f8; font-size: 15px; margin: 0 0 12px; font-weight: 600; }
pre { margin: 0; white-space: pre-wrap; font-size: 13px; line-height: 1.5; }
.pass { color: #6fd088; font-weight: 700; }
.fail { color: #ff6b6b; font-weight: 700; }
.note { color: #8ab4f8; margin-top: 14px; font-size: 13px; }
"""

TERMINAL_PAGE = """<!doctype html><html><head><meta charset="utf-8"><style>{css}</style></head>
<body><h2>{title}</h2>{blocks}<p class="note">{note}</p></body></html>"""

PANEL_SELECTOR = '[data-testid="panel-content"], .panel-content'


def epoch_ms(value: str) -> int:
    return int(datetime.strptime(value, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc).timestamp() * 1000)


def wait_for_charts(page, timeout_ms: int = 30000, settle: int = 4000) -> None:
    """Panels render asynchronously; wait until at least one chart canvas exists so the
    screenshot contains real series, not an empty panel (canvases of off-screen panels are
    attached but not visible, so `attached` is used instead of `visible`)."""
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        if page.locator('canvas').count() > 0:
            break
        time.sleep(0.5)
    page.wait_for_timeout(settle)

def login(page) -> None:
    page.goto(f'{BASE}/login', wait_until='domcontentloaded')
    page.fill('input[name="user"]', USER)
    page.fill('input[name="password"]', PASSWORD)
    page.press('input[name="password"]', 'Enter')
    # The redirect after submit is asynchronous, so wait for the login page itself to go
    # away instead of matching any URL under BASE (which would match /login).
    try:
        page.wait_for_url(lambda url: '/login' not in url, timeout=15000)
    except Exception:
        pass


def shot_grafana(args, page) -> None:
    query = f'?from={epoch_ms(args.from_)}&to={epoch_ms(args.to)}&timezone=utc&refresh=0'
    if args.panel is not None:
        query += f'&viewPanel={args.panel}'
    page.goto(f'{BASE}{DASHBOARD}{query}', wait_until='domcontentloaded')
    # Panels render asynchronously; wait until the charts have drawn (canvas) so the
    # screenshot contains real series, not an empty panel.
    wait_for_charts(page, settle=args.settle)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    if args.panel is not None and page.locator(PANEL_SELECTOR).count():
        page.locator(PANEL_SELECTOR).first.screenshot(path=args.out)
    else:
        page.screenshot(path=args.out, full_page=True)
    print(args.out)


def shot_terminal(args, page) -> None:
    blocks = []
    for path in args.files:
        text = Path(path).read_text(encoding='utf-8', errors='replace').rstrip()
        body = html.escape(text)
        if 'RESULT: PASS' in text:
            body = body.replace('RESULT: PASS', '<span class="pass">RESULT: PASS</span>')
        elif 'RESULT: FAIL' in text:
            body = body.replace('RESULT: FAIL', '<span class="fail">RESULT: FAIL</span>')
        blocks.append(f'<pre>{body}</pre>')
    page.set_content(TERMINAL_PAGE.format(css=TERM_CSS, title=html.escape(args.title),
                                          blocks=''.join(blocks), note=html.escape(args.note)))
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=args.out, full_page=True)
    print(args.out)


def main() -> int:
    ap = argparse.ArgumentParser(description='Capture experiment screenshots for docs/screenshots/')
    sub = ap.add_subparsers(dest='mode', required=True)

    g = sub.add_parser('grafana', help='screenshot a real dashboard view for a time range')
    g.add_argument('--panel', type=int, help='panel id (single-panel view); omit for the whole dashboard')
    g.add_argument('--from', dest='from_', required=True, help='ISO UTC time, e.g. 2026-11-28T09:45:00Z')
    g.add_argument('--to', required=True, help='ISO UTC time')
    g.add_argument('--out', required=True)
    g.add_argument('--settle', type=int, default=4000, help='ms to wait for panels to render')

    t = sub.add_parser('terminal', help='screenshot a saved output file as a terminal page')
    t.add_argument('--title', required=True)
    t.add_argument('--file', dest='files', action='append', required=True, help='output file to render (repeatable)')
    t.add_argument('--note', default='console capture from results/')
    t.add_argument('--out', required=True)

    args = ap.parse_args()
    from playwright.sync_api import sync_playwright
    with sync_playwright() as pw:
        browser = pw.chromium.launch(channel=os.environ.get('BROWSER_CHANNEL', 'chromium'), headless=True)
        page = browser.new_page(viewport={'width': 1400, 'height': 900})
        if args.mode == 'grafana':
            login(page)
            shot_grafana(args, page)
        else:
            shot_terminal(args, page)
        browser.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
