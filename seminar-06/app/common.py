"""Image processing, SQLite transactions and demo helpers."""
import io
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import uuid
from datetime import datetime, timezone

from PIL import Image, ImageOps

DATA = Path(os.environ.get('LAB_DATA', '/data'))
DATA.mkdir(parents=True, exist_ok=True)
DEMO_DELAY = float(os.environ.get('LAB_DEMO_DELAY', '1.5'))
if not math.isfinite(DEMO_DELAY) or not 0 <= DEMO_DELAY <= 5:
    raise ValueError('LAB_DEMO_DELAY must be between 0 and 5 seconds')


def uid(label):
    return f's6.{label}.{uuid.uuid4().hex[:10]}'


def say(label, **values):
    print(json.dumps({'time': datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                      'event': label, **values}, ensure_ascii=False), flush=True)


def pause(connection=None, factor=1):
    seconds = DEMO_DELAY * factor
    if connection is not None:
        connection.sleep(seconds)
    else:
        time.sleep(seconds)


def wait_for(fn, timeout=45, label='condition'):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except Exception as exc:
            last = exc
        time.sleep(0.2)
    raise TimeoutError(f'{label} not reached; last error: {last!r}')


def fixture():
    path = DATA / 'input.png'
    if not path.exists():
        image = Image.new('RGB', (960, 640), '#285b9c')
        image.paste('#9dbce5', (80, 80, 880, 560))
        image.paste('#ffffff', (320, 180, 640, 460))
        image.save(path)
    return path


def open_db(name):
    db = sqlite3.connect(DATA / f'{name}.sqlite', timeout=10)
    db.executescript('''
        CREATE TABLE IF NOT EXISTS processed(event_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS results(job_id TEXT PRIMARY KEY, thumbnail BLOB NOT NULL);
        CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
        INSERT OR IGNORE INTO counters VALUES ('completed', 0);
    ''')
    return db


def process_image(db_name, event, dedupe=False):
    """Store the thumbnail and increment the counter in one SQLite transaction.

    With dedupe=True, record event_id in that transaction and skip existing IDs.
    Image conversion runs before the transaction and may repeat.
    """
    src = fixture() if event.get('valid', True) else DATA / 'invalid-image.txt'
    if not event.get('valid', True):
        src.write_text('This is not an image.')
    with Image.open(src) as image:
        result = ImageOps.fit(image.convert('RGB'), (160, 160))
        buf = io.BytesIO()
        result.save(buf, format='PNG')
    db = open_db(db_name)
    try:
        with db:
            if dedupe:
                inserted = db.execute('INSERT OR IGNORE INTO processed VALUES (?)',
                                      (event['event_id'],)).rowcount
                if not inserted:
                    return False
            db.execute('INSERT OR REPLACE INTO results VALUES (?, ?)',
                       (event['job_id'], buf.getvalue()))
            db.execute("UPDATE counters SET value=value+1 WHERE name='completed'")
        return True
    finally:
        db.close()


def result_count(db_name):
    db = open_db(db_name)
    try:
        return db.execute("SELECT value FROM counters WHERE name='completed'").fetchone()[0]
    finally:
        db.close()


def result_jobs(db_name):
    db = open_db(db_name)
    try:
        return db.execute('SELECT COUNT(*) FROM results').fetchone()[0]
    finally:
        db.close()


def export_result(db_name, job_id):
    db = open_db(db_name)
    try:
        row = db.execute('SELECT thumbnail FROM results WHERE job_id=?', (job_id,)).fetchone()
        if row:
            path = DATA / f'{db_name}.png'
            path.write_bytes(row[0])
            with Image.open(path) as im:
                assert im.size == (160, 160)
            return path.name
    finally:
        db.close()
