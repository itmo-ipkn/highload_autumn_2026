"""Fill only placeholders in the local .env; preserve existing secrets; print none."""
import os
import re
import secrets
from pathlib import Path
root = Path(__file__).resolve().parents[1]
p = root / '.env'
if not p.exists():
    raise SystemExit('First run: cp env.example .env')
values = {}
for line in p.read_text().splitlines():
    if not line.strip() or line.lstrip().startswith('#'):
        continue
    key, sep, value = line.partition('=')
    if not sep or key in values:
        raise SystemExit('Malformed or duplicate .env entry')
    values[key] = value
expected = {'LAB_ADMIN_TOKEN', 'GRAFANA_ADMIN_PASSWORD', 'WORKERS', 'QUEUE_LIMIT'}
if set(values) != expected:
    raise SystemExit('Expected only the four keys from env.example')
for key in ('LAB_ADMIN_TOKEN', 'GRAFANA_ADMIN_PASSWORD'):
    if values[key] == 'GENERATE_LOCALLY':
        values[key] = secrets.token_hex(32)
    if not re.fullmatch(r'[0-9a-f]{32,128}', values[key]):
        raise SystemExit(f'{key}: expected 32..128 lowercase hex characters; value not printed')
for key, hi in [('WORKERS', 64), ('QUEUE_LIMIT', 256)]:
    if not values[key].isdigit() or not 1 <= int(values[key]) <= hi:
        raise SystemExit(f'{key}: expected integer 1..{hi}')
os.chmod(p, 0o600)
p.write_text(''.join(f'{k}={v}\n' for k, v in values.items()))
(root / 'results').mkdir(exist_ok=True)
print('.env configured (mode 0600); secret values were not printed.')
