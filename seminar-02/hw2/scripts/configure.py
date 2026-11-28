"""Fill only placeholders in the local .env; preserve existing secrets; print none."""
import re
import secrets
from pathlib import Path

root = Path(__file__).resolve().parents[1]
p = root / '.env'
if not p.exists():
    raise SystemExit('First run: cp .env.example .env')

values = {}
for line in p.read_text().splitlines():
    line = line.split('#', 1)[0].strip() if not line.strip().startswith('#') else ''
    if not line:
        continue
    key, sep, value = line.partition('=')
    if not sep or key in values:
        raise SystemExit('Malformed or duplicate .env entry')
    values[key] = value

expected = {'GITHUB_LOGIN', 'POSTGRES_USER', 'POSTGRES_DB', 'POSTGRES_PASSWORD',
            'GRAFANA_ADMIN_USER', 'GRAFANA_ADMIN_PASSWORD',
            'USERS_PER_POD', 'EVENTS_PER_SECOND', 'POISON_RATE', 'POISON_MODE',
            'KEY_MODE', 'RUN_DURATION', 'PROCESSING_DELAY_MS', 'MAX_POLL_RECORDS',
            'MAX_ATTEMPTS', 'IDLE_EXIT_SECONDS', 'PERSIST_EVERY', 'KAFKA_VERSION'}
if set(values) != expected:
    missing = sorted(expected - set(values))
    extra = sorted(set(values) - expected)
    raise SystemExit(f'.env keys mismatch; missing={missing} extra={extra}')

if values['GITHUB_LOGIN'] == 'your-github-login':
    raise SystemExit('Set GITHUB_LOGIN to your real GitHub login (verify prints it as the first line)')
if values['GRAFANA_ADMIN_PASSWORD'] != '1qa2ws3ed':
    raise SystemExit('GRAFANA_ADMIN_PASSWORD must be the fixed value 1qa2ws3ed')
if values['POSTGRES_PASSWORD'] == 'GENERATE_LOCALLY':
    values['POSTGRES_PASSWORD'] = secrets.token_hex(32)
if not re.fullmatch(r'[0-9a-f]{12,128}|lab2-demo-password', values['POSTGRES_PASSWORD']):
    raise SystemExit('POSTGRES_PASSWORD: expected a generated token or the demo value')

for key, pattern in (('USERS_PER_POD', r'[1-9][0-9]{0,2}'),
                     ('EVENTS_PER_SECOND', r'[1-9][0-9]{0,2}'),
                     ('RUN_DURATION', r'0|[1-9][0-9]{0,3}'),
                     ('PROCESSING_DELAY_MS', r'[0-9]{1,4}'),
                     ('MAX_POLL_RECORDS', r'[1-9][0-9]{0,3}'),
                     ('MAX_ATTEMPTS', r'[1-9][0-9]{0,1}'),
                     ('IDLE_EXIT_SECONDS', r'0|[1-9][0-9]{0,3}'),
                     ('PERSIST_EVERY', r'[1-9][0-9]{0,4}')):
    if not re.fullmatch(pattern, values[key]):
        raise SystemExit(f'{key}: invalid value')
if not re.fullmatch(r'0\.[0-9]{1,4}', values['POISON_RATE']):
    raise SystemExit('POISON_RATE must be a fraction like 0.02')
if values['POISON_MODE'] not in ('flag', 'corrupt'):
    raise SystemExit('POISON_MODE must be flag or corrupt')
if values['KEY_MODE'] not in ('user_id', 'none'):
    raise SystemExit('KEY_MODE must be user_id or none')

p.write_text('\n'.join(f'{k}={v}' for k, v in values.items()) + '\n')
print('.env checked; placeholders filled, nothing printed beyond this line.')
