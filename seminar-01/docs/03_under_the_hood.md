# Под капотом: какие действия выполняют команды

Упаковка нужна для повторяемости, сохранения доказательств и ограничений безопасности. Она не должна скрывать измеряемую систему. Поэтому первый POST, первый `docker compose run` и первый PromQL-запрос выполняются явно, а повторные опыты используют уже разобранный код. Cloud-init тоже не «готовая ВМ»: это доступная для чтения инструкция первого запуска новой ВМ.

## Карта кода

| Файл | Вход → основное действие → результат | Что он НЕ делает |
|---|---|---|
| `cloud/provision.sh` | env.local + согласие → явные yc create → собственные network/subnet/SG/VM и state | Не авторизует, не расширяет IAM, не создаёт стенды всей группе |
| `cloud/render_cloud_init.py` | Публичный ключ + install-docker.sh → JSON cloud-config | Не вкладывает приватный ключ или облачный токен |
| `cloud/install-docker.sh` | Ubuntu 24.04 → signed apt repo → Docker/Compose | Не разворачивает приложение и не удаляет чужой Docker |
| `cloud/connect.sh` | Ключ/сохранённый IP → SSH shell и четыре loopback-туннеля | Не создаёт ВМ, не устанавливает Docker |
| `lab/scripts/configure.py` | lab/.env по шаблону → локальная генерация двух секретов и проверка полей | Не читает yc credentials и не выводит пароли |
| `lab/scripts/check.sh` | Существующий стенд → config quiet, HTTP probes и targets | Не запускает и не чинит сервисы; добавляет один /api/work |
| `lab/scripts/mode.sh` | Одно из четырёх имён → аутентифицированный POST | Не рестартует, не обнуляет counters, не запускает НТ |
| `lab/load/open.js` | RATE/DURATION/VUS → открытый поток, измерение и JSON summary | Не добавляет retries/sleep; не разрешает внешний target |
| `lab/scripts/load.sh` | 4 аргумента → тот же k6, lock и запись файлов | Не переключает режим, не прячет exit code |
| `lab/scripts/summarize.py` | Готовый JSON → печать сохранённых метрик/thresholds | Не вычисляет серверные квантили и не дописывает успех |
| `lab/scripts/query.sh` | Ключ выражения и необязательное время → HTTP API Prometheus | Не интерполирует сам и не заменяет No data нулём |
| `lab/scripts/collect.sh` | Каталог запуска → снимки и исторические PromQL-ответы | Не записывает конфигурационные секреты и не меняет сервис |
| `cloud/cleanup.sh` | Точные state IDs + CONFIRM_DELETE → проверки принадлежности и удаление | Не ищет и не удаляет все похожие ВМ |

## Почему Bash, Python и JavaScript разделены

Bash связывает процессы, параметры и файлы. Python используется там, где нужны безопасный разбор JSON, работа с датами/секретами или сам учебный HTTP-сервис. JavaScript описывает сценарий k6, который выполняется внутри его runtime. Смешивать всё в большой shell с grep/sed для JSON было бы менее прозрачно, а не проще.

`set -euo pipefail` включает остановку при ошибках, запрет необъявленных переменных и обнаружение отказа раннего процесса в pipeline. Это не транзакция: уже созданная сеть не исчезает при ошибке следующей команды. Поэтому облако хранит state после каждого шага, а load сохраняет stderr и exit code до выхода.

`source` выполняет файл как shell-код; используйте только собственный проверенный `cloud/env.local`. `export` передаёт переменную дочерним процессам. Compose отдельно читает `.env` для интерполяции. Из-за разных машин и читателей эти два конфигурационных файла разделены намеренно.

`mode.sh` передаёт секрет через stdin curl-config, чтобы не оставлять его в аргументах процесса. Но `bash -x` всё равно способен его раскрыть: нельзя трассировать секретоносные блоки на проекторе. Разбор исходника со строкой `$TOKEN` безопаснее печати готового запроса с подставленным значением.

`load.sh` ограничивает только конкурирующие запуски через себя и показанную ручную последовательность. Прямой сторонний docker run может обойти lock; один человек отвечает за свой стенд и отсутствие других генераторов. Ограничения RATE/VUS/DURATION дублируются в JS, чтобы прямой запуск был ограничен тоже.

`collect.sh` не выполняет точное сопоставление каждого запроса клиента и сервера. Он сохраняет доказательства и согласованную историческую точку. Для сравнения квантилей всё равно нужны одинаковые окна, совокупности и определения таймера. Readiness, workload validity, пользовательский SLI и состояние ресурса — четыре разных проверки.

`client_request_start_unix_ms` в k6 хранит UTC-время старта каждой попытки. Нужны только min/max, чтобы выбирать историческое окно по нагрузке, а не по времени удаления контейнера. Это служебная шкала времени, не latency. `collect.sh` отступает 2 секунды от max; для коротких тестов минутное окно всё равно включает время вне нагрузки.

## Полные исходники вспомогательных действий

Оригиналы — по путям ниже. Команды и этот листинг обновляются вместе.

### cloud/provision.sh

```bash
#!/usr/bin/env bash
# Same commands as docs/01_from_zero.md. Run on the laptop, not the VM.
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
# STEP 1 — validate the target and show the exact resource envelope.
: "${YC_PROFILE:?}" "${YC_CLOUD_ID:?}" "${YC_FOLDER_ID:?}" "${YC_ZONE:?}"
: "${LAB_NAME:?}" "${SSH_CIDR:?}" "${SSH_PUBLIC_KEY_FILE:?}"
[[ "$LAB_NAME" =~ ^[a-z][a-z0-9-]{3,39}$ ]] || { echo 'Invalid LAB_NAME'; exit 2; }
[[ -f "$SSH_PUBLIC_KEY_FILE" ]] || { echo 'Public SSH key missing'; exit 2; }
python3 - "$SSH_CIDR" <<'PYCIDR'
import ipaddress, sys
net = ipaddress.ip_network(sys.argv[1], strict=True)
if not (net.version == 4 and net.prefixlen == 32 and net.network_address.is_global):
    raise SystemExit('SSH_CIDR must be your public IPv4/32, not 0.0.0.0/0')
PYCIDR
command -v yc >/dev/null
yc version
yc --profile "$YC_PROFILE" resource-manager folder get --id "$YC_FOLDER_ID" --format json |
  python3 -c 'import json,sys; x=json.load(sys.stdin); assert x["cloud_id"]==sys.argv[1], "Cloud/folder mismatch"; print("Folder:", x["name"], x["id"])' "$YC_CLOUD_ID"
yc --profile "$YC_PROFILE" compute zone get "$YC_ZONE" --format json >/dev/null
printf 'PLAN: cloud=%s folder=%s zone=%s name=%s\n' "$YC_CLOUD_ID" "$YC_FOLDER_ID" "$YC_ZONE" "$LAB_NAME"
echo 'ONE VM: 4 vCPU/100%, 8 GB RAM, 30 GB network-ssd auto-delete boot disk.'
echo 'ONE dynamic public IPv4, own network/subnet/security group; no service account.'
printf 'Ingress: TCP/22 from %s only. This creates billable resources.\n' "$SSH_CIDR"
[[ "${APPROVE_CLOUD_CREATE:-NO}" == YES ]] || { echo 'Plan only. Review costs; set APPROVE_CLOUD_CREATE=YES to create.'; exit 0; }
# STEP 2 — create an append-only local record of exactly OUR resources.
umask 077
STATE="$PWD/cloud/.state/$LAB_NAME"
[[ ! -e "$STATE" ]] || { echo "State exists: $STATE. Inspect it; never erase it to retry."; exit 2; }
mkdir -p "$STATE"
printf '%s\n' "$YC_FOLDER_ID" > "$STATE/folder-id.txt"
printf '%s\n' "$LAB_NAME" > "$STATE/lab-name.txt"
yc version > "$STATE/yc-version.txt"
date -u +%FT%TZ > "$STATE/create-start.txt"
trap 'echo "Stopped: inspect $STATE and actual yc resources/operations before retrying." >&2' ERR
id_of() { python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$1"; }
# STEP 3 — resolve the image and prepare first-boot instructions, with no secrets.
yc --profile "$YC_PROFILE" compute image get-latest-from-family ubuntu-2404-lts \
  --folder-id standard-images --format json > "$STATE/image.json"
IMAGE_ID=$(id_of "$STATE/image.json")
python3 cloud/render_cloud_init.py "$SSH_PUBLIC_KEY_FILE" "$STATE/cloud-init.json"
# STEP 4 — create the network and a subnet in the SAME zone as the VM.
yc --profile "$YC_PROFILE" vpc network create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-net" --labels "course=highload-s1,lab=$LAB_NAME" \
  --format json > "$STATE/network.json"
NETWORK_ID=$(id_of "$STATE/network.json")
yc --profile "$YC_PROFILE" vpc subnet create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-subnet" --zone "$YC_ZONE" --network-id "$NETWORK_ID" \
  --range 10.231.0.0/24 --labels "course=highload-s1,lab=$LAB_NAME" \
  --format json > "$STATE/subnet.json"
SUBNET_ID=$(id_of "$STATE/subnet.json")
# STEP 5 — only SSH comes IN; package/image downloads may go OUT.
yc --profile "$YC_PROFILE" vpc security-group create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-sg" --network-id "$NETWORK_ID" \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --rule "direction=ingress,protocol=tcp,port=22,v4-cidrs=$SSH_CIDR" \
  --rule "direction=egress,protocol=any,from-port=0,to-port=65535,v4-cidrs=0.0.0.0/0" \
  --format json > "$STATE/security-group.json"
SG_ID=$(id_of "$STATE/security-group.json")
# STEP 6 — create a FRESH VM; Docker installs during first boot via cloud-init.
yc --profile "$YC_PROFILE" compute instance create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME" --zone "$YC_ZONE" --platform standard-v3 \
  --cores 4 --core-fraction 100 --memory 8 \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --create-boot-disk "name=$LAB_NAME-boot,type=network-ssd,size=30,image-id=$IMAGE_ID,auto-delete=true" \
  --network-interface "subnet-id=$SUBNET_ID,nat-ip-version=ipv4,security-group-ids=$SG_ID" \
  --metadata-from-file "user-data=$STATE/cloud-init.json" \
  --format json > "$STATE/instance.json"
# STEP 7 — record the returned VM/disk/IP, not names guessed from a console screen.
python3 - "$STATE/instance.json" "$STATE" <<'PYSTATE'
import json, sys
from pathlib import Path
x = json.load(open(sys.argv[1])); p = Path(sys.argv[2])
ip = x['network_interfaces'][0]['primary_v4_address']['one_to_one_nat']['address']
(p/'ip.txt').write_text(ip+'\n')
(p/'instance-id.txt').write_text(x['id']+'\n')
(p/'boot-disk-id.txt').write_text(x['boot_disk']['disk_id']+'\n')
print('VM:', x['id'], 'IP:', ip)
PYSTATE
date -u +%FT%TZ > "$STATE/create-returned.txt"
echo 'VM resource created. This does NOT prove cloud-init, Docker, or the application is ready.'
```

### cloud/render_cloud_init.py

```python
"""Build readable cloud-init using only a public SSH key and the reviewed installer."""
import json
import sys
from pathlib import Path
if len(sys.argv) != 3:
    raise SystemExit("Usage: python3 cloud/render_cloud_init.py public_key output.json")
key = Path(sys.argv[1]).read_text().strip()
if "\n" in key or not key.startswith(("ssh-ed25519 ", "ssh-rsa ", "ecdsa-sha2-")):
    raise SystemExit("Expected exactly one SSH PUBLIC key")
installer = Path(__file__).with_name("install-docker.sh").read_text()
config = {
    "users": [{"name": "lab", "groups": ["sudo"], "shell": "/bin/bash",
               "sudo": ["ALL=(ALL) NOPASSWD:ALL"], "lock_passwd": True,
               "ssh_authorized_keys": [key]}],
    "ssh_pwauth": False,
    "disable_root": True,
    "write_files": [{"path": "/opt/s1/install-docker.sh", "permissions": "0755",
                     "owner": "root:root", "content": installer}],
    "runcmd": [["bash", "/opt/s1/install-docker.sh"]],
}
# JSON is YAML; $$ prevents yc from expanding the laptop environment in user-data.
Path(sys.argv[2]).write_text("#cloud-config\n" + json.dumps(config, indent=2).replace("$", "$$") + "\n")
```

### cloud/install-docker.sh

```bash
#!/usr/bin/env bash
# Execute on the dedicated Ubuntu VM. Uses the official signed Docker apt repository.
set -euo pipefail
source /etc/os-release
[[ "$ID" == ubuntu && "$VERSION_ID" == 24.04 ]] || { echo 'Expected Ubuntu 24.04. Review installation for this OS.'; exit 2; }
if command -v docker >/dev/null; then
  sudo docker version; sudo docker compose version
  echo 'Existing Docker detected; no package replacement performed.'
  exit 0
fi
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y ca-certificates curl python3
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
ARCH=$(dpkg --print-architecture)
printf '%s\n' \
  'Types: deb' 'URIs: https://download.docker.com/linux/ubuntu' \
  "Suites: $VERSION_CODENAME" 'Components: stable' "Architectures: $ARCH" \
  'Signed-By: /etc/apt/keyrings/docker.asc' | sudo tee /etc/apt/sources.list.d/docker.sources >/dev/null
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -o DPkg::Lock::Timeout=300 install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
# Docker group is root-equivalent. Only this isolated lab user gets it.
sudo usermod -aG docker lab
sudo docker version
sudo docker compose version
echo 'Open a NEW SSH session before using docker without sudo.'
```

### cloud/connect.sh

```bash
#!/usr/bin/env bash
# Run AFTER verifying and accepting the host key (docs/01_from_zero.md).
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
: "${LAB_NAME:?}" "${SSH_PRIVATE_KEY_FILE:?}"
IP=$(cat "cloud/.state/$LAB_NAME/ip.txt")
exec ssh -i "$SSH_PRIVATE_KEY_FILE" -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$PWD/cloud/known_hosts" \
  -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 \
  -L "127.0.0.1:${LOCAL_GRAFANA_PORT:-3000}:127.0.0.1:3000" \
  -L "127.0.0.1:${LOCAL_PROMETHEUS_PORT:-9090}:127.0.0.1:9090" \
  -L "127.0.0.1:${LOCAL_API_PORT:-8080}:127.0.0.1:8080" \
  -L "127.0.0.1:${LOCAL_ADMIN_PORT:-8081}:127.0.0.1:8081" \
  "lab@$IP"
```

### lab/scripts/configure.py

```python
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
```

### lab/scripts/check.sh

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo 'Copy env.example to .env and run python3 scripts/configure.py'; exit 1; }
docker compose config --quiet
curl -fsS --max-time 5 http://127.0.0.1:8080/healthz
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:8080/api/work
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:9090/-/ready
curl -fsS --max-time 5 http://127.0.0.1:3000/api/health
printf '\n'
curl -fsS --max-time 5 http://127.0.0.1:9090/api/v1/targets | python3 -c '
import sys,json
v=json.load(sys.stdin)["data"]["activeTargets"]
for x in v: print(x["labels"].get("job"), x["health"], x.get("lastError",""))
assert any(x["labels"].get("job")=="app" and x["health"]=="up" for x in v), "app target is not UP"
'
echo 'Readiness OK. node DOWN is expected without the Linux --profile host.'
```

### lab/scripts/mode.sh

```bash
#!/usr/bin/env bash
# One authenticated POST. No service restart, no load, no counter reset.
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-normal}
case "$MODE" in normal|tail|slow|errors) ;; *) echo 'Use normal|tail|slow|errors' >&2; exit 2;; esac
TOKEN=$(python3 - <<'PYTOKEN'
from pathlib import Path
import re
values = dict(line.split('=', 1) for line in Path('.env').read_text().splitlines() if line and not line.startswith('#'))
v = values['LAB_ADMIN_TOKEN']
if not re.fullmatch(r'[0-9a-f]{32,128}', v):
    raise SystemExit('Run python3 scripts/configure.py')
print(v)
PYTOKEN
)
# Feed the header through stdin: no token in argv/ps or shell trace. Never use bash -x.
printf 'header = "X-Lab-Token: %s"\n' "$TOKEN" | \
  curl --config - --fail-with-body --silent --show-error --max-time 5 \
  -H 'Content-Type: application/json' -d "{\"mode\":\"$MODE\"}" \
  http://127.0.0.1:8081/admin/mode
unset TOKEN
printf '\n'
```

### lab/load/open.js

```javascript
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const rate = Number(__ENV.RATE || 80);
const vus = Number(__ENV.VUS || 160);
if (!(Number.isInteger(rate) && rate > 0 && rate <= 500 && Number.isInteger(vus) && vus > 0 && vus <= 500)) throw new Error('RATE/VUS outside lab limits');
const duration = __ENV.DURATION || '120s';
if (!/^[1-9][0-9]{0,2}s$/.test(duration) || Number(duration.slice(0,-1)) < 5 || Number(duration.slice(0,-1)) > 600) throw new Error('DURATION must be 5s..600s');
if (!['0','1'].includes(__ENV.EXPECT_NORMAL || '0')) throw new Error('EXPECT_NORMAL must be 0 or 1');
const base = __ENV.BASE_URL || 'http://app:8080';
const allowed = /^http:\/\/(app|127\.0\.0\.1|localhost)(:\d+)?$/;
if (!allowed.test(base)) throw new Error('Use only the private lab endpoint; arbitrary public targets are forbidden');
const started = new Counter('client_started');
// Only min/max are used: UTC request-start bounds, not latency or a new SLI.
const requestStarts = new Trend('client_request_start_unix_ms');
const success = new Rate('client_success');
const good = new Rate('client_good');
const wall = new Trend('client_wall_ms', true);
const wallSuccess = new Trend('client_wall_success_ms', true);
const valid = new Rate('client_payload_valid');
export const options = {
  scenarios: { api: {
    executor: 'constant-arrival-rate', rate, timeUnit:'1s',
    duration, preAllocatedVUs:vus, maxVUs:vus,
    gracefulStop:'5s',
  }},
  summaryTrendStats: ['avg','min','med','p(95)','p(99)','max'],
  thresholds: {
    dropped_iterations:['count==0'],
    client_started:['count>0'],
    client_payload_valid:['rate==1'],
    ...(__ENV.EXPECT_NORMAL === '1' ? {client_good:['rate>=0.99'], http_req_failed:['rate<0.01']} : {}),
  },
};
export default function () {
  started.add(1);
  const t = Date.now();
  requestStarts.add(t);
  const r = http.get(`${base}/api/work`, {timeout:'3s', tags:{name:'/api/work'}, redirects:0});
  let payloadOk = false;
  try { const p=r.json(); payloadOk = r.status===200 ? p.ok===true : ([500,503].includes(r.status) && p.ok===false); } catch (_) {}
  const elapsed = Date.now()-t; // HTTP call plus payload validation.
  wall.add(elapsed);
  if (r.status===200 && payloadOk) wallSuccess.add(elapsed);
  success.add(r.status===200 && payloadOk);
  good.add(r.status===200 && payloadOk && elapsed<=300);
  valid.add(payloadOk);
  check(r, {'HTTP 200':x=>x.status===200});
  // No sleep, no retries, one HTTP request per iteration, no redirects.
}
export function handleSummary(data) {
  // Stdout JSON can be redirected without root-owned Docker bind-mount artifacts.
  return { stdout: JSON.stringify({label:__ENV.RUN_LABEL||'manual',
     rate, duration:__ENV.DURATION||'120s', vu_limit:vus, summary:data},null,2)+'\n' };
}
```

### lab/scripts/load.sh

```bash
#!/usr/bin/env bash
# Capture the SAME k6 command used manually. Arguments are not hidden defaults.
set -euo pipefail
cd "$(dirname "$0")/.."
LABEL=${1:-manual}; RATE=${2:-80}; DURATION=${3:-120s}; EXPECT_NORMAL=${4:-0}; VUS=${VUS:-160}
[[ "$LABEL" =~ ^[a-zA-Z0-9_-]{1,60}$ ]] || { echo 'Unsafe run label' >&2; exit 2; }
[[ "$RATE" =~ ^[1-9][0-9]{0,2}$ ]] && (( RATE <= 500 )) || { echo 'RATE must be 1..500' >&2; exit 2; }
[[ "$DURATION" =~ ^[1-9][0-9]{0,2}s$ ]] && (( ${DURATION%s} >= 5 && ${DURATION%s} <= 600 )) || { echo 'DURATION must be 5s..600s' >&2; exit 2; }
[[ "$VUS" =~ ^[1-9][0-9]{0,2}$ ]] && (( VUS <= 500 )) || { echo 'VUS must be 1..500' >&2; exit 2; }
[[ "$EXPECT_NORMAL" == 0 || "$EXPECT_NORMAL" == 1 ]] || { echo 'EXPECT_NORMAL must be 0 or 1' >&2; exit 2; }
mkdir -p results
# mkdir is atomic: two wrappers cannot silently run on the same stand at once.
mkdir results/.load.lock 2>/dev/null || { echo 'A run is already active; inspect docker compose --profile load ps before clearing a stale lock.' >&2; exit 2; }
trap 'rmdir results/.load.lock 2>/dev/null || true' EXIT
OUT=$(mktemp -d "results/$(date -u +%Y%m%dT%H%M%SZ)-$LABEL-XXXXXX")
printf '%s\n' "$OUT" > results/.last-run.txt
date -u +%FT%TZ > "$OUT/start.txt"
printf 'label=%s\nrate=%s\nduration=%s\nvus=%s\nexpect_normal=%s\n' "$LABEL" "$RATE" "$DURATION" "$VUS" "$EXPECT_NORMAL" > "$OUT/parameters.txt"
echo "Running $LABEL: $RATE iterations/s, $DURATION, $VUS VUs. Result: $OUT" >&2
set +e
docker compose --profile load run --rm -T --interactive=false k6 run --quiet \
  -e RATE="$RATE" -e DURATION="$DURATION" -e VUS="$VUS" \
  -e RUN_LABEL="$LABEL" -e EXPECT_NORMAL="$EXPECT_NORMAL" \
  /scripts/open.js > "$OUT/summary.json" 2> "$OUT/stderr.log"
RC=$?
set -e
date -u +%FT%TZ > "$OUT/end.txt"
printf '%s\n' "$RC" > "$OUT/exit-code.txt"
if ! python3 scripts/summarize.py "$OUT/summary.json"; then
  cat "$OUT/stderr.log" >&2
  # A bad/missing summary must not replace k6's failure code with success or another code.
  if (( RC != 0 )); then exit "$RC"; fi
  exit 1
fi
if (( RC != 0 )); then echo "k6 exit=$RC. Read thresholds/stderr. Failures are not suppressed." >&2; fi
exit "$RC"
```

### lab/scripts/summarize.py

```python
"""Read the exact JSON emitted by our k6 handleSummary()."""
import json, sys
if len(sys.argv)!=2: raise SystemExit('Usage: python3 scripts/summarize.py results/file.json')
with open(sys.argv[1],encoding='utf-8') as f: data=json.load(f)
m=data['summary']['metrics']
def values(key): return m.get(key,{}).get('values',{})
print('RUN:',data.get('label'),'requested:',data.get('rate'),'iterations/s')
for key in ['client_started','http_reqs','iterations','dropped_iterations','http_req_failed','client_success','client_good','client_payload_valid','client_wall_ms','client_wall_success_ms','http_req_duration']:
    print(key, json.dumps(values(key),ensure_ascii=False))
print('All threshold failures:')
for key,value in m.items():
    for threshold,result in value.get('thresholds',{}).items():
        if not result.get('ok',True): print('FAIL',key,threshold)
```

### lab/scripts/query.sh

```bash
#!/usr/bin/env bash
# Read one PromQL expression and send an HTTP GET to Prometheus; optional Unix time.
set -euo pipefail
cd "$(dirname "$0")/.."
KEY=${1:-good}; AT=${2:-}
QUERY=$(python3 - "$KEY" <<'PYQUERY'
import json, sys
q=json.load(open('prometheus/queries.json'))
if sys.argv[1] not in q: raise SystemExit('Unknown query key: '+sys.argv[1])
print(q[sys.argv[1]])
PYQUERY
)
ARGS=(--data-urlencode "query=$QUERY")
if [[ -n "$AT" ]]; then ARGS+=(--data-urlencode "time=$AT"); fi
curl --fail --silent --show-error --max-time 10 -G http://127.0.0.1:9090/api/v1/query   "${ARGS[@]}" | python3 -c '
import json,sys
x=json.load(sys.stdin)
if x.get("status") != "success": raise SystemExit(json.dumps(x))
print(json.dumps(x,indent=2))
'
```

### lab/scripts/collect.sh

```bash
#!/usr/bin/env bash
# Save an evidence bundle for ONE run. No cloud calls and no secret dumps.
set -euo pipefail
cd "$(dirname "$0")/.."
RUN=${1:-$(cat results/.last-run.txt)}
[[ -d "$RUN" && -f "$RUN/end.txt" && -f "$RUN/start.txt" ]] || { echo 'Expected a completed run directory' >&2; exit 2; }
OUT="$RUN/observations"
[[ ! -e "$OUT" ]] || { echo 'Observations already exist; refusing overwrite' >&2; exit 2; }
mkdir "$OUT"
# Anchor to actual k6 request starts, not delayed Docker container teardown.
AT=$(python3 - "$RUN/summary.json" <<'PYTIME'
import json,math,sys
x=json.load(open(sys.argv[1]))['summary']['metrics']['client_request_start_unix_ms']['values']
first,last=float(x['min']),float(x['max'])
if not (math.isfinite(first) and math.isfinite(last) and first <= last):
    raise SystemExit('Invalid k6 request time bounds')
# Leave two seconds before the final request. Rate still uses its own 1m window.
print(math.floor(last/1000)-2)
PYTIME
)
printf '%s\n' "$AT" > "$OUT/evaluation-unix.txt"
curl -fsS --max-time 10 http://127.0.0.1:8080/metrics > "$OUT/app.prom"
curl -fsS --max-time 10 http://127.0.0.1:9090/api/v1/targets > "$OUT/targets.json"
curl -fsS --max-time 10 http://127.0.0.1:9090/api/v1/alerts > "$OUT/alerts.json"
docker compose ps > "$OUT/compose-ps.txt"
docker compose images > "$OUT/images.txt"
IDS=$(docker compose --profile host ps -q)
if [[ -n "$IDS" ]]; then docker stats --no-stream $IDS > "$OUT/docker-stats.txt"; fi
docker compose logs --no-color --tail 120 app > "$OUT/app.log"
for KEY in started_rps completed_rps success_rps good_rps errors mean p50 p95 p99 good up inflight active waiting capacity queue_p95 service_mean cpu rss host_cpu host_ram host_busy_core mode resets; do
  bash scripts/query.sh "$KEY" "$AT" > "$OUT/$KEY.json"
done
# A 60 s range ending at AT preserves the transition/plateau instead of one number.
python3 - "$AT" <<'PYRANGE' > "$OUT/good-range.json"
import json,sys,urllib.parse,urllib.request
at=int(sys.argv[1]); query=json.load(open('prometheus/queries.json'))['good']
url='http://127.0.0.1:9090/api/v1/query_range?' + urllib.parse.urlencode({'query':query,'start':at-60,'end':at,'step':5})
with urllib.request.urlopen(url,timeout=10) as r: x=json.load(r)
assert x.get('status')=='success', x
print(json.dumps(x,indent=2))
PYRANGE
echo "$OUT"
```

### cloud/cleanup.sh

```bash
#!/usr/bin/env bash
# Delete only exact recorded IDs, after label/name/folder verification.
set -euo pipefail
cd "$(dirname "$0")/.."
source cloud/env.local
: "${LAB_NAME:?}" "${YC_FOLDER_ID:?}"
STATE="cloud/.state/$LAB_NAME"
[[ -d "$STATE" ]] || { echo 'No local state; refusing discovery-based deletion'; exit 2; }
[[ $(cat "$STATE/folder-id.txt") == "$YC_FOLDER_ID" ]] || { echo 'Folder mismatch'; exit 2; }
[[ "${CONFIRM_DELETE:-}" == "$LAB_NAME" ]] || { echo "Deletion not approved. Set CONFIRM_DELETE exactly to $LAB_NAME after saving evidence."; exit 0; }
id_of() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["id"])' "$1"; }
for ITEM in instance security-group subnet network; do
  FILE="$STATE/$ITEM.json"
  [[ -s "$FILE" ]] || continue
  [[ -f "$STATE/$ITEM.deleted" ]] && continue
  ID=$(id_of "$FILE")
  if [[ "$ITEM" == instance ]]; then CMD=(compute instance); else CMD=(vpc "$ITEM"); fi
  yc --profile "$YC_PROFILE" "${CMD[@]}" get --id "$ID" --folder-id "$YC_FOLDER_ID" --format json > "$STATE/$ITEM.before-delete.json"
  python3 - "$STATE/$ITEM.before-delete.json" "$LAB_NAME" "$YC_FOLDER_ID" <<'PY'
import json,sys
x=json.load(open(sys.argv[1])); assert x.get('labels',{}).get('lab')==sys.argv[2], 'Label mismatch'
assert x.get('folder_id')==sys.argv[3], 'Folder mismatch'
PY
  yc --profile "$YC_PROFILE" "${CMD[@]}" delete --id "$ID" --folder-id "$YC_FOLDER_ID"
  date -u +%FT%TZ > "$STATE/$ITEM.deleted"
done
if [[ -s "$STATE/boot-disk-id.txt" ]]; then
  DISK_ID=$(cat "$STATE/boot-disk-id.txt")
  set +e
  yc --profile "$YC_PROFILE" compute disk get --id "$DISK_ID" \
    --folder-id "$YC_FOLDER_ID" --format json \
    > "$STATE/disk-after.json" 2> "$STATE/disk-after.stderr"
  DISK_RC=$?
  set -e
  if (( DISK_RC == 0 )); then
    echo "Boot disk $DISK_ID still exists. Inspect this exact ID; not deleting it silently." >&2
    exit 1
  fi
  # Accept only the explicit gRPC NotFound status, never auth/network/permission errors.
  if ! grep -Eq 'code[[:space:]]*=[[:space:]]*NotFound' "$STATE/disk-after.stderr"; then
    echo 'Disk absence is NOT verified. Inspect disk-after.stderr / current CLI error format.' >&2
    exit 1
  fi
  date -u +%FT%TZ > "$STATE/boot-disk.absence-verified"
fi
echo 'Recorded VM/network resources deleted. Verify boot disk ID is absent (auto-delete) and review remaining billable resources.'
echo 'Do not delete any other disk or network merely because its name looks similar.'
```
