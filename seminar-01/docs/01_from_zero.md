# С нуля: ВМ, подключение и запуск стенда

## 0. Где выполняются команды

Все относительные пути указаны от корня проекта. Команды для ВМ выполняются в `~/highload-s1/lab`.

Терминал **L** — ноутбук: `yc`, SSH, передача файлов и удаление облачных ресурсов. Терминалы **A** и **B** — ваша ВМ, папка `~/highload-s1/lab`: A запускает нагрузку, B выполняет запросы метрик и переключает режим. Пока SSH держит туннель, браузер ноутбука открывает адреса `127.0.0.1:3000` и `127.0.0.1:9090`.

На macOS по умолчанию может быть zsh. Наши блоки рассчитаны на Bash, включая массивы параметров SSH. В терминале L запустите `/bin/bash`, затем перейдите в корень репозитория. PowerShell напрямую не подходит: используйте WSL2 с Bash. Выполняйте разделы по порядку: создание ВМ и удаление ресурсов требуют отдельного подтверждения.

## 1. Подготовка аккаунта и SSH-ключа

Установите CLI по [официальной инструкции](https://yandex.cloud/ru/docs/cli/quickstart). Старый CLI обновите и **перезапустите терминал**:

```bash
yc components update
# Затем открыть новый терминал, снова /bin/bash, вернуться в корень репозитория.
yc version
yc init
```

В текущем CLI вход выполняется через браузер. Не следуйте старой инструкции с копированием OAuth-токена из Яндекс ID: Yandex Cloud перестал принимать новые такие токены с 1 июня 2026 года. Авторизация нужна только на ноутбуке. `yc config list` может содержать учётные данные; для проверки настроек используйте отдельные команды:

```bash
yc config profile list
yc --profile default config get cloud-id
yc --profile default config get folder-id
yc --profile default config get compute-default-zone
# Посмотрите разрешённые лично вам облака и каталоги:
yc --profile default resource-manager cloud list
yc --profile default resource-manager folder list
```

Имя `default` замените на выбранный профиль, если он другой. Наличие каталога в списке не доказывает права на создание ВМ или наличие квоты. Получите разрешение владельца облака на одну учебную ВМ и проверьте квоты/биллинг. Не передавайте свой профиль другим пользователям.

Создайте отдельный SSH-ключ, не перезаписывая имеющийся:

```bash
mkdir -p "$HOME/.ssh"
chmod 700 "$HOME/.ssh"
if [ ! -f "$HOME/.ssh/highload_s1" ]; then
  ssh-keygen -t ed25519 -f "$HOME/.ssh/highload_s1" -C highload-s1
fi
ssh-keygen -lf "$HOME/.ssh/highload_s1.pub"
```

Ввод пароля ключа приватный. При необходимости загрузите ключ в свой SSH agent: `ssh-add "$HOME/.ssh/highload_s1"`. Файл `.pub` можно передать в metadata ВМ. Файл без `.pub` — приватный ключ: его не копируем на ВМ, не публикуем и не показываем. Согласованные расходы проверяем по [ценам Compute Cloud](https://yandex.cloud/ru/docs/compute/pricing) и [VPC](https://yandex.cloud/ru/docs/vpc/pricing), без предположения, что грант или неработающий процесс делают ресурсы бесплатными.

## 2. Заполняем cloud/env.local на ноутбуке

```bash
# L, корень репозитория
cp cloud/env.example cloud/env.local
chmod 600 cloud/env.local
```

Заполните и сохраните `cloud/env.local`. Значения полей описаны ниже.

| Поле | Откуда значение и зачем оно нужно |
|---|---|
| `YC_PROFILE` | Имя уже настроенного профиля из `yc config profile list`. Профиль содержит аутентификацию и остаётся на ноутбуке. |
| `YC_CLOUD_ID` | ID облака из `config get cloud-id` или `cloud list`. Код проверяет, что выбранный каталог принадлежит этому облаку. |
| `YC_FOLDER_ID` | ID выделенного вам каталога из `folder list`. Не название каталога и не cloud ID. |
| `YC_ZONE` | Доступная зона, например `ru-central1-a`. Подсеть и ВМ должны находиться в одной зоне. |
| `LAB_NAME` | Уникальное имя, например `s1-ivan-01`: латиница в нижнем регистре, цифры и дефисы, 4–40 символов, начинается с буквы. Один человек — один стенд. Для новой независимой попытки новое имя. |
| `SSH_CIDR` | Ваш текущий внешний IPv4 с `/32`, а не адрес Wi-Fi вида `192.168...`. Узнайте его у администратора сети или внешнего сервиса определения IP. При VPN/смене сети значение меняется. |
| `SSH_PUBLIC_KEY_FILE` | Полный путь к публичному SSH-ключу `.pub` на ноутбуке. |
| `SSH_PRIVATE_KEY_FILE` | Полный путь к соответствующему приватному ключу. Это путь, не содержимое ключа. |
| `LOCAL_*_PORT` | Локальные порты туннеля на ноутбуке. По умолчанию 3000, 9090, 8080, 8081. Если заняты, измените, например, на 13000, 19090, 18080, 18081. |

Для удобства текущий исходящий IPv4 можно проверить командой `curl -4 --fail --max-time 10 https://api.ipify.org; printf '\n'`. Это внешнему сервису раскрывает факт запроса и IP; в корпоративной сети используйте разрешённый способ. Впишите результат как `A.B.C.D/32`, не копируйте эту строку буквально. Скрипт отклонит `/0` и частный IPv4.

```bash
# source читает доверенный локальный Bash-файл в текущий shell.
source cloud/env.local
# Проверяем совместимость установленного CLI до первого create.
yc --profile "$YC_PROFILE" compute instance create --help
yc --profile "$YC_PROFILE" vpc security-group create --help
# Без APPROVE_CLOUD_CREATE это только read-only проверки и вывод плана.
APPROVE_CLOUD_CREATE=NO bash cloud/provision.sh
```

`export` делает значение доступным дочернему процессу. Файл `env.example` автоматически никем не читается. `cloud/env.local` считывают наши cloud-скрипты; Docker его не использует. Никогда не вставляйте сюда OAuth/IAM-токен, пароль Grafana или приватный ключ. Токен не нужен даже в metadata ВМ: сервисный аккаунт не подключается.

## 3. Создание ВМ

Выполняйте блоки последовательно в одном терминале L, из корня проекта. Для выбранного `LAB_NAME` ещё не должно быть созданных ресурсов. Эти же шаги выполняет `cloud/provision.sh`.

После проверки лимита расходов явно разрешите создание:

```bash
source cloud/env.local
export APPROVE_CLOUD_CREATE=YES
# Ошибка останавливает последовательность; не продолжайте с пустыми ID.
set -euo pipefail
```

Ниже первый шаг проверяет каталог, IP и согласие; следующие сохраняют точные ID, создают сеть и запускают ВМ. У shell с `set -e` ошибка может закрыть сессию: откройте новый Bash и исследуйте уже созданные ресурсы, не повторяйте `create` автоматически.

```bash
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
```

```bash
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
```

```bash
# STEP 3 — resolve the image and prepare first-boot instructions, with no secrets.
yc --profile "$YC_PROFILE" compute image get-latest-from-family ubuntu-2404-lts \
  --folder-id standard-images --format json > "$STATE/image.json"
IMAGE_ID=$(id_of "$STATE/image.json")
python3 cloud/render_cloud_init.py "$SSH_PUBLIC_KEY_FILE" "$STATE/cloud-init.json"
```

```bash
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
```

```bash
# STEP 5 — only SSH comes IN; package/image downloads may go OUT.
yc --profile "$YC_PROFILE" vpc security-group create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME-sg" --network-id "$NETWORK_ID" \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --rule "direction=ingress,protocol=tcp,port=22,v4-cidrs=$SSH_CIDR" \
  --rule "direction=egress,protocol=any,from-port=0,to-port=65535,v4-cidrs=0.0.0.0/0" \
  --format json > "$STATE/security-group.json"
SG_ID=$(id_of "$STATE/security-group.json")
```

```bash
# STEP 6 — create a FRESH VM; Docker installs during first boot via cloud-init.
yc --profile "$YC_PROFILE" compute instance create --folder-id "$YC_FOLDER_ID" \
  --name "$LAB_NAME" --zone "$YC_ZONE" --platform standard-v3 \
  --cores 4 --core-fraction 100 --memory 8 \
  --labels "course=highload-s1,lab=$LAB_NAME" \
  --create-boot-disk "name=$LAB_NAME-boot,type=network-ssd,size=30,image-id=$IMAGE_ID,auto-delete=true" \
  --network-interface "subnet-id=$SUBNET_ID,nat-ip-version=ipv4,security-group-ids=$SG_ID" \
  --metadata-from-file "user-data=$STATE/cloud-init.json" \
  --format json > "$STATE/instance.json"
```

```bash
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

Сеть изолирует ваш стенд; подсеть выделяет частные адреса в выбранной зоне. Входящее правило разрешает только TCP/22 с вашего IP. Исходящее правило разрешает скачивание пакетов и образов — оно **не открывает входящие подключения**. Публичный IPv4 используется для SSH, не для публикации Grafana.

`--metadata-from-file` передаёт cloud-init публичный SSH-ключ и содержимое `install-docker.sh`. JSON после `#cloud-config` допустим как YAML. CLI подставляет `$NAME` из окружения ноутбука даже при `--metadata-from-file`; генератор удваивает `$`, и на ВМ установщик получает исходные `$ID`, `$VERSION_ID`, `$ARCH`. Без этого проверка ОС и apt source ломаются. На первом старте cloud-init создаёт пользователя `lab`, пишет установщик и запускает его. ВМ не скачивает приватный репозиторий и не получает GitHub/yc-токен. Просмотрите `render_cloud_init.py` и `install-docker.sh`: установка идёт из официального подписанного apt-репозитория Docker, не через `curl | sh`.

Docker устанавливается автоматически при первом запуске ВМ. На свежей Ubuntu unattended-upgrades может удерживать dpkg lock; обе установки пакетов ждут освобождения до 300 секунд, не удаляя lock и не прерывая обновления. `yc compute instance create` возвращает созданный облачный ресурс; это не сигнал готовности cloud-init, Docker или сервиса. Длительность зависит от облака, apt и registry. Не переносите удачный результат предыдущего запуска на новый без проверки.

Повторять весь путь вручную не обязательно: **на новом имени стенда вместо раздела 3**, а не после него, выполните `APPROVE_CLOUD_CREATE=YES bash cloud/provision.sh`. Скрипт содержит те же API-команды и защитные проверки. Не запускайте его на уже созданном стенде и не удаляйте `.state`, чтобы обойти защиту от дубликатов.

## 4. Первое SSH-подключение и проверка bootstrap

```bash
# L, корень репозитория. Работает и в НОВОМ терминале после ручного создания.
source cloud/env.local
STATE="$PWD/cloud/.state/$LAB_NAME"
IP=$(cat "$STATE/ip.txt")
VM_ID=$(cat "$STATE/instance-id.txt")
# cloud-init обычно выводит fingerprints host keys в serial log.
yc --profile "$YC_PROFILE" compute instance get-serial-port-output \
  --id "$VM_ID" --folder-id "$YC_FOLDER_ID" > "$STATE/serial.txt"
# Найдите fingerprint ED25519; весь лог на проекторе не нужен.
grep -A8 -B2 'SSH HOST KEY FINGERPRINTS' "$STATE/serial.txt" || true

# В первом SSH сравните показанный fingerprint с serial log именно этой ВМ.
ssh -i "$SSH_PRIVATE_KEY_FILE" -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=ask -o "UserKnownHostsFile=$PWD/cloud/known_hosts" \
  "lab@$IP"
```

SSH может стать доступен раньше завершения установки. При timeout проверьте RUNNING, IP и актуальный `SSH_CIDR`, затем повторите подключение без пересоздания ВМ. Если fingerprints ещё нет, повторно получите serial log после старта. При несовпадении остановитесь; не используйте `StrictHostKeyChecking=no` и не удаляйте старый ключ вслепую.

```bash
# ВМ, первая SSH-сессия. sudo нужен для чтения статуса/журналов cloud-init.
sudo cloud-init status --wait
# Если cloud-init сообщил ошибку, сначала:
# sudo tail -n 80 /var/log/cloud-init-output.log
sudo docker version
sudo docker compose version
exit
```

После установки открываем **новую** SSH-сессию: членство `lab` в группе `docker` применяется при новом входе. Членство в этой группе эквивалентно административным полномочиям, поэтому это допустимо только на отдельной учебной ВМ. Не давайте так доступ к чужому production-хосту.

## 5. Передаём только код лабораторной

```bash
# L, корень репозитория; не переносим cloud/env.local, ключи, .env и state.
source cloud/env.local
IP=$(cat "cloud/.state/$LAB_NAME/ip.txt")
SSH_OPTS=(-i "$SSH_PRIVATE_KEY_FILE" -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$PWD/cloud/known_hosts")
UPLOAD=$(mktemp /tmp/highload-s1-upload.XXXXXX)
COPYFILE_DISABLE=1 tar --no-xattrs --exclude='._*' --exclude='.DS_Store' \
  --exclude='.env' --exclude='results' --exclude='__pycache__' --exclude='.venv' -czf "$UPLOAD" lab
scp "${SSH_OPTS[@]}" "$UPLOAD" "lab@$IP:/tmp/highload-s1-upload.tgz"
ssh "${SSH_OPTS[@]}" "lab@$IP" \
  'mkdir -p ~/highload-s1 && tar -xzf /tmp/highload-s1-upload.tgz -C ~/highload-s1 && rm /tmp/highload-s1-upload.tgz'
rm "$UPLOAD"
```

`COPYFILE_DISABLE=1`, `--no-xattrs` и исключение `._*` убирают метаданные macOS: Linux иначе получает AppleDouble файлы вроде `._default.yml`, которые Grafana ошибочно читает как provisioning. Секретные/runtime файлы также исключены.

Это не скачивание репозитория из ВМ. Ваш ноутбук отправляет ровно папку `lab/`; следовательно, путь работает и до публикации Git-репозитория, и без доступа ВМ к GitHub.

Теперь откройте туннели и shell в терминале L. Команда полностью приведена в `cloud/connect.sh`:

```bash
bash cloud/connect.sh
# Новый shell уже НА ВМ.
cd ~/highload-s1/lab
```

Туннель `-L 127.0.0.1:3000:127.0.0.1:3000` принимает запрос браузера на ноутбуке и пересылает его на loopback ВМ. Имя `app` доступно только внутри Compose; `127.0.0.1` в браузере, на ВМ и внутри контейнера — три разные точки. Порты контейнеров публикуются только на loopback хоста. Если настроили другие `LOCAL_*_PORT`, в браузере используйте их.

Для терминала B откройте **второй локальный** терминал, снова `/bin/bash`, корень репозитория, затем:

```bash
source cloud/env.local
IP=$(cat "cloud/.state/$LAB_NAME/ip.txt")
ssh -i "$SSH_PRIVATE_KEY_FILE" -o IdentitiesOnly=yes \
  -o StrictHostKeyChecking=yes -o "UserKnownHostsFile=$PWD/cloud/known_hosts" "lab@$IP"
# В shell ВМ:
cd ~/highload-s1/lab
```

Не запускайте `connect.sh` второй раз с теми же локальными портами. Второму shell дополнительные туннели не нужны. Для просмотра исходников можно держать локальную копию в VS Code; изменять файлы нужно на ВМ или заново передавать изменённую `lab/`. Не правьте локальный файл, ожидая, что он автоматически изменится в контейнере.

## 6. Заполняем lab/.env на ВМ

```bash
# A, ВМ, ~/highload-s1/lab
umask 077
# Только при первом запуске. Существующий .env НЕ перезаписывать.
test -f .env || cp env.example .env
python3 scripts/configure.py
```

Шаблон содержит четыре поля. `LAB_ADMIN_TOKEN` — случайный секрет для локальной административной ручки; он не имеет отношения к облаку. `GRAFANA_ADMIN_PASSWORD` — отдельный случайный пароль пользователя `admin`. `WORKERS=8` задаёт число **I/O-слотов**, не число Python-процессов. `QUEUE_LIMIT=32` ограничивает число ожидающих запросов. Для основного опыта оставьте 8 и 32.

`configure.py` заменяет только `GENERATE_LOCALLY` на криптографически случайные hex-строки, проверяет четыре ключа, сохраняет существующие секреты и ставит права 0600. Код генерации открыт: `secrets.token_hex(32)`. На экран значения не выводятся. Чтобы понять заполнение, откройте **env.example и configure.py**, а не готовый `.env`.

Docker Compose автоматически читает `.env` рядом с `compose.yaml` для подстановки `${...}`. Затем только явно перечисленные `environment:` передаются нужному контейнеру. Файл `.env` не становится автоматически окружением вашего Bash. `cloud/env.local` Compose вообще не читает.

Для входа в Grafana получите пароль на ВМ и введите его в браузере под пользователем `admin`. Следующая команда выводит пароль; не публикуйте её результат:

```bash
# Приватный терминал, ВМ/lab. Эта команда действительно выводит пароль.
python3 -c 'from pathlib import Path; print(next(line.split("=",1)[1] for line in Path(".env").read_text().splitlines() if line.startswith("GRAFANA_ADMIN_PASSWORD=")))'
```

Не выводите весь `.env`: для входа нужен только пароль Grafana. Не меняйте пароль в `.env` после первой инициализации Grafana, ожидая смены пароля в уже существующем volume: пользователь хранится в её базе. Для новой одноразовой лабораторной можно пересоздать **её** volumes с явным пониманием потери данных; для сохранения данных используйте штатную смену пароля Grafana.

## 7. Запуск сервисов

```bash
# A, ВМ, lab. Это явная последовательность, а не один установочный wrapper.
docker version
docker compose version
# Только несекретные bind-mounted конфиги; .env остаётся 0600.
chmod 644 prometheus/*.yml grafana/dashboards/*.json \
  grafana/provisioning/dashboards/*.yml grafana/provisioning/datasources/*.yml
docker compose config --quiet
# Сначала загрузка закреплённых образов; её время входит в cold start.
docker compose --profile load --profile host pull prometheus grafana k6 node-exporter
docker compose build --pull app
docker compose --profile host up -d app prometheus grafana node-exporter
docker compose ps
```

Prometheus и Grafana работают от непривилегированных пользователей контейнера. Их bind-mounted конфиги должны быть читаемыми (0644); конфигурация не содержит секретов. `chmod` выше устраняет права 0600 из приватного ZIP, но не касается `.env`.

В `compose.yaml` заданы сборка приложения, образы сервисов, порты, конфигурация мониторинга, volumes и ограничения CPU/RAM. Профиль `load` не запускает k6 постоянно; каждый тест — отдельный `run --rm`. Профиль `host` нужен для node-exporter на Linux. Один Python-процесс сохраняет корректную модель in-process метрик.

```bash
# B, та же ВМ/lab. Повторить после запуска, если ещё нет успешного scrape.
bash scripts/check.sh
docker compose exec -T prometheus promtool check config /etc/prometheus/prometheus.yml
docker compose exec -T prometheus promtool check rules /etc/prometheus/alerts.yml
# Контроль без секретов:
ss -lnt
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS http://127.0.0.1:9090/-/ready
curl -fsS http://127.0.0.1:3000/api/health
```

`check.sh` проверяет и `/api/work`: он добавляет один реальный бизнес-запрос. `/healthz` и `/metrics` счётчик бизнес-запросов не увеличивают. Поэтому не вызывайте `check.sh` многократно внутри измеряемой фазы и не считайте его положительный результат доказательством качества всего потока. Проверка `up=1` означает успешный scrape, а не хорошее обслуживание пользователя.

В браузере ноутбука: Grafana → `http://127.0.0.1:3000` → пользователь `admin` → dashboard **S1 — наблюдение и доказательство**; Prometheus → `http://127.0.0.1:9090`. UID dashboard — `s1-monitoring`, datasource — `s1-prom`. Используйте свои локальные порты при их изменении. Секреты не показывайте в `docker compose config`, полном `docker inspect`, `env`, `set` и `bash -x`.

## 8. Локальный запуск

Без гранта можно запустить тот же `lab/` на своём компьютере с Docker Engine/Desktop и Compose v2. Не запускайте основной и локальный стенд на одинаковых локальных портах одновременно.

```bash
# Ноутбук, корень репозитория, без SSH.
cd lab
umask 077
test -f .env || cp env.example .env
python3 scripts/configure.py
docker compose config --quiet
docker compose --profile load pull prometheus grafana k6
docker compose build app
docker compose up -d app prometheus grafana
bash scripts/check.sh
```

Без `--profile host` target `node` будет DOWN. Это ожидаемо: CPU/RSS приложения доступны отдельно. На macOS/Windows host-метрики Docker Desktop описывают Linux-ВМ Docker, а не весь физический ноутбук. Основной облачный сценарий и Linux node-exporter не считаются проверенными одним локальным запуском. Дальше выполняйте [эксперименты](02_experiments.md) на своём стенде.
