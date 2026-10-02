# С нуля: локальный запуск стенда

Все сервисы запускаются на вашем компьютере. Команды ниже выполняются из `highload_autumn_2026/seminar-01/lab`. Yandex Cloud CLI, SSH-ключи и `cloud/env.local` для этого запуска не нужны.

## 1. Подготовка

Понадобятся Git, Python 3, Bash, curl и Docker с командой `docker compose`.

- macOS: установите и запустите [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/).
- Windows: установите [Docker Desktop с WSL2](https://docs.docker.com/desktop/setup/install/windows-install/), включите интеграцию с вашей WSL-дистрибуцией. Все команды выполняйте в Bash внутри WSL2, репозиторий клонируйте в её домашний каталог.
- Linux: установите Docker Engine и [плагин Docker Compose](https://docs.docker.com/compose/install/). Docker должен работать без `sudo` в вашем терминале.

Для стенда желательно выделить Docker 4 CPU и 4 GB RAM. Закройте другие нагрузочные приложения: API и генератор делят ресурсы компьютера.

```bash
git clone https://github.com/itmo-ipkn/highload_autumn_2026.git
cd highload_autumn_2026/seminar-01/lab
bash
```

Если репозиторий уже клонирован, вместо повторного `git clone` откройте его, выполните `git pull --ff-only` и перейдите в `seminar-01/lab`.

```bash
docker version
docker compose version
python3 --version
curl --version
```

В `docker version` должны быть разделы **Client** и **Server**. Ошибка подключения к Docker означает, что нужно запустить Docker Desktop или Docker Engine.

## 2. Настройки приложения

```bash
test -f .env || cp env.example .env
python3 scripts/configure.py
chmod 644 prometheus/*.yml grafana/dashboards/*.json \
  grafana/provisioning/dashboards/*.yml grafana/provisioning/datasources/*.yml
docker compose config --quiet
```

`configure.py` заполнит два пароля в `lab/.env`, сохранив уже заданные значения. `WORKERS=8` — число учебных I/O-слотов, `QUEUE_LIMIT=32` — максимальное число ожидающих запросов. Docker Compose читает `.env` рядом с `compose.yaml`; выполнять `source .env` не требуется.

`chmod` делает конфигурацию мониторинга читаемой внутри контейнеров. Пароли остаются в `.env` с правами `600`.

## 3. Запуск

```bash
docker compose --profile load pull prometheus grafana k6
docker compose build --pull app
docker compose up -d --wait --wait-timeout 120 app prometheus grafana
docker compose ps
bash scripts/check.sh
```

Загрузка образов и первая сборка требуют интернета. Если `check.sh` сообщает `app target is not UP`, подождите первый сбор метрик и повторите только `bash scripts/check.sh`: Prometheus опрашивает приложение каждые 5 секунд.

Запускаются API, Prometheus и Grafana. k6 запускается отдельным контейнером только при нагрузочном тесте. `--profile host` здесь не используем: node-exporter зависит от Linux-хоста и для экспериментов с API не требуется.

## 4. Открыть интерфейсы

- **Grafana:** <http://127.0.0.1:3000>. Логин — `admin`.
- **Prometheus:** <http://127.0.0.1:9090>. Логин и пароль не нужны.
- **API:** <http://127.0.0.1:8080/healthz>.

Пароль Grafana лежит в `lab/.env`, поле `GRAFANA_ADMIN_PASSWORD`. Чтобы показать только его, выполните в своём терминале:

```bash
python3 -c 'from pathlib import Path; print(next(line.split("=",1)[1] for line in Path(".env").read_text().splitlines() if line.startswith("GRAFANA_ADMIN_PASSWORD=")))'
```

В Grafana откройте **S1 — наблюдение и доказательство**, выберите **Last 15 minutes**, обновление **5s**. Панели CPU и RSS процесса приложения работают локально. Панели CPU/RAM ВМ без node-exporter покажут **No data**, а target `node` в Prometheus — **DOWN**; это ожидаемо.

## 5. Два терминала для экспериментов

Оба терминала работают **на вашем компьютере**, в одной папке `highload_autumn_2026/seminar-01/lab`. Второе SSH-подключение не нужно: просто откройте ещё один терминал, перейдите в эту папку и запустите `bash`.

- **A, первый терминал:** переключать режим, запускать нагрузку, сохранять результаты.
- **B, второй терминал:** выполнять запросы метрик, пока в A идёт тест.
- **Браузер:** наблюдать графики Grafana.

Дальше выполняйте `02_experiments.md`. Нагрузочные команды остаются теми же, например `bash scripts/load.sh normal 80 120s 1`.

## 6. Остановить и запустить снова

После завершения теста:

```bash
docker compose --profile load down
```

Результаты остаются в `lab/results`, метрики и настройки Grafana — в Docker volumes. Для следующего запуска:

```bash
docker compose up -d --wait --wait-timeout 120 app prometheus grafana
bash scripts/check.sh
```

`docker compose down -v` дополнительно удаляет метрики и настройки Grafana. Для обычного завершения занятия `-v` не нужен.
