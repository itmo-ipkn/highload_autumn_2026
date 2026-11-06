# Семинар 5 · REST API, gRPC, балансировка и кэширование

ИТМО, «Принципы построения высоконагруженных систем», W6, 6 ноября 2026.

Один каталог, один товар `book-1`, два экземпляра приложения. Сначала читаем
товар через HTTP JSON и gRPC. Затем наблюдаем распределение запросов. Наконец
снижаем число обращений к источнику и проверяем, когда цена становится устаревшей.

Начните с одного из вариантов запуска ниже. После проверки готовности
переходите к опытам в `demos.md`: команда, прогноз и разбор результата.

[Презентация PDF](dist/S5_REST_gRPC.pdf)

## Что понадобится

- Вариант Docker: работающий Docker Engine / Docker Desktop и Docker Compose v2
  с поддержкой `up --wait`. Python на компьютере не нужен.
- Вариант без Docker: Python **3.12**, `venv`, два терминала. Проверен на macOS
  с Python 3.12.1. На Windows команды ниже выполняйте в WSL2 с Python 3.12.
- Интернет нужен для первоначальной установки зависимостей и образов
  командами ниже. После установки Python-вариант работает без интернета.
- Свободные локальные порты `8080`, `8081`, `8082`, `8090`, `50050`, `50051`,
  `50052`. Все опубликованные порты привязаны к `127.0.0.1`.

Здесь и далее команды выполняются **из папки этого семинара**,
где находятся `README.md`, `requirements.txt` и `compose.yaml`.
Выполняйте команды по очереди; при ошибке сначала устраните её.
Одновременно запускайте только один вариант стенда: перед переходом к Docker
остановите Python-сервер через `Ctrl-C`, а перед запуском Python выполните
`docker compose down`, если до этого использовали Docker.

## Вариант 1. Без Docker

Терминал 1:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python tools/generate.py
.venv/bin/python app/server.py native
```

Если ваш `python3` уже имеет версию 3.12, первая команда может быть
`python3 -m venv .venv`. Дождитесь строки `READY native`. Оставьте терминал
открытым. Не нужно активировать окружение: полный путь к Python исключает
случайный запуск другого интерпретатора.

Терминал 2, из той же папки:

```bash
sh tools/check.sh
.venv/bin/python tools/demo.py contracts --step http
.venv/bin/python tools/demo.py contracts --step grpc
```

`check` проверяет HTTP обоих экземпляров, источник, HTTP-шлюз, gRPC и TCP-шлюз.
Остановка: `Ctrl-C` в терминале 1. Это завершает только процесс учебного стенда.
Для повторного старта установка не нужна:

```bash
.venv/bin/python app/server.py native
```

## Вариант 2. Docker Compose и nginx

Сначала запустите Docker Desktop или Docker Engine. Все команды этого варианта
выполняются в одном терминале из папки семинара; контейнеры работают в фоне.

```bash
docker version
docker compose version
docker compose config --quiet
docker compose build
docker compose up -d --wait
docker compose ps
sh tools/check.sh docker
```

На Windows эти команды можно выполнять в WSL2; в PowerShell вместо последней
команды используйте `docker compose run --rm client check`.

В любом демонстрационном шаге замените префикс
`.venv/bin/python tools/demo.py` на `docker compose run --rm client`:

```bash
docker compose run --rm client contracts --step http
docker compose run --rm client contracts --step grpc
docker compose run --rm client balance --step http
docker compose run --rm client cache --step stampede
```

Сброс и остановка:

```bash
docker compose run --rm client reset
docker compose down
```

Именованных томов нет. Источник хранит данные в памяти, поэтому остановка
контейнера источника удаляет учебные изменения. `down` относится только к
Compose-проекту `itmo-w6-api`; `docker system prune` не нужен.

## Демонстрации по одному шагу

Команды, прогнозы и ожидаемые признаки: [demos.md](demos.md).

```bash
.venv/bin/python tools/demo.py contracts --step conditional
.venv/bin/python tools/demo.py contracts --step errors
.venv/bin/python tools/demo.py contracts --step deadline
.venv/bin/python tools/demo.py balance --step http
.venv/bin/python tools/demo.py balance --step grpc-l4
.venv/bin/python tools/demo.py balance --step grpc-rr
.venv/bin/python tools/demo.py cache --step cold
.venv/bin/python tools/demo.py cache --step stale
.venv/bin/python tools/demo.py cache --step invalidate
.venv/bin/python tools/demo.py cache --step ttl
.venv/bin/python tools/demo.py cache --step stampede
```

Шаги `contracts/http`, `contracts/grpc`, `contracts/conditional` и все `cache`
сами подготавливают исходные данные. `balance` не зависит от текущей цены.
Можно повторить любой шаг, не перезапуская процесс. **Не запускайте два шага
одновременно:** они управляют одним учебным набором данных.

Быстрая проверка всех сценариев с утверждениями о результате:

```bash
.venv/bin/python tools/demo.py check
.venv/bin/python tools/demo.py all
```

Для Docker: `docker compose run --rm client all`.

## Устройство стенда

| Компонент | Локальный адрес | Назначение |
|---|---|---|
| HTTP gateway | `http://127.0.0.1:8080` | Выбор backend на каждый HTTP-запрос |
| backend-a HTTP | `http://127.0.0.1:8081` | Первый экземпляр и его локальный кэш |
| backend-b HTTP | `http://127.0.0.1:8082` | Второй экземпляр и его локальный кэш |
| Origin | `http://127.0.0.1:8090` | Общий источник истины, данные в памяти |
| TCP gateway | `127.0.0.1:50050` | Выбор backend на каждое TCP-соединение |
| backend-a gRPC | `127.0.0.1:50051` | `catalog.v1.CatalogService/GetItem` |
| backend-b gRPC | `127.0.0.1:50052` | Тот же контракт, другой экземпляр |

В Docker HTTP gateway — nginx с обычным round robin. В native-варианте его
заменяет учебный HTTP-прокси на Python. TCP gateway в обоих вариантах —
простой Python-прокси, который пересылает байты и не разбирает HTTP/2.
Это реальное перенаправление TCP-соединений, а не имитация результата gRPC.
Один канал gRPC в опыте использует одно такое соединение.

Native-стенд размещает два независимых объекта `Catalog` с отдельными кэшами
и lock в **одном Python-процессе**. Это сохраняет механику демонстраций, но
не воспроизводит изоляцию CPU, памяти и отказов отдельных процессов.
В Compose каждый backend работает в собственном контейнере.

`GetItem` использует ту же функцию `Catalog.get`, что HTTP-обработчик.
Представление HTTP содержит `id`, `title`, `price_rub`, `version`.
В HTTP метаданные находятся в `X-Backend` и `X-Cache`; в gRPC — в полях
`served_by` и `cache` оболочки ответа. Сам `item` одинаков.

В `proto/catalog.proto` находится исходный контракт. `tools/generate.py`
создаёт `app/catalog_pb2.py` и `app/catalog_pb2_grpc.py`.
Редактировать сгенерированные файлы не нужно.

## Можно проверить руками

Сначала запустите один из вариантов выше и дождитесь успешной проверки.
Перед этим блоком сбросьте данные, чтобы ETag `book-1-v1` соответствовал
текущей версии товара. Для Python:

```bash
.venv/bin/python tools/demo.py reset
```

Для Docker:

```bash
docker compose run --rm client reset
```

Теперь выполните запросы ниже. В Windows используйте WSL2 для этих `curl`-команд.

```bash
curl -i http://127.0.0.1:8081/items/book-1
curl -i -H 'If-None-Match: "book-1-v1"' http://127.0.0.1:8081/items/book-1
curl -i 'http://127.0.0.1:8081/items/book-1?cache=singleflight'
curl -s http://127.0.0.1:8090/stats
curl -X PUT -H 'Content-Type: application/json' \
  -d '{"price_rub":1500}' http://127.0.0.1:8090/items/book-1
curl -X POST http://127.0.0.1:8081/admin/invalidate
```

После обновления номер версии увеличивается. Поэтому известный ETag `v1`
подходит только до обновления; для следующего условного запроса возьмите
новый ETag из ответа. Команда `demo.py reset` возвращает цену 1200 и версию 1.

## Что именно измеряем

- `X-Backend` / `served_by`: какой экземпляр выполнил работу.
- `X-Cache`: `BYPASS`, `MISS`, `HIT`, `COALESCED`. Последнее означает, что
  запрос дождался заполнения кэша другим потоком.
- `origin_reads`: сколько чтений `book-1` действительно началось на источнике.
  Чтение `/stats` этот счётчик не меняет.
- Примерные миллисекунды в выводе помогают увидеть механизм. Это не
  сопоставимый бенчмарк HTTP против gRPC и не обещание конкретной скорости.

В опыте со stampede 12 клиентов одновременно приходят к **одной** холодной
реплике. Без координации несколько запросов читают origin. С per-key lock
и повторной проверкой кэша источник читает один запрос. Остальные ждут;
задержка самого первого чтения не исчезает.

## Границы учебного примера

- HTTP JSON API демонстрирует ресурсы и семантику HTTP. Он не реализует весь
  набор архитектурных ограничений REST, например гипермедийную навигацию.
- HTTP gateway не хранит кэш. Прикладной кэш каждой реплики отделён от HTTP
  кэширования у клиента/прокси. `max-age=0, must-revalidate` требует проверки
  свежести HTTP-представления. Он не отменяет собственный TTL приложения.
- При `304` тело не передаётся, но наша реализация сначала читает источник:
  условный запрос экономит байты, а не обязательно работу сервера.
- TTL здесь 3 секунды с момента заполнения. Это управляемый учебный выбор,
  а не универсальная гарантия бизнес-свежести. Параллельные обновления и
  задержки заполнения требуют отдельного анализа.
- Инвалидация вызывается явно на обеих репликах. В production понадобятся
  стратегия доставки события, обработка потерь и продуманная согласованность.
- Singleflight действует в одном экземпляре `Catalog`. Две реплики могут одновременно
  заполнить один ключ. Распределённый lock здесь не реализован.
- Клиент gRPC `round_robin` получает статический список двух адресов. Это
  демонстрирует выбор endpoint на RPC; discovery и обновление состава вне стенда.
- Нет TLS, аутентификации, rate limiting и безопасной административной API.
  Управляющие endpoints предназначены только для локального эксперимента.
  Порты нельзя открывать в интернет.
- Deadline прекращает ожидание клиента. Блокирующий origin-запрос может
  закончиться позднее; отмена не откатывает уже выполненную работу.
- Образы закреплены тегами, Python-пакеты версиями. Побитовая воспроизводимость
  образов требует digest; в этот комплект такой контракт не входит.

## Если что-то не работает

| Симптом | Проверка и действие |
|---|---|
| `ModuleNotFoundError: grpc` | Используйте `.venv/bin/python`, затем установите `requirements.txt` в это же окружение |
| Нет `catalog_pb2` | Выполните `.venv/bin/python tools/generate.py` |
| `Address already in use` | Не запускайте native и Docker одновременно; найдите владельца нужного порта, не завершайте чужой процесс вслепую |
| `connection refused` | В первом терминале должна быть строка `READY`; в Docker проверьте `docker compose ps` и `docker compose logs --tail=50` |
| Docker CLI есть, сервер недоступен | Запустите Docker Desktop / Docker Engine либо выберите native-вариант |
| `--wait` не поддерживается | Обновите локальный Compose v2 или используйте native-вариант; не подменяйте readiness произвольным `sleep` |
| gRPC deadline иногда отличается в миллисекундах | Проверяем `DEADLINE_EXCEEDED`, а не точное время |
| stampede даёт меньше 12 чтений | Допустим любой результат больше 1; планировщик не гарантирует строго одновременный старт |
| Stampede без lock даёт 1 чтение | Закройте параллельные клиенты и повторите шаг; на сильно перегруженной машине потокам может не хватать времени стартовать в окне задержки |
| Корпоративный proxy вмешивается в localhost | Для этого терминала добавьте локальные адреса в `NO_PROXY`; не отключайте системные настройки глобально |

## Первоисточники

- [gRPC Python Quick start](https://grpc.io/docs/languages/python/quickstart/)
- [gRPC Deadlines](https://grpc.io/docs/guides/deadlines/)
- [gRPC Status Codes](https://grpc.io/docs/guides/status-codes/)
- [nginx HTTP load balancing](https://nginx.org/en/docs/http/load_balancing.html)
- [HTTP Semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110)
- [HTTP Caching, RFC 9111](https://www.rfc-editor.org/rfc/rfc9111)
