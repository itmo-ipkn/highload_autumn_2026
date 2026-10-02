# Эксперименты: от явного запроса до собственного вывода

Все команды выполняются **локально**, в `highload_autumn_2026/seminar-01/lab`. Сначала запустите стенд по `01_from_zero.md`. **A — первый терминал:** режим, нагрузка и сбор результатов. **B — второй терминал в той же папке:** метрики во время нагрузки. Оба терминала работают в Bash на вашем компьютере. Только один генератор одновременно. Режим меняйте после завершения предыдущего запуска и обработки оставшихся запросов.

Перед каждым воздействием запишите прогноз: какой график изменится, какой не должен измениться и что опровергнет объяснение. После опыта сохраните результаты. Не считайте демонстрационные ориентиры обязательным численным ответом.

## 1. Проверка и исходный код

```bash
# B, второй локальный терминал, lab/
docker compose ps
bash scripts/check.sh
curl -fsS http://127.0.0.1:8080/healthz; printf '\n'
curl -fsS http://127.0.0.1:8080/api/work; printf '\n'
curl -fsS http://127.0.0.1:8080/metrics | \
  grep -E '^(lab_requests_total|lab_request_duration_seconds_bucket|lab_pool_)' | sed -n '1,35p'
# В VS Code откройте app/server.py и найдите async def work.
# В терминале то же содержимое:
sed -n '75,175p' app/server.py
```

Найдите общий таймер до получения слота, отдельные таймеры ожидания/удержания и `finally`. `WORKERS` — число слотов семафора. Отмена и ошибка должны освобождать занятый ресурс и уменьшать gauges. Запись `499` в метрике означает отменённый handler, а не доказательство такого ответа по сети.

## 2. Первый режим: сначала явный HTTP POST

Откройте `scripts/mode.sh`. Его существенное действие — этот запрос. Выполните его один раз, не включая shell trace:

```bash
MODE=normal
TOKEN=$(python3 - <<'PYTOKEN'
from pathlib import Path
v=dict(line.split('=',1) for line in Path('.env').read_text().splitlines() if line and not line.startswith('#'))
print(v['LAB_ADMIN_TOKEN'])
PYTOKEN
)
printf 'header = "X-Lab-Token: %s"\n' "$TOKEN" | \
  curl --config - --fail-with-body --silent --show-error --max-time 5 \
  -H 'Content-Type: application/json' -d "{\"mode\":\"$MODE\"}" \
  http://127.0.0.1:8081/admin/mode
unset TOKEN
printf '\n'
```

Вы получили JSON с текущим режимом и параметрами пула. POST не перезапускает сервис, не обнуляет counters и не запускает нагрузку. Начатые запросы сохраняют старый режим; переключение влияет на новые. `mode.sh` добавляет проверку имени режима, формата секрета, фиксирует рабочий каталог и выполняет этот же POST. Дальше используйте `bash scripts/mode.sh tail` и другие режимы без повторного копирования заголовка.

## 3. Baseline: первый нагрузочный тест явно

Сначала откройте `load/open.js`. Одна итерация выполняет один HTTP GET без sleep, retry и redirect; поэтому 80 iterations/s в данной программе означают планируемые 80 запросов/с. Это не общее равенство для любого теста k6. `constant-arrival-rate` планирует поступления независимо от завершения предыдущих; при нехватке VU появляются `dropped_iterations`.

```bash
# A, первый локальный терминал, lab/. Один раз выполняем без load.sh; сохраняем тот же формат результата.
mkdir -p results
mkdir results/.load.lock || { echo "Другой тест уже запущен; сначала проверьте контейнеры." >&2; exit 2; }
trap 'rmdir results/.load.lock 2>/dev/null || true' EXIT
RUN=$(mktemp -d "results/$(date -u +%Y%m%dT%H%M%SZ)-manual-normal-XXXXXX")
printf '%s\n' "$RUN" > results/.last-run.txt
date -u +%FT%TZ > "$RUN/start.txt"
printf 'label=manual-normal\nrate=80\nduration=120s\nvus=160\nexpect_normal=1\n' > "$RUN/parameters.txt"
set +e
docker compose --profile load run --rm -T --interactive=false k6 run --quiet \
  -e RATE=80 -e DURATION=120s -e VUS=160 \
  -e RUN_LABEL=manual-normal -e EXPECT_NORMAL=1 \
  /scripts/open.js > "$RUN/summary.json" 2> "$RUN/stderr.log"
RC=$?
set -e
date -u +%FT%TZ > "$RUN/end.txt"
printf '%s\n' "$RC" > "$RUN/exit-code.txt"
rmdir results/.load.lock
trap - EXIT
python3 scripts/summarize.py "$RUN/summary.json"
printf 'k6 exit code: %s\n' "$RC"
```

Перед следующим опытом убедитесь, что `RC=0`; при другом коде изучите JSON и stderr, не продолжайте как будто baseline валиден. `set +e` здесь нужен только для сохранения кода завершения и отчёта, а не для игнорирования ошибки. В `load.sh` после сохранения файлов выполняется `exit "$RC"`.

`--profile load` делает доступным сервис k6; `run` создаёт разовый контейнер; `--rm` удаляет его после завершения; `-T` отключает псевдотерминал, чтобы JSON можно было корректно направить в файл; `--interactive=false` запрещает контейнеру читать stdin shell и проглатывать команды сохранения RC при выполнении блока через stdin; `-e` передаёт параметры именно k6. Файл `/scripts/open.js` смонтирован из `load/open.js` в read-only режиме. В `handleSummary()` JSON направляется в stdout, а stdout shell сохраняет в локальный `lab/results`, не в эфемерный контейнер. stderr хранится отдельно.

`VUS=160` — запас исполнителей, не 160 RPS и не фиксированная конкурентность. `EXPECT_NORMAL=1` добавляет порог доли хороших ответов. Значение `0` в деградациях снимает только этот порог и normal error threshold; не снимает требования к валидности payload и отсутствию dropped iterations. Не все 5xx в fault injection — поломка теста, но транспортная ошибка или потеря нагрузки требуют расследования.

## 4. Пока идёт baseline: один PromQL-запрос вручную

```bash
# B, в последнюю минуту теста. Не подменяйте окно потока временем после остановки.
QUERY='sum(rate(lab_requests_total{job="app",route="/api/work"}[1m]))'
curl -fsS --max-time 10 -G http://127.0.0.1:9090/api/v1/query   --data-urlencode "query=$QUERY" | python3 -m json.tool
# Это тот же HTTP API, который оборачивает query.sh:
bash scripts/query.sh completed_rps
bash scripts/query.sh good
```

Откройте `scripts/query.sh` и `prometheus/queries.json`: скрипт выбирает выражение по имени и передаёт его Prometheus, а не вычисляет p99 сам. `--data-urlencode` нужен для безопасной передачи пробелов, кавычек и скобок в URL. Результат с `status=success` может содержать пустую выборку/NaN — это не гарантия наличия измерения.

В Grafana откройте **S1 — наблюдение и доказательство**. Диапазон Last 15 minutes, refresh 5s. Создайте копию dashboard или новую панель: datasource `Prometheus` (UID `s1-prom`), Code mode, выражение `errors` из `queries.json`, единица Percent 0–1. Для задержки используйте seconds/duration; PromQL возвращает секунды. Provisioning-файл не изменяется от правки панели в UI: сохраните свою копию и при необходимости экспортируйте её JSON.

После baseline:

```bash
# A. Значение RUN существует после ручного блока.
bash scripts/collect.sh "$RUN"
```

`collect.sh` сохраняет сырые метрики, targets, конфигурацию запущенных сервисов без секретов, диагностические логи, запросы и временной диапазон. Instant PromQL вычисляется за 2 секунды до последней реально начатой попытки k6 (max метрики `client_request_start_unix_ms`), а не по запоздавшему завершению контейнера или уже пустому стенду. В JSON также сохранён min — первая попытка; среднее и перцентили этой служебной метрики не являются задержками. Квантили k6 по всей фазе и минутная оценка Prometheus всё ещё имеют разные окна — один timestamp не делает их тождественными.

## 5. Повторные тесты: теперь понятная обёртка

Откройте `scripts/load.sh` и сопоставьте его центральную команду с уже выполненной. Дополнительная логика: валидация аргументов, один активный wrapper через lock, отдельная папка запуска, UTC-время, JSON/stderr/exit code и печать summary. Скрипт ничего не лечит, не меняет режим и не создаёт ВМ.

```bash
# Формат: load.sh МЕТКА RATE DURATION EXPECT_NORMAL
# Пример повторного baseline (это отдельный запуск, не обязателен на занятии):
bash scripts/mode.sh normal
bash scripts/load.sh repeat-normal 80 120s 1
bash scripts/collect.sh
```

Найти последнюю папку: `cat results/.last-run.txt`. У каждого теста — `start.txt`, `end.txt`, `parameters.txt`, `summary.json`, `stderr.log`, `exit-code.txt`; после collect — `observations/`. `collect.sh` без аргумента читает последний запуск, с аргументом — конкретный. Повторный collect в ту же папку запрещён, чтобы не подменить сохранённые данные; для дополнительного снимка выберите отдельную папку/копию осознанно.

## 6. Редкие задержки: среднее, p95, p99

```bash
# A
bash scripts/mode.sh tail
bash scripts/load.sh tail 80 120s 0
bash scripts/collect.sh
# B, пока идёт последняя минута нагрузки:
# bash scripts/query.sh mean
# bash scripts/query.sh p95
# bash scripts/query.sh p99
# bash scripts/query.sh good
# bash scripts/query.sh errors
```

В модели каждый 50-й запрос дополнительно удерживает слот 900 мс, остальные выполняют 25 мс асинхронной работы. Сначала прогноз, потом график. Почему p95 может остаться близким к baseline, когда доля хороших запросов уже упала? Почему p99 histogram не обязан совпасть с сырыми 925 мс? Какой bucket позволяет оценить долю успешных не дольше 300 мс без интерполяции квантиля?

В 120-секундном тесте с 80 запланированными поступлениями/с ожидается примерно 9 600 итераций; проверяйте фактическое число и dropped. Периодичность хвоста задана специально для контролируемого опыта. Это не модель случайной реальной нагрузки.

## 7. Ограниченный ресурс и проверка восстановления

```bash
# A. На своём стенде вы видите воздействие и должны доказать механизм.
bash scripts/mode.sh slow
bash scripts/load.sh slow 80 120s 0
bash scripts/collect.sh
# B во время последней минуты:
# bash scripts/query.sh active
# bash scripts/query.sh waiting
# bash scripts/query.sh service_mean
# bash scripts/query.sh queue_p95
# bash scripts/query.sh success_rps
# bash scripts/query.sh completed_rps
# bash scripts/query.sh good_rps
# bash scripts/query.sh cpu

# A: обязательная обратная проверка при ТОЙ ЖЕ нагрузке.
bash scripts/mode.sh normal
bash scripts/load.sh recovery 80 120s 1
bash scripts/collect.sh
```

В модели 8 слотов, 200 мс удержания слота в slow, не более 32 ожидающих и timeout ожидания 250 мс. Оценка 8 / 0,2 = 40 операций/с относится к обработке в слотах, не ко всем завершившимся запросам: 503 тоже завершение. Сопоставьте started/completed/success/good, ожидание ресурса и время владения им. Невысокий CPU не опровергает насыщение I/O-пула. Это семафор с асинхронным ожиданием, не настоящая база данных.

После возврата в normal убедитесь, что очередь и качество восстановились. На минутных графиках вначале смешиваются старые и новые запросы. Не добавляйте retry, число слотов или restart в той же фазе: это несколько одновременных воздействий, которые мешают выводу.

## 8. Быстрые ошибки

```bash
bash scripts/mode.sh errors
bash scripts/load.sh errors 80 120s 0
bash scripts/collect.sh
bash scripts/mode.sh normal
```

Во время теста сопоставьте `up`, p99 **успешных** запросов, `errors` и `good`. Каждый пятый запрос в этой модели возвращает HTTP 500. Проверка кода/тела и проверка задержки отвечают на разные вопросы. Нельзя оценивать пользовательскую надёжность только по оставшимся успешным запросам.

## 9. Ваш индивидуальный эксперимент

Работайте на своём локальном стенде. Сначала верните normal, запишите гипотезу, измените только rate:

```bash
bash scripts/mode.sh normal
bash scripts/load.sh my-normal-120 120 120s 1
bash scripts/collect.sh
```

Сравните с baseline 80/120, проверьте фактически поданную нагрузку и интерпретируйте результат. Совпадение или несовпадение с прогнозом само по себе не является оценкой работы: нужен объяснённый вывод с ограничениями и данными.

Дополнительный опыт после основного: VUS=1 в tail. Это намеренно недостаточный генератор; валидным такой профиль объявлять нельзя.

```bash
bash scripts/mode.sh tail
# Здесь ОЖИДАЕТСЯ ненулевой exit code: проверьте dropped_iterations.
VUS=1 bash scripts/load.sh insufficient-generator 80 60s 0
# После расследования верните normal.
bash scripts/mode.sh normal
```

Сравнение с закрытой моделью, необязательно для первого прохода:

```bash
docker compose --profile load run --rm -T --interactive=false k6 run   -e VUS=8 -e DURATION=60s /scripts/closed.js
```

## 10. Сохранить результаты и остановить стенд

Результаты уже находятся на вашем компьютере в `lab/results`. В первом терминале, после завершения нагрузки:

```bash
bash scripts/mode.sh normal
find results -name summary.json -print
docker compose --profile load down
```

`down` останавливает контейнеры, но сохраняет Docker volumes с метриками и настройками Grafana. Каталог `lab/results` сохраняется независимо от контейнеров; передавайте нужную папку запуска из него. Копировать файлы через SSH или запускать `cloud/cleanup.sh` при локальной работе не нужно.

Если нужны графики для отчёта, сохраните их до остановки Grafana. Для продолжения запустите сервисы снова по `01_from_zero.md`, раздел 6.

Только если намеренно хотите удалить метрики Prometheus и настройки Grafana, выполните:

```bash
docker compose --profile load down -v
```

Эта команда не удаляет файлы в `lab/results`.
