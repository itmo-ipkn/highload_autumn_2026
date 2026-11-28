# HW2 — очередь Kafka: producers, consumers, sink в Postgres, DLT, verify CLI

Кластер (3 broker), 3 продюсера, 3 консюмера в группе `events-processor`, sink в Postgres
с dedup по `event_id`, dead-letter topic для poison-событий, verify CLI, Prometheus + Grafana
(дашборд `HW2`, пароль `password`). Всё поднимается через
`docker compose up`.

## Запуск

```bash
cp .env.example .env
python scripts/configure.py
docker compose up -d --build
bash scripts/check.sh
bash scripts/run_load.sh baseline 1
bash scripts/verify.sh manual
```

## Порты

| Сервис | URL |
|---|---|
| Kafka brokers | `127.0.0.1:9094/9095/9096` |
| Postgres | `127.0.0.1:5432` (`lab2` / `lab2-demo-password`) |
| Prometheus | http://127.0.0.1:9090 |
| Grafana | http://127.0.0.1:3000 |
| Метрики pod'ов | `:9404` |

## Topics

| Topic | Partitions | RF | Конфиги |
|---|---|---|---|
| `events` | 3 | 3 | `min.insync.replicas=2`, auto-create off |
| `events.dlt` | 3 | 3 | poison-события после 3 попыток |

## Модель события

```json
{
  "event_id": "uuid",            // ключ dedup
  "user_id": "user_1_01",        // одного пользователя ведёт ровно один producer
  "type": "page_view|add_to_cart|purchase",
  "seq": 1,                      // сквозной счётчик пользователя, начинается с 1
  "produced_at": "2026-11-28T11:41:35+00:00",
  "payload": { ..., "poison": true }   // ~2% событий
}
```

Решение о poison детерминировано: `hash(pod_index, user_id, seq)` против `POISON_RATE`
(`verify/logic.py`, одна функция у продюсеров, consumers и verify CLI). `POISON_MODE=flag` —
handler всегда падает на `payload.poison`, `POISON_MODE=corrupt` — ломается payload.

## Контракты

Продюсер: `acks=all`, `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`,
partitioner `consistent` при key и `random` при `KEY_MODE=none`; счётчики `seq` возобновляются
из таблицы `producer_state`; ошибки доставки логируются через delivery reports и не проглатываются.

Консюмер: `enable.auto.commit=false`, commit только после записи в sink (at-least-once, повторы
гасятся dedup по `event_id`); commit-target = `min(позиция чтения, минимальный failed offset)`,
поэтому poison с <3 попытками не коммитится и читается заново; `on_revoke` коммитит обработанное
перед освобождением partition; `auto.offset.reset=earliest`; `cooperative-sticky`; DLT пишется
только после подтверждения доставки broker'ом. Headers DLT: `x-error`, `x-original-topic`,
`x-original-partition`, `x-original-offset`, `x-attempts`, `x-pod-id`.

Sink: `events` (первая доставка), `deliveries` (каждая попытка с монотонным `attempt_seq` —
измерение порядка записей), `dedup_stats`, `poison_attempts`, `producer_state`.
Идемпотентность — `INSERT ... ON CONFLICT (event_id) DO NOTHING`.

## Verify CLI

1. нет пропусков по `seq` для каждого `user_id` — пропущенный seq допустим только если такое же
   событие найдено в `events.dlt` (читается из topic с offset 0 свежей уникальной группой);
2. сколько повторных доставок отсеяно dedup;
3. сколько записей в sink для одного `user_id` появилось с меньшим `seq` после большего
   (нарушение порядка).