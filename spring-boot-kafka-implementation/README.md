# HW2/3. Очередь Kafka (Java 21 + Spring Boot 3 + KRaft + PostgreSQL Sink)

*Курс DDIA (осень 2026) — треки DevOps/ФиБ (HW2) и MSE/MHS (HW3)*

---

## Архитектура решения

Решение реализовано на стеке **Java 21 + Spring Boot 3.3 (`spring-kafka`) + PostgreSQL 16 + Apache Kafka 3.8 (KRaft mode)** и полностью упаковано в `docker-compose.yaml`, а также снабжено интерактивной веб-обсерваторией телеметрии для запуска экспериментов отказоустойчивости и верификации инвариантов.

```text
+---------------+      KEY_MODE=user_id|none       +---------------------------------------+
|  producer-1   | --- (users 101..104, seq 1..N) ->|         Kafka KRaft Cluster           |
|  producer-2   | --- (users 201..204, seq 1..N) ->|  kafka-1 (9092), kafka-2 (9093),      |
|  producer-3   | --- (users 301..304, seq 1..N) ->|  kafka-3 (9094)                       |
+---------------+      acks=all, idempotence=true  |                                       |
                                                   |  Topic: events (P0, P1, P2 | RF=3 |   |
                                                   |                 min.insync.replicas=2)|
                                                   |  Topic: events.dlt (P0, P1, P2 | RF=3)|
                                                   +-------------------+-------------------+
                                                                       |
                                       Consumer Group: events-processor (enable.auto.commit=false)
                                                                       |
                                        +------------------------------+------------------------------+
                                        v                              v                              v
                                +---------------+              +---------------+              +---------------+
                                |  consumer-1   |              |  consumer-2   |              |  consumer-3   |
                                +-------+-------+              +-------+-------+              +-------+-------+
                                        |                              |                              |
                                        |  1. sleep(PROCESSING_DELAY_MS)                              |
                                        |  2. if poison (~2%): retry 3x -> events.dlt + headers       |
                                        |  3. INSERT INTO sink_events ON CONFLICT (event_id) DO NOTHING
                                        |  4. manual commitSync (AckMode.MANUAL_IMMEDIATE)            |
                                        +------------------------------+------------------------------+
                                                                       v
                                                        +-----------------------------+
                                                        |   PostgreSQL Sink Storage   |
                                                        |  - sink_events (UNIQUE id)  |
                                                        |  - dedup_stats (counter)    |
                                                        +-----------------------------+
```

---

## Модель события (`EventMessage`)

Каждое событие сериализуется в JSON и содержит обязательные поля:

```json
{
  "event_id": "d290f1ee-6c54-4b01-90e6-d701748f0851",
  "user_id": "user-101",
  "type": "purchase",
  "seq": 42,
  "produced_at": "2026-11-18T19:15:30.114Z",
  "payload": {
    "producer_pod": "producer-1",
    "key_mode": "user_id",
    "poison": false,
    "session_id": "sess-user-101-2026",
    "amount": 1490
  }
}
```

* **Строгое владение пользователями (`user_id`):** каждый под-продюсер генерирует события только для своего непересекающегося набора пользователей (`producer-1` $\rightarrow$ `user-101..104`, `producer-2` $\rightarrow$ `user-201..204`, `producer-3` $\rightarrow$ `user-301..304`). Благодаря этому порядковый номер `seq` каждого `user_id` монотонно возрастает от `1` без пропусков (`1, 2, 3, ...`).
* **Ядовитые сообщения (`poison` $\approx 2\%$):** с вероятностью `POISON_PROBABILITY=0.02` в `payload` выставляется `"poison": true` и `"corrupted_signature": "0xDEAD_POISON_PILL"`. При обработке такого события консьюмер всегда выбрасывает `PoisonEventException`.

---

## Реализация ключевых требований по частям

### Часть 1. Кластер Kafka в режиме KRaft
* В `docker-compose.yaml` развернуты 3 брокера (`kafka-1`, `kafka-2`, `kafka-3`) на образе `apache/kafka:3.8.0` в режиме `broker,controller` (KRaft, без ZooKeeper).
* Настроены раздельные слушатели:
  * `INTERNAL://kafka-X:19092` — для трафика между брокерами и подами внутри Docker-сети `kafka-net`;
  * `EXTERNAL://localhost:9092..9094` — для подключения CLI-утилит и скрипта `./verify` с хост-машины;
  * `CONTROLLER://kafka-X:29093` — для кворума контроллеров Raft.
* Автосоздание топиков отключено (`KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"`).
* Одноразовый сервис `kafka-init` дожидается готовности всех трех брокеров (`service_healthy`) и создает топики:
  * `events`: `partitions=3`, `replication-factor=3`, `min.insync.replicas=2`;
  * `events.dlt`: `partitions=3`, `replication-factor=3`, `min.insync.replicas=2`.

### Часть 2. Продюсеры (Spring Boot)
* Конфигурация `KafkaProducerConfig.java`:
  * `acks=all` — лидер партиции ждет подтверждения записи от всех реплик из списка ISR (не менее `min.insync.replicas=2`);
  * `enable.idempotence=true`, `max.in.flight.requests.per.connection=5`, `retries=Integer.MAX_VALUE` — гарантирует отсутствие дубликатов на уровне лога брокера при сетевых ретраях продюсера;
  * выбор ключа управляется переменной окружения `KEY_MODE=user_id|none`: при `user_id` ключом служит `event.userId()`, при `none` ключ равен `null`;
  * ошибки отправки обрабатываются в `CompletableFuture.whenComplete(...)` и логируются через `log.error(...)`;
  * при получении `SIGTERM` метод `@PreDestroy onShutdown()` останавливает генерацию новых сообщений и вызывает `kafkaTemplate.flush()`, досылая все буферизованные батчи.

### Часть 3. Консьюмеры (`events-processor`) и идемпотентный Sink
* **Осознанный выбор `auto.offset.reset=earliest`:** если группа `events-processor` запускается впервые уже после того, как продюсеры записали первые сообщения в `events`, политика `earliest` гарантирует чтение партиции с оффсета `0` без потери стартовых событий. После первого коммита консьюмеры всегда продолжают чтение с последнего закоммиченного оффсета.
* **Ручной коммит оффсетов (`enable.auto.commit=false`, `AckMode.MANUAL_IMMEDIATE`):** оффсет коммитится синхронно (`acknowledgment.acknowledge()`) **только после** успешной транзакции записи в PostgreSQL (`sink_events`).
* **Безопасный ребаланс (`ConsumerAwareRebalanceListener`):** в `onPartitionsRevokedBeforeCommit` перед отдачей партиций при ребалансе выполняется `consumer.commitSync(offsetsToCommit)` для всех уже обработанных оффсетов.
* **Идемпотентная запись и дедупликация в Sink (`PostgresSinkRepository.java`):**
  ```sql
  INSERT INTO sink_events (event_id, user_id, seq, event_type, processor_pod, topic, partition, kafka_offset, produced_at, payload)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)
  ON CONFLICT (event_id) DO NOTHING;
  ```
  Если `insertedRows == 0` (повторная доставка уже записанного `event_id`), событие не дублируется в `sink_events`, а счетчик `dedup_stats.rejected_duplicates` атомарно увеличивается на `1`.
* **Обработка ядовитых событий и Dead Letter Topic (`events.dlt`):**
  Настроен `DefaultErrorHandler` с `FixedBackOff(150L, 2L)` (1 первичная попытка + 2 повтора = **ровно 3 неудачные попытки**), после чего `DeadLetterPublishingRecoverer` отправляет событие в топик `events.dlt` с заголовками:
  * `x-error-reason` — причина ошибки (`PoisonEventException: Poison event detected...`);
  * `x-original-topic` — исходный топик (`events`);
  * `x-original-partition` — исходная партиция (`0`, `1` или `2`);
  * `x-original-offset` — исходный оффсет в топике `events`.
  После публикации в `events.dlt` оффсет в исходной партиции коммитится (`setCommitRecovered(true)`), поэтому партиция не блокируется.

---

## Часть 4. Эксперименты с отказами и проверка `./verify`

### Эксперимент 1. Жесткое убийство (`kill -9`) одного пода-консьюмера

#### Гипотеза
1. При отправке `SIGKILL` (`docker kill -s KILL consumer-2`) под мгновенно прекращает работу, не успевая выполнить graceful shutdown и не отправляя `LeaveGroupRequest` координатору группы.
2. По истечении `session.timeout.ms` (10 секунд) Group Coordinator фиксирует отказ `consumer-2` и запускает ребаланс группы `events-processor`.
3. Оставшиеся живыми `consumer-1` и `consumer-3` вызывают `ConsumerAwareRebalanceListener.onPartitionsRevokedBeforeCommit` (фиксируя свои текущие оффсеты) и забирают осиротевшую партицию `P1`, которой владел `consumer-2`.
4. Если `consumer-2` был убит в промежутке **после** выполнения `INSERT INTO sink_events`, но **до** сетевого вызова `commitSync()` (окно во время `PROCESSING_DELAY_MS` / коммита), новый владелец партиции `P1` вычитает последний незакоммиченный оффсет повторно. Идемпотентная вставка `ON CONFLICT (event_id) DO NOTHING` отбросит дубликат и увеличит счетчик `rejected_duplicates` на `1`. Ни одно событие не будет потеряно (`missing_gaps = 0`).

#### Результат выполнения `./verify` после `kill -9 consumer-2`

```text
itmo-ddia-student | 2026-11-18 19:24:12 MSK
================================================================================
  KAFKA PIPELINE VERIFICATION REPORT (SINK + events.dlt)
================================================================================
1. Per-User Sequence Integrity (sink + events.dlt):
   * user-101   | seq: 1..48   | sink: 47   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-102   | seq: 1..45   | sink: 44   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-103   | seq: 1..50   | sink: 49   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-104   | seq: 1..46   | sink: 45   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-201   | seq: 1..49   | sink: 48   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-202   | seq: 1..44   | sink: 43   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-203   | seq: 1..47   | sink: 46   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-204   | seq: 1..45   | sink: 44   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-301   | seq: 1..51   | sink: 50   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-302   | seq: 1..46   | sink: 45   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-303   | seq: 1..48   | sink: 47   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-304   | seq: 1..47   | sink: 46   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
--------------------------------------------------------------------------------
2. Summary Metrics:
   - Total Valid Events in Sink        : 554
   - Total Poison Events in events.dlt : 12
   - Missing Sequence Numbers (Gaps)   : 0  [PASS: 0 LOST EVENTS]
   - Rejected Duplicate Deliveries     : 2
   - Out-of-Order Sink Writes          : 0  [STRICT ORDER PRESERVED]
================================================================================
```

#### Объяснение результатов
Гипотеза полностью подтвердилась: после `kill -9 consumer-2` партиция `events-1` перешла к `consumer-1` с последнего закоммиченного оффсета. Сообщения, которые `consumer-2` успел записать в PostgreSQL прямо перед гибелью, но не успел подтвердить оффсетом в `__consumer_offsets`, были перечитаны `consumer-1` и отфильтрованы уникальным индексом `event_id` (`Rejected Duplicate Deliveries: 2`). Пропусков `seq` и нарушений порядка нет (`0`).

---

### Эксперимент 2. Остановка одного брокера, затем второго брокера и последующее восстановление

#### Гипотеза
1. **Остановка первого брокера (`docker stop kafka-1`):**
   * В кластере остается 2 живых узла (`kafka-2`, `kafka-3`), что сохраняет кворум контроллеров Raft ($2 \ge \lfloor 3/2 \rfloor + 1$).
   * Для партиций, лидером которых был `kafka-1`, контроллер выбирает нового лидера из списка ISR (`kafka-2` или `kafka-3`).
   * Размер ISR всех партиций сокращается с `3` до `2`. Поскольку `min.insync.replicas=2`, условие записи с `acks=all` ($|\text{ISR}| = 2 \ge 2$) **выполняется**: продюсеры и консьюмеры продолжают работу без простоев после краткого обновления метаданных.
2. **Остановка второго брокера (`docker stop kafka-2`):**
   * В живых остается только 1 брокер (`kafka-3`). Размер ISR падает до `1`, что **меньше** `min.insync.replicas=2` (кроме того, теряется кворум Raft-контроллеров 1 из 3).
   * Продюсеры с настройкой `acks=all` больше не могут подтвердить запись и начинают логировать ошибки `org.apache.kafka.common.errors.NotEnoughReplicasException` / `TimeoutException`, удерживая сообщения в буфере и выполняя ретраи.
3. **Возврат брокеров (`docker start kafka-1 kafka-2`):**
   * Кворум контроллеров восстанавливается, реплики догоняют лидера и возвращаются в ISR ($|\text{ISR}| = 3 \ge 2$).
   * Идемпотентные продюсеры (`enable.idempotence=true`) досылают буферизованные сообщения без дублей и без пропусков `seq`.

#### Результат выполнения `./verify` после остановки двух брокеров и восстановления кластера

```text
itmo-ddia-student | 2026-11-18 19:31:45 MSK
================================================================================
  KAFKA PIPELINE VERIFICATION REPORT (SINK + events.dlt)
================================================================================
1. Per-User Sequence Integrity (sink + events.dlt):
   * user-101   | seq: 1..64   | sink: 62   | dlt: 2   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-102   | seq: 1..61   | sink: 60   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-103   | seq: 1..66   | sink: 65   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-104   | seq: 1..62   | sink: 61   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-201   | seq: 1..65   | sink: 64   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-202   | seq: 1..60   | sink: 58   | dlt: 2   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-203   | seq: 1..63   | sink: 62   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-204   | seq: 1..61   | sink: 60   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-301   | seq: 1..67   | sink: 65   | dlt: 2   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-302   | seq: 1..62   | sink: 61   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-303   | seq: 1..64   | sink: 63   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
   * user-304   | seq: 1..63   | sink: 62   | dlt: 1   | missing_gaps: 0  | ooo_writes: 0   | OK (NO GAPS)
--------------------------------------------------------------------------------
2. Summary Metrics:
   - Total Valid Events in Sink        : 743
   - Total Poison Events in events.dlt : 15
   - Missing Sequence Numbers (Gaps)   : 0  [PASS: 0 LOST EVENTS]
   - Rejected Duplicate Deliveries     : 3
   - Out-of-Order Sink Writes          : 0  [STRICT ORDER PRESERVED]
================================================================================
```

#### Объяснение результатов
При отключении одного брокера кластер продолжил принимать запись, так как $|\text{ISR}|=2 \ge \text{min.insync.replicas}=2$. При отключении второго брокера продюсеры заблокировали подтверждение записи (`NotEnoughReplicasException`), защищая данные от потери при несогласованном лидере. После запуска `kafka-1` и `kafka-2` репликация восстановилась, а благодаря `enable.idempotence=true` (Producer ID + Sequence Number на уровне партиции) повторные отправки батчей не создали ни пропусков, ни нарушений порядка.

---

### Эксперимент 3. Запуск продюсеров с `KEY_MODE=none` против `KEY_MODE=user_id`

#### Гипотеза
1. В режиме `KEY_MODE=user_id` стандартный партиционер Kafka (`murmur2(user_id) % 3`) детерминированно направляет все события одного `user_id` в **одну и ту же партицию**. Так как каждую партицию внутри одной consumer group в каждый момент времени читает **ровно один** под-консьюмер, события одного пользователя обрабатываются строго последовательно ($\text{ooo\_writes} = 0$).
2. В режиме `KEY_MODE=none` ключ сообщения равен `null`. Продюсеры распределяют сообщения одного и того же `user_id` по разным партициям (`P0`, `P1`, `P2`).
3. Разные партиции (`P0`, `P1`, `P2`) читаются параллельно тремя разными консьюмерами (`consumer-1`, `consumer-2`, `consumer-3`). Из-за разницы во времени опроса (`poll`), сетевых задержек, ретраев на ядовитых сообщениях (3 попытки $\times$ 150 мс в одной из партиций) и `PROCESSING_DELAY_MS` консьюмер одной партиции записывает событие с бо́льшим `seq` (например, `seq=15` из `P2`) в `sink_events` **раньше**, чем другой консьюмер дописывает событие с меньшим `seq` (`seq=14` из `P0`, задержанной ретраем).
4. При этом сами события не теряются (`missing_gaps = 0`), но `./verify` зафиксирует ненулевое количество нарушений порядка (`Out-of-Order Sink Writes > 0`).

#### Результат выполнения `./verify` при `KEY_MODE=none`

```text
itmo-ddia-student | 2026-11-18 19:39:08 MSK
================================================================================
  KAFKA PIPELINE VERIFICATION REPORT (SINK + events.dlt)
================================================================================
1. Per-User Sequence Integrity (sink + events.dlt):
   * user-101   | seq: 1..55   | sink: 54   | dlt: 1   | missing_gaps: 0  | ooo_writes: 11  | OK (NO GAPS)
   * user-102   | seq: 1..52   | sink: 51   | dlt: 1   | missing_gaps: 0  | ooo_writes: 9   | OK (NO GAPS)
   * user-103   | seq: 1..56   | sink: 55   | dlt: 1   | missing_gaps: 0  | ooo_writes: 14  | OK (NO GAPS)
   * user-104   | seq: 1..53   | sink: 52   | dlt: 1   | missing_gaps: 0  | ooo_writes: 8   | OK (NO GAPS)
   * user-201   | seq: 1..54   | sink: 53   | dlt: 1   | missing_gaps: 0  | ooo_writes: 12  | OK (NO GAPS)
   * user-202   | seq: 1..50   | sink: 49   | dlt: 1   | missing_gaps: 0  | ooo_writes: 7   | OK (NO GAPS)
   * user-203   | seq: 1..53   | sink: 52   | dlt: 1   | missing_gaps: 0  | ooo_writes: 10  | OK (NO GAPS)
   * user-204   | seq: 1..51   | sink: 50   | dlt: 1   | missing_gaps: 0  | ooo_writes: 9   | OK (NO GAPS)
   * user-301   | seq: 1..57   | sink: 55   | dlt: 2   | missing_gaps: 0  | ooo_writes: 13  | OK (NO GAPS)
   * user-302   | seq: 1..52   | sink: 51   | dlt: 1   | missing_gaps: 0  | ooo_writes: 8   | OK (NO GAPS)
   * user-303   | seq: 1..54   | sink: 53   | dlt: 1   | missing_gaps: 0  | ooo_writes: 11  | OK (NO GAPS)
   * user-304   | seq: 1..53   | sink: 52   | dlt: 1   | missing_gaps: 0  | ooo_writes: 10  | OK (NO GAPS)
--------------------------------------------------------------------------------
2. Summary Metrics:
   - Total Valid Events in Sink        : 627
   - Total Poison Events in events.dlt : 13
   - Missing Sequence Numbers (Gaps)   : 0  [PASS: 0 LOST EVENTS]
   - Rejected Duplicate Deliveries     : 3
   - Out-of-Order Sink Writes          : 122  [ORDER VIOLATIONS DETECTED]
================================================================================
```

#### Объяснение результатов
Kafka гарантирует порядок сообщений **только внутри одной партиции**. При `KEY_MODE=none` события одного `user_id` попадают в разные партиции (`P0`, `P1`, `P2`) и обрабатываются конкурентно тремя независимыми подами `consumer-1..3`. Как только в одной из партиций случается задержка (например, 3 попытки обработки poison-сообщения или сдвиг цикла `poll`), соседние партиции обгоняют её, и события с бо́льшим `seq` записываются в PostgreSQL раньше событий с меньшим `seq` (`122` нарушения порядка при `0` потерянных сообщений).

---

## Инструкция по запуску

### 1. Запуск кластера Kafka и микросервисов Spring Boot через Docker Compose
```bash
docker compose up -d --build
```

### 2. Запуск скрипта верификации `./verify`
```bash
GITHUB_LOGIN="your-github-login" ./verify
```

### 3. Команды для воспроизведения экспериментов вручную
```bash
# Эксперимент 1: kill -9 одного пода-консьюмера
docker kill -s KILL consumer-2
sleep 12
./verify
docker start consumer-2

# Эксперимент 2: последовательная остановка двух брокеров и восстановление
docker stop kafka-1
sleep 8
docker stop kafka-2
sleep 10
docker start kafka-1 kafka-2
sleep 12
./verify

# Эксперимент 3: перезапуск продюсеров с KEY_MODE=none
KEY_MODE=none docker compose up -d producer-1 producer-2 producer-3
sleep 15
./verify
```

---

## Инструкция по запуску веб-обсерватории (Next.js)

Веб-интерфейс — самодостаточное Next.js 16-приложение (симулятор Kafka-пайплайна). Для работы ему нужен только PostgreSQL из compose-стека (`postgres-sink`); сами брокеры Kafka и Spring Boot сервисы для него не обязательны.

### 1. Предварительные требования
* Запущенный Docker Desktop и compose-стек: `docker compose up -d --build` (нужен как минимум контейнер `postgres-sink`);
* Node.js ≥ 20, npm.

### 2. Установка зависимостей (один раз)
```bash
npm install
```

### 3. Переменные окружения (один раз)
Создать файл `.env.local` в корне проекта:
```bash
echo DATABASE_URL=postgresql://postgres:postgres@localhost:5432/kafka_sink > .env.local
```

### 4. Создание таблиц веб-приложения (один раз)
В schema.sql Java-сервиса есть только `sink_events` и `dedup_stats`; веб-приложению дополнительно нужны `dlt_events`, `kafka_log_records`, `cluster_state`, `system_logs`. Создать их одним из способов:
```bash
# Вариант А — drizzle-kit (стандартный путь):
npx drizzle-kit push --dialect=postgresql --schema=./src/db/schema.ts \
  --url=postgresql://postgres:postgres@localhost:5432/kafka_sink

# Вариант Б — готовый SQL (если drizzle-kit молча падает в неинтерактивной среде):
docker exec -i postgres-sink psql -U postgres -d kafka_sink < db-init.sql
```

### 5. Запуск dev-сервера
```bash
npm run dev
```
Веб-интерфейс доступен на **http://localhost:3000**.

### Типичные проблемы
* **«Port 3000 is in use» / «Another next dev server is already running»** — остался старый процесс dev-сервера: убейте его `taskkill /PID <pid> /F` (PID указан в сообщении) и запустите `npm run dev` заново.
* **Страница загружается, но данные/действия падают с ошибкой БД** — не выполнен шаг 4 (таблицы) либо compose-стек остановлен: поднимите его `docker compose up -d`.
* Веб-приложение использует ту же БД `kafka_sink`, что и Java-сервисы, но свои таблицы — данные симулятора и реального пайплайна не конфликтуют.
