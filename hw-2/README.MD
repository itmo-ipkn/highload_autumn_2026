# HW2 — Kafka

Первый этап: три узла `apache/kafka:4.1.1` в режиме KRaft, каждый совмещает роли broker и controller. ZooKeeper не используется. Нужны Docker Compose и свободные порты 19092, 29092, 39092. Все команды ниже выполняются из `hw-2`.

## Команды Make

Из каталога `hw-2` выполните `make` или `make help`, чтобы увидеть все команды.

```bash
make kafka-up          # Только кластер и создание топиков
make producers-up      # Сборка и запуск трёх продюсеров
make producers-logs    # Наблюдение за отправкой
make read              # Новые сообщения с ключами, партициями и offsets
make topics
make quorum
make offsets
make producers-stop
make producers-up KEY_MODE=none EVENT_INTERVAL_MS=200
make read-history TOPIC=events.dlt
make test
make down              # Данные Kafka сохраняются
```

`make up` запускает весь стенд со сборкой. `make producers-up` пересоздаёт продюсеры, чтобы применить настройки: **seq сбрасывается с каждым запуском этой команды**. Остальные env-настройки также можно передавать как аргументы Make. `make send` читает строки из stdin, например `printf 'hello-kafka\n' | make send`.

Разрушительный сброс вынесен отдельно: `make reset CONFIRM=delete-all-data`. Он останавливает стенд и **удаляет Kafka и PostgreSQL volumes вместе со всеми сообщениями, offsets, метаданными, sink и счётчиками**. Без точного подтверждения команда ничего не удаляет. После ручного сброса запуск выполняется отдельно: `make up`.

## Запуск

```bash
docker compose up -d
docker compose ps -a
docker compose logs kafka-init
```

Можно запускать и обычным `docker compose up`, чтобы видеть логи. Брокеры стартуют параллельно; после простого API healthcheck всех трёх узлов одноразовый `kafka-init` создаёт топики и завершается с кодом 0 (`Exited (0)` в `ps -a`). Дождитесь его завершения перед отправкой сообщений.

| Топик | Партиции | Replication factor | min.insync.replicas |
|---|---:|---:|---:|
| `events` | 3 | 3 | 2 |
| `events.dlt` | 3 | 3 | 1 (по умолчанию) |

Автосоздание топиков отключено. Init использует `--if-not-exists`: существующие топики не пересоздаются и их параметры автоматически не исправляются.

Общие настройки узлов вынесены в YAML anchors в Compose. У каждого узла свой `node.id` и volume; `CLUSTER_ID` и список `controller.quorum.voters` общие. Официальный entrypoint выбранной версии форматирует хранилище для статического quorum; динамический bootstrap не используется.

Heap каждого брокера по умолчанию ограничен `-Xms256m -Xmx512m`. При необходимости переопределить:

```bash
KAFKA_HEAP_OPTS='-Xms256m -Xmx768m' docker compose up -d
```

## Адреса подключения

| Узел | Из Docker Compose (INTERNAL) | С хоста (EXTERNAL) |
|---|---|---|
| kafka-1 | `kafka-1:9092` | `localhost:19092` |
| kafka-2 | `kafka-2:9092` | `localhost:29092` |
| kafka-3 | `kafka-3:9092` | `localhost:39092` |

Controller listener использует порт 9093 внутри Compose. В `advertised.listeners` включены только INTERNAL и EXTERNAL: клиент получает адреса, доступные из его сети.

## Проверка кластера и топиков

Стандартные Kafka CLI уже есть в контейнерах:

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092
docker compose exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status
docker compose exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe
```

В первом выводе должны присутствовать брокеры с IDs 1, 2, 3, в quorum — три voters и выбранный `LeaderId`. В описании каждого учебного топика — три партиции с тремя репликами. `Leader` — брокер-лидер партиции, `Replicas` — назначенные реплики, `Isr` — синхронизированные реплики. В исправном кластере после синхронизации у каждой партиции три ISR. У `events` в `Configs` указан `min.insync.replicas=2`.

Если на хосте распакован дистрибутив Kafka 4.1.1 и установлена Java 17+, подключение с хоста проверяется теми же CLI (укажите свой путь):

```bash
KAFKA_BIN=/path/to/kafka/bin
"$KAFKA_BIN/kafka-broker-api-versions.sh" --bootstrap-server localhost:19092,localhost:29092,localhost:39092
```

В ответе будут внешние адреса трёх брокеров. Для работы через `docker compose exec` устанавливать Kafka или Java на хост не требуется.

## Отправка и чтение сообщения

```bash
printf 'hello-kafka\n' | docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic events --producer-property acks=all
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic events --from-beginning --max-messages 1 --timeout-ms 10000
```

На новом стенде consumer напечатает `hello-kafka`. Если в топике уже есть сообщения, `--from-beginning --max-messages 1` прочитает одно из ранее записанных сообщений.

## Остановка

```bash
docker compose down
```

Named volumes сохраняются для следующего запуска и экспериментов. Полный сброс **с удалением данных Kafka и PostgreSQL**:

```bash
docker compose down -v
```

## Этап 2: продюсеры

Один код Node.js/TypeScript/KafkaJS работает в трёх контейнерах. Раздел описывает этап 2; consumers и sink добавлены в этапе 3 ниже, verify пока не реализован. Для локальной разработки нужен Node.js 24 или 25 и pnpm 10.19.0; Docker-образ фиксирует Node.js 24.11.1.

```bash
pnpm install --frozen-lockfile
pnpm test
pnpm build
docker compose up -d --build producer-1 producer-2 producer-3
docker compose logs -f producer-1 producer-2 producer-3
```

Зависимость от успешного `kafka-init` задана в Compose. Все экземпляры используют один образ и INTERNAL listeners. Локально с хоста можно запустить один экземпляр: `PRODUCER_ID=producer-1 pnpm start`; сначала остановите одноимённый контейнер, чтобы не создавать двух владельцев пользователей.

| Переменная | По умолчанию | Значение |
|---|---|---|
| PRODUCER_ID | producer-1 локально | producer-1, producer-2, producer-3; в Compose задана отдельно |
| KAFKA_BROKERS | localhost:19092,localhost:29092,localhost:39092 локально | В Compose kafka-1:9092,kafka-2:9092,kafka-3:9092 |
| KAFKA_TOPIC | events | Топик должен существовать |
| KEY_MODE | user_id | user_id или none |
| EVENT_INTERVAL_MS | 100 | Положительная целая пауза после каждой подтверждённой отправки на экземпляр |
| USER_COUNT | 30 | Общее положительное целое число пользователей, одинаковое для трёх экземпляров |
| POISON_RATE | 0.02 | Вероятность poison от 0 до 1 |

Параметры проверяются до подключения и генерации. Пустые значения не заменяются defaults. При значениях по умолчанию верхняя оценка суммарной скорости — около 30 событий/с; задержка Kafka снижает её. Пакетной отправки нет.

Пользователь user-N принадлежит экземпляру с индексом `(N-1)%3`. При USER_COUNT=9 наборы равны 1/4/7, 2/5/8, 3/6/9. Экземпляр циклически обходит своих пользователей. Если его набор пуст, он ожидает остановки без отправок. Нельзя запускать два экземпляра с одинаковым PRODUCER_ID.

Формат события:

```json
{"event_id":"d458e2cb-6fe1-481e-92f4-74c145d72c10","user_id":"user-1","type":"page_view","seq":1,"produced_at":"2026-11-24T12:00:00.000Z","payload":{}}
```

Тип выбирается случайно из page_view, add_to_cart, purchase. UUID и ISO-время генерируются средствами Node.js. Каждый пользователь имеет собственный seq от 1 без пропусков в пределах запуска. Poison также расходует seq и сохраняет event_id/user_id: примерно 2% событий имеют `payload.poison=true`. Это валидный JSON, который успешно записывается в Kafka. Будущий консьюмер должен намеренно бросать ошибку обработки по этому признаку. Вероятность 0.02 не означает ровно два poison в каждой сотне.

### Ключи и просмотр партиций

В режиме user_id ключ равен event.user_id, поэтому при неизменных трёх партициях все события пользователя попадают в одну партицию. В режиме none ключ равен null, partition не задаётся; привязка пользователя к партиции и межпартиционный порядок отсутствуют. Генератор в обоих режимах одинаков.

```bash
docker compose stop producer-1 producer-2 producer-3
KEY_MODE=none docker compose up -d --force-recreate producer-1 producer-2 producer-3
# Возврат к ключам, также с перезапуском и сбросом seq:
docker compose stop producer-1 producer-2 producer-3
KEY_MODE=user_id docker compose up -d --force-recreate producer-1 producer-2 producer-3
```

Другие настройки меняются аналогично, например `USER_COUNT=9 EVENT_INTERVAL_MS=200 docker compose up -d --force-recreate producer-1 producer-2 producer-3`.

Чтение новых сообщений с ключом, номером партиции и offset стандартной утилитой (Ctrl+C завершает просмотр):

```bash
docker compose exec kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic events --consumer-property auto.offset.reset=latest --property print.key=true --property print.partition=true --property print.offset=true
```

Для чтения истории замените `--consumer-property auto.offset.reset=latest` на `--from-beginning`; история может содержать сообщения предыдущих запусков и контрольное hello-kafka. Партиция также видна в логах подтверждённых отправок:

```text
producer-1 | sent event_id=... user=user-1 seq=5 type=purchase poison=false partition=2
```

### Надёжность и завершение

Используются `acks=-1` (all), `idempotent=true`, `maxInFlightRequests=1`, `allowAutoTopicCreation=false`. Одновременно выполняется только один send одного сообщения. Ошибка после исчерпания повторов логируется с event_id/user_id/seq, генерация прекращается, процесс завершается с кодом 1. Повторы KafkaJS ограничены тремя retries (до четырёх попыток), задержка каждого повтора ограничена 1 с; вложенные клиентские retries отключены. Автоперезапуск контейнеров не включён.

SIGTERM и SIGINT прекращают генерацию и прерывают паузу, но не отменяют текущий send. После его завершения вызывается disconnect, затем логируется итоговый код. Собственной очереди нет: досылка означает ожидание единственной текущей отправки. Идемпотентность относится к повторам в сессии KafkaJS, а не к рестартам приложения. Ошибка подтверждения может означать неопределённый результат записи; при недоступной Kafka успешная досылка не гарантируется, ошибка не скрывается.

`stop_grace_period: 300s` даёт запас для завершения KafkaJS. Явные таймауты: connectionTimeout=3 с, requestTimeout=10 с с enforceRequestTimeout=true, broker produce timeout=5 с. Последний не является общим deadline send: возможны metadata-запросы и переподключения. Для оценки учитываем до четырёх попыток, на каждую резервируем produce-запрос (10 с), metadata до и после ошибки (2×10 с), подключения ко всем трём адресам (3×3 с), а также до 3 с backoff: около 159 с. Оставшееся время до 300 с — запас на discovery, инициализацию producer ID, закрытие соединений и планирование. Это эксплуатационный запас для данного стенда, а не универсальная жёсткая верхняя граница KafkaJS при произвольных изменениях топологии. В исправном кластере остановка обычно занимает доли секунды. Node запускается напрямую через exec-form CMD и получает сигнал Docker.

```bash
docker compose stop producer-1 producer-2 producer-3
docker compose logs --tail=10 producer-1 producer-2 producer-3
docker compose ps -a producer-1 producer-2 producer-3
# Отдельная проверка SIGINT после повторного запуска:
docker compose up -d producer-1 producer-2 producer-3
docker compose kill -s SIGINT producer-1 producer-2 producer-3
```

Ожидаются `stopping signal=...`, завершение текущей отправки, `shutdown complete exitCode=0` и `Exited (0)`. Новые события после сигнала не генерируются; строка подтверждения уже начатой отправки может появиться после строки stopping. Ошибка досылки/закрытия даёт код 1.

### Независимые эксперименты и ручной сброс

Счётчики находятся в памяти: любой перезапуск начинает seq с 1. Старые сообщения остаются в Kafka, поэтому история разных запусков не является одной последовательностью. Для независимого эксперимента используйте чистое состояние данных. Обычная остановка продюсеров и `docker compose down` сохраняют volumes.

**Следующая процедура разрушительна: удаление Kafka volumes уничтожает все сообщения, offsets и метаданные этого кластера.** Предварительно сохраните нужные данные и остановите стенд. Выполняйте её только вручную, когда действительно нужен новый эксперимент:

```bash
docker compose down
# Только после осознанного решения удалить данные:
docker compose down -v
docker compose up -d --build
```

Приложение и тесты никогда не выполняют reset автоматически. Интеграционные проверки могут вместо reset зафиксировать начальные offsets партиций и читать только новые записи одного ограниченного запуска.

### Результат проверки этапа 2 (24.11.2026)

Проверены установка по frozen lockfile, TypeScript build, 11 тестов (в том числе под Node.js 24.11.1 в контейнере), `docker compose config`, сборка образа и два ограниченных запуска на существующем трёхброкерном кластере. Перед каждым запуском зафиксированы offsets всех партиций; проверялись только новые записи до offsets после остановки. Сброса данных не было. Временный тестовый читатель не является приложением консьюмера или CLI verify и в проект не добавлен.

| Режим | Новые события | Пользователи | Poison | Партиции | Завершение |
|---|---:|---:|---:|---|---|
| user_id | 162 | 30 | 4 | 0, 1, 2 | SIGTERM, все три exit 0 |
| none | 173 | 30 | 5 | 0, 1, 2 | SIGINT, все три exit 0 |

Для обоих запусков подтверждены уникальные event_id, формат события, правильные ключи, непересекающиеся владельцы и полные последовательности seq от 1. Для user_id дополнительно проверены фактический порядок seq внутри партиции и одна партиция на пользователя. В none межпартиционный порядок не утверждается. По логам подтверждено завершение уже начатой отправки после SIGINT, без генерации следующего события. После проверок продюсеры остановлены, все брокеры healthy, volumes сохранены.

| Требование | Где реализовано | Как проверено |
|---|---|---|
| Node.js, TypeScript, KafkaJS, без HTTP/framework | package.json, tsconfig.json, src, Dockerfile | frozen install, build, сборка образа |
| Одно приложение и три независимых экземпляра | docker-compose.yaml, src/config.ts | три контейнера одного образа, разные PRODUCER_ID |
| Поля события, UUID, допустимые типы, ISO-время, payload | src/events.ts | events.test.ts, чтение новых сообщений |
| Владение по модулю 3, seq от 1 без пропусков | src/events.ts | тесты разных USER_COUNT, 30 пользователей в каждом smoke-run, проверка логов владельцев |
| Последовательная отправка | src/producer.ts | await send; тест блокированной отправки, проверка порядка keyed-записей |
| KEY_MODE=user_id/none | src/config.ts, src/events.ts | unit-тесты ключей, оба запуска в Kafka |
| Примерно 2% poison, идентификаторы сохранены | src/events.ts | тест порога 0.02 и границ 0/1, 4 и 5 poison в двух выборках |
| acks=all, идемпотентность | src/producer.ts: makeProducer и send | явные настройки, успешный InitProducerId и реальные отправки; unit-проверка acks |
| Конечные retries, ошибки видимы, после ошибки остановка | src/producer.ts, src/main.ts | тесты ошибок connect/send/disconnect; никаких последующих отправок; ненулевой результат передаётся в process.exitCode |
| SIGTERM/SIGINT, досылка и disconnect | src/producer.ts, Dockerfile | тест сигнала при send/паузе/повторного сигнала, реальные SIGTERM и SIGINT с exit 0 |
| Семь env-параметров и defaults | src/config.ts, docker-compose.yaml | тесты корректной и ошибочной конфигурации, сверка таблицы настроек |
| Старт после init, INTERNAL listeners, сохранение Kafka | docker-compose.yaml | compose config, реальный запуск; сравнение разрешённой конфигурации kafka-1..3, kafka-init и volumes с исходной — без изменений |
| Логи экземпляра, отправки, партиции, ошибок, остановки | src/producer.ts | unit-пути ошибок и реальные логи с partition, seq и итоговым exitCode |
| README, ограничения рестарта и безопасный ручной reset | README.MD | сохранён этап 1, проверено чтение key/partition/offset стандартным CLI; reset не выполнялся |
| Нет функциональности следующих этапов | src и состав Compose | только продюсеры; консьюмеры, sink, DLT processing и verify отсутствуют |

Созданы: `src/config.ts`, `src/events.ts`, `src/producer.ts`, `src/main.ts`, `test/events.test.ts`, `test/producer.test.ts`, `tsconfig.json`, `pnpm-lock.yaml`, `Dockerfile`, `.dockerignore`, `.gitignore`. Изменены: `package.json`, `docker-compose.yaml`, `README.MD`; отмечены задачи OpenSpec change.

Ограничения проверки: отказ брокеров не моделировался (следующий этап); окончательные ошибки и сигнал во время заблокированной отправки проверены управляемыми тестами. Успешный запуск с идемпотентностью не является экспериментальным доказательством exactly-once при всех отказах. KafkaJS выводит предупреждение о конечных retries у идемпотентного продюсера — они ограничены намеренно с прекращением процесса при ошибке. В Node.js 24 также наблюдался `TimeoutNegativeWarning` внутри KafkaJS; сообщения доставлены и все процессы завершились с кодом 0, зависимость не патчилась и предупреждение не подавлялось. На одном подключении был временный ответ coordinator loading, после которого KafkaJS успешно подключился в пределах retry budget.


## Этап 3: consumers, PostgreSQL и DLT

```bash
make up                 # Весь стенд одной командой
make consumers-up       # Или только consumers с зависимостями
make consumers-logs
make consumer-group     # Назначение партиций
make consumer-lag       # Committed offsets и lag
make processed-count
make duplicates
make dlt                # DLT с key, headers, partition и offset; Ctrl+C
make consumers-stop     # SIGTERM и завершение текущей обработки
```

Три consumers одной группы `events-processor` используют существующий образ приложения с командой `node dist/src/consumer-main.js`. При трёх активных consumers и трёх партициях ожидается по одной партиции на экземпляр; ручного закрепления нет. Consumers зависят от успешного kafka-init и здорового PostgreSQL. PostgreSQL `17.7-bookworm` хранит данные в отдельном named volume, порт на хост не публикуется. `postgres/init.sql` создаёт таблицы при инициализации нового volume. Для существующего PostgreSQL volume изменения SQL автоматически не применяются.

| Переменная | По умолчанию |
|---|---|
| CONSUMER_ID | consumer-1 (в Compose consumer-1..3) |
| KAFKA_BROKERS | localhost:19092,localhost:29092,localhost:39092; в Compose INTERNAL |
| KAFKA_TOPIC | events |
| KAFKA_DLT_TOPIC | events.dlt |
| KAFKA_GROUP_ID | events-processor |
| DATABASE_URL | Обязательна; в Compose postgresql://events:events@postgres:5432/events |
| PROCESSING_DELAY_MS | 100, целое >= 0 |
| MAX_PROCESSING_ATTEMPTS | 3, целое 1..3 |
| RETRY_DELAY_MS | 300, положительное целое |

Параметры проверяются до подключения. Для подключения с хоста задайте доступный ему DATABASE_URL: Compose по умолчанию не открывает PostgreSQL наружу.

### Sink и дедупликация

`processed_events` содержит event_id UUID PRIMARY KEY, user_id, type, seq, produced_at, processed_at, payload JSONB и обязательные consumer_id, topic, partition, offset. Offset хранится как BIGINT и передаётся строкой, без потери точности JavaScript Number. `processed_at` использует clock_timestamp() в момент вставки.

Запись выполняется через INSERT ON CONFLICT (event_id) DO NOTHING RETURNING event_id, без предварительного SELECT. При конфликте в той же транзакции увеличивается `consumer_stats.duplicates` для экземпляра. Метаданные первой успешной вставки сохраняются. Счётчик переживает перезапуск consumers и PostgreSQL при сохранении volume. Он отражает подтверждённые БД попытки дедупликации; при неопределённом результате SQL повтор может увеличить его ещё раз.

### Обработка, offsets и rebalance

Используются eachBatch, autoCommit=false, eachBatchAutoResolve=false, partitionsConsumedConcurrently=1. Порядок обработки сообщений одной партиции последовательный. Подписка fromBeginning=true означает earliest при отсутствии или недействительности сохранённого offset; существующая позиция группы сохраняется.

После подтверждения транзакции PostgreSQL либо DLT send вызывается batch callback commitOffsetsIfNecessary с явным объектом topics/partitions и offset+1, вычисленным через BigInt. В KafkaJS 2.2.4 явные offsets вызывают commit непосредственно; вызов без аргумента здесь не используется. Только после подтверждённого commit вызывается resolveOffset текущего сообщения: это локальное продвижение, не эквивалент commit.

Любая ошибка heartbeat или commit прекращает batch через исключение. Следующее сообщение не начинается; собственного retry commit и переноса старой позиции через rejoin нет. KafkaJS обрабатывает rejoin; запрос commit содержит generation/member ID, координатор отвергает устаревшую генерацию. isStale проверяет seek, а не владение партицией. REBALANCING логируется. После ошибки batch блокируются также уже стоящие в очереди callbacks; блокировка снимается только GROUP_JOIN после завершённого join/sync. GROUP_JOIN не используется как барьер отзыва и не переносит старый commit. Между проверкой и commit возможна гонка: клиент не может гарантировать отсутствие попытки запроса после ещё не обнаруженной потери membership.

Heartbeat обслуживается отдельным последовательным циклом во время обработки и ожидания SQL/DLT. Его ошибка запоминается, новые операции запрещаются; уже отправленная операция ожидается перед выходом из batch, commit после обнаруженной ошибки запрещён. SQL имеет connection/statement/query timeouts; DLT producer использует acks=all, idempotent=true, maxInFlightRequests=1, конечные retries и сетевые таймауты.

Poison вызывает намеренную ошибку обработки, по умолчанию ровно три раза с RETRY_DELAY_MS между попытками, затем отправляется в events.dlt. Некорректный JSON или схема проходят тот же путь постоянной ошибки сообщения. В DLT сохраняются исходные Buffer value/key и исходные headers, дополненные error-message, processing-attempts, source-topic, source-partition, source-offset. Poison не записывается в sink. Временные ошибки PostgreSQL и DLT повторяются с паузой без подтверждения offset и без превращения в poison; новая работа той же партиции блокируется до восстановления. Инфраструктурные повторы прекращаются при остановке.

Семантика at-least-once: сбой после SQL до commit даёт повторную доставку и дедупликацию по event_id. DLT и commit неатомарны, поэтому DLT может содержать повторные записи одного исходного события. Kafka transactions не используются.

### Завершение

SIGTERM/SIGINT устанавливает stopping и вызывает pause назначенных партиций. Новые сообщения не начинаются; текущая обработка может закончить запись и commit при действующем membership. После 30 секунд запрещаются дальнейшие commits; уже отправленная ограниченная таймаутом операция ожидается до cleanup. Поэтому 30 секунд — срок допуска commit, а не жёсткая граница завершения процесса. Затем выполняются consumer.stop, disconnect consumer/DLT producer и pool.end. Compose предоставляет 120 секунд grace period. Повторный сигнал не запускает параллельное завершение. При аварийном прерывании неподтверждённая работа доставляется повторно.

### Проверки

`pnpm test` использует node:test и управляемые mock-объекты: конфигурация, обычная запись, дедупликация, SQL-транзакции, poison/DLT, временная недоступность PostgreSQL, ошибки DLT/commit, ошибки heartbeat до и во время SQL, stale batch, deadline и SIGTERM/SIGINT во время обработки. Отдельно проверяется отсутствие следующей обработки после ошибки commit.

Реализация и команды относятся только к этапу 3. CLI verify и отказные эксперименты пока отсутствуют. Данные автоматически не сбрасываются; `make reset CONFIRM=delete-all-data` удаляет также sink и счётчики.


### Результат проверки этапа 3 (24.11.2026)

- 35 тестов прошли под Node.js 24.21.0 на хосте и Node.js 24.11.1 в контейнере; TypeScript build успешен. Ошибки commit, heartbeat/membership, очередь batch и остановка во время обработки проверены управляемыми mocks.
- `docker compose config --quiet`, сборка общего образа и `openspec validate kafka-event-consumers --strict` прошли.
- Исправленный init.sql проверен на отдельном чистом PostgreSQL с временным хранилищем: обе таблицы созданы автоматически. Временный контейнер остановлен и удалён.
- Три consumers на существующем кластере получили по одной партиции. После ограниченного smoke-запуска в sink 1119 событий с Kafka-метаданными, poison в sink отсутствуют. Прочитано DLT-сообщение с исходным JSON, key и error/source headers.
- Дедупликация дополнительно проверена прямым вызовом saveEvent на реальном PostgreSQL: две повторные записи существующего события дали +2 к постоянному счётчику, количество строк sink не изменилось. Счётчик прочитан после завершения проверочного процесса; это проверка sink, а не эксперимент повторной доставки Kafka.
- При SIGTERM все три consumers завершили текущую запись и commit, затем вышли с кодом 0. Consumers оставлены остановленными; PostgreSQL и три брокера healthy, все volumes сохранены. Существующие продюсеры не запускались и их seq не сбрасывались.

CLI verify не создавался; отказ брокеров, kill -9 и сравнение KEY_MODE не проводились. Эти эксперименты относятся к следующему этапу.
