# Kafka HW2

Стенд: три брокера Kafka 4.1.1 в KRaft, три продюсера, три консьюмера группы `events-processor`, PostgreSQL и топики `events` / `events.dlt`.

## Запуск

Команды выполняются из `hw-2`:

```bash
make up
make brokers
make quorum
make topics
make consumer-lag
```

Из контейнеров Kafka доступна по `kafka-1:9092`, `kafka-2:9092`, `kafka-3:9092`. С хоста доступны `localhost:19092`, `localhost:29092`, `localhost:39092`.

Для остановки стенда без удаления данных: `make down`. Команда `make reset CONFIRM=delete-all-data` удаляет **все** Kafka и PostgreSQL volumes; используйте её только для намеренного сброса между отдельными прогонами.

## Monitoring / Grafana

После `make up` запустите мониторинг:

```bash
make monitoring-up
make monitoring-ps
```

Откройте [Grafana](http://localhost:3000) и дашборд **Kafka HW2 → Kafka HW2 Experiments**. По умолчанию локальный вход `admin` / `admin`; для своего пароля задайте `GRAFANA_ADMIN_PASSWORD` до первого запуска. Prometheus доступен на [localhost:9090](http://localhost:9090). Дашборд и оба источника данных (Prometheus, PostgreSQL) создаются автоматически. Интервал обновления 10 секунд, начальный диапазон 30 минут; диапазон можно менять в правом верхнем углу.

Панели **Kafka Cluster** показывают отдельный статус каждого брокера по JMX `up`, under-replicated и under-min-ISR partitions, offline partitions, события ISR shrink/expand и скорость роста конечных offsets `events`. **Consumers** показывает суммарный lag группы, lag каждой партиции, скорость продвижения committed offsets и скорость роста offsets по партициям. **Application Processing** показывает сохранённые события и дубликаты из PostgreSQL, нарушения порядка только среди строк с известным `processing_order`, число старых строк с неизвестным порядком и количество записей, находящихся в DLT-топике.

Для скриншотов (не более двух на эксперимент):

| Эксперимент | Первый скриншот | Второй скриншот |
| --- | --- | --- |
| Завершение консьюмера | `Consumer lag by partition` и суммарный lag | `Committed offsets advanced / second` и `Persisted events over time` |
| Отключение брокеров | `Kafka Brokers Status` и `Under-replicated partitions` | `Under-min-ISR partitions`, `Offline partitions` и ISR events |
| `KEY_MODE` | `Appended records by partition / second` | `Processed events`, `Ordering violations` и результат `make verify` |

При отказе одного консьюмера остальные могут быстро принять его партиции, поэтому суммарный lag не обязан заметно вырасти. При отказе двух совмещённых broker/controller узлов возможна потеря кворума KRaft; часть Kafka-метрик на это время исчезнет, а `up` JMX endpoints сохранит информацию об их доступности. Эксперименты с отказами мониторинг сам не запускает.

**Ограничения метрик:** скорость committed offsets не равна точной скорости успешной обработки; продвижение включает сообщения, отправленные в DLT, и дубликаты. Панель DLT показывает число записей между oldest и latest offsets, включая невалидные записи; окончательное число валидных DLT-событий даёт `make verify GITHUB_HANDLE=<имя>`. Накопительные счётчики processed и duplicates не подчиняются выбранному диапазону времени; отдельная панель processed в диапазоне использует `processed_at`. Строки с `processing_order=NULL` исключаются из проверки порядка и показаны отдельно. В текущих данных нет `run_id`, поэтому два режима `KEY_MODE` нельзя честно сравнивать в общей накопительной статистике.

Для отдельных прогонов `KEY_MODE=user_id` и `KEY_MODE=none`: остановите продюсеров, сохраните скриншоты и вывод `make verify GITHUB_HANDLE=<имя>`, остановите мониторинг и затем при необходимости сознательно сбросьте Kafka и PostgreSQL командой `make reset CONFIRM=delete-all-data`. Вновь запустите `make up`, `make monitoring-up` и продюсеров с нужным `KEY_MODE`. Сброс удаляет данные Kafka и PostgreSQL, но сохраняет историю Prometheus в отдельном volume; учитывайте границу нового прогона на временных графиках. Не смешивайте накопительные значения двух прогонов.

Остановка только мониторинга:

```bash
make monitoring-down
```

Команда не останавливает Kafka, PostgreSQL, продюсеров или консьюмеров и не удаляет их volumes. Данные Prometheus и Grafana сохраняются в своих volumes при перезапуске контейнеров.
