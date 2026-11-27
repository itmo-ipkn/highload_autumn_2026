# Системы работы с большими данными

[Слайды семинара (PDF)](slides/dist/S8_BigData.pdf)

Откройте терминал в папке с `orders.py`. Во всех трёх опытах одни и те же 12 000 заказов. Магазину 0 принадлежат 9 600 из них.

## Частичные суммы

Достаточно Python 3.10 или новее. Каждый запуск создаёт временный файл обмена, считывает его и сверяет суммы всех 21 магазина.

```sh
python3 orders.py --chunks 4
python3 orders.py --chunks 8
```

Сколько записей попадёт в файл, если каждый кусок отдаст по одной сумме на магазин? Сравните `records`, размер файла в `bytes` и выручку в `revenue`.

## Соединение в Spark

Нужны Python 3.10–3.12, JDK 17 и около 2 ГБ на диске для установки. [Скачать JDK 17](https://adoptium.net/temurin/releases/?version=17).

На macOS выберите установленный JDK в текущем терминале.

```sh
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"
```

На Linux укажите в `JAVA_HOME` каталог своего JDK 17. Затем выполните в папке опыта.

```sh
python3 -m venv .venv
.venv/bin/python -m pip --disable-pip-version-check install -r requirements.txt
.venv/bin/python spark_lab.py --parts 4
.venv/bin/python spark_lab.py --parts 8
```

На Windows эти команды можно выполнить в WSL2 с Python и JDK внутри Linux. Для третьего опыта понадобится интеграция Docker Desktop с этим дистрибутивом. В примере Spark выполняет до двух задач одновременно.

В строке `shuffle` перечислены размеры партиций по порядку. Найдите самую большую до и после смены настройки. В строках `join` проверьте, сколько перераспределений по хэшу и рассылок понадобилось самому соединению. Итоговые строки обоих планов должны совпасть.

```sh
.venv/bin/python spark_lab.py --parts 4 --plan | less
```

В `less` введите `/== Physical Plan ==` и нажмите Enter. Клавиша `n` перейдёт ко второму дереву, `q` вернёт в терминал. Оба дерева читаются от источников к верхней строке. Для диагностики ключ `--spark-logs` возвращает служебные сообщения запуска Java и Spark.

```sh
.venv/bin/python spark_lab.py --parts 4 --duplicate
```

Появилась вторая строка магазина 0 в справочнике. Предскажите новое число строк и сумму, затем сверьтесь с результатом.

## Сортировка в YTsaurus

Нужен работающий Docker с Compose v2 или новее. На Mac с Apple Silicon включите поддержку контейнеров amd64 в Docker Desktop. Для первого запуска понадобится несколько гигабайт на диске. [Установка Docker](https://docs.docker.com/get-started/get-docker/).

```sh
docker compose up -d --wait
docker compose exec -T yt python3 /lesson/yt_lab.py --parts 1
docker compose exec -T yt python3 /lesson/yt_lab.py --parts 4
docker compose down -v
```

Если Docker отвечает `denied` при загрузке публичного образа GHCR, выполните `python3 fetch_yt.py` и повторите `docker compose up -d --wait`. Скрипт загрузит закреплённую сборку из того же реестра и проверит контрольные суммы. Если контейнер не стал готов, откройте `docker compose logs --tail 40 yt`.

Сравните `job_types` и ключ `sorted_by`. Флаг `equal` проверяет весь результат по эталону Python. Идентификаторы и длительность меняются между запусками. Для измерения `wait_ms` вход уже загружен, для `sort_ms` список уже находится в памяти. В конце каждого опыта его временные таблицы удаляются.
