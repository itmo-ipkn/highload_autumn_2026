.DEFAULT_GOAL := help

COMPOSE := docker compose
PRODUCERS := producer-1 producer-2 producer-3
KAFKA := $(COMPOSE) exec -T kafka-1 /opt/kafka/bin
TOPIC ?= events

.PHONY: help up down stop ps logs config kafka-up kafka-logs init-logs brokers quorum topics offsets send read read-history producers-up producers-stop producers-sigint producers-logs install build test image reset

help: ## Показать команды
	@awk 'BEGIN {FS = ":.*## "} /^[a-z-]+:.*## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Собрать и запустить весь стенд
	$(COMPOSE) up -d --build

down: ## Удалить контейнеры и сеть, сохранив Kafka volumes
	$(COMPOSE) down

stop: ## Остановить весь стенд, сохранив контейнеры и данные
	$(COMPOSE) stop

ps: ## Показать состояния всех контейнеров
	$(COMPOSE) ps -a

logs: ## Следить за всеми логами (Ctrl+C завершает просмотр)
	$(COMPOSE) logs -f

config: ## Проверить конфигурацию Compose
	$(COMPOSE) config --quiet

kafka-up: ## Запустить только Kafka и инициализацию топиков
	$(COMPOSE) up -d kafka-init

kafka-logs: ## Следить за логами трёх брокеров
	$(COMPOSE) logs -f kafka-1 kafka-2 kafka-3

init-logs: ## Посмотреть результат создания топиков
	$(COMPOSE) logs kafka-init

brokers: ## Проверить доступность брокеров
	$(KAFKA)/kafka-broker-api-versions.sh --bootstrap-server kafka-1:9092

quorum: ## Показать состояние KRaft quorum
	$(KAFKA)/kafka-metadata-quorum.sh --bootstrap-server kafka-1:9092 describe --status

topics: ## Показать партиции, реплики, ISR и настройки топиков
	$(KAFKA)/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe

offsets: ## Показать конечные offsets партиций (TOPIC=events)
	$(KAFKA)/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 --topic "$(TOPIC)" --time latest

send: ## Отправлять строки из stdin с acks=all; завершение Ctrl+D
	$(KAFKA)/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic "$(TOPIC)" --producer-property acks=all

read: ## Читать новые записи с ключом, партицией и offset (TOPIC=events)
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic "$(TOPIC)" --consumer-property auto.offset.reset=latest --property print.key=true --property print.partition=true --property print.offset=true

read-history: ## Читать с начала; Ctrl+C завершает просмотр (TOPIC=events)
	$(KAFKA)/kafka-console-consumer.sh --bootstrap-server kafka-1:9092 --topic "$(TOPIC)" --from-beginning --property print.key=true --property print.partition=true --property print.offset=true

producers-up: ## Собрать и пересоздать три продюсера; KEY_MODE=user_id или none
	$(COMPOSE) up -d --build --force-recreate $(PRODUCERS)

producers-stop: ## Корректно остановить продюсеры через SIGTERM
	$(COMPOSE) stop $(PRODUCERS)

producers-sigint: ## Отправить продюсерам SIGINT для проверки завершения
	$(COMPOSE) kill -s SIGINT $(PRODUCERS)

producers-logs: ## Следить за событиями и партициями в логах продюсеров
	$(COMPOSE) logs -f $(PRODUCERS)

install: ## Установить зависимости по lockfile
	pnpm install --frozen-lockfile

build: ## Скомпилировать TypeScript
	pnpm build

test: ## Выполнить сборку и автоматизированные тесты
	pnpm test

image: ## Собрать общий Docker-образ продюсеров
	$(COMPOSE) build producer-1

reset: ## УДАЛИТЬ все данные Kafka; требуется CONFIRM=delete-kafka-data
	@test "$(CONFIRM)" = "delete-kafka-data" || { echo 'Удаляются сообщения, offsets и метаданные Kafka. Для подтверждения: make reset CONFIRM=delete-kafka-data'; exit 1; }
	$(COMPOSE) down -v
