SHELL := /bin/bash
COMPOSE ?= docker compose
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP_INTERNAL := kafka-1:19092,kafka-2:19092,kafka-3:19092
PG_USER ?= events
PG_DB ?= events

.PHONY: help env up down reset ps logs init-logs topics groups quorum psql verify test integration lint lint-install exp1 exp2 exp3

help: ## Показать список целей
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | awk -F':.*?## ' '{printf "  %-12s %s\n", $$1, $$2}'

env: ## Создать .env из .env.example (если .env ещё нет)
	@test -f .env || cp .env.example .env

up: ## Поднять стенд
	$(COMPOSE) up -d --wait --remove-orphans
	@$(COMPOSE) ps -a

down: ## Остановить стенд, данные сохраняются
	$(COMPOSE) down --remove-orphans

reset: ## Удалить всё, включая volumes (топики и sink), и поднять стенд заново
	$(COMPOSE) down -v --remove-orphans
	$(MAKE) up

ps: ## Состояние сервисов
	$(COMPOSE) ps -a

logs: ## Логи всех сервисов (follow)
	$(COMPOSE) logs -f --tail=100

init-logs: ## Логи kafka-init
	$(COMPOSE) logs kafka-init

topics: ## Описание топиков: лидеры, ISR
	$(COMPOSE) exec kafka-1 $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server $(BOOTSTRAP_INTERNAL) --describe --exclude-internal

groups: ## Consumer groups и lag
	$(COMPOSE) exec kafka-1 $(KAFKA_BIN)/kafka-consumer-groups.sh --bootstrap-server $(BOOTSTRAP_INTERNAL) --describe --all-groups

quorum: ## Состояние кворума KRaft
	$(COMPOSE) exec kafka-1 $(KAFKA_BIN)/kafka-metadata-quorum.sh --bootstrap-server $(BOOTSTRAP_INTERNAL) describe --status

psql: ## Консоль Postgres
	$(COMPOSE) exec postgres psql -U $(PG_USER) -d $(PG_DB)

verify: ## Проверить события в sink и DLT
	$(COMPOSE) run --rm verify

test: ## Запустить unit-тесты producer и consumer
	cd producer && go test ./...
	cd consumer && go test ./...
	bash scripts/test_lag.sh

integration: ## Проверить доставку и ребаланс на реальных Kafka и Postgres (требуется Docker)
	cd consumer/integration && go test -v -count=1 -timeout=6m ./...

lint: ## Запустить golangci-lint для producer и consumer
	$(MAKE) -C producer lint
	$(MAKE) -C consumer lint

lint-install: ## Установить закреплённую версию golangci-lint
	$(MAKE) -C producer lint-install

exp1: ## Эксперимент: kill -9 консьюмера и восстановление после ребаланса
	bash scripts/exp1_kill_consumer.sh

exp2: ## Эксперимент: остановка двух Kafka-узлов и восстановление кворума
	bash scripts/exp2_brokers.sh

exp3: ## Эксперимент: публикация без ключа и поиск нарушений порядка
	bash scripts/exp3_keyless.sh
