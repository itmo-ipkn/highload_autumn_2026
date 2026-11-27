SHELL := /bin/bash
COMPOSE ?= docker compose
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP_INTERNAL := kafka-1:19092,kafka-2:19092,kafka-3:19092
PG_USER ?= events
PG_DB ?= events

.PHONY: help env up down reset ps logs init-logs topics groups quorum psql verify

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
