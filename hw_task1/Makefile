ADDR       ?= :8080
POD_ID     ?= 1
DATACENTER ?= dc1
ERROR_RATE ?= 0.02
LATENCY_MS ?= 5

COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help run up down restart clean ps logs grafana load

help:
	@echo "make run      - запустить приложение локально без Docker"
	@echo "make up       - собрать и поднять стек в фоне, дождаться генерации дашборда"
	@echo "make down     - остановить стек"
	@echo "make restart  - down + up (перегенерирует дашборд)"
	@echo "make clean    - остановить стек и удалить данные Prometheus/Grafana"
	@echo "make ps       - статус контейнеров"
	@echo "make logs     - логи всех сервисов (make logs s=app-1 - одного)"
	@echo "make grafana  - открыть дашборд в браузере"
	@echo "make load     - перезапустить нагрузку k6"

run:
	ADDR=$(ADDR) POD_ID=$(POD_ID) DATACENTER=$(DATACENTER) ERROR_RATE=$(ERROR_RATE) LATENCY_MS=$(LATENCY_MS) \
		go run ./cmd/app

.env:
	cp .env.example .env

up: | .env
	$(COMPOSE) up -d --build

down: | .env
	$(COMPOSE) down

restart: down up

clean: | .env
	$(COMPOSE) down -v

ps: | .env
	$(COMPOSE) ps

logs: | .env
	$(COMPOSE) logs -f --tail=100 $(s)

grafana:
	xdg-open http://localhost:3000/d/hw1-handlers

load: | .env
	$(COMPOSE) restart k6
