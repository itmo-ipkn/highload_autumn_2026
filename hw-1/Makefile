.DEFAULT_GOAL := help
.PHONY: help up stop down rebuild ps logs

help:
	@echo "make up       — запустить контейнеры в фоне и дождаться готовности"
	@echo "make stop     — остановить контейнеры"
	@echo "make down     — остановить и удалить контейнеры, сохранив данные Prometheus и Grafana"
	@echo "make rebuild  — пересобрать API и пересоздать контейнеры"
	@echo "make ps       — показать состояние контейнеров"
	@echo "make logs     — смотреть логи (Ctrl+C завершает просмотр)"

.env:
	cp .env.example .env

up: | .env
	docker compose up -d --wait

stop: | .env
	docker compose stop

down: | .env
	docker compose down

rebuild: | .env
	docker compose up -d --build --force-recreate --wait

ps: | .env
	docker compose ps

logs: | .env
	docker compose logs -f --tail=100

build:
	pnpm build

generate-dashboards:
	pnpm build
	node --env-file=.env dist/src/dashboard-generator/index.js


start-load-generator: | .env
	pnpm build
	node dist/src/load-generator/index.js