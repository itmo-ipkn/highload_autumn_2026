.PHONY: up down status logs dashboard check test

up:
	docker compose up -d --build

down:
	docker compose down

status:
	docker compose ps -a

logs:
	docker compose logs --tail=20 load-generator dashboard-generator

dashboard:
	docker compose run --rm --no-deps dashboard-generator

check:
	docker compose run --rm --no-deps dashboard-generator python scripts/check.py

test:
	docker compose run --rm --no-deps -v "$(CURDIR)/tests:/app/tests:ro" dashboard-generator python -m unittest discover -s tests -v
