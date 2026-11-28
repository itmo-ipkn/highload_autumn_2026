up:
	docker compose up --build -d

down:
	docker compose down

down-clean:
	docker compose down -v

verify:
	docker compose run --rm verify

key-none:
	KEY_MODE=none docker compose up -d producer-1 producer-2 producer-3

key-user:
	KEY_MODE=user_id docker compose up -d producer-1 producer-2 producer-3

logs:
	docker compose logs -f --tail=100
