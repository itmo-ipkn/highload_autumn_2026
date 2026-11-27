.PONY: cp_env
cp_env:
	cp .env.example .env
	cp pgbouncer.env.example pgbouncer.env
	cp postgres.env.example postgres.env

.PONY: build
build:
	docker build . -t highload-system-kafka

.PONY: up_kafka
up_kafka:
	docker compose up -d kafka-0 kafka-1 kafka-2 kafbat-ui kafka_topic_creator

.PONY: down_kafka
down_kafka:
	docker compose down kafka-0 kafka-1 kafka-2 kafbat-ui kafka_topic_creator

.PONY: up_producers
up_producers:
	docker compose up -d producer-0 producer-1 producer-2

.PONY: down_producers
down_producers:
	docker compose down producer-0 producer-1 producer-2

.PONY: up_db
up_db:
	docker compose up -d postgres pgbouncer

.PONY: down_db
down_db:
	docker compose down postgres pgbouncer

.PONY: up_consumers
up_consumers:
	docker compose up -d consumer-0 consumer-1 consumer-2

.PONY: down_consumers
down_consumers:
	docker compose down consumer-0 consumer-1 consumer-2

.PONY: rm_volumes
rm_volumes:
	docker volume rm homework_2_kafka_0_data homework_2_kafka_1_data homework_2_kafka_2_data homework_2_postgres_data
