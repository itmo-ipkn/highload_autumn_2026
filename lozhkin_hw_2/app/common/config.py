import os

KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-1:9092,kafka-2:9092,kafka-3:9092",
)
TOPIC_EVENTS = "events"
TOPIC_DLT = "events.dlt"

PG_DSN = os.getenv("PG_DSN", "host=postgres port=5432 dbname=events user=app password=app")
GITHUB_LOGIN = os.getenv("GITHUB_LOGIN", "LozhkinAndrey")

POD_ID = os.getenv("POD_ID", "0")
KEY_MODE = os.getenv("KEY_MODE", "user_id")          # user_id | none
PROCESSING_DELAY_MS = int(os.getenv("PROCESSING_DELAY_MS", "50"))
POISON_MAX_ATTEMPTS = int(os.getenv("POISON_MAX_ATTEMPTS", "3"))
