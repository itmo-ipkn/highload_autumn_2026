import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic

from common.config import KAFKA_BOOTSTRAP, TOPIC_EVENTS, TOPIC_DLT

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s | %(message)s")
log = logging.getLogger("init-topics")

TOPICS = [
    NewTopic(TOPIC_EVENTS, num_partitions=3, replication_factor=3,
             config={"min.insync.replicas": "2"}),
    NewTopic(TOPIC_DLT, num_partitions=3, replication_factor=3, config={}),
]


def main():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for attempt in range(1, 31):
        fs = admin.create_topics(TOPICS, operation_timeout=10)
        ok = True
        for topic, f in fs.items():
            try:
                f.result(timeout=10)
                log.info("topic %s created", topic)
            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" in str(e) or "already exists" in str(e):
                    log.info("topic %s already exists", topic)
                else:
                    log.warning("topic %s not created yet (%s), retry %d/30",
                                topic, e, attempt)
                    ok = False
        if ok:
            log.info("all topics ready")
            return
        time.sleep(2)
    raise RuntimeError("failed to create topics")


if __name__ == "__main__":
    main()
