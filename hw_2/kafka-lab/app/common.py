import logging
import os
import time
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
LOG = logging.getLogger('kafka-lab')
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP', 'kafka-1:9092,kafka-2:9092,kafka-3:9092')
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://lab:lab@postgres:5432/lab')


def producer_config():
    return {'bootstrap.servers': BOOTSTRAP, 'acks': 'all',
            'enable.idempotence': True, 'max.in.flight.requests.per.connection': 5,
            'retries': 2147483647, 'delivery.timeout.ms': 30000,
            'request.timeout.ms': 10000}


def deliver_confirmed(producer: Producer, topic, value, key=None, headers=None, timeout=45):
    result = []
    def callback(err, msg):
        result.append((err, msg))
        if err:
            LOG.error('delivery failed topic=%s error=%s', topic, err)
    deadline = time.monotonic() + timeout
    while True:
        try:
            producer.produce(topic, value=value, key=key, headers=headers, on_delivery=callback)
            break
        except BufferError:
            if time.monotonic() > deadline:
                raise TimeoutError('Kafka local buffer full')
            producer.poll(0.2)
    while not result:
        if time.monotonic() > deadline:
            raise TimeoutError(f'Kafka delivery callback timeout for {topic}')
        producer.poll(0.2)
    err, msg = result[0]
    if err is not None:
        raise RuntimeError(f'Kafka delivery failed: {err}')
    return msg
