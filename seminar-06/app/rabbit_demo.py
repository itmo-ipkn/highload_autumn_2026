"""RabbitMQ routing, acknowledgements and redelivery experiments."""
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter

import pika
from PIL import UnidentifiedImageError

from .common import DATA, uid, say, pause, wait_for, process_image, result_count, result_jobs, export_result


def connect():
    params = pika.URLParameters(os.environ['RABBIT_URL'])
    params.heartbeat = 10
    params.blocked_connection_timeout = 15
    params.socket_timeout = 5
    return pika.BlockingConnection(params)


def setup(label, dead_letter=False):
    name = uid(label)
    c = connect()
    ch = c.channel()
    ch.exchange_declare(name, 'topic', durable=True)
    args = {}
    if dead_letter:
        ch.exchange_declare(name + '.dlx', 'direct', durable=True)
        ch.queue_declare(name + '.dead', durable=True)
        ch.queue_bind(name + '.dead', name + '.dlx', 'failed')
        args = {'x-dead-letter-exchange': name + '.dlx', 'x-dead-letter-routing-key': 'failed'}
    ch.queue_declare(name + '.work', durable=True, arguments=args)
    ch.queue_bind(name + '.work', name, 'image.uploaded')
    ch.confirm_delivery()
    return name, c, ch


def publish(ch, exchange, event, key='image.uploaded'):
    ch.basic_publish(exchange, key, json.dumps(event).encode(), mandatory=True,
                     properties=pika.BasicProperties(content_type='application/json',
                         delivery_mode=2, message_id=event['event_id']))


def get(ch, queue):
    def attempt():
        method, props, body = ch.basic_get(queue, auto_ack=False)
        return (method, props, body) if method else None
    return wait_for(attempt, timeout=15, label='Rabbit delivery')


def queue_ready(ch, queue):
    result = ch.queue_declare(queue, passive=True)
    say('rabbit.queue', queue=queue.rsplit('.', 1)[-1], ready=result.method.message_count)


def routing():
    name, conn, ch = setup('routing')
    ch.queue_declare(name + '.audit', durable=True)
    ch.queue_bind(name + '.audit', name, 'image.#')
    received, errors = [], []
    stop = threading.Event()
    gate = threading.Event()
    ready = [threading.Event(), threading.Event()]

    def worker(index):
        c = None
        try:
            c = connect()
            channel = c.channel()
            channel.basic_qos(prefetch_count=1)
            def consume(channel, method, props, body):
                event = json.loads(body)
                worker_name = f'worker-{index+1}'
                say('rabbit.delivery', queue='work', worker=worker_name,
                    event_id=event['event_id'], redelivered=method.redelivered)
                queue_ready(channel, name + '.work')
                pause(c)
                channel.basic_ack(method.delivery_tag)
                say('rabbit.ack', queue='work', worker=worker_name, event_id=event['event_id'])
                received.append((worker_name, event['event_id']))
            channel.basic_consume(name + '.work', consume, auto_ack=False)
            ready[index].set()
            if not gate.wait(10):
                raise TimeoutError('routing workers did not start together')
            while not stop.is_set():
                c.process_data_events(time_limit=0.1)
        except Exception as exc:
            errors.append(repr(exc))
        finally:
            if c and c.is_open:
                c.close()

    threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(2)]
    try:
        for i in range(6):
            publish(ch, name, {'event_id': f'event-{i}', 'job_id': f'image-{i}'})
        say('rabbit.confirm', events=6, routing_key='image.uploaded')
        queue_ready(ch, name + '.work')
        queue_ready(ch, name + '.audit')
        pause(conn)
        for t in threads:
            t.start()
        assert all(e.wait(10) for e in ready), errors
        gate.set()
        wait_for(lambda: len(received) == 6, label='6 work deliveries')
        queue_ready(ch, name + '.work')
        audit = []
        for _ in range(6):
            m, _, body = get(ch, name + '.audit')
            event_id = json.loads(body)['event_id']
            audit.append(event_id)
            say('rabbit.delivery', queue='audit', event_id=event_id, redelivered=m.redelivered)
            pause(conn, factor=0.5)
            ch.basic_ack(m.delivery_tag)
        queue_ready(ch, name + '.audit')
        assert len(set(e for _, e in received)) == 6
        assert len(set(w for w, _ in received)) == 2
        assert sorted(audit) == sorted(e for _, e in received)
        try:
            publish(ch, name, {'event_id': 'unroutable', 'job_id': 'none'}, key='video.uploaded')
        except pika.exceptions.UnroutableError:
            say('rabbit.return', event_id='unroutable', routing_key='video.uploaded', returned=True)
        else:
            raise AssertionError('Unroutable publication unexpectedly accepted')
        say('check.passed', name='routing', work=len(received), audit=len(audit),
            workers=dict(Counter(w for w, _ in received)))
    finally:
        stop.set()
        for t in threads:
            t.join(5)
        conn.close()
    assert not errors, errors


def crash_worker(queue, db_name, dedupe):
    """A separate process dies after its DB COMMIT, before Rabbit basic_ack."""
    conn = connect()
    ch = conn.channel()
    method, _, body = get(ch, queue)
    event = json.loads(body)
    say('worker.started', pid=os.getpid())
    say('rabbit.delivery', event_id=event['event_id'], redelivered=method.redelivered)
    pause(conn)
    applied = process_image(db_name, event, dedupe)
    say('db.committed', event_id=event['event_id'], applied=applied, completed_counter=result_count(db_name))
    pause(conn)
    os._exit(23)


def redelivery(dedupe=False):
    name, conn, ch = setup('idempotency' if dedupe else 'redelivery')
    db_name = name
    event = {'event_id': 'upload-17', 'job_id': 'image-17'}
    try:
        publish(ch, name, event)
        say('rabbit.confirm', event_id=event['event_id'])
        queue_ready(ch, name + '.work')
        pause(conn)
        child = subprocess.Popen([sys.executable, '-u', '-m', 'app.cli', '_rabbit-worker',
                                  name + '.work', db_name, str(int(dedupe))])
        try:
            code = child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            child.kill(); child.wait()
            raise
        say('worker.exited', pid=child.pid, exit_code=code)
        assert code == 23, code
        assert result_count(db_name) == 1
        say('db.state', completed_counter=result_count(db_name), unique_jobs=result_jobs(db_name))
        pause(conn)
        method, _, body = get(ch, name + '.work')
        assert method.redelivered is True
        say('rabbit.delivery', event_id=json.loads(body)['event_id'], redelivered=method.redelivered)
        pause(conn)
        applied = process_image(db_name, json.loads(body), dedupe)
        say('db.committed' if applied else 'db.duplicate', event_id=event['event_id'],
            applied=applied, completed_counter=result_count(db_name))
        pause(conn)
        ch.basic_ack(method.delivery_tag)
        say('rabbit.ack', event_id=event['event_id'])
        count = result_count(db_name)
        expected = 1 if dedupe else 2
        assert count == expected, (count, expected)
        preview = export_result(db_name, 'image-17')
        assert preview
        jobs = result_jobs(db_name)
        assert jobs == 1, jobs
        say('check.passed', name='idempotency' if dedupe else 'redelivery',
            redelivered=method.redelivered, applied=applied,
            completed_counter=count, unique_jobs=jobs, preview=preview)
    finally:
        conn.close()


def dlq():
    name, conn, ch = setup('dlq', dead_letter=True)
    try:
        event = {'event_id': 'bad-image-1', 'job_id': 'broken', 'valid': False}
        publish(ch, name, event)
        say('rabbit.confirm', event_id=event['event_id'])
        queue_ready(ch, name + '.work')
        pause(conn)
        method, _, body = get(ch, name + '.work')
        say('rabbit.delivery', queue='work', event_id=event['event_id'], redelivered=method.redelivered)
        try:
            process_image(name, json.loads(body))
        except UnidentifiedImageError:
            say('worker.error', event_id=event['event_id'], error='UnidentifiedImageError')
            pause(conn)
            ch.basic_nack(method.delivery_tag, requeue=False)
            say('rabbit.nack', event_id=event['event_id'], requeue=False)
        else:
            raise AssertionError('Invalid image unexpectedly processed')
        dead, props, body = get(ch, name + '.dead')
        reason = props.headers['x-death'][0]['reason']
        assert reason == 'rejected'
        assert json.loads(body)['event_id'] == event['event_id']
        assert result_count(name) == 0
        say('rabbit.dead_letter', event_id=json.loads(body)['event_id'], reason=reason,
            deaths=props.headers['x-death'][0]['count'])
        pause(conn)
        ch.basic_ack(dead.delivery_tag)
        say('check.passed', name='dlq', completed_counter=result_count(name), dead_letter_queue=name + '.dead')
    finally:
        conn.close()


def prefetch():
    """Measure unacknowledged push deliveries for different prefetch values."""
    observations = {}
    for limit in (1, 4):
        name, conn, ch = setup('prefetch')
        try:
            for i in range(6):
                publish(ch, name, {'event_id': str(i), 'job_id': str(i)})
            deliveries = []
            ch.basic_qos(prefetch_count=limit)
            tag = ch.basic_consume(name + '.work', lambda _, m, p, b: deliveries.append(m.delivery_tag), auto_ack=False)
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                conn.process_data_events(time_limit=0.1)
            assert len(deliveries) == limit, deliveries
            observations[limit] = len(deliveries)
            say('rabbit.prefetch', prefetch=limit, unacked=len(deliveries))
            ch.basic_ack(deliveries[-1], multiple=True)
            ch.basic_cancel(tag)
        finally:
            conn.close()
    say('check.passed', name='prefetch', observations=observations)
