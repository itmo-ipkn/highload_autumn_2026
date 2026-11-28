"""Kafka consumer groups, offsets and broker failover experiments."""
import json
import os
import subprocess
import sys
import threading
import time
from collections import Counter

from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigEntry, AlterConfigOpType

from .common import DATA, uid, say, pause, wait_for, process_image, result_count

BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP']
STATE = DATA / 'cluster-demo.json'


def admin():
    return AdminClient({'bootstrap.servers': BOOTSTRAP, 'socket.timeout.ms': 10000, 'log_level': 0})


def partition_info(topic):
    metadata = admin().list_topics(topic, timeout=8)
    item = metadata.topics.get(topic)
    if not item or item.error:
        return None
    return {p: {'leader': v.leader, 'replicas': list(v.replicas), 'isr': list(v.isrs)}
            for p, v in item.partitions.items()}


def new_topic(label, partitions=2, min_isr=2):
    topic = uid(label)
    spec = NewTopic(topic, num_partitions=partitions,
                    replica_assignment=[[1, 2, 3] for _ in range(partitions)],
                    config={'min.insync.replicas': str(min_isr), 'retention.ms': '86400000',
                            'unclean.leader.election.enable': 'false'})
    client = admin()
    client.create_topics([spec])[topic].result(timeout=30)
    wait_for(lambda: (info := partition_info(topic)) and len(info) == partitions
             and all(len(v['isr']) == 3 and v['leader'] > 0 for v in info.values()),
             label='all topic replicas in ISR')
    return topic


def producer(**overrides):
    cfg = {'bootstrap.servers': BOOTSTRAP, 'acks': 'all', 'enable.idempotence': True,
           'delivery.timeout.ms': 15000, 'request.timeout.ms': 3000,
           'partitioner': 'murmur2_random', 'log_level': 0}
    cfg.update(overrides)
    return Producer(cfg)


def send(topic, records, partition=None, **overrides):
    p = producer(**overrides)
    delivered, errors = [], []
    def callback(err, msg):
        if err:
            errors.append(err)
        else:
            delivered.append((msg.partition(), msg.offset()))
    for record in records:
        args = {'key': record.get('album', record['job_id']), 'value': json.dumps(record), 'on_delivery': callback}
        if partition is not None:
            args['partition'] = partition
        p.produce(topic, **args)
    pending = p.flush(20)
    assert pending == 0, f'{pending} messages still pending'
    return delivered, errors


def consumer(group, **overrides):
    cfg = {'bootstrap.servers': BOOTSTRAP, 'group.id': group,
           'auto.offset.reset': 'earliest', 'enable.auto.commit': False,
           'enable.auto.offset.store': False, 'session.timeout.ms': 6000,
           'heartbeat.interval.ms': 2000, 'partition.assignment.strategy': 'range',
           'group.protocol': 'classic', 'log_level': 0}
    cfg.update(overrides)
    return Consumer(cfg)


def poll_record(c, timeout=15):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        msg = c.poll(0.2)
        if msg is None:
            continue
        if msg.error():
            raise RuntimeError(msg.error())
        return msg
    return None


def read_n(c, count):
    found = []
    for _ in range(count):
        msg = poll_record(c)
        assert msg is not None, f'only {len(found)} of {count} records arrived'
        found.append(msg)
    return found


def groups():
    topic = new_topic('groups')
    # Route events with the same album key to the same partition.
    records = [{'event_id': f'done-{i}', 'job_id': f'image-{i}', 'album': f'album-{i % 6}'} for i in range(12)]
    delivered, errors = send(topic, records)
    assert not errors, errors
    assert {p for p, _ in delivered} == {0, 1}, delivered
    say('kafka.published', topic=topic, records=len(delivered), partitions=sorted({p for p, _ in delivered}))
    group = uid('analytics')
    assigned, messages, failures = {}, [], []
    stop = threading.Event()
    gate = threading.Event()
    threads = []

    def worker(index):
        c = consumer(group)
        name = f'C{index + 1}'
        try:
            held = []
            def on_assign(client, partitions):
                assigned[name] = [p.partition for p in partitions]
                client.assign(partitions)
            def on_revoke(client, partitions):
                assigned.pop(name, None)
                held.clear()
            c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
            deadline = time.monotonic() + 120
            while not stop.is_set() and time.monotonic() < deadline:
                msg = c.poll(0.1)
                if msg:
                    if msg.error():
                        raise RuntimeError(msg.error())
                    held.append(msg)
                if gate.is_set():
                    for msg in held:
                        event_id = json.loads(msg.value())['event_id']
                        say('kafka.record', group='analytics', consumer=name,
                            partition=msg.partition(), offset=msg.offset(), event_id=event_id)
                        pause()
                        c.commit(message=msg, asynchronous=False)
                        messages.append((name, msg.partition(), msg.offset(), event_id))
                    held.clear()
        except Exception as exc:
            failures.append(repr(exc))
        finally:
            c.close()

    try:
        for i in range(3):
            t = threading.Thread(target=worker, args=(i,), daemon=True)
            threads.append(t)
            t.start()
        wait_for(lambda: len(assigned) == 3 and sorted(p for ps in assigned.values() for p in ps) == [0, 1],
                 timeout=30, label='3 group members assigned 2 partitions')
        snapshot = dict(assigned)
        assert sum(not ps for ps in snapshot.values()) == 1, snapshot
        say('kafka.assignment', group='analytics', topic=topic, assignments=snapshot)
        pause()
        gate.set()
        wait_for(lambda: len(messages) >= 12 or failures, timeout=90, label='group A consumes')
        assert not failures, failures
        assert len(messages) == 12 and len({m[3] for m in messages}) == 12, messages
        say('kafka.group_result', group='analytics', by_consumer=dict(Counter(m[0] for m in messages)), records=len(messages))
    finally:
        stop.set()
        for t in threads:
            t.join(10)
    assert not failures, failures
    audit = consumer(uid('audit'))
    try:
        audit.subscribe([topic])
        copy = read_n(audit, 12)
        for msg in copy:
            audit.commit(message=msg, asynchronous=False)
        assert {json.loads(m.value())['event_id'] for m in copy} == {x['event_id'] for x in records}
        say('kafka.group_result', group='audit', records=len(copy),
            event_ids=[json.loads(m.value())['event_id'] for m in copy])
    finally:
        audit.close()
    say('check.passed', name='groups', partitions=2, analytics_members=3, analytics_events=len(messages), audit_events=len(copy))


def replay():
    topic = new_topic('replay', partitions=1)
    records = [{'event_id': f'event-{i}', 'job_id': f'image-{i}'} for i in range(3)]
    _, errors = send(topic, records, partition=0)
    assert not errors, errors
    c = consumer(uid('replay-reader'))
    try:
        c.assign([TopicPartition(topic, 0, 0)])
        first = read_n(c, 3)
        c.commit(message=first[-1], asynchronous=False)
        committed_before = c.committed([TopicPartition(topic, 0)])[0].offset
        say('kafka.read', pass_number=1, offsets=[m.offset() for m in first], committed_offset=committed_before)
        pause()
        c.seek(TopicPartition(topic, 0, 0))
        say('kafka.seek', partition=0, offset=0)
        pause()
        second = read_n(c, 3)
        committed_after = c.committed([TopicPartition(topic, 0)])[0].offset
        assert [m.value() for m in first] == [m.value() for m in second]
        assert committed_before == committed_after == 3
        say('kafka.read', pass_number=2, offsets=[m.offset() for m in second], committed_offset=committed_after)
        say('check.passed', name='replay', records_still_available=len(second), topic=topic)
    finally:
        c.close()


def crash_worker(topic, group, db_name, when):
    c = consumer(group)
    c.subscribe([topic])
    msg = poll_record(c)
    assert msg is not None
    say('worker.started', pid=os.getpid(), commit_timing=when)
    say('kafka.record', partition=msg.partition(), offset=msg.offset(), event_id=json.loads(msg.value())['event_id'])
    pause()
    if when == 'before':
        c.commit(message=msg, asynchronous=False)
        say('kafka.committed', record_offset=msg.offset(),
            committed_offset=c.committed([TopicPartition(topic, 0)])[0].offset)
    else:
        process_image(db_name, json.loads(msg.value()))
        say('db.committed', event_id=json.loads(msg.value())['event_id'], completed_counter=result_count(db_name))
    pause()
    os._exit(24)


def commit():
    results = {}
    for when in ('before', 'after'):
        topic = new_topic('commit-' + when, partitions=1)
        group, db_name = uid('commit-group'), uid('commit-result')
        _, errors = send(topic, [{'event_id': 'event-17', 'job_id': 'image-17'}], partition=0)
        assert not errors, errors
        say('kafka.commit_case', commit_timing=when)
        child = subprocess.Popen([sys.executable, '-u', '-m', 'app.cli', '_kafka-worker', topic, group, db_name, when])
        try:
            code = child.wait(timeout=40)
        except subprocess.TimeoutExpired:
            child.kill(); child.wait()
            raise
        say('worker.exited', pid=child.pid, exit_code=code)
        assert code == 24, code
        c = consumer(group)
        try:
            # Rejoin the same group after the crashed member expires. This also
            # establishes valid membership for the next offset commit.
            stored = c.committed([TopicPartition(topic, 0)])[0].offset
            assert stored == 1 if when == 'before' else stored < 0, stored
            say('kafka.resume', commit_timing=when, stored_offset=stored, completed_counter=result_count(db_name))
            c.subscribe([topic])
            msg = poll_record(c, timeout=20)
            assert c.assignment(), 'restarted consumer did not join its group'
            if msg:
                say('kafka.record', partition=msg.partition(), offset=msg.offset(), event_id=json.loads(msg.value())['event_id'])
                pause()
                process_image(db_name, json.loads(msg.value()))
                say('db.committed', event_id=json.loads(msg.value())['event_id'], completed_counter=result_count(db_name))
                c.commit(message=msg, asynchronous=False)
            count = result_count(db_name)
            assert (count, msg is not None) == ((0, False) if when == 'before' else (2, True))
            results[when] = {'effects': count, 'replayed': msg is not None,
                             'committed_offset': c.committed([TopicPartition(topic, 0)])[0].offset}
            say('kafka.resume_result', commit_timing=when, **results[when])
        finally:
            c.close()
    say('check.passed', name='commit', early_commit_effects=results['before']['effects'],
        late_commit_effects=results['after']['effects'])


def baseline():
    topic = new_topic('replication', partitions=1)
    delivered, errors = send(topic, [{'event_id': 'before-failure', 'job_id': 'image-1'}], partition=0)
    assert not errors, errors
    info = partition_info(topic)[0]
    STATE.write_text(json.dumps({'topic': topic, 'leader': info['leader']}))
    say('kafka.partition', topic=topic, partition=0, **info)
    pause()
    say('check.passed', name='baseline', acks='all', min_insync_replicas=2, confirmed=delivered)


def state():
    return json.loads(STATE.read_text())


def leader():
    print(state()['leader'])


def after_stop():
    data = state()
    topic, previous = data['topic'], data['leader']
    last = None
    def ready():
        nonlocal last
        info = partition_info(topic)[0]
        if info != last:
            say('kafka.partition', previous_leader=previous, **info)
            last = info
        return info if info['leader'] != previous and info['leader'] > 0 and len(info['isr']) == 2 else None
    info = wait_for(ready, timeout=45, label='new partition leader with ISR=2')
    pause()
    delivered, errors = send(topic, [{'event_id': 'after-failure', 'job_id': 'image-2'}], partition=0)
    assert not errors, errors
    c = consumer(uid('verify-failover'))
    try:
        c.assign([TopicPartition(topic, 0, 0)])
        read = read_n(c, 2)
        events = [json.loads(m.value())['event_id'] for m in read]
        assert events == ['before-failure', 'after-failure'], events
    finally:
        c.close()
    say('check.passed', name='failover', confirmed=delivered, read_events=events)


def set_min_isr(topic, value):
    client = admin()
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic,
        incremental_configs=[ConfigEntry('min.insync.replicas', str(value), incremental_operation=AlterConfigOpType.SET)])
    for f in client.incremental_alter_configs([resource]).values():
        f.result(timeout=15)
    def propagated():
        resource = ConfigResource(ConfigResource.Type.TOPIC, topic)
        cfg = client.describe_configs([resource])[resource].result(timeout=10)
        return cfg['min.insync.replicas'].value == str(value)
    wait_for(propagated, label='topic config propagated')


def min_isr():
    topic = state()['topic']
    assert len(partition_info(topic)[0]['isr']) == 2
    set_min_isr(topic, 3)
    say('kafka.config', min_insync_replicas=3, isr=partition_info(topic)[0]['isr'])
    pause()
    try:
        delivered, errors = send(topic, [{'event_id': 'must-reject', 'job_id': 'image-3'}], partition=0,
                                 **{'enable.idempotence': False, 'retries': 0, 'delivery.timeout.ms': 8000})
        assert not delivered and errors, (delivered, errors)
        allowed = {KafkaError.NOT_ENOUGH_REPLICAS, KafkaError.NOT_ENOUGH_REPLICAS_AFTER_APPEND}
        assert all(e.code() in allowed for e in errors), [str(e) for e in errors]
        say('kafka.rejected', isr=partition_info(topic)[0]['isr'],
            min_insync_replicas=3, errors=[e.name() for e in errors],
            acks='all', idempotence=False, retries=0)
        pause()
    finally:
        set_min_isr(topic, 2)
    confirmed, errors = send(topic, [{'event_id': 'accepted-with-min2', 'job_id': 'image-4'}], partition=0)
    assert not errors, errors
    say('check.passed', name='min-isr', min_insync_replicas_restored=2, confirmed=confirmed)


def recovered():
    if STATE.exists():
        topic = state()['topic']
        def ready():
            info = partition_info(topic)[0]
            return info if len(info['isr']) == 3 else None
        info = wait_for(ready, timeout=90, label='all replicas recovered')
        say('kafka.recovered', topic=topic, **info)


def check():
    metadata = admin().list_topics(timeout=10)
    assert set(metadata.brokers) == {1, 2, 3}, metadata.brokers
    say('kafka.ready', brokers=sorted(metadata.brokers))
