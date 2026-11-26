#!/usr/bin/env python3
"""Опыты с локальным HTTP-сервисом. Только стандартная библиотека."""
import argparse
import http.client
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def emit(event, **fields):
    print(event, *(f'{key}={value}' for key, value in fields.items()), flush=True)


class Service(ThreadingHTTPServer):
    def __init__(self, work_limit=None):
        super().__init__(('127.0.0.1', 0), Handler)
        self.calls = Counter()
        self.changed = threading.Condition()
        self.release_slow = threading.Event()
        self.recovered = threading.Event()
        self.work_slots = threading.BoundedSemaphore(work_limit) if work_limit is not None else None
        self.decisions = Counter()

    def limited_work(self):
        admitted = self.work_slots.acquire(blocking=False)
        with self.changed:
            self.decisions['admitted' if admitted else 'rejected'] += 1
            self.changed.notify_all()
        if not admitted:
            return 503
        try:
            return 200 if self.release_slow.wait(timeout=6) else 503
        finally:
            self.work_slots.release()

    def count(self, path):
        with self.changed:
            return self.calls[path]

    def wait_for_slow(self, count):
        with self.changed:
            if not self.changed.wait_for(lambda: self.calls['/slow'] >= count, timeout=5):
                raise RuntimeError('Медленный запрос не дошёл до сервера')

    def get(self, path):
        conn = http.client.HTTPConnection(*self.server_address, timeout=8)
        try:
            conn.request('GET', path)
            response = conn.getresponse()
            response.read()
            return response.status
        finally:
            conn.close()


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        s = self.server
        with s.changed:
            s.calls[self.path] += 1
            s.changed.notify_all()
        if self.path == '/work' and s.work_slots is not None:
            status = s.limited_work()
        elif self.path == '/slow':
            status = 200 if s.release_slow.wait(timeout=6) else 503
        elif self.path == '/recommendations':
            status = 200 if s.recovered.is_set() else 503
        elif self.path == '/price':
            status = 200
        else:
            status = 404
        self.send_response(status)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def log_message(self, *args):
        pass


@contextmanager
def local_service(work_limit=None):
    with Service(work_limit) as service:
        thread = threading.Thread(target=service.serve_forever, kwargs={'poll_interval': 0.05})
        thread.start()
        try:
            yield service
        finally:
            service.release_slow.set()
            service.shutdown()
            thread.join()


def admission(limit, pause):
    batch = 6
    with local_service(work_limit=limit) as service, ThreadPoolExecutor(max_workers=batch) as workers:
        try:
            pending = [workers.submit(service.get, '/work') for _ in range(batch)]
            with service.changed:
                if not service.changed.wait_for(lambda: sum(service.decisions.values()) == batch, timeout=5):
                    raise RuntimeError('Не все запросы дошли до проверки допуска')
                emit('gate', received=service.calls['/work'], in_work=service.decisions['admitted'],
                     rejected=service.decisions['rejected'])
            time.sleep(pause)
        finally:
            service.release_slow.set()
        statuses = Counter(request.result() for request in pending)
        emit('batch', http_200=statuses[200], http_503=statuses[503])
        status = service.get('/work')
        emit('next_request', http=status)
        if statuses[200] != limit or statuses[503] != batch - limit or status != 200:
            raise RuntimeError(f'Неожиданные ответы для лимита {limit}, {statuses}, следующий {status}')


def retries(budget, pause):
    paths = ['/recommendations', '/price', '/recommendations', '/recommendations']
    for mode in ('two_layers', 'one_layer', 'budget'):
        tokens = budget
        successes = 0
        with local_service() as service:
            for request, path in enumerate(paths, 1):
                codes = []
                # Внешний слой повторяет весь вызов внутреннего.
                for outer in range(3 if mode == 'two_layers' else 1):
                    for inner in range(3):
                        if inner and mode == 'budget':
                            if tokens == 0:
                                break
                            tokens -= 1
                        codes.append(service.get(path))
                        if codes[-1] != 503:
                            break
                    if codes[-1] != 503:
                        break
                successes += codes[-1] == 200
                emit('request', mode=mode, id=request, path=path,
                     responses=','.join(map(str, codes)))
                time.sleep(pause)
            seen = service.count('/price') + service.count('/recommendations')
            expected = {'two_layers': 28, 'one_layer': 10, 'budget': 4 + min(budget, 6)}
            if seen != expected[mode] or successes != 1:
                raise RuntimeError(f'Неожиданный результат {mode}, calls={seen}, ok={successes}')
            emit('total', mode=mode, requests=len(paths), backend_calls=seen, ok=successes)


def admitted(service, pool, path):
    if not pool.acquire(blocking=False):
        return 'pool_full'
    try:
        return service.get(path)
    finally:
        pool.release()


def compare_pools(split):
    name = 'split' if split else 'shared'
    with local_service() as service, ThreadPoolExecutor(max_workers=2) as workers:
        optional = threading.BoundedSemaphore(1 if split else 2)
        critical = threading.BoundedSemaphore(1) if split else optional
        try:
            first = workers.submit(admitted, service, optional, '/slow')
            service.wait_for_slow(1)
            second = workers.submit(admitted, service, optional, '/slow')
            if split:
                if second.result(timeout=3) != 'pool_full':
                    raise RuntimeError('Второй запрос должен получить локальный отказ')
            else:
                service.wait_for_slow(2)
            price = admitted(service, critical, '/price')
            emit('pools', mode=name, slow_inflight=service.count('/slow'),
                 price=price, price_backend_calls=service.count('/price'))
        finally:
            service.release_slow.set()
        results = [first.result(), second.result()]
        if (price, results) != ((200, [200, 'pool_full']) if split else ('pool_full', [200, 200])):
            raise RuntimeError(f'Неожиданный результат {name}, price={price}, slow={results}')


class Breaker:
    """Последовательный клиент. Одна проба после истечения паузы."""
    def __init__(self, cooldown=1.5):
        self.state = 'closed'
        self.failures = 0
        self.probe_at = 0.0
        self.cooldown = cooldown

    def call(self, service):
        if self.state == 'open':
            if time.monotonic() < self.probe_at:
                return 'local', 'basic'
            self.state = 'half_open'
        path = 'probe' if self.state == 'half_open' else 'remote'
        status = service.get('/recommendations')
        if status == 200:
            self.failures = 0
            self.state = 'closed'
            return path, 'personalized'
        self.failures += 1
        if self.failures >= 2:
            self.state = 'open'
            self.probe_at = time.monotonic() + self.cooldown
        return path, 'basic'


def isolation(pause, recover_after):
    compare_pools(split=False)
    time.sleep(pause)
    compare_pools(split=True)
    time.sleep(pause)
    with local_service() as service:
        breaker = Breaker()
        usual = [
                ('remote', 'basic', 'closed', 1), ('remote', 'basic', 'open', 2),
                ('local', 'basic', 'open', 2), ('probe', 'personalized', 'closed', 3),
                ('remote', 'personalized', 'closed', 4)]
        for step in range(1, 6):
            if step == recover_after + 1:
                service.recovered.set()
            if step == 4 and breaker.state == 'open':
                time.sleep(max(0, breaker.probe_at - time.monotonic()))
            route, page = breaker.call(service)
            count = service.count('/recommendations')
            emit('breaker', step=step, dependency='up' if service.recovered.is_set() else 'down',
                 via=route, page=page, state=breaker.state, backend_calls=count)
            expected = usual[step - 1] if recover_after == 2 or step == 1 else (
                'remote', 'personalized', 'closed', step)
            if (route, page, breaker.state, count) != expected:
                raise RuntimeError(f'Неожиданное состояние на шаге {step}')


def main():
    parser = argparse.ArgumentParser(description='Паттерны устойчивости')
    commands = parser.add_subparsers(dest='demo', required=True)
    gate = commands.add_parser('admission', help='шесть запросов при ограниченной одновременности')
    gate.add_argument('--limit', type=int, choices=range(1, 7), default=2, metavar='1..6')
    retry = commands.add_parser('retries', help='повторы в двух слоях и общий бюджет')
    retry.add_argument('--budget', type=int, choices=range(7), default=2, metavar='0..6')
    isolate = commands.add_parser('isolation', help='пулы, circuit breaker и запасной ответ')
    isolate.add_argument('--recover-after', type=int, choices=(1, 2), default=2,
                         help='вернуть рекомендации после указанного числа неудачных вызовов')
    for command in [gate, retry, isolate]:
        command.add_argument('--fast', action='store_true', help='пропустить паузы между наблюдениями')
    args = parser.parse_args()
    pause = 0 if args.fast else 0.7
    if args.demo == 'admission':
        admission(args.limit, pause)
    elif args.demo == 'retries':
        retries(args.budget, pause)
    else:
        isolation(pause, args.recover_after)


if __name__ == '__main__':
    main()
