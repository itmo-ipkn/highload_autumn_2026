"""S1 teaching service: observable bounded I/O pool, NOT a database benchmark.

Run exactly ONE Python process. API metrics cover /api/work handler lifetime,
including pool wait, excluding TCP/TLS, response transmission and /healthz.
An admin listener is separate and requires LAB_ADMIN_TOKEN.
"""
from __future__ import annotations
import asyncio
import hmac
import json
import os
import signal
import time
import uuid
from dataclasses import dataclass
from aiohttp import web
from prometheus_client import (
    CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector,
    PlatformCollector, GCCollector, generate_latest, CONTENT_TYPE_LATEST,
)

BUCKETS = (.005, .01, .025, .05, .1, .2, .3, .5, .75, 1., 1.5, 2.5, 5.)
MODES = ('normal', 'tail', 'slow', 'errors')
ROUTE = '/api/work'

def log(**fields: object) -> None:
    print(json.dumps({'ts': time.time(), **fields}, ensure_ascii=False), flush=True)

@dataclass(frozen=True)
class Config:
    workers: int = 8
    queue_limit: int = 32
    queue_timeout: float = .250
    base_seconds: float = .025
    slow_seconds: float = .200
    tail_extra_seconds: float = .900

    def __post_init__(self) -> None:
        if not 1 <= self.workers <= 64 or not 1 <= self.queue_limit <= 256:
            raise ValueError('workers must be 1..64; queue_limit must be 1..256')
        if min(self.queue_timeout, self.base_seconds, self.slow_seconds) <= 0:
            raise ValueError('durations must be positive')
        if self.tail_extra_seconds < 0:
            raise ValueError('tail_extra_seconds must be nonnegative')

class Lab:
    def __init__(self, token: str, config: Config | None = None) -> None:
        if len(token) < 16:
            raise ValueError('LAB_ADMIN_TOKEN must contain at least 16 characters')
        self.token = token
        self.config = config or Config()
        self.mode = 'normal'
        self.sequence = 0
        self.waiting_count = 0
        self.pool = asyncio.Semaphore(self.config.workers)
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        self.started = Counter('lab_requests_started_total', 'Entered API handlers', ['route'], registry=self.registry)
        self.finished = Counter('lab_requests_total', 'Terminated API handlers; 499 means cancelled, not a wire response', ['route','status'], registry=self.registry)
        self.duration = Histogram('lab_request_duration_seconds', 'API handler lifetime including pool wait, not network transmission', ['route','status'], buckets=BUCKETS, registry=self.registry)
        self.queue_duration = Histogram('lab_pool_wait_seconds', 'Time acquiring simulated I/O pool', ['outcome'], buckets=BUCKETS, registry=self.registry)
        self.service_duration = Histogram('lab_service_seconds', 'Time holding simulated I/O slot', buckets=BUCKETS, registry=self.registry)
        self.inflight = Gauge('lab_inflight', 'Entered API handlers not yet terminated', registry=self.registry)
        self.active = Gauge('lab_pool_active', 'Occupied simulated I/O slots', registry=self.registry)
        self.waiting = Gauge('lab_pool_waiting', 'Handlers waiting for simulated I/O slots', registry=self.registry)
        self.capacity = Gauge('lab_pool_capacity', 'Configured simulated I/O slots', registry=self.registry)
        self.rejected = Counter('lab_rejected_total', 'Overload rejection reasons', ['reason'], registry=self.registry)
        self.mode_metric = Gauge('lab_mode', 'One-hot finite mode label', ['mode'], registry=self.registry)
        self.capacity.set(self.config.workers)
        self.started.labels(ROUTE).inc(0)
        for status in ('200','499','500','503'):
            self.finished.labels(ROUTE,status).inc(0)
            self.duration.labels(ROUTE,status)
        for outcome in ('acquired','timeout'):
            self.queue_duration.labels(outcome)
        for reason in ('queue_full','queue_timeout'):
            self.rejected.labels(reason).inc(0)
        self.set_mode('normal')

    def set_mode(self, mode: str) -> None:
        if mode not in MODES:
            raise ValueError('unsupported mode')
        self.mode, self.sequence = mode, 0
        for name in MODES:
            self.mode_metric.labels(name).set(int(name == mode))

    async def work(self, request: web.Request) -> web.Response:
        start = time.perf_counter()
        self.sequence += 1
        seq, mode = self.sequence, self.mode   # snapshot; current requests keep old mode
        request_id = uuid.uuid4().hex
        self.started.labels(ROUTE).inc()
        self.inflight.inc()
        status, acquired, queue_seconds, service_start = 500, False, 0., None
        reason = ''
        try:
            if self.waiting_count >= self.config.queue_limit:
                status, reason = 503, 'queue_full'
                self.rejected.labels(reason).inc()
                return web.json_response({'ok':False,'reason':reason}, status=status, headers={'X-Request-ID':request_id})
            self.waiting_count += 1
            self.waiting.inc()
            queue_start = time.perf_counter()
            try:
                await asyncio.wait_for(self.pool.acquire(), timeout=self.config.queue_timeout)
                acquired = True
                queue_seconds = time.perf_counter() - queue_start
                self.queue_duration.labels('acquired').observe(queue_seconds)
            except asyncio.TimeoutError:
                queue_seconds = time.perf_counter() - queue_start
                self.queue_duration.labels('timeout').observe(queue_seconds)
                status, reason = 503, 'queue_timeout'
                self.rejected.labels(reason).inc()
                return web.json_response({'ok':False,'reason':reason}, status=status, headers={'X-Request-ID':request_id})
            finally:
                self.waiting_count -= 1
                self.waiting.dec()
            self.active.inc()
            service_start = time.perf_counter()
            delay = self.config.slow_seconds if mode == 'slow' else self.config.base_seconds
            if mode == 'tail' and seq % 50 == 0:
                delay += self.config.tail_extra_seconds
            # Simulated asynchronous I/O. Low CPU is intentional; NOT time.sleep().
            await asyncio.sleep(delay)
            status = 500 if mode == 'errors' and seq % 5 == 0 else 200
            return web.json_response({'ok':status == 200}, status=status, headers={'X-Request-ID':request_id})
        except asyncio.CancelledError:
            status, reason = 499, 'handler_cancelled'
            raise
        finally:
            if acquired:
                if service_start is not None:
                    self.service_duration.observe(time.perf_counter() - service_start)
                    self.active.dec()
                self.pool.release()
            elapsed = time.perf_counter() - start
            self.duration.labels(ROUTE,str(status)).observe(elapsed)
            self.finished.labels(ROUTE,str(status)).inc()
            self.inflight.dec()
            # Sample normal traffic and repeated failures; IDs belong in logs, not labels.
            if seq % 50 == 0 or (status != 200 and seq % 10 == 0):
                log(event='request', request_id=request_id, route=ROUTE, mode=mode,
                    status=status, duration_ms=round(elapsed*1000,3),
                    pool_wait_ms=round(queue_seconds*1000,3), reason=reason)

    async def health(self, request: web.Request) -> web.Response:
        return web.json_response({'status':'alive','note':'liveness is not user SLI'})

    async def metrics(self, request: web.Request) -> web.Response:
        return web.Response(body=generate_latest(self.registry), headers={'Content-Type':CONTENT_TYPE_LATEST})

    async def admin(self, request: web.Request) -> web.Response:
        supplied = request.headers.get('X-Lab-Token','')
        if not hmac.compare_digest(supplied, self.token):
            raise web.HTTPUnauthorized(text='invalid lab token')
        if request.method == 'POST':
            try:
                data = await request.json()
                mode = data.get('mode') if isinstance(data,dict) else None
                if mode not in MODES:
                    raise ValueError('invalid mode')
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(text='JSON body: {"mode":"normal|tail|slow|errors"}')
            self.set_mode(mode)
            log(event='mode_change', mode=mode)
        return web.json_response({'mode':self.mode, 'workers':self.config.workers,
                                  'queue_limit':self.config.queue_limit,
                                  'queue_timeout_ms':self.config.queue_timeout*1000})

    def apps(self) -> tuple[web.Application, web.Application]:
        api, admin = web.Application(), web.Application(client_max_size=1024)
        api.router.add_get(ROUTE, self.work)
        api.router.add_get('/healthz', self.health)
        api.router.add_get('/metrics', self.metrics)
        admin.router.add_get('/admin/mode', self.admin)
        admin.router.add_post('/admin/mode', self.admin)
        return api, admin

async def main() -> None:
    config = Config(workers=int(os.getenv('WORKERS','8')), queue_limit=int(os.getenv('QUEUE_LIMIT','32')))
    lab = Lab(os.environ.get('LAB_ADMIN_TOKEN',''), config)
    host = os.getenv('BIND_HOST','0.0.0.0')
    api_port, admin_port = int(os.getenv('API_PORT','8080')), int(os.getenv('ADMIN_PORT','8081'))
    runners = [web.AppRunner(app, access_log=None, shutdown_timeout=5) for app in lab.apps()]
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT,signal.SIGTERM):
        loop.add_signal_handler(sig,stop.set)
    try:
        for runner,port in zip(runners,(api_port,admin_port)):
            await runner.setup()
            await web.TCPSite(runner,host,port).start()
        log(event='started', api_port=api_port, admin_port=admin_port, workers=config.workers)
        await stop.wait()
    finally:
        for runner in reversed(runners):
            await runner.cleanup()

if __name__ == '__main__':
    asyncio.run(main())
