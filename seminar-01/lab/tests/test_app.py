import asyncio
import sys
import unittest
from pathlib import Path
from aiohttp.test_utils import TestClient, TestServer
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'app'))
from server import Lab, Config, ROUTE

TOKEN='test-only-not-a-production-secret'
class AppTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.lab=Lab(TOKEN)
        api,admin=self.lab.apps()
        self.api=TestClient(TestServer(api)); self.admin=TestClient(TestServer(admin))
        await self.api.start_server(); await self.admin.start_server()
    async def asyncTearDown(self):
        await self.api.close(); await self.admin.close()
    async def test_01_success_and_accounting(self):
        r=await self.api.get(ROUTE)
        self.assertEqual(r.status,200); self.assertTrue((await r.json())['ok'])
        self.assertIn('X-Request-ID',r.headers)
        self.assertEqual(self.lab.inflight._value.get(),0)
        self.assertEqual(self.lab.finished.labels(ROUTE,'200')._value.get(),1)
    async def test_02_probes_not_business_requests(self):
        await self.api.get('/healthz'); await self.api.get('/metrics')
        self.assertEqual(self.lab.started.labels(ROUTE)._value.get(),0)
    async def test_03_separate_authenticated_admin(self):
        self.assertEqual((await self.api.post('/admin/mode',json={'mode':'slow'})).status,404)
        self.assertEqual((await self.admin.post('/admin/mode',json={'mode':'slow'})).status,401)
        r=await self.admin.post('/admin/mode',json={'mode':'slow'},headers={'X-Lab-Token':TOKEN})
        self.assertEqual(r.status,200); self.assertEqual(self.lab.mode,'slow')
    async def test_04_invalid_admin_body(self):
        for body in ({'mode':'bogus'},[],{}):
            r=await self.admin.post('/admin/mode',json=body,headers={'X-Lab-Token':TOKEN})
            self.assertEqual(r.status,400)
    async def test_05_error_frequency(self):
        self.lab.set_mode('errors')
        statuses=[(await self.api.get(ROUTE)).status for _ in range(20)]
        self.assertEqual(statuses.count(500),4); self.assertEqual(statuses.count(200),16)
    async def test_06_cumulative_buckets(self):
        for _ in range(4): await self.api.get(ROUTE)
        from prometheus_client.parser import text_string_to_metric_families
        text=await (await self.api.get('/metrics')).text()
        buckets={s.labels['le']:s.value for m in text_string_to_metric_families(text) for s in m.samples
                 if s.name=='lab_request_duration_seconds_bucket' and s.labels.get('status')=='200'}
        self.assertEqual(buckets['0.3'],4); self.assertEqual(buckets['+Inf'],4)
        ordered=[buckets[str(x)] for x in (0.005,0.01,0.025,0.05,0.1,0.2,0.3)]
        self.assertEqual(ordered,sorted(ordered))
    async def test_07_zero_series_present(self):
        text=await (await self.api.get('/metrics')).text()
        self.assertIn('lab_requests_total{route="/api/work",status="500"} 0.0',text)
    async def test_08_tail_is_deterministic(self):
        self.lab.config=Config(base_seconds=.001,tail_extra_seconds=.06)
        self.lab.set_mode('tail'); elapsed=[]
        import time
        for _ in range(100):
            t=time.perf_counter(); await self.api.get(ROUTE); elapsed.append(time.perf_counter()-t)
        self.assertGreater(elapsed[49],.05); self.assertGreater(elapsed[99],.05)
        self.assertEqual(self.lab.finished.labels(ROUTE,'200')._value.get(),100)
    async def test_09_unbounded_values_not_labels(self):
        await self.api.get(ROUTE+'?user_id=secret-example-837')
        text=await (await self.api.get('/metrics')).text()
        self.assertNotIn('secret-example-837',text); self.assertNotIn('request_id=',text)
    async def test_10_cancellation_releases_slot(self):
        self.lab.set_mode('slow')
        task=asyncio.create_task(self.lab.work(None)); await asyncio.sleep(.03); task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(self.lab.active._value.get(),0)
        self.assertEqual(self.lab.inflight._value.get(),0)
        self.assertEqual(self.lab.finished.labels(ROUTE,'499')._value.get(),1)
    async def test_11_overload_bounded_and_recovers(self):
        lab=Lab(TOKEN,Config(workers=2,queue_limit=4,queue_timeout=.02,slow_seconds=.08))
        lab.set_mode('slow')
        out=await asyncio.gather(*(lab.work(None) for _ in range(20)))
        statuses=[r.status for r in out]
        self.assertIn(503,statuses); self.assertIn(200,statuses)
        self.assertEqual(lab.active._value.get(),0); self.assertEqual(lab.waiting_count,0)
        lab.set_mode('normal'); self.assertEqual((await lab.work(None)).status,200)
    async def test_12_invalid_configuration(self):
        with self.assertRaises(ValueError): Config(workers=0)
        with self.assertRaises(ValueError): Lab('short')
if __name__=='__main__': unittest.main(verbosity=2)
