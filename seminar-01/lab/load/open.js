import http from 'k6/http';
import { check } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

const rate = Number(__ENV.RATE || 80);
const vus = Number(__ENV.VUS || 160);
if (!(Number.isInteger(rate) && rate > 0 && rate <= 500 && Number.isInteger(vus) && vus > 0 && vus <= 500)) throw new Error('RATE/VUS outside lab limits');
const duration = __ENV.DURATION || '120s';
if (!/^[1-9][0-9]{0,2}s$/.test(duration) || Number(duration.slice(0,-1)) < 5 || Number(duration.slice(0,-1)) > 600) throw new Error('DURATION must be 5s..600s');
if (!['0','1'].includes(__ENV.EXPECT_NORMAL || '0')) throw new Error('EXPECT_NORMAL must be 0 or 1');
const base = __ENV.BASE_URL || 'http://app:8080';
const allowed = /^http:\/\/(app|127\.0\.0\.1|localhost)(:\d+)?$/;
if (!allowed.test(base)) throw new Error('Use only the private lab endpoint; arbitrary public targets are forbidden');
const started = new Counter('client_started');
// Only min/max are used: UTC request-start bounds, not latency or a new SLI.
const requestStarts = new Trend('client_request_start_unix_ms');
const success = new Rate('client_success');
const good = new Rate('client_good');
const wall = new Trend('client_wall_ms', true);
const wallSuccess = new Trend('client_wall_success_ms', true);
const valid = new Rate('client_payload_valid');
export const options = {
  scenarios: { api: {
    executor: 'constant-arrival-rate', rate, timeUnit:'1s',
    duration, preAllocatedVUs:vus, maxVUs:vus,
    gracefulStop:'5s',
  }},
  summaryTrendStats: ['avg','min','med','p(95)','p(99)','max'],
  thresholds: {
    dropped_iterations:['count==0'],
    client_started:['count>0'],
    client_payload_valid:['rate==1'],
    ...(__ENV.EXPECT_NORMAL === '1' ? {client_good:['rate>=0.99'], http_req_failed:['rate<0.01']} : {}),
  },
};
export default function () {
  started.add(1);
  const t = Date.now();
  requestStarts.add(t);
  const r = http.get(`${base}/api/work`, {timeout:'3s', tags:{name:'/api/work'}, redirects:0});
  let payloadOk = false;
  try { const p=r.json(); payloadOk = r.status===200 ? p.ok===true : ([500,503].includes(r.status) && p.ok===false); } catch (_) {}
  const elapsed = Date.now()-t; // HTTP call plus payload validation.
  wall.add(elapsed);
  if (r.status===200 && payloadOk) wallSuccess.add(elapsed);
  success.add(r.status===200 && payloadOk);
  good.add(r.status===200 && payloadOk && elapsed<=300);
  valid.add(payloadOk);
  check(r, {'HTTP 200':x=>x.status===200});
  // No sleep, no retries, one HTTP request per iteration, no redirects.
}
export function handleSummary(data) {
  // Stdout JSON can be redirected without root-owned Docker bind-mount artifacts.
  return { stdout: JSON.stringify({label:__ENV.RUN_LABEL||'manual',
     rate, duration:__ENV.DURATION||'120s', vu_limit:vus, summary:data},null,2)+'\n' };
}
