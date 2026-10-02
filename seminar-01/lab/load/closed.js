// Optional appendix only. Same endpoint/timeout, fixed concurrency, no think time.
import http from 'k6/http';
const vus=Number(__ENV.VUS||8);
const duration=__ENV.DURATION||'60s';
if (!Number.isInteger(vus) || vus<1 || vus>500 || !/^[1-9][0-9]{0,2}s$/.test(duration) || Number(duration.slice(0,-1))<5 || Number(duration.slice(0,-1))>600) throw new Error('Invalid lab workload envelope');
export const options = {vus, duration,
  summaryTrendStats:['avg','med','p(95)','p(99)','max']};
export default function () {
  http.get('http://app:8080/api/work',{timeout:'3s',tags:{name:'/api/work'},redirects:0});
}
