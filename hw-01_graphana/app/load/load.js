import http from 'k6/http';

const TARGETS = (__ENV.TARGETS || 'http://app-1:8080,http://app-2:8080').split(',');
const RPS = Number(__ENV.TARGET_RPS || 40);
const RAMP_MINUTES = Number(__ENV.RAMP_MINUTES || 3);
const JSON_HDR = { headers: { 'Content-Type': 'application/json' } };

http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 404, 500));

export const options = {
    scenarios: {
        load: {
            executor: 'ramping-arrival-rate',
            startRate: 1,
            timeUnit: '1s',
            preAllocatedVUs: 50,
            maxVUs: 200,
            stages: [
                { target: RPS, duration: `${RAMP_MINUTES}m` },
                { target: RPS, duration: '24h' },
            ],
        },
    },
};

export function setup() {
    const ids = {};
    for (const base of TARGETS) {
        ids[base] = [];
        for (let i = 0; i < 20; i++) {
            const r = http.post(`${base}/items`, JSON.stringify({ name: `seed-${i}` }), JSON_HDR);
            if (r.status === 200 || r.status === 201) ids[base].push(r.json('id'));
        }
    }
    return ids;
}

export default function (ids) {
    const base = TARGETS[Math.floor(Math.random() * TARGETS.length)];
    const known = ids[base];
    const id = known.length ? known[Math.floor(Math.random() * known.length)] : 1;
    const body = JSON.stringify({ name: `item-${Math.floor(Math.random() * 1e6)}` });
    const p = Math.random();

    if (p < 0.58) {
        http.get(`${base}/items/${id}`);
    } else if (p < 0.78) {
        http.put(`${base}/items/${id}`, body, JSON_HDR);
    } else if (p < 0.96) {
        http.post(`${base}/items`, body, JSON_HDR);
    } else {
        http.get(`${base}/items/invalid-id`);
    }
}