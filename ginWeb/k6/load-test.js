import http from 'k6/http';
import { check } from 'k6';

const apps = [
    'http://app-1:8080',
    'http://app-2:8080',
];

export const options = {
    scenarios: {
        background_load: {
            executor: 'ramping-arrival-rate',
            startRate: 5,
            timeUnit: '1s',
            preAllocatedVUs: 10,
            maxVUs: 60,
            stages: [
                { target: 30, duration: '1m' },
                { target: 30, duration: '5m' },
            ],
        },
    },
};

export default function () {
    const app = apps[__VU % 2];
    const step = __ITER % 5;
    const id = `${__VU}-${Math.floor(__ITER / 5)}`;

    const item = JSON.stringify({
        id: id,
        title: 'Test Album',
        artist: 'Test Artist',
        price: 19.99,
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
        },
    };

    if (step === 0) {
        const response = http.post(`${app}/items`, item, params);
        check(response, {
            'POST item: 201': (r) => r.status === 201,
        });
    } else if (step === 1) {
        const response = http.get(`${app}/items/${id}`);
        check(response, {
            'GET item: 200': (r) => r.status === 200,
        });
    } else if (step === 2) {
        const response = http.put(`${app}/items/${id}`, item, params);
        check(response, {
            'PUT item: 200': (r) => r.status === 200,
        });
    } else if (step === 3) {
        const response = http.get(`${app}/items/nonexistent-${id}`);
        check(response, {
            'GET missing item: 404': (r) => r.status === 404,
        });
    } else {
        const response = http.post(
            `${app}/items`,
            '{invalid json',
            params
        );
        check(response, {
            'POST invalid JSON: 400': (r) => r.status === 400,
        });
    }
}
