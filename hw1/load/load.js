import http from 'k6/http';

export const options = {
    scenarios: {
        ramp_up: {
            executor: 'ramping-arrival-rate',
            startRate: 1,
            timeUnit: '1s',
            preAllocatedVUs: 20,
            maxVUs: 50,
            stages: [
                { target: 10, duration: '1m' },
                { target: 20, duration: '1m' },
                { target: 30, duration: '1m' },
            ],
        },
        steady: {
            executor: 'constant-arrival-rate',
            rate: 30,
            timeUnit: '1s',
            duration: '87600h',
            startTime: '3m',
            preAllocatedVUs: 20,
            maxVUs: 50,
        },
    },
};

export default function () {
    const services = [
        __ENV.URL_SERVICE_1,
        __ENV.URL_SERVICE_2,
    ]
    const randomIndex = Math.floor(Math.random() * services.length)
    const baseUrl = services[randomIndex]

    const operation = Math.floor(Math.random() * 3)
    const shouldFail = Math.random() < 0.03

    if (shouldFail) {
        failOperations(operation, baseUrl)
        return
    }

    successOperations(operation, baseUrl)
}

function successOperations(operation, baseUrl) {
    const headers = {
        'Content-Type': 'application/json',
    };

    if (operation === 0) {
        const itemNumber = Math.floor(Math.random() * 1000) + 1
        return http.post(
            `${baseUrl}/items`,
            JSON.stringify({
                name: `item-${itemNumber}`,
                description: `description item-${itemNumber}`
            }),
            { headers }
        );
    }

    const randomId = Math.floor(Math.random() * 100) + 1
    if (operation === 1) {
        return http.get(`${baseUrl}/items/${randomId}`);
    }

    const itemNumber = Math.floor(Math.random() * 1000) + 1
    return http.put(
        `${baseUrl}/items/${randomId}`,
        JSON.stringify({
            name: `updated item-${itemNumber}`,
            description: `updated description item-${itemNumber}`
        }),
        { headers }
    );
}

function failOperations(operation, baseUrl) {
    const headers = {
        'Content-Type': 'application/json',
    };

    if (operation === 0) {
        return http.post(
            `${baseUrl}/items`,
            JSON.stringify({ name: `item-fail` }),
            { headers }
        );
    }

    const id = Number.MAX_SAFE_INTEGER
    if (operation === 1) {
        return http.get(`${baseUrl}/items/${id}`)
    }

    return http.put(
        `${baseUrl}/items/${id}`,
        JSON.stringify({
            name: `updated item-fail`,
            description: `update description item-fail`
        }),
        { headers }
    );
}