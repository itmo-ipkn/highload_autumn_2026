import json
import re

import pytest


async def create_item(service_client, name='initial'):
    response = await service_client.post('/items', json={'name': name})
    assert response.status == 201
    item = response.json()
    assert item['name'] == name
    assert isinstance(item['id'], int) and item['id'] > 0
    return item


async def test_crud(service_client):
    item = await create_item(service_client, 'начальное имя')
    path = f"/items/{item['id']}"
    response = await service_client.get(path)
    assert response.status == 200
    assert response.json() == item
    response = await service_client.put(path, json={'name': 'updated'})
    assert response.status == 200
    assert response.json() == {'id': item['id'], 'name': 'updated'}
    response = await service_client.get(path)
    assert response.json() == {'id': item['id'], 'name': 'updated'}
    another = await create_item(service_client)
    assert another['id'] != item['id']


@pytest.mark.parametrize('method', ['GET', 'PUT'])
async def test_missing_item(service_client, method):
    response = await service_client.request(
        method, '/items/18446744073709551615', json={'name': 'missing'})
    assert response.status == 404
    assert response.json()['error'] == 'Item not found'


@pytest.mark.parametrize('method', ['GET', 'PUT'])
@pytest.mark.parametrize('item_id', ['abc', '-1', '1tail', '18446744073709551616'])
async def test_invalid_id(service_client, method, item_id):
    response = await service_client.request(
        method, f'/items/{item_id}', json={'name': 'invalid'})
    assert response.status == 400


@pytest.mark.parametrize('body', ['{', 'null', '[]', '{}', '{"name":1}', '{"name":""}'])
@pytest.mark.parametrize('method', ['POST', 'PUT'])
async def test_invalid_body(service_client, method, body):
    path = '/items' if method == 'POST' else '/items/1'
    response = await service_client.request(
        method, path, data=body, headers={'Content-Type': 'application/json'})
    assert response.status == 400


async def test_health_and_handler_discovery(service_client):
    response = await service_client.get('/health')
    assert response.status == 200
    response = await service_client.get('/handlers')
    assert response.status == 200
    assert {(h['method'], h['path']) for h in response.json()['handlers']} == {
        ('POST', '/items'), ('GET', '/items/{id}'), ('PUT', '/items/{id}')}
    for path in ('/hello', '/ping'):
        response = await service_client.get(path)
        assert response.status == 404


def metric_value(exposition, name, labels):

    for line in exposition.splitlines():
        match = re.fullmatch(r'([a-zA-Z_:][a-zA-Z0-9_:]*)\{(.*)\} ([^ ]+)', line)
        if not match or match[1] != name:
            continue
        actual = dict(re.findall(r'(\w+)=("(?:[^"\\]|\\.)*")', match[2]))
        actual = {key: json.loads(value) for key, value in actual.items()}
        if all(actual.get(key) == value for key, value in labels.items()):
            return float(match[3])
    raise AssertionError(f'Missing metric {name} with labels {labels}')


@pytest.mark.parametrize('method,route', [('POST', '/items'), ('PUT', '/items/{id}')])
async def test_unused_error_codes_export_zero(monitor_client, method, route):
    exposition = await monitor_client.metrics_raw('prometheus', prefix='http_request')
    labels = {'method': method, 'route': route, 'status': '500',
              'pod_id': 'tests-pod', 'datacenter': 'tests-dc'}
    assert metric_value(exposition, 'http_request_errors_total', labels) == 0


async def test_prometheus_final_status_and_latency(service_client, monitor_client):
    item = await create_item(service_client)
    labels = {'method': 'GET', 'route': '/items/{id}',
              'pod_id': 'tests-pod', 'datacenter': 'tests-dc'}
    before = await monitor_client.metrics_raw('prometheus', prefix='http_request')
    for path, status in ((f"/items/{item['id']}", 200),
                         ('/items/18446744073709551615', 404),
                         ('/items/0?simulate_error=1', 500)):
        response = await service_client.get(path)
        assert response.status == status
    response = await service_client.get('/health')
    assert response.status == 200
    response = await service_client.get('/handlers')
    assert response.status == 200
    after = await monitor_client.metrics_raw('prometheus', prefix='http_request')
    for name in ('http_request_total', 'http_request_duration_seconds_count'):
        assert metric_value(after, name, labels) - metric_value(before, name, labels) == 3
    for status in ('404', '500'):
        error_labels = {**labels, 'status': status}
        name = 'http_request_errors_total'
        assert metric_value(after, name, error_labels) - metric_value(before, name, error_labels) == 1
    assert '# TYPE http_request_duration_seconds histogram' in after
    assert '# TYPE http_request_total counter' in after
    assert metric_value(after, 'http_request_duration_seconds_bucket',
                        {**labels, 'le': '+Inf'}) == metric_value(
                            after, 'http_request_duration_seconds_count', labels)
    assert '/items/18446744073709551615' not in after
    for path in ('/health', '/handlers', '/metrics'):
        assert f'route="{path}"' not in after
