import pytest

pytest_plugins = ['pytest_userver.plugins.core']


@pytest.fixture(scope='session')
def service_env():
    return {
        'ENABLE_TEST_ERRORS': '1',
        'POD_ID': 'tests-pod',
        'DATACENTER': 'tests-dc',
    }


@pytest.fixture(scope='session')
def service_http_ping_url(service_baseurl):
    return f"{service_baseurl.rstrip('/')}/health"
