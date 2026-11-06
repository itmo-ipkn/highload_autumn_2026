import json
import logging
import os

import requests

from metrics_dashboard import generate_dashboard

logging.basicConfig(
    level=logging.INFO, format="[%(asctime)s] - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_handlers_list(app_host: str) -> list[str]:
    response = requests.get(f"http://{app_host}:{app_port}/handlers")
    return response.json()


class GrafanaDashboardAPI:
    def __init__(
        self,
        grafana_host: str,
        grafana_port: str,
        grafana_admin_name: str,
        grafana_admin_password,
    ) -> None:
        self.base_url_with_creds = f"http://{grafana_admin_name}:{grafana_admin_password}@{grafana_host}:{grafana_port}"
        self.dashboard_url = "/api/dashboards/db"
        self._headers: dict[str] = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def create_dashboard(self, dashboard_json: str) -> requests.Response:
        url = f"{self.base_url_with_creds}{self.dashboard_url}"
        payload = {"dashboard": json.loads(dashboard_json), "overwrite": True}
        return requests.post(url, headers=self._headers, json=payload)


if __name__ == "__main__":
    logger.info("Start dashboard creation.")

    app_host: str = os.getenv("APP_HOST")
    app_port: str = os.getenv("APP_PORT")

    grafana_host: str = os.getenv("GF_HOST")
    grafana_port: str = os.getenv("GF_PORT")
    grafana_admin_name: str = os.getenv("GF_SECURITY_ADMIN_USER")
    grafana_admin_password: str = os.getenv("GF_SECURITY_ADMIN_PASSWORD")

    handlers_list = get_handlers_list(app_host)
    dashboard_stringified = generate_dashboard(handlers_list)

    dashboard_api = GrafanaDashboardAPI(
        grafana_host, grafana_port, grafana_admin_name, grafana_admin_password
    )
    response = dashboard_api.create_dashboard(dashboard_stringified)

    logger.info("Dashboard created.")
