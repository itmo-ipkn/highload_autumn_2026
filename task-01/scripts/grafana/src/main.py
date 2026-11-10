import json
import os
import sys
import time
from pathlib import Path

import requests

from helpers import create_dashboard


USER = os.environ["GF_SECURITY_ADMIN_USER"]
PASSWORD = os.environ["GF_SECURITY_ADMIN_PASSWORD"]
GRAFANA_URL = os.environ["GRAFANA_URL"]
DASHBOARD_API_URL = f"{GRAFANA_URL}/apis/dashboard.grafana.app/v2/namespaces/default/dashboards"

API_URL_1 = os.environ["API_URL_1"]

DASHBOARD_NAME = "task_1"
DASHBOARD_TEMPLATE_PATH = Path(__file__).resolve().parent / f"dashboards/{DASHBOARD_NAME}.json"


def main() -> None:
    response = requests.get(f"{API_URL_1}/handlers")
    handlers = response.json()
    
    dashboard_template = json.loads(DASHBOARD_TEMPLATE_PATH.read_text())
    dashboard = create_dashboard(dashboard_template, handlers)

    response = requests.post(
        DASHBOARD_API_URL,
        json=dashboard,
        auth=(USER, PASSWORD),
    )
    status_code = response.status_code

    if status_code in [200, 201]:
        print(f"Created dashboard {DASHBOARD_NAME}")
    elif status_code == 409:
        response = requests.put(
            f"{DASHBOARD_API_URL}/{DASHBOARD_NAME}",
            json=dashboard,
            auth=(USER, PASSWORD),
        )

        if response.status_code in [200, 201]:
            print(f"Updated dashboard {DASHBOARD_NAME}")
    else:
        print(f"Status code: {status_code}")

if __name__ == "__main__":
    main()
