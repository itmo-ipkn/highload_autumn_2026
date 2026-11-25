import random
import string
import uuid

from locust import HttpUser, between, task


class QuickstartUser(HttpUser):
    target_url: str = "http://nginx/api/v1/items"
    wait_time = between(0.1, 3)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.error_threshold: float = 0.001
        self.created_item_ids: list[str] = []  # for GET/PUT requests

    def _create_payload(self) -> str:
        return "".join(random.sample(string.ascii_lowercase, 5))

    def _process_post_response(self, response) -> None:
        if response.status_code == 200:
            response_json = response.json()
            item_id = response_json["id"]
            self.created_item_ids.append(item_id)
        return None

    def on_start(self):
        self.created_item_ids.append(str(uuid.uuid4()))

    @task(10)
    def create_item(self):
        if random.random() < self.error_threshold:
            payload = {"value": 42}  # will not pass validation, str required
        else:
            payload = {"value": self._create_payload()}

        response = self.client.post(self.target_url, json=payload)
        self._process_post_response(response)

    @task(10)
    def get_item(self):
        item_id = random.choice(self.created_item_ids)
        self.client.get(f"{self.target_url}/{item_id}")

    @task(10)
    def update_item(self):
        if random.random() < self.error_threshold:
            payload = {"value": 142}  # will not pass validation, str required
        else:
            payload = {"value": self._create_payload()}

        item_id = random.choice(self.created_item_ids)
        self.client.put(f"{self.target_url}/{item_id}", json=payload)

    @task
    def create_item_404(self):
        payload = {"value": self._create_payload()}
        response = self.client.post("http://nginx/api/v2/items", json=payload)
        self._process_post_response(response)

    @task
    def get_item_404(self):
        item_id = uuid.uuid4()
        self.client.get(f"{self.target_url}/{item_id}")

    @task
    def update_item_404(self):
        item_id = uuid.uuid4()
        payload = {"value": self._create_payload()}
        self.client.put(f"{self.target_url}/{item_id}", json=payload)
