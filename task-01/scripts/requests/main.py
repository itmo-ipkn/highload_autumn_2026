import asyncio
import os
import random
import time

import aiohttp


API_URL_1 = os.environ["API_URL_1"]
API_URL_2 = os.environ["API_URL_2"]

API_URLS = [
    API_URL_1,
    API_URL_2,
]

METHODS = [
    "GET",
    "POST",
    "PUT",
]

REQUEST_RATE_LIMIT = 30
TICK_LENGTH_SECONDS = 5
MAX_ITEM_ID = 100


async def get_item(session, api_instance, item_id):
    url = f"{api_instance}/items/{item_id}"

    async with session.get(url) as response:
        data = await response.json()
        return data

async def create_item(session, api_instance, item_id):
    url = f"{api_instance}/items"

    item = {
        "id": item_id,
        "name": f"item-{item_id}",
        "price": 100,
    }

    async with session.post(url, json=item) as response:
        data = await response.json()
        return data

async def update_item(session, api_instance, item_id):
    url = f"{api_instance}/items/{item_id}"

    item = {
        "id": item_id,
        "name": f"item-{item_id}",
        "price": 110,
    }

    async with session.put(url, json=item) as response:
        data = await response.json()
        return data

async def send_request(session):
    try:
        api_instance = random.choice(API_URLS)
        item_id = random.randint(0, MAX_ITEM_ID)
        method = random.choice(METHODS)

        if method == "GET":
            await get_item(session, api_instance, item_id)
        elif method == "POST":
            await create_item(session, api_instance, item_id)
        elif method == "PUT":
            await update_item(session, api_instance, item_id)

    except aiohttp.ClientError:
        pass

async def run_requests():
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        while True:
            time_from_start = time.time() - start_time
            tick_number = int(time_from_start // TICK_LENGTH_SECONDS) + 1

            if tick_number > REQUEST_RATE_LIMIT:
                current_rps_limit =  REQUEST_RATE_LIMIT
            else:
                current_rps_limit =  tick_number

            for i in range(current_rps_limit):
                asyncio.create_task(send_request(session))

            await asyncio.sleep(1)


def main() -> None:
    asyncio.run(run_requests())

if __name__ == "__main__":
    main()
