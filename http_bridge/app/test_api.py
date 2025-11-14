import asyncio
import httpx

BASE_URL = "http://127.0.0.1:8000"

async def set_key(client, key, value):
    try:
        resp = await client.post(f"{BASE_URL}/set/{key}/{value}")
        print(f"SET {key}={value} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"SET {key} failed: {e!r}")

async def get_key(client, key):
    try:
        resp = await client.get(f"{BASE_URL}/get/{key}")
        print(f"GET {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET {key} failed: {e!r}")

async def remove_key(client, key):
    try:
        resp = await client.delete(f"{BASE_URL}/delete/{key}")
        print(f"REMOVE {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"REMOVE {key} failed: {e!r} ")

async def create_cursor(client, name, key):
    try:
        resp = await client.post(f"{BASE_URL}/create_cursor/{name}/{key}")
        print(f"CREATE CURSOR {name} {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"CREATE CURSOR {name} {key} failed: {e!r}")

async def delete_cursor(client, name):
    try:
        resp = await client.delete(f"{BASE_URL}/delete_cursor/{name}")
        print(f"DELETE CURSOR {name} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"DELETE CURSOR {name} failed: {e!r}")

async def get_ff(client, cursor, amount):
    try:
        resp = await client.get(f"{BASE_URL}/get_ff/{cursor}/{amount}")
        print(f"GET_FF {cursor} {amount} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET_FF {cursor} failed: {e!r}")



async def simulate_clients(num_clients=20):
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(num_clients):
            key = f"key_{i}"
            value = f"value_{i}"
            name = f"name_{i}"
            # Schedule both SET and GET for each key
            tasks.append(set_key(client, key, value))
            tasks.append(get_key(client, key))
            tasks.append(remove_key(client, key))
            tasks.append(create_cursor(client, name, key))
            tasks.append(delete_cursor(client, name))
            #tasks.append(get_ff(client, name, i))

        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(simulate_clients())
