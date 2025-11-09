import asyncio
import httpx

BASE_URL = "http://127.0.0.1:8000"

async def set_key(client, key, value):
    try:
        resp = await client.post(f"{BASE_URL}/set/{key}/{value}")
        print(f"SET {key}={value} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"SET {key} failed: {e}")

async def get_key(client, key):
    try:
        resp = await client.get(f"{BASE_URL}/get/{key}")
        print(f"GET {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET {key} failed: {e}")

async def remove_key(client, key):
    try:
        resp = await client.delete(f"{BASE_URL}/delete/{key}")
        print(f"REMOVE {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"REMOVE {key} failed: {e} ")

async def simulate_clients(num_clients=100):
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(num_clients):
            key = f"key_{i}"
            value = f"value_{i}"
            # Schedule both SET and GET for each key
            tasks.append(set_key(client, key, value))
            tasks.append(get_key(client, key))
            tasks.append(remove_key(client, key))
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(simulate_clients())
