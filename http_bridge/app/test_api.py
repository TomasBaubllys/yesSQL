import asyncio
import httpx

BASE_URL = "http://127.0.0.1:8000"

# --- HTTP functions with headers ---
async def set_key(client, key, value, headers):
    try:
        resp = await client.post(f"{BASE_URL}/set/{key}/{value}", headers=headers)
        print(f"SET {key}={value} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"SET {key} failed: {e!r}")

async def get_key(client, key, headers):
    try:
        resp = await client.get(f"{BASE_URL}/get/{key}", headers=headers)
        print(f"GET {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET {key} failed: {e!r}")

async def remove_key(client, key, headers):
    try:
        resp = await client.delete(f"{BASE_URL}/delete/{key}", headers=headers)
        print(f"REMOVE {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"REMOVE {key} failed: {e!r} ")

async def create_cursor(client, name, key, headers):
    try:
        resp = await client.post(f"{BASE_URL}/create_cursor/{name}/{key}", headers=headers)
        print(f"CREATE CURSOR {name} {key} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"CREATE CURSOR {name} {key} failed: {e!r}")

async def delete_cursor(client, name, headers):
    try:
        resp = await client.delete(f"{BASE_URL}/delete_cursor/{name}", headers=headers)
        print(f"DELETE CURSOR {name} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"DELETE CURSOR {name} failed: {e!r}")

async def get_ff(client, cursor, amount, headers):
    try:
        resp = await client.get(f"{BASE_URL}/get_ff/{cursor}/{amount}", headers=headers)
        print(f"GET_FF {cursor} {amount} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET_FF {cursor} failed: {e!r}")

async def get_fb(client, cursor, amount, headers):
    try:
        resp = await client.get(f"{BASE_URL}/get_fb/{cursor}/{amount}", headers=headers)
        print(f"GET_FB {cursor} {amount} -> {resp.json()}")
    except httpx.RequestError as e:
        print(f"GET_FB {cursor} failed: {e!r}")

# --- Single client simulation ---
async def simulate_single(i, client, session_id):
    headers = {"X-Session-Id": session_id}
    key = f"key_{i}"
    value = f"value_{i}"
    name = f"name_{i}"

    await set_key(client, key, value, headers)
    await get_key(client, key, headers)
    await remove_key(client, key, headers)
    await create_cursor(client, name, key, headers)
    await delete_cursor(client, name, headers)

# --- Multiple clients simulation ---
async def simulate_clients(num_clients=5):
    async with httpx.AsyncClient(http2=False) as client:  # force HTTP/1.1
        tasks = []
        for i in range(num_clients):
            session_id = f"client_{i}"
            tasks.append(simulate_single(i, client, session_id))
        await asyncio.gather(*tasks)

# --- Run ---
if __name__ == "__main__":
    asyncio.run(simulate_clients(5))
