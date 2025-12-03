import asyncio
import httpx

BASE_URL = "http://127.0.0.1:8000"

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


async def create_cursor(client, name, headers, key=None):
    try:
        params = {"key": key} if key else {}
        resp = await client.post(f"{BASE_URL}/create_cursor/{name}", headers=headers, params=params)
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

semaphore = asyncio.Semaphore(200)  # realistic max concurrent operations


async def safe_set_key(client, key, value, headers):
    async with semaphore:
        return await set_key(client, key, value, headers)


async def session_worker(session_id, num_keys=1000, batch_size=100):
    headers = {"X-Session-Id": session_id}

    limits = httpx.Limits(
        max_connections=20,            # realistic pool
        max_keepalive_connections=10,
    )

    async with httpx.AsyncClient(http2=False, limits=limits) as client:
        for start in range(0, num_keys, batch_size):
            end = min(start + batch_size, num_keys)

            tasks = []
            for i in range(start, end):
                key = f"{session_id}_key_{i}"
                value = f"value_{i}"

                tasks.append(
                    safe_set_key(client, key, value, headers)  # <<–– uses YOUR set()
                )

            await asyncio.gather(*tasks)
            print(f"{session_id}: Inserted {end}/{num_keys}")


async def simulate_realistic(num_sessions=50, num_keys=2000, batch_size=100):
    tasks = []
    for i in range(num_sessions):
        session_id = f"session_{i}"
        tasks.append(session_worker(session_id, num_keys, batch_size))

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(simulate_realistic(
        num_sessions=1,
        num_keys=10000,
        batch_size=1
    ))
