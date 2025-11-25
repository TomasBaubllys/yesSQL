import asyncio
import httpx

BASE_URL = "http://127.0.0.1:8000"

# --- Helper functions (your API wrappers) ---
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


# --- Console Program ---
async def console_client():
    session_id = input("Enter session id: ")
    headers = {"X-Session-Id": session_id}

    print("\nCommands:")
    print("  set <key> <value>")
    print("  get <key>")
    print("  del <key>")
    print("  cursor.create <name> <key>")
    print("  cursor.delete <name>")
    print("  ff <cursor> <amount>")
    print("  fb <cursor> <amount>")
    print("  exit\n")

    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

    async with httpx.AsyncClient(http2=False, limits=limits) as client:
        while True:
            cmd = input("> ").strip().split()

            if not cmd:
                continue

            if cmd[0] == "exit":
                print("Goodbye.")
                break

            if cmd[0] == "set" and len(cmd) == 3:
                await set_key(client, cmd[1], cmd[2], headers)

            elif cmd[0] == "get" and len(cmd) == 2:
                await get_key(client, cmd[1], headers)

            elif cmd[0] == "del" and len(cmd) == 2:
                await remove_key(client, cmd[1], headers)



            elif cmd[0] == "cursor.create":
                if len(cmd) == 2:
                    await create_cursor(client, cmd[1], headers)
                elif len(cmd) == 3:
                    await create_cursor(client, cmd[1], headers, cmd[2])
                else:
                    print("Usage: cursor.create <name> [key]")

            elif cmd[0] == "cursor.delete" and len(cmd) == 2:
                await delete_cursor(client, cmd[1], headers)

            elif cmd[0] == "ff" and len(cmd) == 3:
                await get_ff(client, cmd[1], cmd[2], headers)

            elif cmd[0] == "fb" and len(cmd) == 3:
                await get_fb(client, cmd[1], cmd[2], headers)

            else:
                print("Invalid command or wrong arguments.")


if __name__ == "__main__":
    asyncio.run(console_client())
