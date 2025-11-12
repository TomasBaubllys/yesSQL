import socket
import struct
import threading
import time
import os
import random

# ==================================================
# Configuration
# ==================================================
HOST = "127.0.0.1"
PORT = 8080

# ==================================================
# Command Identifiers
# ==================================================
CMD_OK = "OK"
CMD_ERR = "ERR"
CMD_GET = "GET"
CMD_SET = "SET"
CMD_REMOVE = "REMOVE"
CMD_DATA_NOT_FOUND = "DATA_NOT_FOUND"

COMMAND_CODES = {
    0: CMD_OK,
    1: CMD_ERR,
    2: CMD_GET,
    3: CMD_SET,
    8: CMD_REMOVE,
    9: CMD_DATA_NOT_FOUND
}
COMMAND_IDS = {v: k for k, v in COMMAND_CODES.items()}

class Color:
    RED = "\033[91m"
    GREEN = "\033[92m"
    RESET = "\033[0m"


# ==================================================
# Command Builders
# ==================================================
def build_set_command(key: str, value: str) -> bytes:
    command_code = COMMAND_IDS[CMD_SET]
    key_bytes = key.encode()
    value_bytes = value.encode()
    key_len = len(key_bytes)
    value_len = len(value_bytes)
    total_len = 8 + 8 + 2 + 2 + key_len + 4 + value_len

    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", key_len)
    msg += key_bytes
    msg += struct.pack(">I", value_len)
    msg += value_bytes
    return msg


def build_get_command(key: str) -> bytes:
    command_code = COMMAND_IDS[CMD_GET]
    key_bytes = key.encode()
    key_len = len(key_bytes)
    total_len = 8 + 8 + 2 + 2 + key_len
    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", key_len)
    msg += key_bytes
    return msg


def build_remove_command(key: str) -> bytes:
    command_code = COMMAND_IDS[CMD_REMOVE]
    key_bytes = key.encode()
    key_len = len(key_bytes)
    total_len = 8 + 8 + 2 + 2 + key_len
    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", key_len)
    msg += key_bytes
    return msg


# ==================================================
# Helpers
# ==================================================
def random_key():
    return os.urandom(random.randint(16, 128)).hex()

def random_value():
    return os.urandom(random.randint(1, 1024)).hex()


# ==================================================
# Response Parser
# ==================================================
def parse_response(data: bytes):
    if len(data) < 18:
        return None, {}
    total_len, num_elements, command_code = struct.unpack_from(">QQH", data, 0)
    pos = 8 + 8 + 2
    cmd_name = COMMAND_CODES.get(command_code, f"UNKNOWN({command_code})")

    results = {}
    for _ in range(num_elements):
        if pos + 2 > len(data):
            break
        key_len = struct.unpack_from(">H", data, pos)[0]
        pos += 2
        key = data[pos:pos + key_len].decode()
        pos += key_len
        if pos >= len(data):
            results[key] = None
            continue
        value_len = struct.unpack_from(">I", data, pos)[0]
        pos += 4
        value = data[pos:pos + value_len].decode()
        pos += value_len
        results[key] = value

    return cmd_name, results


# ==================================================
# recv_exact with timeout
# ==================================================
def recv_exact(sock: socket.socket, timeout: float = 20.0):
    sock.settimeout(timeout)
    start_time = time.time()

    header = b""
    while len(header) < 8:
        if time.time() - start_time > timeout:
            raise TimeoutError("Timed out waiting for header.")
        chunk = sock.recv(8 - len(header))
        if not chunk:
            raise ConnectionError("Connection closed while reading header")
        header += chunk

    total_len = struct.unpack(">Q", header)[0]
    data = header
    while len(data) < total_len:
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timed out waiting for full {total_len} bytes.")
        chunk = sock.recv(total_len - len(data))
        if not chunk:
            raise ConnectionError(f"Connection closed early ({len(data)}/{total_len} bytes)")
        data += chunk

    sock.settimeout(None)
    return data


# ==================================================
# LOOPSET Worker
# ==================================================
import time
import socket
import random
import threading

def loopset_worker(thread_id: int, count: int, results: dict, lock: threading.Lock):
    local_stats = {
        "set_ok": 0,
        "get_match": 0,
        "get_mismatch": 0,
        "remove_ok": 0,
        "remove_not_found": 0,
    }

    local_times = {}  # record timing per phase

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))

            keys = [random_key() for _ in range(count)]
            values = [random_value() for _ in range(count)]

            # --------------------------
            # Phase 1: SET all keys
            # --------------------------
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                s.sendall(build_set_command(key, value))
                cmd, _ = parse_response(recv_exact(s))
                if cmd == CMD_OK:
                    local_stats["set_ok"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] SET {i + 1}/{count}")
            local_times["set"] = time.perf_counter() - start

            # --------------------------
            # Phase 2: GET all keys
            # --------------------------
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                s.sendall(build_get_command(key))
                cmd, data = parse_response(recv_exact(s))
                if cmd == CMD_OK and key in data:
                    if data[key] == value:
                        local_stats["get_match"] += 1
                    else:
                        local_stats["get_mismatch"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] GET {i + 1}/{count}")
            local_times["get"] = time.perf_counter() - start

            # --------------------------
            # Phase 3: REMOVE all keys
            # --------------------------
            start = time.perf_counter()
            for i, key in enumerate(keys):
                s.sendall(build_remove_command(key))
                cmd, _ = parse_response(recv_exact(s))
                if cmd == CMD_OK:
                    local_stats["remove_ok"] += 1
                elif cmd == CMD_DATA_NOT_FOUND:
                    local_stats["remove_not_found"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] REMOVE {i + 1}/{count}")
            local_times["remove"] = time.perf_counter() - start

            # --------------------------
            # Phase 4: Confirm removals (optional)
            # --------------------------
            start = time.perf_counter()
            for i, key in enumerate(keys):
                s.sendall(build_get_command(key))
                cmd, _ = parse_response(recv_exact(s))
                if cmd == CMD_DATA_NOT_FOUND:
                    local_stats["remove_not_found"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] CONFIRM REMOVE {i + 1}/{count}")
            local_times["confirm"] = time.perf_counter() - start

    except Exception as e:
        print(f"[Thread {thread_id}] Error: {e}")

    # Merge into global stats
    with lock:
        for k, v in local_stats.items():
            results[k] += v

        # Add timing
        if "timing" not in results:
            results["timing"] = []
        results["timing"].append(local_times)

# ==================================================
# Pretty Summary Printer
# ==================================================
class Color:
    RED = "\033[91m"
    GREEN = "\033[92m"
    RESET = "\033[0m"


def print_summary(global_stats, threads, count, elapsed):
    total_ops = count * threads * 4
    total_get = global_stats["get_match"] + global_stats["get_mismatch"]

    if global_stats["get_mismatch"] == 0 and total_get > 0:
        color = Color.GREEN
        status = "ALL MATCH ✅"
    else:
        color = Color.RED
        status = "MISMATCHES DETECTED ❌"

    print("\n===== LOOPSET Summary =====")
    print(f"Threads:          {threads}")
    print(f"Total cycles:     {count * threads}")
    print(f"Total operations: {total_ops}")
    print(f"Elapsed:          {elapsed:.2f}s")
    print(f"Ops/sec:          {total_ops / elapsed:.2f}")
    print("-----------------------------")
    print(f"SET OK:           {global_stats['set_ok']}")
    print(f"{color}GET match:        {global_stats['get_match']} / {total_get}  ({status}){Color.RESET}")
    print(f"GET mismatch:     {global_stats['get_mismatch']}")
    print(f"REMOVE OK:        {global_stats['remove_ok']}")
    print(f"REMOVE not found: {global_stats['remove_not_found']}")
    print("=============================\n")



# ==================================================
# Main CLI Loop
# ==================================================
def main():
    print("Connecting to server...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Connected to server. Type 'exit' to quit.")
        print("Commands: SET <key> <value>, GET <key>, REMOVE <key>, LOOPSET <count> <threads>")

        while True:
            try:
                line = input("yessql> ").strip()
                if not line:
                    continue
                if line.lower() == "exit":
                    break

                parts = line.split()
                op = parts[0].upper()

                # --- Standard commands ---
                if op == "SET" and len(parts) >= 3:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    s.sendall(build_set_command(key, value))
                    cmd, _ = parse_response(recv_exact(s))
                    print("✅ OK" if cmd == CMD_OK else f"❌ {cmd}")

                elif op == "GET" and len(parts) == 2:
                    key = parts[1]
                    s.sendall(build_get_command(key))
                    cmd, results = parse_response(recv_exact(s))
                    if cmd == CMD_OK and results:
                        for k, v in results.items():
                            print(f"✅ {k} = {v}")
                    elif cmd == CMD_DATA_NOT_FOUND:
                        print("❌ NOT_FOUND")
                    else:
                        print(f"⚠️ Unexpected: {cmd}")

                elif op == "REMOVE" and len(parts) == 2:
                    key = parts[1]
                    s.sendall(build_remove_command(key))
                    cmd, _ = parse_response(recv_exact(s))
                    print("✅ REMOVED" if cmd == CMD_OK else f"❌ {cmd}")

                # --- LOOPSET benchmark ---
                elif op == "LOOPSET" and len(parts) == 3:
                    count = int(parts[1])
                    threads = int(parts[2])
                    print(f"🚀 Running LOOPSET with {threads} threads × {count} iterations each...")

                    global_stats = {
                        "set_ok": 0,
                        "get_match": 0,
                        "get_mismatch": 0,
                        "remove_ok": 0,
                        "remove_not_found": 0,
                    }
                    lock = threading.Lock()
                    thread_list = []

                    start_time = time.time()

                    for t in range(threads):
                        th = threading.Thread(target=loopset_worker, args=(t, count, global_stats, lock))
                        th.start()
                        thread_list.append(th)

                    for th in thread_list:
                        th.join()

                    elapsed = time.time() - start_time

                    # 🔥 Clean, colorized summary
                    print_summary(global_stats, threads, count, elapsed)
                else:
                    print("Usage: SET <key> <value> | GET <key> | REMOVE <key> | LOOPSET <count> <threads>")

            except Exception as e:
                print(f"Error: {e}")
                break


# ==================================================
# Entry Point
# ==================================================
if __name__ == "__main__":
    main()
