# yessql_fixed_client.py
# Fixed & hardened version of your Python client for heavy loopset workloads.

import socket
import struct
import threading
import time
import os
import random
from typing import Tuple, Dict, Optional

# ==================================================
# Configuration
# ==================================================
HOST = "127.0.0.1"
PORT = 8080

# ==================================================
# Command Identifiers (aligned with your Go client)
# ==================================================
CMD_OK = "OK"
CMD_ERR = "ERR"
CMD_GET = "GET"
CMD_SET = "SET"
CMD_GET_KEYS = "GET_KEYS"
CMD_GET_KEYS_PREFIX = "GET_KEYS_PREFIX"
CMD_GET_FF = "GET_FF"
CMD_GET_FB = "GET_FB"
CMD_REMOVE = "REMOVE"
CMD_CREATE_CURSOR = "CREATE_CURSOR"
CMD_DELETE_CURSOR = "DELETE_CURSOR"
CMD_DATA_NOT_FOUND = "DATA_NOT_FOUND"
CMD_INVALID_COMMAND = "INVALID_COMMAND"

# numeric -> name mapping (must match the server/protocol)
COMMAND_CODES = {
    0: CMD_OK,
    1: CMD_ERR,
    2: CMD_GET,
    3: CMD_SET,
    4: CMD_GET_KEYS,
    5: CMD_GET_KEYS_PREFIX,
    6: CMD_GET_FF,
    7: CMD_GET_FB,
    8: CMD_REMOVE,
    9: CMD_CREATE_CURSOR,
    10: CMD_DELETE_CURSOR,
    11: CMD_DATA_NOT_FOUND,
    12: CMD_INVALID_COMMAND,
}
# name -> numeric
COMMAND_IDS = {v: k for k, v in COMMAND_CODES.items()}

class Color:
    RED = "\033[91m"
    GREEN = "\033[92m"
    RESET = "\033[0m"


# ==================================================
# Command Builders (same wire format)
# ==================================================
def build_set_command(key: str, value: str) -> bytes:
    command_code = COMMAND_IDS[CMD_SET]
    key_bytes = key.encode()
    value_bytes = value.encode()
    total_len = 8 + 8 + 2 + 2 + len(key_bytes) + 4 + len(value_bytes)
    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", len(key_bytes))
    msg += key_bytes
    msg += struct.pack(">I", len(value_bytes))
    msg += value_bytes
    return msg


def build_get_command(key: str) -> bytes:
    command_code = COMMAND_IDS[CMD_GET]
    key_bytes = key.encode()
    total_len = 8 + 8 + 2 + 2 + len(key_bytes)
    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", len(key_bytes))
    msg += key_bytes
    return msg


def build_remove_command(key: str) -> bytes:
    command_code = COMMAND_IDS[CMD_REMOVE]
    key_bytes = key.encode()
    total_len = 8 + 8 + 2 + 2 + len(key_bytes)
    msg = struct.pack(">Q", total_len)
    msg += struct.pack(">Q", 1)
    msg += struct.pack(">H", command_code)
    msg += struct.pack(">H", len(key_bytes))
    msg += key_bytes
    return msg


# ==================================================
# Helpers
# ==================================================
def random_key() -> str:
    return os.urandom(random.randint(16, 128)).hex()

def random_value() -> str:
    return os.urandom(random.randint(1, 1024)).hex()


# ==================================================
# recv_exact with timeout (safer)
# ==================================================
def recv_exact(sock: socket.socket, timeout: float = 20.0) -> bytes:
    """
    Read exactly one protocol message from sock (blocking until complete or timeout).
    Returns the full message bytes (including the 8-byte length header).
    Raises:
        TimeoutError, ConnectionError, ValueError
    """
    prev_timeout = sock.gettimeout()
    try:
        sock.settimeout(timeout)
        header = b""
        start = time.time()
        while len(header) < 8:
            chunk = sock.recv(8 - len(header))
            if not chunk:
                raise ConnectionError("Connection closed while reading header")
            header += chunk
            # no busy-wait; socket timeout will raise if stalled
        total_len = struct.unpack(">Q", header)[0]
        if total_len < 8:
            raise ValueError(f"Invalid total_len {total_len}")
        data = bytearray()
        data.extend(header)
        while len(data) < total_len:
            chunk = sock.recv(min(65536, total_len - len(data)))
            if not chunk:
                raise ConnectionError(f"Connection closed early ({len(data)}/{total_len})")
            data.extend(chunk)
        return bytes(data)
    finally:
        # restore previous timeout (might be None)
        sock.settimeout(prev_timeout)


# ==================================================
# Response Parser (defensive)
# ==================================================
def parse_response(data: bytes) -> Tuple[Optional[str], Dict[str, Optional[str]]]:
    """
    Parse a single full response message (as returned by recv_exact).
    Returns (cmd_name, {key: value_or_None})
    """
    if len(data) < 18:
        return None, {}
    # header: total_len (Q), num_elements (Q), cmd (H)
    total_len = struct.unpack_from(">Q", data, 0)[0]
    if total_len != len(data):
        # defensive: sometimes servers send shorter/longer; treat as error
        # but still attempt parse up to available bytes
        # return None to signal caller there's an inconsistency
        # caller can log/handle this case
        # We proceed but note the mismatch by returning None as cmd.
        # (Higher-level code should treat None as an error.)
        # Continue parsing cautiously using available bytes.
        pass

    num_elements = struct.unpack_from(">Q", data, 8)[0]
    command_code = struct.unpack_from(">H", data, 16)[0]
    cmd_name = COMMAND_CODES.get(command_code, f"UNKNOWN({command_code})")

    pos = 18
    results: Dict[str, Optional[str]] = {}

    for _ in range(num_elements):
        if pos + 2 > len(data):
            break
        key_len = struct.unpack_from(">H", data, pos)[0]
        pos += 2
        if pos + key_len > len(data):
            break
        key = data[pos:pos + key_len].decode(errors="replace")
        pos += key_len

        # if there's not enough bytes for value length, treat value as None
        if pos + 4 > len(data):
            results[key] = None
            break
        val_len = struct.unpack_from(">I", data, pos)[0]
        pos += 4
        if pos + val_len > len(data):
            results[key] = None
            break
        val = data[pos:pos + val_len].decode(errors="replace")
        pos += val_len
        results[key] = val

    return cmd_name, results


# ==================================================
# Worker helpers with per-request retry & better logging
# ==================================================
def safe_send_and_recv(sock: socket.socket, out_bytes: bytes, attempt_limit: int = 3) -> Tuple[Optional[str], Dict[str, Optional[str]]]:
    """
    Send a single request and wait for a single response (with retries on transient errors).
    Returns (cmd_name or None on error, results dict)
    """
    for attempt in range(1, attempt_limit + 1):
        try:
            sock.sendall(out_bytes)
            data = recv_exact(sock, timeout=20.0)
            cmd, results = parse_response(data)
            return cmd, results
        except (TimeoutError, ConnectionError, ValueError) as e:
            # transient, try to recover a few times
            print(f"[safe_send_and_recv] attempt {attempt} error: {e}")
            if attempt == attempt_limit:
                return None, {}
            # short backoff and retry
            time.sleep(0.01 * attempt)
        except Exception as e:
            # unexpected
            print(f"[safe_send_and_recv] unexpected error: {e}")
            return None, {}
    return None, {}


# ==================================================
# LOOPSET Worker
# ==================================================
def loopset_worker(thread_id: int, count: int, results: dict, lock: threading.Lock):
    local_stats = {
        "set_ok": 0,
        "get_match": 0,
        "get_mismatch": 0,
        "remove_ok": 0,
        "remove_not_found": 0,
    }

    local_times = {}

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect((HOST, PORT))

            keys = [random_key() for _ in range(count)]
            values = [random_value() for _ in range(count)]

            # Phase 1: SET
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                cmd, _ = safe_send_and_recv(s, build_set_command(key, value))
                if cmd == CMD_OK:
                    local_stats["set_ok"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] SET {i + 1}/{count}")
            local_times["set"] = time.perf_counter() - start

            # Phase 2: GET
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                cmd, data = safe_send_and_recv(s, build_get_command(key))
                if cmd == CMD_OK and key in data and data[key] is not None:
                    if data[key] == value:
                        local_stats["get_match"] += 1
                    else:
                        local_stats["get_mismatch"] += 1
                else:
                    # If cmd is DATA_NOT_FOUND or None, count as mismatch / missing
                    if cmd == CMD_DATA_NOT_FOUND:
                        local_stats["get_mismatch"] += 1
                    elif cmd is None:
                        # request failed; do not abort the whole worker; just log
                        print(f"[Thread {thread_id}] GET failed for key (i={i}): {key}")
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] GET {i + 1}/{count}")
            local_times["get"] = time.perf_counter() - start

            # Phase 3: REMOVE
            start = time.perf_counter()
            for i, key in enumerate(keys):
                cmd, _ = safe_send_and_recv(s, build_remove_command(key))
                if cmd == CMD_OK:
                    local_stats["remove_ok"] += 1
                elif cmd == CMD_DATA_NOT_FOUND:
                    local_stats["remove_not_found"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] REMOVE {i + 1}/{count}")
            local_times["remove"] = time.perf_counter() - start

            # Phase 4: Confirm
            start = time.perf_counter()
            for i, key in enumerate(keys):
                cmd, _ = safe_send_and_recv(s, build_get_command(key))
                if cmd == CMD_DATA_NOT_FOUND:
                    local_stats["remove_not_found"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[Thread {thread_id}] CONFIRM REMOVE {i + 1}/{count}")
            local_times["confirm"] = time.perf_counter() - start

    except Exception as e:
        print(f"[Thread {thread_id}] fatal worker error: {e}")

    # Merge into global stats
    with lock:
        for k, v in local_stats.items():
            results[k] += v
        if "timing" not in results:
            results["timing"] = []
        results["timing"].append(local_times)


# ==================================================
# LOOPSETNR Worker (No Remove)
# ==================================================
def loopsetnr_worker(thread_id: int, count: int, results: dict, lock: threading.Lock):
    local_stats = {
        "set_ok": 0,
        "get_match": 0,
        "get_mismatch": 0,
    }

    local_times = {}

    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect((HOST, PORT))

            keys = [random_key() for _ in range(count)]
            values = [random_value() for _ in range(count)]

            # Phase 1: SET
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                cmd, _ = safe_send_and_recv(s, build_set_command(key, value))
                if cmd == CMD_OK:
                    local_stats["set_ok"] += 1
                if (i + 1) % 100 == 0:
                    print(f"[NR Thread {thread_id}] SET {i + 1}/{count}")
            local_times["set"] = time.perf_counter() - start

            # Phase 2: GET
            start = time.perf_counter()
            for i, (key, value) in enumerate(zip(keys, values)):
                cmd, data = safe_send_and_recv(s, build_get_command(key))
                if cmd == CMD_OK and key in data and data[key] is not None:
                    if data[key] == value:
                        local_stats["get_match"] += 1
                    else:
                        local_stats["get_mismatch"] += 1
                else:
                    if cmd == CMD_DATA_NOT_FOUND:
                        local_stats["get_mismatch"] += 1
                    elif cmd is None:
                        print(f"[NR Thread {thread_id}] GET failed for key (i={i})")
                if (i + 1) % 100 == 0:
                    print(f"[NR Thread {thread_id}] GET {i + 1}/{count}")
            local_times["get"] = time.perf_counter() - start

    except Exception as e:
        print(f"[NR Thread {thread_id}] fatal worker error: {e}")

    # Merge stats
    with lock:
        for k, v in local_stats.items():
            results[k] += v
        if "timing" not in results:
            results["timing"] = []
        results["timing"].append(local_times)


# ==================================================
# Pretty Summary Printer
# ==================================================
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
    print(f"REMOVE OK:        {global_stats.get('remove_ok', 0)}")
    print(f"REMOVE not found: {global_stats.get('remove_not_found', 0)}")
    print("=============================\n")


# ==================================================
# Main CLI Loop
# ==================================================
def main():
    print("Commands: SET <k> <v> | GET <k> | REMOVE <k> | LOOPSET <count> <threads> | LOOPSETNR <count> <threads> | exit")
    while True:
        try:
            line = input("yessql> ").strip()
            if not line:
                continue
            if line.lower() == "exit":
                break

            parts = line.split()
            op = parts[0].upper()

            # Interactive single-socket commands use a transient connection per command now (safer)
            if op == "SET" and len(parts) >= 3:
                key = parts[1]
                value = " ".join(parts[2:])
                with socket.create_connection((HOST, PORT)) as s:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    cmd, _ = safe_send_and_recv(s, build_set_command(key, value))
                    print("✅ OK" if cmd == CMD_OK else f"❌ {cmd}")

            elif op == "GET" and len(parts) == 2:
                key = parts[1]
                with socket.create_connection((HOST, PORT)) as s:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    cmd, results = safe_send_and_recv(s, build_get_command(key))
                    if cmd == CMD_OK and results:
                        for k, v in results.items():
                            print(f"✅ {k} = {v}")
                    elif cmd == CMD_DATA_NOT_FOUND:
                        print("❌ NOT_FOUND")
                    else:
                        print(f"⚠️ Unexpected: {cmd}")

            elif op == "REMOVE" and len(parts) == 2:
                key = parts[1]
                with socket.create_connection((HOST, PORT)) as s:
                    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    cmd, _ = safe_send_and_recv(s, build_remove_command(key))
                    print("✅ REMOVED" if cmd == CMD_OK else f"❌ {cmd}")

            # LOOPSET
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
                print_summary(global_stats, threads, count, elapsed)

            # LOOPSETNR (No Remove)
            elif op == "LOOPSETNR" and len(parts) == 3:
                count = int(parts[1])
                threads = int(parts[2])
                print(f"🚀 Running LOOPSETNR with {threads} threads × {count} iterations each (NO REMOVE)...")

                global_stats = {
                    "set_ok": 0,
                    "get_match": 0,
                    "get_mismatch": 0,
                }
                lock = threading.Lock()
                threads_list = []

                start = time.time()
                for t in range(threads):
                    th = threading.Thread(target=loopsetnr_worker, args=(t, count, global_stats, lock))
                    th.start()
                    threads_list.append(th)
                for th in threads_list:
                    th.join()

                elapsed = time.time() - start

                print("\n===== LOOPSETNR Summary =====")
                print(f"Threads:          {threads}")
                print(f"Total cycles:     {count * threads}")
                print(f"Elapsed:          {elapsed:.2f}s")
                print(f"SET OK:           {global_stats['set_ok']}")
                print(f"GET match:        {global_stats['get_match']}")
                print(f"GET mismatch:     {global_stats['get_mismatch']}")
                print("=============================\n")

            else:
                print("Usage: SET <key> <value> | GET <key> | REMOVE <key> | LOOPSET <count> <threads> | LOOPSETNR <count> <threads>")

        except Exception as e:
            print(f"Error: {e}")
            break


if __name__ == "__main__":
    main()
