import socket
import struct
import random
import string
import threading
import time
import os

i = 1

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


# ==================================================
# Helpers
# ==================================================

def random_key():
    #length = random.randint(2, 128)
    return os.urandom(random.randint(2, 128)).hex()  # returns bytes
    #global i
    #i += 1
    #return str(i)

def random_value():
    return os.urandom(random.randint(1, 1024)).hex()  # 64 random bytes


# ==================================================
# recv_exact with timeout
# ==================================================
def recv_exact(sock: socket.socket, timeout: float = 5.0):
    """Receive exactly N bytes (including the 8-byte header) with a timeout."""
    sock.settimeout(timeout)
    start_time = time.time()

    # --- Read 8-byte total_len ---
    header = b""
    while len(header) < 8:
        if time.time() - start_time > timeout:
            raise TimeoutError("Timed out waiting for header.")
        try:
            chunk = sock.recv(8 - len(header))
        except socket.timeout:
            raise TimeoutError("Socket timed out waiting for header.")
        if not chunk:
            raise ConnectionError("Connection closed while reading total_len header")
        header += chunk

    total_len = struct.unpack(">Q", header)[0]
    #print(f"[recv_full] expecting {total_len} bytes total")

    # --- Read until we have total_len bytes total (header + rest) ---
    data = header
    while len(data) < total_len:
        #print(f"[DEBUG] waiting for header, already received {len(header)} bytes, elapsed {time.time() - start_time:.2f}s")
        if time.time() - start_time > timeout:
            raise TimeoutError(f"Timed out waiting for full {total_len} bytes.")
        try:
            chunk = sock.recv(total_len - len(data))
        except socket.timeout:
            raise TimeoutError(f"Socket timed out waiting for data ({len(data)}/{total_len})")
        if not chunk:
            raise ConnectionError(f"Connection closed early: got {len(data)}/{total_len} bytes")
        data += chunk
        #print(f"[recv_full] got chunk {len(chunk)} bytes, total={len(data)}/{total_len}")

    #print(f"[recv_full] DONE ({len(data)} bytes total)\n")
    sock.settimeout(None)
    return data


# ==================================================
# Thread Worker for Loop Set
# ==================================================
def worker_loop_set(count: int, thread_id: int):
    print(f"[Thread {thread_id}] Connecting to server...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print(f"[Thread {thread_id}] Connected.")

        keys = []
        sets_done = 0

        # --- SET phase ---
        try:
            for i in range(count):
                key = random_key()
                value = random_value()
                msg = build_set_command(key, value)
                s.sendall(msg)
                response = recv_exact(s)
                parse_response(response)
                keys.append((key, value))
                sets_done += 1
                time.sleep(0.01)
        except Exception as e:
            print(f"[Thread {thread_id}] Error during SET: {e}")
        finally:
            print(f"[Thread {thread_id}] SET summary: {sets_done}/{count} completed.")

        # --- GET verification ---
        success = 0
        failed = 0
        try:
            for i, (key, expected_value) in enumerate(keys, 1):
                msg = build_get_command(key)
                s.sendall(msg)
                response = recv_exact(s)
                result = parse_response(response)
                if result and key in result and result[key] == expected_value:
                    #print(key, result[key],  "expected value - ", expected_value)
                    success += 1
                else:
                    failed += 1
        except Exception as e:
            print(f"[Thread {thread_id}] Timeout or error during GET: {e}")
        finally:
            total_done = success + failed
            print(f"[Thread {thread_id}] GET summary: {success} OK, {failed} FAILED, {total_done}/{len(keys)} completed.")

        # --- REMOVE cleanup ---
        removed = 0
        try:
            for key, _ in keys:
                command_code = COMMAND_IDS[CMD_REMOVE]
                key_bytes = key.encode()
                key_len = len(key_bytes)
                total_len = 8 + 8 + 2 + 2 + key_len
                msg = struct.pack(">Q", total_len)
                msg += struct.pack(">Q", 1)
                msg += struct.pack(">H", command_code)
                msg += struct.pack(">H", key_len)
                msg += key_bytes
                s.sendall(msg)
                response = recv_exact(s)
                parse_response(response)
                removed += 1
        except Exception as e:
            print(f"[Thread {thread_id}] Error during REMOVE: {e}")
        finally:
            print(f"[Thread {thread_id}] REMOVE summary: {removed}/{len(keys)} completed.")


# ==================================================
# Response Parser
# ==================================================
def parse_response(data: bytes):
    if len(data) < 18:
        print("Invalid response length.")
        return None, None

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
# Main CLI Loop
# ==================================================
def main():
    print("Connecting to server...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOST, PORT))
        print("Connected to server. Type 'exit' to quit.")
        print("Commands: SET <key> <value>, GET <key>, REMOVE <key>, LOOP SET <count> <threads>")

        while True:
            try:
                line = input("yessql> ").strip()
                if not line:
                    continue
                if line.lower() == "exit":
                    break

                parts = line.split()
                op = parts[0].upper()

                if op == "SET" and len(parts) >= 3:
                    key = parts[1]
                    value = " ".join(parts[2:])
                    msg = build_set_command(key, value)
                    s.sendall(msg)
                    response = recv_exact(s)
                    cmd_name, results = parse_response(response)
                    print(f"Server responded: {cmd_name}")
                    if results:
                        print(results)

                elif op == "GET" and len(parts) == 2:
                    key = parts[1]
                    msg = build_get_command(key)
                    s.sendall(msg)
                    response = recv_exact(s)
                    cmd_name, results = parse_response(response)
                    print(f"Server responded: {cmd_name}")
                    if results:
                        print(results)

                elif op == "REMOVE" and len(parts) == 2:
                    key = parts[1]
                    command_code = COMMAND_IDS[CMD_REMOVE]
                    key_bytes = key.encode()
                    key_len = len(key_bytes)
                    total_len = 8 + 8 + 2 + 2 + key_len
                    msg = struct.pack(">Q", total_len)
                    msg += struct.pack(">Q", 1)
                    msg += struct.pack(">H", command_code)
                    msg += struct.pack(">H", key_len)
                    msg += key_bytes
                    s.sendall(msg)
                    response = recv_exact(s)
                    cmd_name, results = parse_response(response)
                    print(f"Server responded: {cmd_name}")
                    if results:
                        print(results)

                elif op == "LOOP" and len(parts) >= 4 and parts[1].upper() == "SET":
                    count = int(parts[2])
                    threads = int(parts[3])
                    print(f"Starting {threads} threads with {count} sets each...")
                    thread_list = []
                    for i in range(threads):
                        t = threading.Thread(target=worker_loop_set, args=(count, i + 1))
                        t.start()
                        thread_list.append(t)
                    for t in thread_list:
                        t.join()
                    print("All threads finished.")

                else:
                    print("Usage: SET <key> <value> | GET <key> | REMOVE <key> | LOOP SET <count> <threads>")

            except Exception as e:
                print("Error:", e)
                break

# ==================================================
# Entry Point
# ==================================================
if __name__ == "__main__":
    main()
