import builder as bld
import parser as prs
import constants as cnt
import socket
import traceback

HOST = "127.0.0.1"
PORT = 8080

try:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        print("Connecting to the main server...")
        s.connect((HOST, PORT))
        print("Connected!")

        s.sendall(bld.build_set_request("1", "Alio"))
        data = s.recv(4096)

        try:

            parsed_response = prs.dispatch(data)
            
            print("Parsed:", parsed_response)

        except Exception:
            print("Error while parsing response!")
            traceback.print_exc()  # prints full line number and error


        # Send a GET request for key "1"
        s.sendall(bld.build_get_request("1"))

        # Receive the response
        data = s.recv(4096)
        try:

            parsed_response = prs.dispatch(data)
            
            print("Parsed:", parsed_response)

        except Exception:
            print("Error while parsing response!")
            traceback.print_exc()  # prints full line number and error


        # Send a GET request for key "1"
        s.sendall(bld.build_remove("1"))

        # Receive the response
        data = s.recv(4096)
        try:

            parsed_response = prs.dispatch(data)
            
            print("Parsed:", parsed_response)

        except Exception:
            print("Error while parsing response!")
            traceback.print_exc()  # prints full line number and error
except ConnectionRefusedError:
    print("Failed to connect: is the server running?")
except Exception as e:
    print("Error:", e)


"""
msg = bld.build_get_request("foo")

print(msg)

total_length, num_elements = struct.unpack_from("!QQ", msg, 0)
command_number = struct.unpack_from("!H", msg, 16)[0]
key_len = struct.unpack_from("!H", msg, 18)[0]
key_bytes = msg[20:20+key_len]


print(f"Total length: {total_length}")
print(f"Num elements: {num_elements}")
print(f"Command number: {command_number}")
print(f"Key length: {key_len}")
print(f"Key bytes: {key_bytes}")
print(f"Key string: {key_bytes.decode()}")


msg = bld.build_set_request("foo", "kaka")

print(msg)

total_length, num_elements = struct.unpack_from("!QQ", msg, 0)
offset = 16
command_number = struct.unpack_from("!H", msg, offset)[0]
offset += cnt.COMMAND_LENGTH_COMMAND_MESSAGE
key_len = struct.unpack_from("!H", msg, offset)[0]
offset += cnt.COMMAND_LENGTH_KEY_LENGTH
key_bytes = msg[offset:offset+key_len]
offset += key_len
value_len = struct.unpack_from("!I", msg, offset)[0]
offset += cnt.COMMAND_LENGTH_VALUE_LENGTH
value_bytes = msg[offset:offset+value_len]


print(f"Total length: {total_length}")
print(f"Num elements: {num_elements}")
print(f"Command number: {command_number}")
print(f"Key length: {key_len}")
print(f"Key string: {key_bytes.decode()}")
print(f"Value length: {value_len}")
print(f"Value string: {value_bytes.decode()}")
"""