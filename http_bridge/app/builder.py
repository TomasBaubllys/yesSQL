import struct
from . import constants as cnt
from . import command_codes

#! -> big-endian
# H = uint16 // 2
# Q = uint64 // 8
# I = uint32 // 4

def build_get_request(key: str) -> bytes:
    
    key_bytes = key.encode("utf-8")
    # <uint16_t> command number
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET)
    body += struct.pack("!H", len(key_bytes)) + key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS

    header = struct.pack("!QQ", total_length, num_elements)

    #print(header + body)

    return header + body

def build_set_request(key: str, value: str) -> bytes:
    key_bytes = key.encode("utf-8")
    value_bytes = value.encode("utf-8")

    body = struct.pack("!H", command_codes.COMMAND_CODE_SET)
    body += struct.pack("!H", len(key_bytes)) + key_bytes
    body += struct.pack("!I", len(value_bytes)) + value_bytes

    num_elements = 1

    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS

    header = struct.pack("!QQ", total_length, num_elements)
    #print(header + body)


    return header + body


def build_remove(key: str) -> bytes:
    key_bytes = key.encode("utf-8")
    body = struct.pack("!H", command_codes.COMMAND_CODE_REMOVE)
    body += struct.pack("!H", len(key)) + key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def create_cursor(name:str, key: str='0'):
    key_bytes = key.encode("utf-8")
    name_bytes = name.encode("utf-8")
    body = struct.pack("!H", command_codes.CREATE_CURSOR)
    body += struct.pack("!B", len(name_bytes)) + name_bytes
    if key == "0":
        body += struct.pack("!H", 0)
    else:
        body += struct.pack("!H", len(key_bytes)) + key_bytes

    number_of_elements = 1
    
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, number_of_elements)

    return header + body

def delete_cursor(name: str):
    name_bytes = name.encode("utf-8")

    # total length = 8 + 8 + 2 + 1 + len(name_bytes)
    total_length = 8 + 8 + 2 + 1 + len(name_bytes)
    number_of_elements = 1

    # Header part (total length + number_of_elements)
    header = struct.pack("!Q", total_length)
    header += struct.pack("!Q", number_of_elements)

    # Body part (command + name)
    body = struct.pack("!H", command_codes.DELETE_CURSOR)  # 2 bytes
    body += struct.pack("!B", len(name_bytes)) + name_bytes  # 1 + len(name_bytes)

    return header + body


#! -> big-endian
# H = uint16 // 2
# Q = uint64 // 8
# I = uint32 // 4
def build_get_ff(cursor: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    reserved = b"\x00" * 6
    amount_bytes = struct.pack("!H", int(amount));


    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_FF)
    body += struct.pack("!B", len(cursor_bytes)) + cursor_bytes


    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + reserved + amount_bytes

    return header + body


def build_get_fb(cursor: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    amount_bytes = struct.pack("!H", int(amount));
    reserved = b"\x00" * 6

    
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_FB)
    body += struct.pack("!B", len(cursor_bytes)) + cursor_bytes


    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + reserved + amount_bytes

    return header + body

def build_get_keys(cursor: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    amount_bytes = struct.pack("!H", int(amount));
    reserved = b"\x00" * 6

    
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_KEYS)
    body += struct.pack("!B", len(cursor_bytes)) + cursor_bytes

    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + reserved + amount_bytes

    return header + body


def build_get_keys_prefix(cursor: str, prefix: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    amount_bytes = struct.pack("!H", int(amount))
    prefix_bytes = prefix.encode("utf-8")
    reserved = b"\x00" * 6

    
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_KEYS_PREFIX)
    body += struct.pack("!B", len(cursor_bytes)) + cursor_bytes
    body += struct.pack("!H", len(prefix_bytes)) + prefix_bytes

    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + reserved + amount_bytes

    return header + body








