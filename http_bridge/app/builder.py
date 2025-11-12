import struct
from . import constants as cnt

def build_get_request(key: str) -> bytes:
    
    key_bytes = key.encode("utf-8")
    # <uint16_t> command number
    body = struct.pack("!H", cnt.COMMAND_CODE_GET)
    body += struct.pack("!H", len(key_bytes)) + key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS

    header = struct.pack("!QQ", total_length, num_elements)

    print(header + body)

    return header + body

def build_set_request(key: str, value: str) -> bytes:
    key_bytes = key.encode("utf-8")
    value_bytes = value.encode("utf-8")

    body = struct.pack("!H", cnt.COMMAND_CODE_SET)
    body += struct.pack("!H", len(key_bytes)) + key_bytes
    body += struct.pack("!I", len(value_bytes)) + value_bytes

    num_elements = 1

    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS

    header = struct.pack("!QQ", total_length, num_elements)
    print(header + body)


    return header + body

def build_get_keys_request() -> bytes:
    body = struct.pack("!H", cnt. COMMAND_CODE_GET_KEYS)

    num_elements = 0
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS

    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def build_get_keys_prefix(prefix: str) -> bytes:
    prefix_bytes = prefix.encode("utf-8")
    body = struct.pack("!H", cnt. COMMAND_CODE_GET_KEYS_PREFIX)
    body += struct.pack("!H", len(prefix_bytes)) + prefix_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def build_get_ff(start_key: str = "") -> bytes:
    start_key_bytes = start_key.encode("utf-8")
    body = struct.pack("!H", cnt. COMMAND_CODE_GET_FF)
    body += struct.pack("!H", len(start_key)) + start_key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def build_get_Fb(start_key: str = "") -> bytes:
    start_key_bytes = start_key.encode("utf-8")
    body = struct.pack("!H", cnt. COMMAND_CODE_GET_FB)
    body += struct.pack("!H", len(start_key)) + start_key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def build_remove(key: str) -> bytes:
    key_bytes = key.encode("utf-8")
    body = struct.pack("!H", cnt. COMMAND_CODE_REMOVE)
    body += struct.pack("!H", len(key)) + key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body




