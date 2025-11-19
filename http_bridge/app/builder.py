import struct
import constants as cnt
import command_codes

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

def build_get_keys_request() -> bytes:
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_KEYS)

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
    body = struct.pack("!H", command_codes. COMMAND_CODE_GET_FF)
    body += struct.pack("!H", len(start_key)) + start_key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

    return header + body

def build_get_Fb(start_key: str = "") -> bytes:
    start_key_bytes = start_key.encode("utf-8")
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_FB)
    body += struct.pack("!H", len(start_key)) + start_key_bytes

    num_elements = 1
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, num_elements)

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
# CREATE/DELETE cursor
 #  uint64[msg_len]uint64[array_len]  uint16[cursor_cmd] uint8[curs_len][curs_name]uint16[key_len][key]
 #
    key_bytes = key.encode("utf-8")
    name_bytes = name.encode("utf-8")
    body = struct.pack("!H", command_codes.CREATE_CURSOR)
    body += struct.pack("!B", len(name)) + name_bytes
    if key == "0":
        body += struct.pack("!H", 0)
    else:
        body += struct.pack("!H", len(key)) + key_bytes

    number_of_elements = 0
    
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, number_of_elements)

    return header + body

def delete_cursor(name:str):
# CREATE/DELETE cursor
 #  uint64[msg_len]uint64[array_len]  uint16[cursor_cmd] uint8[curs_len][curs_name]uint16[key_len][key]
 #
    name_bytes = name.encode("utf-8")
    body = struct.pack("!H", command_codes.DELETE_CURSOR)
    body += struct.pack("!B", len(name)) + name_bytes
   

    number_of_elements = 0
    
    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!QQ", total_length, number_of_elements)

    return header + body


 #For client [msg_len][num_of_els]GET_FF/GET_FB uint8[curs_name_len][cursorname]

def get_ff(cursor: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    amount_bytes = amount.encode("utf-8")
    
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_FF)
    body += struct.pack("!B", len(cursor)) + cursor_bytes


    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + amount_bytes

    return header + body


def get_fb(cursor: str, amount: str):
    cursor_bytes = cursor.encode("utf-8")
    amount_bytes = amount.encode("utf-8")
    
    body = struct.pack("!H", command_codes.COMMAND_CODE_GET_FB)
    body += struct.pack("!B", len(cursor)) + cursor_bytes


    total_length = len(body) + cnt.COMMAND_LENGTH_TOTAL_MESSAGE + cnt.COMMAND_LENGTH_NUM_ELEMENTS
    header = struct.pack("!Q", total_length) + amount_bytes

    return header + body








