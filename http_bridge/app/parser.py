import struct
#from .command_codes import COMMAND_CODE_OK, COMMAND_CODE_ERR, COMMAND_CODE_DATA_NOT_FOUND, COMMAND_CODE_GET_KEYS, COMMAND_CODE_GET_KEYS_PREFIX
#from .constants import COMMAND_LENGTH_COMMAND_MESSAGE, COMMAND_LENGTH_KEY_LENGTH, COMMAND_LENGTH_TOTAL_MESSAGE, COMMAND_LENGTH_VALUE_LENGTH, COMMAND_LENGTH_NUM_ELEMENTS

from command_codes import COMMAND_CODE_OK, COMMAND_CODE_ERR, COMMAND_CODE_DATA_NOT_FOUND, COMMAND_CODE_GET_KEYS, COMMAND_CODE_GET_KEYS_PREFIX
from constants import COMMAND_LENGTH_COMMAND_MESSAGE, COMMAND_LENGTH_KEY_LENGTH, COMMAND_LENGTH_TOTAL_MESSAGE, COMMAND_LENGTH_VALUE_LENGTH, COMMAND_LENGTH_NUM_ELEMENTS

#! -> big-endian
# H = uint16 // 2
# Q = uint64 // 8
# I = uint32 // 4


KEYS_ONLY_COMMANDS = {COMMAND_CODE_GET_KEYS, COMMAND_CODE_GET_KEYS_PREFIX}
def handle_ok(raw_data: bytes, keys_only: bool) -> dict:
    offset = 0
    total_length, num_elements = struct.unpack_from("!QQ", raw_data, offset)
    offset += COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS

    command_number = struct.unpack_from("!H", raw_data, offset)[0]
    offset += COMMAND_LENGTH_COMMAND_MESSAGE

    data_dict = {}

    for _ in range(num_elements):
        # 1. Parse Key Length
        key_len = struct.unpack_from("!H", raw_data, offset)[0]
        offset += COMMAND_LENGTH_KEY_LENGTH

        # 2. Parse Key
        key_bytes = raw_data[offset:offset+key_len]
        key = key_bytes.decode("utf-8")
        offset += key_len

        # 3. Check if we should parse Value
        if keys_only:
            # For GET_KEYS, there is no value. 
            # We assign an empty string or None, depending on your preference.
            data_dict[key] = "" 
        else:
            # Standard SET/GET/GET_FF/GET_FB response: Parse Value Length + Value
            value_len = struct.unpack_from("!I", raw_data, offset)[0]
            offset += COMMAND_LENGTH_VALUE_LENGTH

            value_bytes = raw_data[offset:offset+value_len]
            value = value_bytes.decode("utf-8")
            offset += value_len

            data_dict[key] = value

    return {"status": "OK", "data": data_dict}

def handle_err(raw_data: bytes, keys_only: bool) -> dict:
    #skip total length, num elemends, command number
    offset = COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS + COMMAND_LENGTH_COMMAND_MESSAGE
    error_code = struct.unpack_from("!H", raw_data, offset)[0]
    return {"status": "ERROR", "code": error_code}

def handle_not_found(raw_data: bytes, keys_only: bool) -> dict:
    return {"status": "DATA NOT FOUND"}

COMMAND_HANDLERS = {
    COMMAND_CODE_OK: handle_ok,
    COMMAND_CODE_ERR: handle_err,
    COMMAND_CODE_DATA_NOT_FOUND: handle_not_found,
}


def dispatch(data: bytes, keys_only: bool=False) -> dict:
    command_number = struct.unpack_from("!H", data, 16)[0]  
    handler = COMMAND_HANDLERS.get(command_number)



    if handler:
        return handler(data, keys_only)
    else:
        return {"status": f"UNKNOWN:{command_number})"}
