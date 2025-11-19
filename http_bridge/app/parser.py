import struct
from command_codes import COMMAND_CODE_OK, COMMAND_CODE_ERR, COMMAND_CODE_DATA_NOT_FOUND
from constants import COMMAND_LENGTH_COMMAND_MESSAGE, COMMAND_LENGTH_KEY_LENGTH, COMMAND_LENGTH_TOTAL_MESSAGE, COMMAND_LENGTH_VALUE_LENGTH, COMMAND_LENGTH_NUM_ELEMENTS
#! -> big-endian
# H = uint16
# Q = uint64
# I = uint32


def handle_ok(raw_data: bytes) -> dict:
    offset = 0
    total_length, num_elements = struct.unpack_from("!QQ", raw_data, offset)
    offset += COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS

    command_number = struct.unpack_from("!H", raw_data, offset)[0]
    offset += COMMAND_LENGTH_COMMAND_MESSAGE

    data_dict = {}

    for _ in range(num_elements):
        key_len = struct.unpack_from("!H", raw_data, offset)[0]
        offset += COMMAND_LENGTH_KEY_LENGTH

        key_bytes = raw_data[offset:offset+key_len]
        key = key_bytes.decode("utf-8")
        offset += key_len

        value_len = struct.unpack_from("!I", raw_data, offset)[0]
        offset += COMMAND_LENGTH_VALUE_LENGTH

        value_bytes = raw_data[offset:offset+value_len]
        value = value_bytes.decode("utf-8")
        offset += value_len

        data_dict[key] = value

    return {"status": "OK", "data": data_dict}


def handle_err(raw_data: bytes) -> dict:
    #skip total length, num elemends, command number
    #offset = COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS + COMMAND_LENGTH_COMMAND_MESSAGE
    #error_len = struct.unpack_from("!H", raw_data, offset)[0]
    #offset += COMMAND_LENGTH_KEY_LENGTH
    #error_bytes = raw_data[offset:offset+error_len]
    #error_msg = error_bytes.decode("utf-8")
    return {"status": "ERROR"}

def handle_not_found(raw_data: bytes) -> dict:
    return {"status": "DATA NOT FOUND"}

COMMAND_HANDLERS = {
    COMMAND_CODE_OK: handle_ok,
    COMMAND_CODE_ERR: handle_err,
    COMMAND_CODE_DATA_NOT_FOUND: handle_not_found,
}


def dispatch(data: bytes) -> dict:

    command_number = struct.unpack_from("!H", data, 16)[0]  
    handler = COMMAND_HANDLERS.get(command_number)



    if handler:
        return handler(data)
    else:
        return {"status": f"UNKNOWN:{command_number})"}
