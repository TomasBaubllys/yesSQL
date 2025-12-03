import struct
from fastapi.responses import JSONResponse
import json

from command_codes import (
    COMMAND_CODE_OK,
    COMMAND_CODE_ERR,
    COMMAND_CODE_DATA_NOT_FOUND,
    COMMAND_CODE_GET_KEYS,
    COMMAND_CODE_GET_KEYS_PREFIX,
)

from constants import (
    COMMAND_LENGTH_COMMAND_MESSAGE,
    COMMAND_LENGTH_KEY_LENGTH,
    COMMAND_LENGTH_TOTAL_MESSAGE,
    COMMAND_LENGTH_VALUE_LENGTH,
    COMMAND_LENGTH_NUM_ELEMENTS,
)

KEYS_ONLY_COMMANDS = {COMMAND_CODE_GET_KEYS, COMMAND_CODE_GET_KEYS_PREFIX}


def handle_ok(raw_data: bytes, keys_only: bool):
    offset = 0

    total_length, num_elements = struct.unpack_from("!QQ", raw_data, offset)
    offset += COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS

    command_number = struct.unpack_from("!H", raw_data, offset)[0]
    offset += COMMAND_LENGTH_COMMAND_MESSAGE

    data_list = []

    for _ in range(num_elements):
        key_len = struct.unpack_from("!H", raw_data, offset)[0]
        offset += COMMAND_LENGTH_KEY_LENGTH

        key_bytes = raw_data[offset:offset+key_len]
        key = key_bytes.decode("utf-8")
        offset += key_len

        if keys_only:
            data_list.append({"key": key})
        else:
            value_len = struct.unpack_from("!I", raw_data, offset)[0]
            offset += COMMAND_LENGTH_VALUE_LENGTH

            value_bytes = raw_data[offset:offset+value_len]
            value = value_bytes.decode("utf-8")
            offset += value_len

            data_list.append({"key": key, "value": value})

    return JSONResponse({"status": "OK", "data": data_list})


def handle_err(raw_data: bytes, keys_only: bool):
    offset = COMMAND_LENGTH_TOTAL_MESSAGE + COMMAND_LENGTH_NUM_ELEMENTS + COMMAND_LENGTH_COMMAND_MESSAGE
    error_code = struct.unpack_from("!H", raw_data, offset)[0]
    return JSONResponse({"status": "ERROR", "code": error_code})


def handle_not_found(raw_data: bytes, keys_only: bool):
    return JSONResponse({"status": "DATA NOT FOUND"})


COMMAND_HANDLERS = {
    COMMAND_CODE_OK: handle_ok,
    COMMAND_CODE_ERR: handle_err,
    COMMAND_CODE_DATA_NOT_FOUND: handle_not_found,
}


def dispatch(data: bytes, keys_only: bool = False):
    command_number = struct.unpack_from("!H", data, 16)[0]
    handler = COMMAND_HANDLERS.get(command_number)

    if handler:
        return handler(data, keys_only)

    return JSONResponse({"status": f"UNKNOWN:{command_number}"})