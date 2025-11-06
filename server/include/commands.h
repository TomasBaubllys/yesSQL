#ifndef YSQL_COMMANDS_H_INCLUDED
#define YSQL_COMMANDS_H_INCLUDED

#include <cstdint>

#define COMMAND_OK "OK"// OK
#define COMMAND_ERR "ERR" // NOT OK
#define COMMAND_GET "GET" // GET <KEY>
#define COMMAND_SET "SET" // SET <KEY> <VALUE>
#define COMMAND_GET_KEYS "GET_KEYS" // GET_KEYS 
#define COMMAND_GET_KEYS_PREFIX "GET_KEYS_PREFIX" // GET_KEYS_PREFIX <PREFIX> 
#define COMMAND_GET_FF "GET_FF" // GET_FF <KEY>
#define COMMAND_GET_FB "GET_FB" // GET_FB <KEY>
#define COMMAND_REMOVE "REMOVE" // REMOVE <KEY>

using command_code_type = uint16_t;
#define command_hton(x) htons(x)
#define command_ntoh(x) ntohs(x)

typedef enum Command_Code : command_code_type {
    COMMAND_CODE_OK,
    COMMAND_CODE_ERR,
    COMMAND_CODE_GET,
    COMMAND_CODE_SET,
    COMMAND_CODE_GET_KEYS,
    COMMAND_CODE_GET_KEYS_PREFIX,
    COMMAND_CODE_GET_FF,
    COMMAND_CODE_GET_FB,
    COMMAND_CODE_REMOVE,

    COMMAND_CODE_DATA_NOT_FOUND,
    INVALID_COMMAND_CODE
} Command_Code;

#endif // YSQL_COMMANDS_H_INCLUDED
