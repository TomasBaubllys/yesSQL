#ifndef YSQL_COMMANDS_H_INCLUDED
#define YSQL_COMMANDS_H_INCLUDED

#define COMMAND_OK "OK"// OK
#define COMMAND_ERR "ERR" // NOT OK
#define COMMAND_GET "GET" // GET <KEY>
#define COMMAND_SET "SET" // SET <KEY> <VALUE>
#define COMMAND_GET_KEYS "GET_KEYS" // GET_KEYS 
#define COMMAND_GET_KEYS_PREFIX "GET_KEYS_PREFIX" // GET_KEYS_PREFIX <PREFIX> 
#define COMMAND_GET_FF "GET_FF" // GET_FF <KEY>
#define COMMAND_GET_FB "GET_FB" // GET_FB <KEY>
#define COMMAND_REMOVE "REMOVE" // REMOVE <KEY>


/* SET COMMAND EXCHANGE ()

   The JS API sends a single, length-prefixed command message:
        ^L
        *3
        $3
        SET
        $<key_length>
        <key>
        $<value_length>
        <value>

    The format:
        ^<LEN>             - Length of the entire message (including delimeters) (uint64_t)
        *<N>               - Number of elements in the array (here: 3) MAX() (uint64_t)
        $<len>             - Length of the following string (binary-safe) (uint64_t)
        <string>           - Actual data (command / key / value) 

   Example:
        ^L\r\n
       *3\r\n
       $3\r\n
       SET\r\n
       $3\r\n
       foo\r\n
       $5\r\n
       hello\r\n

   Server processing:
       1. Reads one complete RESP array from the socket.
       2. Extracts the command (e.g., "SET"), key, and value.
       3. Finds the partition responsible for <key>.
       4. Forwards the same RESP command to that partition.
       5. Partition executes it and replies with:
            +OK\r\n      → success
            -ERR <msg>\r\n  → error

   JS API receives either "+OK" or "-ERR <reason>".
*/

typedef enum Command_Code {
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
    COMMAND_CODE_DATA_ARRAY_SIZE,
} Command_Code;

#endif // YSQL_COMMANDS_H_INCLUDED