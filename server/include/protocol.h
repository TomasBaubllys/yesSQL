#ifndef YSQL_PROTOCOL_H_INCLUDED
#define YSQL_PROTOCOL_H_INCLUDED

#include "commands.h"
#include "../../lsm_tree/include/entry.h"

using protocol_message_len_type = uint64_t;
using protocol_array_type = uint64_t;
using protocol_key_len_type = key_len_type;
using procotol_data_len_type = value_len_type;

#define PROTOCOL_COMMAND_NUMBER_POS (sizeof(protocol_message_len_type) + sizeof(protocol_array_type))

#define PROTOCOL_FIRST_KEY_LEN_POS (PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_type))

using socket_t =
#ifdef _WIN32
    SOCKET;
#else
    int;
#endif

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #define htonll(x) __builtin_bswap64(x)
    #define ntohll(x) __builtin_bswap64(x)
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    #define htonll(x) (x)
    #define ntohll(x) (x)
#endif
/* General command exchange
 *
 * <(uint64_t) length of the whole message>
 * <(uint64_t) number of elements in the message>
 * <(uint16_t) command number>
 *
 * [<(uint32_t) key lenght> (if command contains the key, key must be limited to the length of uint16_t, however protocol is made to handle extensions)
 * <string key>]
 * [<(uint64_t) value length>
 * <value string>]
 *
 *
 * */

#endif // YSQL_PROTOCOL_H_INCLUDED 
