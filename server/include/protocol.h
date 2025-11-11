#ifndef YSQL_PROTOCOL_H_INCLUDED
#define YSQL_PROTOCOL_H_INCLUDED

#include "commands.h"
#include "../../lsm_tree/include/entry.h"

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
#else
    #error "Unknown system endianess!"
#endif

using protocol_msg_len_t = uint64_t;
#define protocol_msg_len_hton(x) htonll(x)
#define protocol_msg_len_ntoh(x) ntohll(x)

using protocol_array_len_type = uint64_t;
#define protocol_arr_len_hton(x) htonll(x)
#define protocol_arr_len_ntoh(x) ntohll(x)

using protocol_key_len_type = key_len_type;
#define protocol_key_len_hton(x) htons(x)
#define protocol_key_len_ntoh(x) ntohs(x)

using protocol_value_len_type = value_len_type;
#define protocol_value_len_hton(x) htonl(x)
#define protocol_value_len_ntoh(x) ntohl(x)

using protocol_id_t = uint64_t;
#define protocol_id_hton(x) htonll(x)
#define protocol_id_ntoh(x) ntohll(x)

#define PROTOCOL_COMMAND_NUMBER_POS_NOCID (sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_type))
#define PROTOCOL_COMMAND_NUMBER_POS (sizeof(protocol_id_t) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_type))

#define PROTOCOL_FIRST_KEY_LEN_POS_NOCID (PROTOCOL_COMMAND_NUMBER_POS_NOCID + sizeof(command_code_type))
#define PROTOCOL_FIRST_KEY_LEN_POS (PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_type))

/* General command exchange
 * <(uint64_t) length of the whole message>
 * [uint64_t] client_id - only for the internal messages
 * <(uint64_t) number of elements in the message>
 * <(uint16_t) command number>
 *
 * [<(uint16_t) key lenght> (if command contains the key, key must be limited to the length of uint16_t, however protocol is made to handle extensions)
 * <string key>]
 * [<(uint32_t) value length>
 * <value string>]
 *
 *
 * */

#endif // YSQL_PROTOCOL_H_INCLUDED 
