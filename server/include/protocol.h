#ifndef YSQL_PROTOCOL_H_INCLUDED
#define YSQL_PROTOCOL_H_INCLUDED

#include "commands.h"
#include "../../lsm_tree/include/entry.h"
#include "arpa/inet.h"

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

using protocol_array_len_t = uint64_t;
#define protocol_arr_len_hton(x) htonll(x)
#define protocol_arr_len_ntoh(x) ntohll(x)

using protocol_key_len_t = key_len_type;
#define protocol_key_len_hton(x) htons(x)
#define protocol_key_len_ntoh(x) ntohs(x)

using protocol_value_len_t = value_len_type;
#define protocol_value_len_hton(x) htonl(x)
#define protocol_value_len_ntoh(x) ntohl(x)

using protocol_id_t = uint64_t;
#define protocol_id_hton(x) htonll(x)
#define protocol_id_ntoh(x) ntohll(x)

#define PROTOCOL_COMMAND_NUMBER_POS_NOCID (sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_t))
#define PROTOCOL_COMMAND_NUMBER_POS (sizeof(protocol_id_t) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_t))

#define PROTOCOL_FIRST_KEY_LEN_POS_NOCID (PROTOCOL_COMMAND_NUMBER_POS_NOCID + sizeof(command_code_t))
#define PROTOCOL_FIRST_KEY_LEN_POS (PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_t))

#define PROTOCOL_CURSOR_LEN_POS (PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_t))
#define PROTOCOL_FB_EDGE_FLAG_BIT 0x01


/**
 * Commands for exchanging servers <-> server
 *
 *
 * Commands for exchanging server <-> client
 *
 *
 *
 */

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


/*   GET_FF, GET_KEYS, GET_FB, GET_KEYS_PREFIX
 *   For client [msg_len]GET_FF/GET_FB[num_of_els][curs_name_len][cursorname]
 *   For partition [msg_len]GET_FF[num_of_els][cid][key_len][key][curs_len][cursor_name]
*    For primary [msg_len]GET_FF[num_of_els][cid][key_len][key][val_len][val]....[next_key_len][next_key][curs_len][cursor_name]
*/

/* CREATE/DELETE cursor
 *  [msg_len][cursor_size][cursor_cmd][curs_len][curs_name][key_len][key]
 */

#endif // YSQL_PROTOCOL_H_INCLUDED 
