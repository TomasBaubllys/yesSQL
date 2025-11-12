#ifndef YSQL_CURSOR_H_INCLUDED
#define YSQL_CURSOR_H_INCLUDED

#include "server_message.h"

#define CURSOR_MAX_NAME_LEN 64
#define CURSOR_MAX_SIZE 0xff

#define CURSOR_SIZE_TOO_BIG_ERR_MSG "Cursor size exceeds maximum allowed size\n"
#define CURSOR_NAME_TOO_BIG_ERR_MSG "Cursor name exceeds maximum allowed size\n"

using cursor_len_t = uint8_t;

class Cursor {
    private:
        // client id 
        protocol_id_t cid;
        // stores current server message
        Server_Message curr_msg;
        // cursor name that the client uses
        std::string name;
        // size which the cursor tries to fetch
        cursor_len_t size;
        // string of the next key
        std::string next_key_str;

    public:
        Cursor(std::string cursor_name, protocol_id_t client_id, cursor_len_t size);
        Cursor(std::string cursor_name, protocol_id_t client_id, cursor_len_t size, std::string next_key_str);      

        protocol_id_t get_cid();
        void set_cid();
};

#endif // YSQL_CURSORS_H_INCLUDED