#ifndef YSQL_CURSOR_H_INCLUDED
#define YSQL_CURSOR_H_INCLUDED

#include "server_message.h"

#define cursor_cap_len_hton(x) htons(x)
#define cursor_cap_ntoh(x) ntohs(x)

#define cursor_name_len_hton(x) (x)
#define cursor_name_len_ntoh(x) (x)

#define CURSOR_MAX_NAME_LEN 64
#define CURSOR_MAX_SIZE 0xff

#define CURSOR_CAPACITY_TOO_BIG_ERR_MSG "Cursor size exceeds maximum allowed size\n"
#define CURSOR_NAME_TOO_BIG_ERR_MSG "Cursor name exceeds maximum allowed size\n"

using cursor_name_len_t = uint8_t;
using cursor_cap_t = uint16_t;

class Cursor {
    private:
        // client id 
        protocol_id_t cid;
        // stores current server message
        Server_Message curr_msg;
        // cursor name that the client uses
        std::string name;
        // size which the cursor tries to fetch
        cursor_cap_t size;
        cursor_cap_t capacity;
        // string of the next key
        std::string next_key_str;

    public:
        Cursor();

        Cursor(const std::string& cursor_name, const cursor_cap_t& cursor_capacity);
        // TRHOWS
        Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const cursor_cap_t& cursor_capacity);
        // THROWS
        Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const cursor_cap_t& cursor_capacity, const std::string& next_key);      

        // returns clients id
        protocol_id_t get_cid() const;

        // constructs a message to the client to send collected keys
        Server_Message get_client_msg() const;

        // constructs a message to the server to get missing keys
        Server_Message get_server_msg() const;

        // clear current saved data, resets size to 1 (the next_saved key)
        void clear_msg();
        
        void set_cid(const protocol_id_t& client_id);

        void set_next_key(const std::string& key);

        std::string get_next_key() const;

        std::string get_name() const;
        // used for debugging
        void print();
};

#endif // YSQL_CURSORS_H_INCLUDED