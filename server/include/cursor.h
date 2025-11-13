#ifndef YSQL_CURSOR_H_INCLUDED
#define YSQL_CURSOR_H_INCLUDED

#include "protocol.h"
#include "server_message.h"
#include <cstring>

#define CURSOR_MAX_NAME_LEN 64
#define CURSOR_MAX_SIZE 0xff

#define CURSOR_CAPACITY_TOO_BIG_ERR_MSG "Cursor size exceeds maximum allowed size\n"
#define CURSOR_NAME_TOO_BIG_ERR_MSG "Cursor name exceeds maximum allowed size\n"

using cursor_name_len_t = uint8_t;
using cursor_cap_t = uint16_t;

#define cursor_cap_hton(x) htons(x)
#define cursor_cap_ntoh(x) ntohs(x)

#define cursor_name_len_hton(x) (x)
#define cursor_name_len_ntoh(x) (x)

class Cursor {
    private:
        // client id 
        protocol_id_t cid;
        // stores current server message
        std::vector<Entry> fetched_entries;
        // cursor name that the client uses
        std::string name;
        // size which the cursor tries to fetch
        cursor_cap_t size;
        cursor_cap_t capacity;
        // string of the next key
        std::string next_key_str;
        protocol_key_len_t next_key_len;

    public:
        Cursor();

        Cursor(const std::string& cursor_name);
        // TRHOWS
        Cursor(const std::string& cursor_name, const protocol_id_t& client_id);
        // THROWS
        Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const std::string& next_key, const protocol_key_len_t& key_len);

        // returns clients id
        protocol_id_t get_cid() const;

        // constructs a message to the client to send collected keys
        Server_Message get_client_msg() const;

        // constructs a message to the server to get missing keys
        Server_Message get_server_msg(Command_Code com_code) const;

        void set_capacity(cursor_cap_t new_cap);

        cursor_cap_t get_capacity();

        // returns the remaining entries to fetch
        cursor_cap_t get_remaining();

        // clear current saved data, resets size to 1 (the next_saved key)
        void clear_msg();
        
        void set_cid(const protocol_id_t& client_id);

        void set_next_key(const std::string& key, const protocol_key_len_t& key_len);

        std::string get_next_key() const;

        std::string get_name() const;

        protocol_key_len_t get_key_len();

        // used for debugging
        void print();
};

#endif // YSQL_CURSORS_H_INCLUDED
