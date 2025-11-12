#include "../include/cursor.h"

Cursor::Cursor(std::string cursor_name, protocol_id_t client_id, cursor_len_t cursor_size) {
    if(cursor_size > CURSOR_MAX_SIZE) {
        throw std::runtime_error(CURSOR_SIZE_TOO_BIG_ERR_MSG);
    }

    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::runtime_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> cid = client_id;
    this -> name = cursor_name;
    this -> size = cursor_size;
    this -> next_key_str = "";
}

Cursor::Cursor(std::string cursor_name, protocol_id_t client_id, cursor_len_t cursor_size, std::string next_key_str) {

}