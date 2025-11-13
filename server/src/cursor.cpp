#include "../include/cursor.h"

Cursor::Cursor() : cid(0), curr_msg(), name(""), size(0), capacity(0), next_key_str("") {

}

Cursor::Cursor(const std::string& cursor_name, const cursor_cap_t& cursor_capacity) {
    if(cursor_capacity > CURSOR_MAX_SIZE) {
        throw std::length_error(CURSOR_CAPACITY_TOO_BIG_ERR_MSG);
    }

    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::length_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> capacity = cursor_capacity;
    this -> name = cursor_name;
    this -> size = 0;
}


Cursor::Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const cursor_cap_t& cursor_capacity) {
    if(cursor_capacity > CURSOR_MAX_SIZE) {
        throw std::length_error(CURSOR_CAPACITY_TOO_BIG_ERR_MSG);
    }

    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::length_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> cid = client_id;
    this -> name = cursor_name;
    this -> capacity = cursor_capacity;
    this -> size = 0;
    this -> next_key_str = "";
}

Cursor::Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const cursor_cap_t& cursor_capacity, const std::string& next_key) {
    if(cursor_capacity > CURSOR_MAX_SIZE) {
        throw std::runtime_error(CURSOR_CAPACITY_TOO_BIG_ERR_MSG);
    }

    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::runtime_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> cid = client_id;
    this -> name = cursor_name;
    this -> capacity = cursor_capacity;
    this -> next_key_str = next_key;
    this -> size = 1;
}

protocol_id_t Cursor::get_cid() const {
    return this -> cid;
}

void Cursor::set_cid(const protocol_id_t& client_id) {
    this -> cid = client_id;
}

Server_Message Cursor::get_client_msg() const {
    return Server_Message();
}

void Cursor::clear_msg() {
    this -> curr_msg.clear();
    this -> size = 1;
}

Server_Message Cursor::get_server_msg() const {
    return Server_Message();
}

void Cursor::set_next_key(const std::string& key) {
    this -> next_key_str = key;
}

std::string Cursor::get_name() const {
    return this -> name;
}

void Cursor::print() {
    std::cout << "name" << name << "\n" << "capacity: " << capacity << std::endl; 
}

std::string Cursor::get_next_key() const {
    return this -> next_key_str;
}