#include "../include/cursor.h"
#include <cstring>
#include <stdexcept>

Cursor::Cursor() : cid(0), name(""), size(0), capacity(0), next_key_str(""), max_key(false) {

}

Cursor::Cursor(const std::string& cursor_name) {
    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::length_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> capacity = 0;
    this -> name = cursor_name;
    this -> size = 0;
    this -> max_key = false;
}


Cursor::Cursor(const std::string& cursor_name, const protocol_id_t& client_id) {
    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::length_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> cid = client_id;
    this -> name = cursor_name;
    this -> capacity = 0;
    this -> size = 0;
    this -> next_key_str = "";
    this -> max_key = false;
}

Cursor::Cursor(const std::string& cursor_name, const protocol_id_t& client_id, const std::string& next_key, const protocol_key_len_t& key_len) {
    if(cursor_name.size() > CURSOR_MAX_NAME_LEN) {
        throw std::runtime_error(CURSOR_NAME_TOO_BIG_ERR_MSG);
    }

    this -> cid = client_id;
    this -> name = cursor_name;
    this -> capacity = 0;
    this -> next_key_str = next_key;
    this -> next_key_len = key_len;
    this -> size = 0;
    this -> max_key = false;
}

protocol_id_t Cursor::get_cid() const {
    return this -> cid;
}

void Cursor::set_cid(const protocol_id_t& client_id) {
    this -> cid = client_id;
}

// Server_Message Cursor::get_client_msg() const {
//     return Server_Message();
//}

void Cursor::clear_msg() {
    this -> fetched_entries.clear();
    this -> size = 0;
}

void Cursor::set_next_key(const std::string& key, const protocol_key_len_t& key_len) {
    this -> next_key_str = key;
    this -> next_key_len = key_len;
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

void Cursor::set_capacity(cursor_cap_t new_cap) {
    if(new_cap > CURSOR_MAX_SIZE) {
        throw std::length_error(CURSOR_CAPACITY_TOO_BIG_ERR_MSG);
    }

    this -> capacity = new_cap;
}

cursor_cap_t Cursor::get_capacity() {
    return this -> capacity;
}

cursor_cap_t Cursor::get_remaining() {
    return this -> capacity - this -> size;
}


uint32_t Cursor::get_last_called_part_id() {
    return this -> l_called_pid;
}

void Cursor::set_last_called_part_id(uint16_t last_called_partition_id) {
    this -> l_called_pid = last_called_partition_id;
}

void Cursor::update_receive(cursor_cap_t received) {
    this -> size += received;
}

void Cursor::add_new_entries(std::vector<Entry>&& entries) {
    this -> fetched_entries.insert(this -> fetched_entries.end(), std::make_move_iterator(entries.begin()), std::make_move_iterator(entries.end()));
    //std::sort(this -> fetched_entries.begin(), this -> fetched_entries.end());
    this -> size = this -> fetched_entries.size();
}

bool Cursor::is_complete() {
    return this -> capacity <= this -> size;
}


void Cursor::incr_pid(){
    ++this -> l_called_pid;
}

void Cursor::decr_pid() {
    --this -> l_called_pid;
}

protocol_key_len_t Cursor::get_next_key_size() {
    return this -> next_key_len;
}

cursor_name_len_t Cursor::get_name_size() {
    return this -> name.size();
}

std::vector<Entry> Cursor::get_entries() {
    return this -> fetched_entries;
}

bool Cursor::is_max_key() {
    return this -> max_key; 
}

void Cursor::set_max_key(bool max_key) {
    this -> max_key = max_key;
}

void Cursor::set_prefix(std::string&& prefix) {
    this -> prefix = std::move(prefix);
}

std::string Cursor::get_prefix() const {
    return this -> prefix;
}

