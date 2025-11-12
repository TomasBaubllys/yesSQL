#include "../include/server_message.h"
#include <cstring>

Server_Message::Server_Message(std::string& message, protocol_id_t client_id) : msg(message),  bytes_processed(0), bytes_to_process(message.size()),  cid(client_id) {

}

Server_Message::Server_Message() : bytes_processed(0), bytes_to_process(0), cid(0) {

}

Server_Message::Server_Message(std::string& message) : msg(message), bytes_processed(0), bytes_to_process(message.size()), cid(0) {

}

std::string Server_Message::string() const {
    return this -> msg;
}

void Server_Message::remove_cid() {
    this -> msg.erase(sizeof(protocol_msg_len_t), sizeof(protocol_id_t));
    this -> bytes_to_process -= sizeof(protocol_id_t);
    protocol_msg_len_t net_len = protocol_msg_len_hton(this -> bytes_to_process);
    memcpy(&this -> msg[0], &net_len, sizeof(net_len));
}

void Server_Message::add_cid(protocol_id_t client_id) {
    protocol_msg_len_t b_to_prcs = this -> bytes_to_process + sizeof(protocol_id_t);
    this -> bytes_to_process = b_to_prcs;
    b_to_prcs = protocol_msg_len_hton(b_to_prcs);
    protocol_id_t net_cid = protocol_id_hton(client_id);

    this -> msg.insert(sizeof(protocol_msg_len_t), sizeof(net_cid),'\0');
    memcpy(&this -> msg[sizeof(protocol_msg_len_t)], &net_cid, sizeof(net_cid));
    memcpy(&this -> msg[0], &b_to_prcs, sizeof(b_to_prcs));
    this -> cid = client_id;
}

protocol_id_t Server_Message::get_cid() const {
    return this -> cid;
}

protocol_msg_len_t Server_Message::to_process() const {
    return this -> bytes_to_process;
}

void Server_Message::set_to_process(protocol_msg_len_t bytes_to_process) {
    this -> bytes_to_process = bytes_to_process;
}

void Server_Message::add_processed(protocol_msg_len_t bytes_processed) {
    this -> bytes_processed += bytes_processed;
}

protocol_msg_len_t Server_Message::processed() const {
    return this -> bytes_processed;
}

bool Server_Message::is_fully_read() const {
    return (this -> bytes_to_process <= this -> bytes_processed) && this -> bytes_to_process != 0;
}

bool Server_Message::is_empty() const {
    return this -> msg.empty();
}

void Server_Message::reset_processed() {
    this -> bytes_processed = 0;
}

void Server_Message::append_string(std::string string_to_append, size_t bytes_to_append) {
    this -> msg.append(string_to_append, bytes_to_append);
    this -> bytes_processed += bytes_to_append;
}

void Server_Message::set_cid(protocol_id_t client_id) {
    this -> cid = client_id;
}

void Server_Message::reset() {
    this -> bytes_processed = 0;
    this -> bytes_to_process = 0;
    this -> cid = 0;
    this -> msg.clear();
}

std::string& Server_Message::get_string_data() {
    return this -> msg;
}

void Server_Message::print() const {
    std::cout << this -> msg << std::endl;
}

void Server_Message::clear() {
    this -> msg.clear();
    this -> bytes_processed = 0;
    this -> bytes_to_process = 0;
}

protocol_msg_len_t Server_Message::size() const {
    return this -> msg.size();
}

const char* Server_Message::c_str() const {
    return this -> msg.c_str();
}