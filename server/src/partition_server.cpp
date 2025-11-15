#include "../include/partition_server.h"
#include <cstdint>
#include <cstring>
#include <stdexcept>

Partition_Server::Partition_Server(uint16_t port, uint8_t verbose, uint32_t thread_pool_size) : Server(port, verbose, thread_pool_size), lsm_tree() {

}

Partition_Server::~Partition_Server() {

}

int8_t Partition_Server::start() {
    if (listen(this -> server_fd, SOMAXCONN) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    while (true) {
        int32_t ready_fd_count = this -> server_epoll_wait();
        if(ready_fd_count < 0) {
            if(errno == EINTR) {
                continue;
            }
            std::string epoll_wait_fail_str(SERVER_EPOLL_WAIT_FAILED_ERR_MSG);
            epoll_wait_fail_str += SERVER_ERRNO_STR_PREFIX;
            epoll_wait_fail_str += std::to_string(errno);
            throw std::runtime_error(epoll_wait_fail_str);
        }

        for(int32_t i = 0; i < ready_fd_count; ++i) {
            socket_t socket_fd = this -> epoll_events.at(i).data.fd;
            if(socket_fd == this -> wakeup_fd) {
                uint64_t dummy = 0;
                read(wakeup_fd, &dummy, sizeof(dummy));
                this -> apply_epoll_mod_q();

                continue;
            }
            
            if(socket_fd == this -> server_fd) {
                std::vector<socket_t> client_fds = this -> add_client_socket_to_epoll();
                for(socket_t& client_fd : client_fds) {
                    if(client_fd >= 0) { 
                        this -> fd_type_map[client_fd] = Fd_Type::LISTENER;
                    }
                }
                continue;
            }
            
            if(this -> epoll_events[i].events & EPOLLIN) {
                std::vector<Server_Message> serv_msgs;
                try {
                    serv_msgs = this -> read_messages(socket_fd);
                    for(const Server_Message& serv_msg : serv_msgs) {
                        if(serv_msg.is_empty()) {
                            continue;
                        }

                        this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                        this -> handle_client(socket_fd, serv_msg);
                        });
                    }
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }
                    this -> request_to_remove_fd(socket_fd);
                }
            }
           
            if(this -> epoll_events[i].events & EPOLLOUT) {
                // Acquire lock to check/reload buffer
                std::unique_lock<std::shared_mutex> lock(this -> write_buffers_mutex);
                std::unordered_map<socket_t, Server_Message>::iterator it = this -> write_buffers.find(socket_fd);
                
                // If no buffer exists, try to reload from queue
                if (it == this -> write_buffers.end()) {
                    Server_Message new_msg;
                    
                    // Release lock before calling tactical_reload
                    bool loaded = this -> tactical_reload_partition(socket_fd, new_msg);
                    
                    if (!loaded) {
                        // Nothing to write, return to EPOLLIN
                        this -> request_epoll_mod(socket_fd, EPOLLIN);
                        lock.unlock();
                        continue;
                    }
                    
                    // Re-acquire lock to insert the new message
                    std::pair<std::unordered_map<socket_t, Server_Message>::iterator, bool> insert_result = this -> write_buffers.emplace(socket_fd, std::move(new_msg));
                    if(insert_result.second) {
                        it = insert_result.first;
                    }
                    else {
                        throw std::runtime_error(SERVER_FAILED_PARTITION_WRITE_BUFFER_ERR_MSG);
                    }
                }

                Server_Message& serv_req = it -> second;
                
                try {
                    this -> send_message(socket_fd, serv_req);
                } 
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }
                    this -> request_to_remove_fd(socket_fd);
                }
                
                // Check if entire message has been sent
                if (serv_req.is_fully_read()) {
                    // Message complete, remove from buffer
                    this -> write_buffers.erase(socket_fd);
                    
                    // Try to load next message from queue
                    Server_Message next_msg;
                    
                    bool has_more = this -> tactical_reload_partition(socket_fd, next_msg);
                    
                    if (!has_more) {
                        this -> request_epoll_mod(socket_fd, EPOLLIN);
                    } else {
                        this -> write_buffers[socket_fd] = std::move(next_msg);
                        this -> request_epoll_mod(socket_fd, EPOLLIN | EPOLLOUT);
                        // Stay in EPOLLOUT mode - will trigger again
                    }
                    lock.unlock();
                }
                
            }
            else if (this -> epoll_events[i].events & (EPOLLHUP | EPOLLERR)) {
                // Client hang-up or error
                if (verbose > 0)
                    std::cerr << "Client error/hang-up: fd=" << socket_fd << std::endl;
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
                close(socket_fd);
            }
        }

        this -> process_remove_queue();
    }

    return 0;
}


std::string Partition_Server::extract_value(const std::string& raw_message) const {
    // read the key length
    size_t curr_pos = PROTOCOL_FIRST_KEY_LEN_POS;
    protocol_key_len_t key_len;
    if(curr_pos + sizeof(key_len) > raw_message.size()) {
        throw std::runtime_error(PARTITION_SERVER_FAILED_TO_EXTRACT_DATA_ERR_MSG);
    }

    memcpy(&key_len, &raw_message[curr_pos], sizeof(key_len));
    key_len = protocol_key_len_ntoh(key_len);
    curr_pos += key_len + sizeof(key_len);
    
    protocol_value_len_t value_len;
    if(curr_pos + sizeof(value_len) > raw_message.size()) {
        throw std::runtime_error(PARTITION_SERVER_FAILED_TO_EXTRACT_DATA_ERR_MSG);
    }

    memcpy(&value_len, &raw_message[curr_pos], sizeof(value_len));
    value_len = protocol_value_len_ntoh(value_len);
    curr_pos += sizeof(value_len);

    if(curr_pos + value_len > raw_message.size()) {
        throw std::runtime_error(PARTITION_SERVER_FAILED_TO_EXTRACT_DATA_ERR_MSG);
    }
    
    std::string value_str(value_len, '\0');
    memcpy(&value_str[0], &raw_message[curr_pos], value_len);
    return value_str;
}

int8_t Partition_Server::process_request(socket_t socket_fd, Server_Message& serv_msg) {
        // extract the command code
    Command_Code com_code = this -> extract_command_code(serv_msg.string(), true);

    switch(com_code) {
        case COMMAND_CODE_GET: {
            return this -> handle_get_request(socket_fd, serv_msg);
        }
        case COMMAND_CODE_SET: {
            // extract the key
            return this -> handle_set_request(socket_fd, serv_msg);
        }

        case COMMAND_CODE_GET_KEYS: {
            return this -> handle_get_keys_request(socket_fd, serv_msg);
        }

        case COMMAND_CODE_GET_KEYS_PREFIX: {

            break;
        }

        case COMMAND_CODE_GET_FF: 
        case COMMAND_CODE_GET_FB: {
            return this -> handle_get_fx_request(socket_fd, serv_msg, com_code);
        }

        case COMMAND_CODE_REMOVE: {
            return this -> handle_remove_request(socket_fd, serv_msg);
        }

        default: {

        }
    }

    return 0;
}


int8_t Partition_Server::handle_set_request(socket_t socket_fd, const Server_Message& serv_msg) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(serv_msg.string(), true);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
        std::cerr << e.what() << std::endl;
        }

        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    // extract the data
    std::string value_str;
    try {
        value_str = this -> extract_value(serv_msg.string());
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << e.what();
        }

        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    // insert the key value pair into the inner lsm tree
    bool set = false;
    {
        std::unique_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
        set = this -> lsm_tree.set(key_str, value_str);
    }

    if(set) {
        this -> queue_socket_for_ok_response(socket_fd, serv_msg.get_cid());
        return 0;
    }
    else {
        this -> queue_socket_for_err_response(socket_fd,serv_msg.get_cid());
        return 0;
    } 
    return -1;
}

int8_t Partition_Server::handle_get_request(socket_t socket_fd, const Server_Message& serv_msg) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(serv_msg.string(), true);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }
        
        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    std::string value_str;
    try {
        Entry entry(Bits(ENTRY_PLACEHOLDER_KEY), Bits(ENTRY_PLACEHOLDER_VALUE));
        {
            std::shared_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
            entry = lsm_tree.get(key_str);
        }
        if(entry.is_deleted() || entry.get_string_key_bytes() == ENTRY_PLACEHOLDER_KEY) {
            this -> queue_socket_for_not_found_response(socket_fd, serv_msg.get_cid());
            return 0;
        }
        else {
            std::string entries_resp = this -> create_entries_response({entry}, true, serv_msg.get_cid());
            Server_Message serv_resp(entries_resp, serv_msg.get_cid());
            this -> queue_partition_for_response(socket_fd, std::move(serv_resp));
            return 0;
        }
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }
    return -1;
}

int8_t Partition_Server::handle_remove_request(socket_t socket_fd, const Server_Message& serv_msg) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(serv_msg.string(), true);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }
        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    bool remove = false;
    {
        std::unique_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
        remove = lsm_tree.remove(key_str);
    }

    if(remove) {
        this -> queue_socket_for_ok_response(socket_fd, serv_msg.get_cid());
        return 0;
    }
    else {
        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    return -1;
}

void Partition_Server::handle_client(socket_t socket_fd, Server_Message message) {
    try{
        if(this -> process_request(socket_fd, message) < 0) {
            this -> request_to_remove_fd(socket_fd);
        }
    }   
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> request_to_remove_fd(socket_fd);
    }
}

void Partition_Server::queue_socket_for_not_found_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_DATA_NOT_FOUND, true, client_id);
    this -> queue_partition_for_response(socket_fd, std::move(resp));
}

void Partition_Server::queue_socket_for_err_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_ERR, true, client_id);
    this -> queue_partition_for_response(socket_fd, std::move(resp));
}

void Partition_Server::queue_socket_for_ok_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_OK, true, client_id);
    this -> queue_partition_for_response(socket_fd, std::move(resp));
}

void Partition_Server::process_remove_queue() { 
    std::vector<socket_t> to_remove;
    {
        std::unique_lock<std::mutex> lock_rm_q(this -> remove_mutex);
        if(this -> remove_queue.empty()) {
            return;
        }
        to_remove.swap(this -> remove_queue);
    }

    for(socket_t& sock_fd : to_remove) {
        {
            std::unique_lock<std::shared_mutex> lock_e_mod(this -> epoll_mod_mutex);
            this -> epoll_mod_map.erase(sock_fd);
        }
        epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, sock_fd, nullptr);

        this -> read_buffers.erase(sock_fd);
        // clear read / write buffer
        {
            std::unique_lock<std::shared_mutex> lock_w_buf(this -> write_buffers_mutex);
            this -> write_buffers.erase(sock_fd);
        }

        {
        std::unique_lock<std::shared_mutex> lock_fd_m(this -> fd_type_map_mutex);
            std::unordered_map<socket_t, Fd_Type>::iterator found_fd = fd_type_map.find(sock_fd);
            if(found_fd != this -> fd_type_map.end()) {
                if(found_fd -> second == Fd_Type::PARTITION) {
                    this -> partition_queues.erase(sock_fd);
                }
            }
            this -> fd_type_map.erase(sock_fd); 
        }

        close(sock_fd);
    }
}

std::pair<std::string, Cursor_Info> Partition_Server::extract_key_and_cursinf(Server_Message& message) {
    Cursor_Info curs_inf;

    // check if the command is long enough
    uint64_t pos = sizeof(protocol_msg_len_t) + sizeof(protocol_id_t) + sizeof(protocol_array_len_t) - sizeof(cursor_cap_t);
    cursor_cap_t cap = 0;
    memcpy(&cap, &message.c_str()[pos], sizeof(cursor_cap_t));
    cap = cursor_cap_ntoh(cap);

    pos = PROTOCOL_FIRST_KEY_LEN_POS;

    if(pos + sizeof(protocol_key_len_t)  > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_t key_len = 0;

    memcpy(&key_len, &message.c_str()[pos], sizeof(key_len));
    key_len = ntohs(key_len);
    pos += sizeof(protocol_key_len_t);

    if(pos + key_len > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    std::string key_str(key_len, '\0');
    memcpy(&key_str[0], &message.c_str()[pos], key_len);
    pos += key_len;

    if(pos + sizeof(cursor_name_len_t) > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    cursor_name_len_t curs_name_len = 0;
    memcpy(&curs_name_len, &message.c_str()[pos], sizeof(cursor_name_len_t));
    curs_name_len = cursor_name_len_ntoh(curs_name_len);
    pos += sizeof(cursor_name_len_t);

    if(pos + curs_name_len > message.size()) {
        throw std::length_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    std::string curs_name(curs_name_len, '\0');
    memcpy(&curs_name[0], &message.c_str()[pos], curs_name_len);

    curs_inf.name = std::move(curs_name);
    curs_inf.cap = cap;
    curs_inf.name_len = curs_name_len;

    return std::make_pair(key_str, curs_inf);
}
// NOT CURRENTLY WORKING!!!! STOPPED CODDING FROM HERE
int8_t Partition_Server::handle_get_keys_request(socket_t socket_fd, Server_Message& message) {
    std::pair<std::string, Cursor_Info> key_and_curs;
    std::pair<std::set<Bits>, std::string> entries_key;
    try {
        key_and_curs = this -> extract_key_and_cursinf(message);
        // std::cout << key_and_curs.second.name << "   " << key_and_curs.second.name_len << std::endl;
        std::shared_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
        if(this -> is_fb_edge_flag_set(message.get_string_data())) {
            std::string max_key(UINT16_MAX, '\xFF');
            entries_key = this -> lsm_tree.get_keys(max_key, key_and_curs.second.cap);
        }
        else {
            entries_key = this -> lsm_tree.get_keys(key_and_curs.first, key_and_curs.second.cap);
        }

        return 0;

               // std::cout << key_and_curs.second.name << "   " << key_and_curs.second.name_len << std::endl;
        Server_Message serv_msg = this -> create_keys_set_resp(Command_Code::COMMAND_CODE_GET_KEYS, entries_key.first, entries_key.second, message.get_cid(), key_and_curs.second);
        this -> queue_partition_for_response(socket_fd, std::move(serv_msg));

    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> queue_socket_for_err_response(socket_fd, message.get_cid());
        return 0;
    }

    return 0;
}

int8_t Partition_Server::handle_get_fx_request(socket_t socket_fd, Server_Message& message, Command_Code com_code) {
    std::pair<std::string, Cursor_Info> key_and_curs;
    std::pair<std::set<Entry>, std::string> entries_key;
    try {
        key_and_curs = this -> extract_key_and_cursinf(message);
        // std::cout << key_and_curs.second.name << "   " << key_and_curs.second.name_len << std::endl;
        std::shared_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
        if(com_code == Command_Code::COMMAND_CODE_GET_FB) {
            if(this -> is_fb_edge_flag_set(message.get_string_data())) {
                std::string max_key(UINT16_MAX, '\xFF');
                entries_key = this -> lsm_tree.get_fb(max_key, key_and_curs.second.cap);
            }
            else {
                entries_key = this -> lsm_tree.get_fb(key_and_curs.first, key_and_curs.second.cap);
            }
        }
        else if(com_code == Command_Code::COMMAND_CODE_GET_FF) {
             if(this -> is_fb_edge_flag_set(message.get_string_data())) {
                std::string max_key(UINT16_MAX, '\xFF');
                entries_key = this -> lsm_tree.get_ff(max_key, key_and_curs.second.cap);
            }
            else {
                entries_key = this -> lsm_tree.get_ff(key_and_curs.first, key_and_curs.second.cap);
            }
        }
        else {
            this -> queue_socket_for_err_response(socket_fd, message.get_cid());
            return 0;
        }

               // std::cout << key_and_curs.second.name << "   " << key_and_curs.second.name_len << std::endl;
        Server_Message serv_msg = this -> create_entries_set_resp(com_code, entries_key.first, entries_key.second, message.get_cid(), key_and_curs.second);
        this -> queue_partition_for_response(socket_fd, std::move(serv_msg));

    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> queue_socket_for_err_response(socket_fd, message.get_cid());
        return 0;
    }

    return 0;
}

Server_Message Partition_Server::create_entries_set_resp(Command_Code com_code, std::set<Entry> entries_set, std::string next_key, protocol_id_t client_id, Cursor_Info& curs_info) {
    protocol_msg_len_t msg_len = sizeof(protocol_id_t) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_t) + sizeof(command_code_t) + next_key.size() + sizeof(protocol_key_len_t) + sizeof(cursor_name_len_t) + curs_info.name_len;
    for(const Entry& entry : entries_set) {
        msg_len += sizeof(protocol_key_len_t) + sizeof(protocol_value_len_t);
        msg_len += entry.get_key_length() + entry.get_value_length();
    }

    protocol_array_len_t array_len = entries_set.size();
    command_code_t net_com_code = command_hton(com_code);
    array_len = protocol_arr_len_hton(array_len);
    protocol_msg_len_t net_msg_len = protocol_msg_len_hton(msg_len);

    size_t curr_pos = 0;
    std::string raw_message(msg_len, '\0');
    memcpy(&raw_message[0], &net_msg_len, sizeof(net_msg_len));
    curr_pos += sizeof(net_msg_len);

    protocol_id_t net_cid = protocol_id_hton(client_id);
    memcpy(&raw_message[curr_pos], &net_cid, sizeof(net_cid));
    curr_pos += sizeof(protocol_id_t);

    memcpy(&raw_message[curr_pos], &array_len, sizeof(array_len));
    curr_pos += sizeof(protocol_array_len_t);

    memcpy(&raw_message[curr_pos], &net_com_code, sizeof(net_com_code));
    curr_pos += sizeof(command_code_t);

    for(const Entry& entry : entries_set) {
        protocol_key_len_t key_len = entry.get_key_length();
        protocol_value_len_t value_len = entry.get_value_length();
        protocol_key_len_t net_key_len = protocol_key_len_hton(key_len);
        protocol_value_len_t net_value_len = protocol_value_len_hton(value_len);

        memcpy(&raw_message[curr_pos], &net_key_len, sizeof(net_key_len));
        curr_pos += sizeof(net_key_len);

        std::string key_bytes = entry.get_key_string();
        memcpy(&raw_message[curr_pos], &key_bytes[0], key_len);
        curr_pos += key_len;

        memcpy(&raw_message[curr_pos], &net_value_len, sizeof(net_value_len));
        curr_pos += sizeof(net_value_len);

        std::string value_bytes = entry.get_value_string();
        memcpy(&raw_message[curr_pos], &value_bytes[0], value_len);
        curr_pos += value_len;
    }

    // IN SQL WE TRUST IF THIS EVER FAILS, DONT BLAME !
    protocol_key_len_t key_len = static_cast<protocol_key_len_t>(next_key.size());
    protocol_key_len_t net_key_len = protocol_key_len_hton(key_len);
    memcpy(&raw_message[curr_pos], &net_key_len, sizeof(protocol_key_len_t));
    curr_pos += sizeof(protocol_key_len_t);

    memcpy(&raw_message[curr_pos], &next_key[0], key_len);
    curr_pos += key_len;

    // append the cursor info to the end
    cursor_name_len_t net_curs_name = cursor_name_len_hton(curs_info.name_len);
    memcpy(&raw_message[curr_pos], &net_curs_name, sizeof(cursor_name_len_t));
    curr_pos += sizeof(cursor_name_len_t);

    memcpy(&raw_message[curr_pos], &curs_info.name[0], curs_info.name_len);

    Server_Message serv_msg;
    serv_msg.set_message_eat(std::move(raw_message));
    serv_msg.set_cid(client_id);
    return serv_msg;
}

bool Partition_Server::is_fb_edge_flag_set(std::string& msg) {
    uint8_t flag = 0;
    memcpy(&flag, &msg[PROTOCOL_EDGE_FB_FLAG_POS], sizeof(uint8_t));
    return flag & PROTOCOL_FB_EDGE_FLAG_BIT;
}

Server_Message Partition_Server::create_keys_set_resp(Command_Code com_code, std::set<Bits> bits_set, std::string next_key, protocol_id_t client_id, Cursor_Info& curs_info) {
 protocol_msg_len_t msg_len = sizeof(protocol_id_t) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_t) + sizeof(command_code_t) + next_key.size() + sizeof(protocol_key_len_t) + sizeof(cursor_name_len_t) + curs_info.name_len;
    for(const Bits& bit : bits_set) {
        msg_len += sizeof(protocol_key_len_t) + sizeof(protocol_value_len_t);
        msg_len += bit.size();
    }

    protocol_array_len_t array_len = bits_set.size();
    command_code_t net_com_code = command_hton(com_code);
    array_len = protocol_arr_len_hton(array_len);
    protocol_msg_len_t net_msg_len = protocol_msg_len_hton(msg_len);

    size_t curr_pos = 0;
    std::string raw_message(msg_len, '\0');
    memcpy(&raw_message[0], &net_msg_len, sizeof(net_msg_len));
    curr_pos += sizeof(net_msg_len);

    protocol_id_t net_cid = protocol_id_hton(client_id);
    memcpy(&raw_message[curr_pos], &net_cid, sizeof(net_cid));
    curr_pos += sizeof(protocol_id_t);

    memcpy(&raw_message[curr_pos], &array_len, sizeof(array_len));
    curr_pos += sizeof(protocol_array_len_t);

    memcpy(&raw_message[curr_pos], &net_com_code, sizeof(net_com_code));
    curr_pos += sizeof(command_code_t);

    for(const Bits& bit : bits_set) {
        protocol_key_len_t key_len = bit.size();
        protocol_key_len_t net_key_len = protocol_key_len_hton(key_len);

        memcpy(&raw_message[curr_pos], &net_key_len, sizeof(net_key_len));
        curr_pos += sizeof(net_key_len);

        std::string key_bytes = bit.get_string();
        memcpy(&raw_message[curr_pos], &key_bytes[0], key_len);
        curr_pos += key_len;
    }

    // IN SQL WE TRUST IF THIS EVER FAILS, DONT BLAME !
    protocol_key_len_t key_len = static_cast<protocol_key_len_t>(next_key.size());
    protocol_key_len_t net_key_len = protocol_key_len_hton(key_len);
    memcpy(&raw_message[curr_pos], &net_key_len, sizeof(protocol_key_len_t));
    curr_pos += sizeof(protocol_key_len_t);

    memcpy(&raw_message[curr_pos], &next_key[0], key_len);
    curr_pos += key_len;

    // append the cursor info to the end
    cursor_name_len_t net_curs_name = cursor_name_len_hton(curs_info.name_len);
    memcpy(&raw_message[curr_pos], &net_curs_name, sizeof(cursor_name_len_t));
    curr_pos += sizeof(cursor_name_len_t);

    memcpy(&raw_message[curr_pos], &curs_info.name[0], curs_info.name_len);

    Server_Message serv_msg;
    serv_msg.set_message_eat(std::move(raw_message));
    serv_msg.set_cid(client_id);
    return serv_msg;
}
