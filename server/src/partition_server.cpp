#include "../include/partition_server.h"

uint64_t ppmsgsent = 0;
uint64_t ppmsgrecv = 0;

Partition_Server::Partition_Server(uint16_t port, uint8_t verbose) : Server(port, verbose), lsm_tree() {

}

int8_t Partition_Server::start() {
    if (listen(this->server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    while (true) {
        std::cout << "Received messages from clients: " << ppmsgrecv << std::endl;
        std::cout << "Messages sent to clients: " << ppmsgsent << std::endl;
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
            }
            
            if(this -> epoll_events[i].events & EPOLLIN) {
                Server_Message serv_msg;
                try {
                    serv_msg = this -> read_message(socket_fd);

                    if(serv_msg.is_empty()) {
                        continue;
                    }
                    ++ppmsgrecv;
                    serv_msg.print();
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }
                    this -> request_to_remove_fd(socket_fd);
                }
                
                this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                    this -> handle_client(socket_fd, serv_msg);
                });
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
                    int64_t bytes_sent = this -> send_message(socket_fd, serv_req);
                    
                    if (bytes_sent < 0) {
                        this -> request_to_remove_fd(socket_fd);
                        lock.unlock();
                        continue;
                    }
                } 
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cout << e.what() << std::endl;
                    }
                    this -> request_to_remove_fd(socket_fd);
                }
                
                // Check if entire message has been sent
                if (serv_req.is_fully_read()) {
                    ++ppmsgsent;
                    // Message complete, remove from buffer
                    this -> write_buffers.erase(socket_fd);
                    
                    // Try to load next message from queue
                    Server_Message next_msg;
                    
                    bool has_more = this -> tactical_reload_partition(socket_fd, next_msg);
                    
                    if (!has_more) {
                        this -> request_epoll_mod(socket_fd, EPOLLIN);
                    } else {
                        this -> write_buffers[socket_fd] = std::move(next_msg);
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
    protocol_key_len_type key_len;
    if(curr_pos + sizeof(key_len) > raw_message.size()) {
        throw std::runtime_error(PARTITION_SERVER_FAILED_TO_EXTRACT_DATA_ERR_MSG);
    }

    memcpy(&key_len, &raw_message[curr_pos], sizeof(key_len));
    key_len = protocol_key_len_ntoh(key_len);
    curr_pos += key_len + sizeof(key_len);
    
    protocol_value_len_type value_len;
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

std::string Partition_Server::create_entries_response(const std::vector<Entry>& entry_array, protocol_id_t client_id) const{
    protocol_msg_len_t msg_len = sizeof(protocol_id_t) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_type) + sizeof(command_code_type);
    for(const Entry& entry : entry_array) {
        msg_len += sizeof(protocol_key_len_type) + sizeof(protocol_value_len_type);
        msg_len += entry.get_key_length() + entry.get_value_length();
    }

    protocol_array_len_type array_len = entry_array.size();
    command_code_type com_code = COMMAND_CODE_OK;
    array_len = protocol_arr_len_hton(array_len);
    protocol_msg_len_t net_msg_len = protocol_msg_len_hton(msg_len);
    com_code = command_hton(com_code);
    protocol_id_t net_cid = protocol_id_hton(client_id); 

    size_t curr_pos = 0;
    std::string raw_message(msg_len, '\0');
    memcpy(&raw_message[0], &net_msg_len, sizeof(net_msg_len));
    curr_pos += sizeof(net_msg_len);

    memcpy(&raw_message[curr_pos], &net_cid, sizeof(net_cid));
    curr_pos += sizeof(net_cid);
    
    memcpy(&raw_message[curr_pos], &array_len, sizeof(array_len));
    curr_pos += sizeof(array_len);
    
    memcpy(&raw_message[curr_pos], &com_code, sizeof(com_code));
    curr_pos += sizeof(com_code);

    for(const Entry& entry : entry_array) {
        protocol_key_len_type key_len = entry.get_key_length();
        protocol_value_len_type value_len = entry.get_value_length();
        protocol_key_len_type net_key_len = protocol_key_len_hton(key_len);
        protocol_value_len_type net_value_len = protocol_value_len_hton(value_len);

        memcpy(&raw_message[curr_pos], &net_key_len, sizeof(net_key_len));
        curr_pos += sizeof(net_key_len);

        std::string key_bytes = entry.get_key_string();
        memcpy(&raw_message[curr_pos], &key_bytes[0], key_len);
        curr_pos += key_len;

        memcpy(&raw_message[curr_pos], &net_value_len, sizeof(net_value_len));
        curr_pos += sizeof(net_value_len);

        std::string value_bytes = entry.get_value_string();
        // std::cout << "Sending value: " << value_bytes << std::endl;
        memcpy(&raw_message[curr_pos], &value_bytes[0], value_len);
        curr_pos += value_len;
    }

    return raw_message;
}

int8_t Partition_Server::process_request(socket_t socket_fd, const Server_Message& serv_msg) {
        // extract the command code
    Command_Code com_code = this -> extract_command_code(serv_msg.string(), true);

    switch(com_code) {
        case COMMAND_CODE_GET: {
            std::shared_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
            return this -> handle_get_request(socket_fd, serv_msg);
        }
        case COMMAND_CODE_SET: {
            // extract the key
            std::unique_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
            return this -> handle_set_request(socket_fd, serv_msg);
        }

        case COMMAND_CODE_GET_KEYS: {

            break;
        }

        case COMMAND_CODE_GET_KEYS_PREFIX: {

            break;
        }

        case COMMAND_CODE_GET_FF: {

            break;
        }

        case COMMAND_CODE_GET_FB: {

            break;
        }

        case COMMAND_CODE_REMOVE: {
            std::unique_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
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
    if(this -> lsm_tree.set(key_str, value_str)) {
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

    bool found = false;
    std::string value_str;
    try {
        Entry entry = lsm_tree.get(key_str);
        if(entry.is_deleted() || entry.get_string_key_bytes() == ENTRY_PLACEHOLDER_KEY) {
            this -> queue_socket_for_not_found_response(socket_fd, serv_msg.get_cid());
            return 0;
        }
        else {
            std::string entries_resp = this -> create_entries_response({entry}, serv_msg.get_cid());
            Server_Message serv_resp(entries_resp, serv_msg.get_cid());
            this -> queue_socket_for_response(socket_fd, serv_resp);
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

    if(lsm_tree.remove(key_str)) {
        this -> queue_socket_for_ok_response(socket_fd, serv_msg.get_cid());
        return 0;
    }
    else {
        this -> queue_socket_for_err_response(socket_fd, serv_msg.get_cid());
        return 0;
    }

    return -1;
}

void Partition_Server::handle_client(socket_t socket_fd, const Server_Message& message) {
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
    this -> queue_socket_for_response(socket_fd, resp);
}

void Partition_Server::queue_socket_for_err_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_ERR, true, client_id);
    this -> queue_socket_for_response(socket_fd, resp);
}

void Partition_Server::queue_socket_for_ok_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_OK, true, client_id);
    this -> queue_socket_for_response(socket_fd, resp);
}