#include "../include/partition_server.h"

uint64_t msg_sent = 0;
uint64_t msg_recv = 0;

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
        this -> apply_epoll_mod_q();
        std::cout << "messages recv: " << msg_recv << std::endl << "messages sent: " << msg_sent << std::endl;
        int32_t ready_fd_count = this -> server_epoll_wait();
        if(ready_fd_count < 0) {
            if(errno == EINTR) {
                continue;
            }
            if(this -> verbose > 0) {
                std::cerr << SERVER_EPOLL_WAIT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                break;
            }
        }

        for(int32_t i = 0; i < ready_fd_count; ++i) {
            socket_t socket_fd = this -> epoll_events.at(i).data.fd;
            if(socket_fd == this -> wakeup_fd) {
                uint64_t dummy = 0;
                read(wakeup_fd, &dummy, sizeof(dummy));
                continue;
            }

            
            if(socket_fd == this -> server_fd) {
                std::vector<socket_t> client_fds = this -> add_client_socket_to_epoll();
                for(socket_t& client_fd : client_fds) {
                    if(client_fd >= 0) { 
                        this -> sockets_type_map[client_fd] = Fd_Type::LISTENER;
                    }
                }
            }
            else if(this -> epoll_events[i].events & EPOLLIN) {
                Server_Message serv_msg = this -> read_message(socket_fd);
                // std::cout << "CLIENT_SENT MESSAGE: " << serv_msg.client_id << std::endl;

                if(serv_msg.message.empty()) {
                    continue;
                }
                
                ++msg_recv;
                this -> thread_pool.enqueue([this, socket_fd, serv_msg](){
                    this -> handle_client(socket_fd, serv_msg);
                });
            }
           else if(this -> epoll_events[i].events & EPOLLOUT) {
                // Acquire lock to check/reload buffer
                std::unique_lock<std::mutex> lock(this -> partial_buffer_mutex);
                auto it = this -> partial_write_buffers.find(socket_fd);
                
                // If no buffer exists, try to reload from queue
                if (it == this -> partial_write_buffers.end()) {
                    Server_Request new_msg;
                    
                    // Release lock before calling tactical_reload
                    lock.unlock();
                    bool loaded = this -> tactical_reload_partition(socket_fd, new_msg);
                    
                    if (!loaded) {
                        // Nothing to write, return to EPOLLIN
                        this -> modify_socket_for_receiving_epoll(socket_fd);
                        continue;
                    }
                    
                    // Re-acquire lock to insert the new message
                    lock.lock();
                    auto insert_result = this -> partial_write_buffers.emplace(socket_fd, std::move(new_msg));
                    it = insert_result.first;
                }

                // USE REFERENCE - modifications will persist!
                Server_Request& serv_req = it -> second;
                
                // Release lock before potentially blocking send() call
                lock.unlock();

                // Send data - this modifies serv_req.bytes_processed
                int64_t bytes_sent = this -> send_message(socket_fd, serv_req);
                
                for(int i = 0; i < bytes_sent; ++i) {
                    // std::cout << int(serv_req.message[i]) << " ";
                }
                // std::cout << std::endl << "CID" << serv_req.client_id << std::endl;
                
                if (bytes_sent < 0) {
                    this -> request_to_remove_fd(socket_fd);
                    continue;
                }

                // Re-acquire lock to check if message is complete
                lock.lock();
                
                // Check if entire message has been sent
                if (serv_req.bytes_processed >= serv_req.bytes_to_process) {
                    ++msg_sent;
                    // Message complete, remove from buffer
                    this -> partial_write_buffers.erase(socket_fd);
                    
                    // Try to load next message from queue
                    Server_Request next_msg;
                    lock.unlock();
                    
                    bool has_more = this -> tactical_reload_partition(socket_fd, next_msg);
                    
                    if (!has_more) {
                        // No more messages, switch back to EPOLLIN
                        this -> modify_socket_for_receiving_epoll(socket_fd);
                    } else {
                        // More messages available, keep in EPOLLOUT mode
                        lock.lock();
                        this -> partial_write_buffers[socket_fd] = std::move(next_msg);
                        lock.unlock();
                        // Stay in EPOLLOUT mode - will trigger again
                    }
                }
    // else: partial send, stay in EPOLLOUT, will retry with updated bytes_processed
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
    protocol_message_len_type msg_len = sizeof(protocol_id_t) + sizeof(protocol_message_len_type) + sizeof(protocol_array_len_type) + sizeof(command_code_type);
    for(const Entry& entry : entry_array) {
        msg_len += sizeof(protocol_key_len_type) + sizeof(protocol_value_len_type);
        msg_len += entry.get_key_length() + entry.get_value_length();
    }

    protocol_array_len_type array_len = entry_array.size();
    command_code_type com_code = COMMAND_CODE_OK;
    array_len = protocol_arr_len_hton(array_len);
    protocol_message_len_type net_msg_len = protocol_msg_len_hton(msg_len);
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
    Command_Code com_code = this -> extract_command_code(serv_msg.message, true);

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
        key_str = this -> extract_key_str_from_msg(serv_msg.message, true);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
        std::cerr << e.what() << std::endl;
        }

        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }

    // extract the data
    std::string value_str;
    try {
        value_str = this -> extract_value(serv_msg.message);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << e.what();
        }

        this -> prepare_socket_for_err_response(socket_fd, true , serv_msg.client_id);
        return 0;
    }

    // insert the key value pair into the inner lsm tree
    if(this -> lsm_tree.set(key_str, value_str)) {
        std::cout << "SET: <key> " << key_str << " <value> " << value_str << std::endl; 
        this -> prepare_socket_for_ok_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }
    else {
        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    } 
    return -1;
}

int8_t Partition_Server::handle_get_request(socket_t socket_fd, const Server_Message& serv_msg) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(serv_msg.message, true);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
        std::cerr << e.what() << std::endl;
        }
        
        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }

    bool found = false;
    std::string value_str;
    try {
        Entry entry = lsm_tree.get(key_str);
        if(entry.is_deleted() || entry.get_string_key_bytes() == ENTRY_PLACEHOLDER_KEY) {
            this -> prepare_socket_for_not_found_response(socket_fd, serv_msg.client_id);
            return 0;
        }
        else {
            std::cout << "GET: <key> " << key_str << " <value> " << entry.get_value_string() << std::endl; 
            std::string entries_resp = this -> create_entries_response({entry}, serv_msg.client_id);
            Server_Message serv_resp;
            serv_resp.bytes_processed = 0;
            serv_resp.message = entries_resp;
            serv_resp.bytes_to_process = entries_resp.size();
            serv_resp.client_id = serv_msg.client_id;
            // std::cout << "HERE" << std::endl;
            this -> prepare_socket_for_response(socket_fd, serv_resp);
            return 0;
        }
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }
    return -1;
}

int8_t Partition_Server::handle_remove_request(socket_t socket_fd, const Server_Message& serv_msg) {
    std::cout << "REMOVE: <key> " << std::endl;         
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(serv_msg.message, true);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }
        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }

    if(lsm_tree.remove(key_str)) {
        this -> prepare_socket_for_ok_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }
    else {
        this -> prepare_socket_for_err_response(socket_fd, true, serv_msg.client_id);
        return 0;
    }

    return -1;
}

void Partition_Server::prepare_socket_for_not_found_response(socket_t socket_fd, protocol_id_t client_id) {
    Server_Message resp = this -> create_status_response(Command_Code::COMMAND_CODE_DATA_NOT_FOUND, true, client_id);
    this -> modify_socket_for_sending_epoll(socket_fd);
    this -> add_message_to_response_queue(socket_fd, resp);
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