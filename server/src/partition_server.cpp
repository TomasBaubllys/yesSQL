#include "../include/partition_server.h"

Partition_Server::Partition_Server(uint16_t port, uint8_t verbose) : Server(port, verbose), lsm_tree() {

}

int8_t Partition_Server::start() {
    if (listen(this->server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    if(this -> init_epoll() < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_CREATE_ERR_MSG);
    }

    if (this -> add_this_to_epoll() < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }

    while (true) {
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
            
            if(socket_fd == this -> server_fd) {
                socket_t client_fd = this -> add_client_socket_to_epoll();
                this -> sockets_map[client_fd] = Socket_Types::MAIN_SERVER_SOCKET;
            }
            else if(this -> epoll_events[i].events & EPOLLIN) {
                std::string message = this -> read_message(socket_fd);
                if(message.empty()) {
                    continue;
                }

                this -> thread_pool.enqueue([this, socket_fd, message](){
                    this -> handle_client(socket_fd, message);
                });
            }
            else if(this -> epoll_events[i].events & EPOLLOUT) {
                std::lock_guard<std::mutex> lock(this -> response_queue_mutex);
                std::unordered_map<socket_t, Server_Response>::iterator msg = this -> partial_write_buffers.find(socket_fd);
                
                if(msg != this -> partial_write_buffers.end()) {
                    std::string& data = msg -> second.message;
                    int64_t bytes_sent = this -> send_message(socket_fd, data);
                    if(bytes_sent > 0) {
                        msg -> second.bytes_processed += bytes_sent;
                    }

                    if(msg -> second.bytes_to_process <= msg -> second.bytes_processed) {
                        this -> partial_write_buffers.erase(msg);
                        // return to listening stage
                        try {
                            this -> modify_epoll_event(server_fd, EPOLLIN | EPOLLET);
                        }
                        catch(const std::exception& e) {
                            ///
                        } 
                    }
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



int8_t Partition_Server::send_not_found_response(socket_t socket) const {
    return this -> send_status_response(COMMAND_CODE_DATA_NOT_FOUND, socket);
}

int8_t Partition_Server::send_entries_response(const std::vector<Entry>& entry_array, socket_t socket) const {
    // calculate the total amount of space needed
    protocol_message_len_type msg_len = sizeof(protocol_message_len_type) + sizeof(protocol_array_len_type) + sizeof(command_code_type);
    for(const Entry& entry : entry_array) {
        msg_len += sizeof(protocol_key_len_type) + sizeof(protocol_value_len_type);
        msg_len += entry.get_key_length() + entry.get_value_length();
    }

    protocol_array_len_type array_len = entry_array.size();
    command_code_type com_code = COMMAND_CODE_OK;
    array_len = protocol_arr_len_hton(array_len);
    protocol_message_len_type net_msg_len = protocol_msg_len_hton(msg_len);
    com_code = command_hton(com_code);

    size_t curr_pos = 0;
    std::string raw_message(msg_len, '\0');
    memcpy(&raw_message[0], &net_msg_len, sizeof(net_msg_len));
    curr_pos += sizeof(net_msg_len);
    
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

    try {
        this -> send_message(socket, raw_message);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        return -1;
    }

    return 0;
}

int8_t Partition_Server::process_request(socket_t socket_fd, const std::string& message) {
        // extract the command code
    Command_Code com_code = this -> extract_command_code(message);

    switch(com_code) {
        case COMMAND_CODE_GET: {
            std::shared_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
            return this -> handle_get_request(socket_fd, message);
        }
        case COMMAND_CODE_SET: {
            // extract the key
            std::unique_lock<std::shared_mutex> lsm_lock(this -> lsm_tree_mutex);
            return this -> handle_set_request(socket_fd, message);
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
            return this -> handle_remove_request(socket_fd, message);
        }

        default: {

        }
    }

    return 0;
}


int8_t Partition_Server::handle_set_request(socket_t socket_fd, const std::string& message) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(message);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
        std::cerr << e.what() << std::endl;
        }

        return this -> send_error_response(socket_fd);
    }

    // extract the data
    std::string value_str;
    try {
        value_str = this -> extract_value(message);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << e.what();
        }

        return this -> send_error_response(socket_fd);
    }

    // insert the key value pair into the inner lsm tree
    if(this -> lsm_tree.set(key_str, value_str)) {
        return this -> send_ok_response(socket_fd);
    }
    else {
        return this -> send_error_response(socket_fd);
    } 
    return -1;
}

int8_t Partition_Server::handle_get_request(socket_t socket_fd, const std::string& message) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(message);
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
        std::cerr << e.what() << std::endl;
        }

        return this -> send_error_response(socket_fd);
    }

    bool found = false;
    std::string value_str;
    try {
        Entry entry = lsm_tree.get(key_str);
        if(entry.is_deleted() || entry.get_string_key_bytes() == ENTRY_PLACEHOLDER_KEY) {
            return this -> send_not_found_response(socket_fd);
        }
        else {
            return this -> send_entries_response({entry}, socket_fd);
        }
    }
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        return this -> send_error_response(socket_fd);
    }
    return -1;
}

int8_t Partition_Server::handle_remove_request(socket_t socket_fd, const std::string& message) {
    std::string key_str;
    try {
        key_str = this -> extract_key_str_from_msg(message);
    }
    catch (const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }
        return this -> send_error_response(socket_fd);
    }

    if(lsm_tree.remove(key_str)) {
        return this -> send_ok_response(socket_fd);
    }
    else {
        return this -> send_error_response(socket_fd);
    }

    return -1;
}