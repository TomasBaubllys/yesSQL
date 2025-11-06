#include "../include/partition_server.h"

Partition_Server::Partition_Server(uint16_t port, uint8_t verbose) : Server(port, verbose), lsm_tree() {

}

int8_t Partition_Server::start() {
    socket_t client_socket;
    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    if (listen(this->server_fd, 3) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    // std::cout << "Partition server listening on port " << port << "..." << std::endl;

    while (true) {
        std::cout << "Waiting for connection..." << std::endl;

        client_socket = accept(this -> server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_socket < 0) {
            if(this -> verbose > 0) {
                std::cerr << SERVER_FAILED_ACCEPT_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            }
            continue; // skip this client
        }

        std::string raw_message;
        try {
            raw_message = this -> read_message(client_socket);
        }
        catch (const std::exception& e) {
            if(this -> verbose > 0) {
                std::cerr << e.what() << std::endl;
            }
            // skip this client
            close(client_socket);
            continue;
        }
        // extract the command code
        Command_Code com_code = this -> extract_command_code(raw_message);

        switch(com_code) {
            case COMMAND_CODE_GET: {
                // extract the key
                std::string key_str;
                try {
                    key_str = this -> extract_key_str_from_msg(raw_message);
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                       std::cerr << e.what() << std::endl; 
                    }

                    this -> send_error_response(client_socket);
                    break;
                }
                // search for the value
                bool found = false;
                std::string value_str;
                // REMOVE THIS TRY CATCH AFTER IT HAS BEEN FIGURED OUT!!!!
                try {
                    Entry entry = lsm_tree.get(key_str);
                    if(entry.is_deleted() || entry.get_string_key_bytes() == ENTRY_PLACEHOLDER_KEY) {
                        this -> send_not_found_response(client_socket);
                        break;
                    }
                    else {
                        this -> send_entries_response({entry}, client_socket);
                    }
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }

                    this -> send_error_response(client_socket);
                    break;
                }

                break;
            }
            case COMMAND_CODE_SET: {
                // extract the key
                std::string key_str;
                try {
                    key_str = this -> extract_key_str_from_msg(raw_message);
                }
                catch(const std::exception& e) {
                    if(this -> verbose > 0) {
                       std::cerr << e.what() << std::endl; 
                    }

                    this -> send_error_response(client_socket);
                    break;
                }

                // extract the data
                std::string value_str;
                try {
                    value_str = this -> extract_value(raw_message);
                }
                catch (const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << e.what();
                    }

                    this -> send_error_response(client_socket);
                    break;
                }

                // insert the key value pair into the inner lsm tree
                if(this -> lsm_tree.set(key_str, value_str)) {
                    this -> send_ok_response(client_socket);
                }
                else {
                    this -> send_error_response(client_socket);
                }

                break;
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
                std::string key_str;
                try {
                    key_str = this -> extract_key_str_from_msg(raw_message);
                }
                catch (const std::exception& e) {
                    if(this -> verbose > 0) {
                        std::cerr << e.what() << std::endl;
                    }
                    this -> send_error_response(client_socket);
                    break;
                }

                if(lsm_tree.remove(key_str)) {
                    this -> send_ok_response(client_socket);
                }                
                else {
                    this -> send_error_response(client_socket);
                }

                break;
            }

            default: {

            }
        }

        // close(client_socket);
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

