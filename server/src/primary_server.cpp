#include "../include/primary_server.h"

Primary_Server::Primary_Server(uint16_t port) : Server(port) {
    const char* partition_count_str = std::getenv(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING);
    if(!partition_count_str) {
        throw std::runtime_error(PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG);
    }

    this -> partition_count = atoi(partition_count_str);
    if(this -> partition_count == 0) { // || this -> partition_count >= PRIMARY_SERVER_MAX_PARTITION_COUNT) {
        throw std::runtime_error(PRIMARY_SERVER_PARTITION_COUNT_ZERO_ERR_MSG);
    }

    this -> partition_range_length = std::numeric_limits<uint32_t>::max() / this -> partition_count;

    this -> partitions.reserve(partition_count);

    for(uint32_t i = 0; i < this -> partition_count; ++i) {
        Partition_Entry partition_entry;
        partition_entry.partition_name = PARTITION_SERVER_NAME_PREFIX + std::to_string(i + 1);
        
        partition_entry.range.beg = this -> partition_range_length * i;
        partition_entry.range.end = this ->  partition_range_length * (i + 1);

        // for the last partition is everything until the max index
        if(i == this -> partition_count - 1) {
            partition_entry.range.end = std::numeric_limits<uint32_t>::max();
        }

        this -> partitions.push_back(partition_entry);
    }
}

void Primary_Server::start_partition_monitor_thread() const {
    std::thread status_thread([this]() {
        while(true) {
            this -> display_partitions_status();
            std::this_thread::sleep_for(std::chrono::seconds(PRIMARY_SERVER_PARTITION_CHECK_INTERVAL));
        }
    });
    status_thread.detach();
}

int8_t Primary_Server::start() {
    uint32_t new_socket;
    uint32_t address_length = sizeof(this -> address);
    const char* msg = PRIMARY_SERVER_HELLO_MSG;

    if(listen(server_fd, PRIMARY_SERVER_DEFAULT_LISTEN_VALUE) < 0) {
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    #ifdef PRIMARY_SERVER_PARTITION_MONITORING
        this -> start_partition_monitor_thread();
    #endif // PRIMARY_SERVER_PARTITION_MONITORING

    #ifdef PRIMARY_SERVER_DEBUG
        std::cout << SERVER_LISTENING_ON_PORT_MSG << port << "..." << std::endl;
    #endif // PRIMARY_SERVER_DEBUG

    // accept a client, read their message, if(key needs range) check ranges

    while (true) {
        // Accept one client
        struct sockaddr_in client_addr;
        socklen_t client_addr_len = sizeof(client_addr);
        new_socket = accept(server_fd, (struct sockaddr*)&client_addr, &client_addr_len);
        if (new_socket < 0) {
            std::cerr << SERVER_FAILED_ACCEPT_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            continue; // Skip this client and keep listening
        }

        // print the message received from the client
        std::string rec_msg = this -> read_message(new_socket);
        std::cout << rec_msg.substr(8) << std::endl;

        // Print client info
        #ifdef PRIMARY_SERVER_DEBUG
            char client_ip[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &(address.sin_addr), client_ip, INET_ADDRSTRLEN);
            std::cout << "Client connected from " << client_ip << ":" << ntohs(address.sin_port) << std::endl;
        #endif // PRIMARY_SERVER_DEBUG 

        // Send message
        send(new_socket, msg, strlen(msg), 0);

        // Close client socket
        close(new_socket);
    }

    Server::start();
    return 0;
}

void Primary_Server::display_partitions_status() const {
    std::vector<bool> status = this -> get_partitions_status();

    for(uint32_t i = 0; i < status.size(); ++i) {
        std::cout << this -> partitions.at(i).partition_name << " " << (status.at(i)? PARTITION_ALIVE_MSG : PARTITION_DEAD_MSG) << std::endl;
    }
}

std::vector<bool> Primary_Server::get_partitions_status() const {
    std::vector<bool> partitions_status;
    partitions_status.reserve(this -> partition_count);

    for(uint32_t i = 0; i < this -> partition_count; ++i) {
        partitions_status.push_back(try_connect(this -> partitions.at(i).partition_name, this -> partitions.at(i).port));
    }

    return partitions_status;
}

uint32_t Primary_Server::key_prefix_to_uint32(const std::string& key) const {
    uint32_t uint32_prefix_key = 0;

    uint32_t key_limit = std::min<uint32_t>(PRIMARY_SERVER_BYTES_IN_KEY_PREFIX, key.length());

    for(uint32_t i = 0; i < key_limit; ++i) {
        uint32_prefix_key |= static_cast<uint32_t>(static_cast<uint8_t>(key[i]) << (8 * (PRIMARY_SERVER_BYTES_IN_KEY_PREFIX - 1 - i)));
    }

    return uint32_prefix_key;
}

Partition_Entry Primary_Server::get_partition_for_key(const std::string& key) const {
    uint32_t uint32_key_prefix = this -> key_prefix_to_uint32(key);

    uint32_t partition_index = static_cast<uint32_t>(uint32_key_prefix / this -> partition_range_length);

    if(partition_index >= this -> partitions.size()) {
        --partition_index;
    }

    return this -> partitions.at(partition_index);
}

std::vector<Partition_Entry> Primary_Server::get_partitions_ff(const std::string& key) const {
    return std::vector<Partition_Entry>();
}

std::vector<Partition_Entry> Primary_Server::get_partitions_fb(const std::string& key) const {
    return std::vector<Partition_Entry>();
}

std::string Primary_Server::extract_key_str_from_msg(const std::string& raw_message) const {
    // check if the command is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type)  > raw_message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_type key_len = 0;
    memcpy(&key_len, &raw_message[PROTOCOL_FIRST_KEY_LEN_POS], sizeof(key_len));

    // check if msg is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type) + key_len > raw_message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return raw_message.substr(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type), key_len);
}

int8_t Primary_Server::handle_client_request(socket_t client_socket) const {
    // read the message
    std::string raw_message;
    try{
        raw_message = this -> read_message(client_socket);
    }
    catch(const std::exception &e) {
        std::cerr << e.what() << std::endl; // add some variable that checks if the server is in printing mode
        close(client_socket);
        return -1;
    }
    // extract the command code
    Command_Code com_code = this -> extract_command_code(raw_message);

    // decide how to handle it
    switch(com_code) {
        case COMMAND_CODE_GET: {
            break;
        }
        case COMMAND_CODE_SET: {
            // extract the key string
            std::string key_str;
            try {
                std::string key_str = this -> extract_key_str_from_msg(raw_message);
            }
            catch(const std::exception& e) {
                std::cerr << e.what() << std::endl; // add some variable that checks if the server is in printing mode
                // send a message to the client about invalid operation
                close(client_socket);
                return -1;
            }
            // find to which partition entry it belongs to
            Partition_Entry partition_entry = this -> get_partition_for_key(key_str);

            // forward the message there
            std::string partition_response = this -> query_parition(partition_entry,raw_message);

            // forward the response to the client


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
            break;
        }
        default: {

            break;
        }

    }

    return 0;
}


std::string Primary_Server::query_parition(const Partition_Entry& partition, const std::string &raw_message) const {
    bool success = false;
    socket_t partition_socket = this -> connect_to(partition.partition_name, partition.port, success);
    if(!success) {
        std::string fail_str(PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG);
        fail_str += partition.partition_name;
        throw std::runtime_error(fail_str);
    }

    uint64_t bytes_sent = this -> send_message(partition_socket, raw_message);
    if(bytes_sent == 0) {
        std::string fail_str(PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG);
        fail_str += partition.partition_name;
        close(partition_socket);
        throw std::runtime_error(fail_str);
    }

    std::string response;
    try {
        response = this -> read_message(partition_socket);
    }
    catch(...) {
        close(partition_socket);
        throw;
    }

    close(partition_socket);
    return response;
}
