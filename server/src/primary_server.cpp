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
        new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&address_length);
        if (new_socket < 0) {
            std::cerr << SERVER_FAILED_ACCEPT_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            continue; // Skip this client and keep listening
        }

        // Print client info
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(address.sin_addr), client_ip, INET_ADDRSTRLEN);

        #ifdef PRIMARY_SERVER_DEBUG
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
        partitions_status.push_back(try_connect(this -> partitions.at(i).partition_name, PARTITION_SERVER_PORT));
    }

    return partitions_status;
}

bool Primary_Server::try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec) const {
    int32_t sock = socket(AF_INET, SOCK_STREAM, 0);
    if(sock < 0) {
        return false;
    }

    struct hostent* server = gethostbyname(hostname.c_str());
    if(!server) {
        std::cerr << PRIMARY_SERVER_FAILED_HOSTNAME_RESOLVE << hostname << std::endl;
        close(sock);
        return false;
    }

    struct sockaddr_in serv_addr{};
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    std::memcpy(&serv_addr.sin_addr.s_addr, server -> h_addr, server -> h_length);

    bool success = (connect(sock, (struct sockaddr*)& serv_addr, sizeof(serv_addr)) == 0);

    close(sock);

    return success;
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

}

std::vector<Partition_Entry> Primary_Server::get_partitions_fb(const std::string& key) const {

}