#include "../include/primary_server.h"

Primary_Server::Primary_Server(uint16_t port) : Server(port) {

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
        std::string listen_failed_str(PRIMARY_SERVER_FAILED_LISTEN_ERR_MSG);
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

    while (true) {
        // Accept one client
        new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&address_length);
        if (new_socket < 0) {
            std::cerr << PRIMARY_SERVER_FAILED_ACCEPT_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
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
        std::cout << PRIMARY_SERVER_PARTITION_STR_PREFIX << i << " " << (status.at(i)? PARTITION_ALIVE_MSG : PARTITION_DEAD_MSG) << std::endl;
    }
}

std::vector<bool> Primary_Server::get_partitions_status() const {
    std::vector<std::pair<std::string, uint16_t>> partitons = {
        {PARTITION_SERVER_1_NAME, PARTITION_SERVER_1_PORT},
        {PARTITION_SERVER_2_NAME, PARTITION_SERVER_2_PORT},
        {PARTITION_SERVER_3_NAME, PARTITION_SERVER_3_PORT},
        {PARTITION_SERVER_4_NAME, PARTITION_SERVER_4_PORT},
        {PARTITION_SERVER_5_NAME, PARTITION_SERVER_5_PORT},
        {PARTITION_SERVER_6_NAME, PARTITION_SERVER_6_PORT},
        {PARTITION_SERVER_7_NAME, PARTITION_SERVER_7_PORT},
        {PARTITION_SERVER_8_NAME, PARTITION_SERVER_8_PORT},
    };

    std::vector<bool> partitions_status;
    partitions_status.reserve(PARTITION_COUNT);

    for(uint32_t i = 0; i < PARTITION_COUNT; ++i) {
        partitions_status.push_back(try_connect(partitons.at(i).first, partitons.at(i).second));
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





