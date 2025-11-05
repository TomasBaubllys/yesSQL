#include "../include/partition_server.h"

Partition_Server::Partition_Server(uint16_t port, uint8_t verbose) : Server(port, verbose), lsm_tree() {

}

int8_t Partition_Server::start() {
    socket_t new_socket;
    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    const char* msg = "I'm alive\n";

    if (listen(this->server_fd, 3) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    // std::cout << "Partition server listening on port " << port << "..." << std::endl;

    while (true) {
        std::cout << "Waiting for connection..." << std::endl;

        new_socket = accept(this -> server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (new_socket < 0) {
            std::cerr << SERVER_FAILED_ACCEPT_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
            continue; // skip this client
        }

        std::string msg = this -> read_message(new_socket);
        std::cout << "msg received:" << std::endl;
        for(int i = 0; i < msg.size(); ++i) {
            std::cout << msg[i] << std::endl;
        }

        // char client_ip[INET_ADDRSTRLEN];
        // inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        // std::cout << "Connection from " << client_ip << ":" << ntohs(client_addr.sin_port) << std::endl;

        // send(new_socket, msg, strlen(msg), 0);
        // std::cout << "Alive message sent!" << std::endl;

        close(new_socket);
    }

    return 0;
}

