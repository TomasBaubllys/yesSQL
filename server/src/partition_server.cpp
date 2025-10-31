#include "../include/partition_server.h"

Partition_Server::Partition_Server(uint16_t port) : Server(port) {

}

int8_t Partition_Server::start() {
    int32_t new_socket;
    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    const char* msg = "I'm alive\n";

    if (listen(this->server_fd, 3) < 0) {
        perror("listen");
        return -1;
    }

    std::cout << "Partition server listening on port " << port << "..." << std::endl;

    while (true) {
        std::cout << "Waiting for connection..." << std::endl;

        new_socket = accept(this->server_fd, (struct sockaddr*)&client_addr, &client_len);
        // new_socket = accept(this->server_fd, nullptr, nullptr);
        if (new_socket < 0) {
            perror("accept");
            continue;
        }

        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, INET_ADDRSTRLEN);
        std::cout << "Connection from " << client_ip << ":" << ntohs(client_addr.sin_port) << std::endl;

        send(new_socket, msg, strlen(msg), 0);
        std::cout << "Alive message sent!" << std::endl;

        close(new_socket);
    }

    return 0;
}

