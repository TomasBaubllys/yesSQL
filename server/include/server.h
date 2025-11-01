#ifndef YSQL_SERVER_H_INCLUDED
#define YSQL_SERVER_H_INCLUDED

#include <cstdint>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdexcept>
#include <cstdio>
#include <string>
#include "protocol.h"

#define SERVER_LISTENING_ON_PORT_MSG "Listening on port: "

#define SERVER_BIND_FAILED_ERR_MSG "bind() failed\n"
#define SERVER_SET_SOCK_OPT_FAILED_ERR_MSG "setsockopt() failed\n"

#define SERVER_SOCKET_FAILED_ERR_MSG "socket() failed\n"

#define SERVER_ERRNO_STR_PREFIX "errno: "

#define SERVER_FAILED_LISTEN_ERR_MSG "Listen failed: "
#define SERVER_FAILED_ACCEPT_ERR_MSG "Accept failed: "
#define SERVER_FAILED_RECV_ERR_MSG "Recv failed: "
#define SERVER_FAILED_SEND_ERR_MSG "Send failed: "

#define SERVER_MESSAGE_BLOCK_SIZE 1024

#define SERVER_OK_MSG "OK"

class Server {
    protected:
        uint16_t port;
        int32_t server_fd;
        struct sockaddr_in address;

    public:
        // THROWS
        Server(uint16_t port);

        virtual int8_t start();

        std::string read_message(uint32_t socket) const;

        int64_t send_message(uint32_t socket, const std::string& message) const;
};

#endif // YSQL_SERVER_H_INCLUDED
