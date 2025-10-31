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

#define SERVER_LISTENING_ON_PORT_MSG "Listening on port: "

#define SERVER_BIND_FAILED_ERR_MSG "bind() failed\n"
#define SERVER_SET_SOCK_OPT_FAILED_ERR_MSG "setsockopt() failed\n"

#define SERVER_SOCKET_FAILED_ERR_MSG "socket() failed\n"

#define SERVER_ERRNO_STR_PREFIX "errno: "

#define SERVER_FAILED_LISTEN_ERR_MSG "Listen failed: "
#define SERVER_FAILED_ACCEPT_ERR_MSG "Accept failed: "

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
};

#endif // YSQL_SERVER_H_INCLUDED
