#ifndef YSQL_SERVER_H_INCLUDED
#define YSQL_SERVER_H_INCLUDED

#include <cstdint>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

// server must
// create a tcp port
// etc...

class Server {
    protected:
        uint16_t port;
        int32_t server_fd;
        struct sockaddr_in address;

    public:
        Server(uint16_t port);

        virtual int8_t start();

};

#endif // YSQL_SERVER_H_INCLUDED
