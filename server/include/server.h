#ifndef YSQL_SERVER_H_INCLUDED
#define YSQL_SERVER_H_INCLUDED

#include <cstdint>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>

class Server {
    private:
        uint32_t port;

    public:
        Server(uint32_t port);

        int8_t start();

};

#endif // YSQL_SERVER_H_INCLUDED
