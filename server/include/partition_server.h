#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "server.h"

#define PARTITION_COUNT 8

#define PARTITION_SERVER_1_NAME "partition_server1"
#define PARTITION_SERVER_1_PORT 9001
#define PARTITION_SERVER_2_NAME "partition_server2"
#define PARTITION_SERVER_2_PORT 9002
#define PARTITION_SERVER_3_NAME "partition_server3"
#define PARTITION_SERVER_3_PORT 9003
#define PARTITION_SERVER_4_NAME "partition_server4"
#define PARTITION_SERVER_4_PORT 9004
#define PARTITION_SERVER_5_NAME "partition_server5"
#define PARTITION_SERVER_5_PORT 9005
#define PARTITION_SERVER_6_NAME "partition_server6"
#define PARTITION_SERVER_6_PORT 9006
#define PARTITION_SERVER_7_NAME "partition_server7"
#define PARTITION_SERVER_7_PORT 9007
#define PARTITION_SERVER_8_NAME "partition_server8"
#define PARTITION_SERVER_8_PORT 9008

#define COLOR_RED     "\033[31m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_RESET   "\033[0m"

#define PARTITION_ALIVE_MSG "[" COLOR_GREEN "V" COLOR_RESET "]"
#define PARTITION_DEAD_MSG  "[" COLOR_RED   "X" COLOR_RESET "]"

class Partition_Server : public Server {
    private:

    public:
        Partition_Server(uint16_t port);

        int8_t start() override;
};

#endif // YSQL_PARTITION_SERVER_H_INCLUDED