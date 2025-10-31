#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "server.h"
#include <cstdio>
// #include "../../lsm_tree/include/lsm_tree.h"

#define PARTITION_SERVER_NAME_PREFIX "yessql-partition_server-"
#define PARTITION_SERVER_PORT 9000

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