#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "server.h"

class Partition_Server : public Server {
    private:

    public:
        Partition_Server(uint16_t port);

        int8_t start() override;
};

#endif // YSQL_PARTITION_SERVER_H_INCLUDED