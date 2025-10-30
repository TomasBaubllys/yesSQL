#ifndef YSQL_PRIMARY_SERVER_H_INLCUDED
#define YSQL_PRIMARY_SERVER_H_INCLUDED

#include "server.h"

#define PARTITION_COUNT 8

// primary server must:
// periodically send request to all partitions to figure out if they are all alive
// rerout requests based on which partition we want to send to
// contain a table of key range [a, b] to know where to rerout to
// ONCE BOOTED GET THE IPS OF ALL THE PARTITIONS TO CHECK IF THEY ARE REACHABLE

class Primary_Server : public Server {
    private:

    public:
        Primary_Server(uint16_t port);
        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED