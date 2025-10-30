#ifndef YSQL_PRIMARY_SERVER_H_INCLUDED
#define YSQL_PRIMARY_SERVER_H_INCLUDED

#include "server.h"
#include <utility>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <netdb.h>
#include <string>

#define PARTITION_COUNT 8
#define PRIMARY_SERVER_DEFAULT_CONNECTION_TIMEOUT

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

// primary server must:
// periodically send request to all partitions to figure out if they are all alive
// rerout requests based on which partition we want to send to
// contain a table of key range [a, b] to know where to rerout to
// ONCE BOOTED GET THE IPS OF ALL THE PARTITIONS TO CHECK IF THEY ARE REACHABLE

class Primary_Server : public Server {
    private:
         std::vector<bool> get_partitions_status() const;

         bool try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec = 1) const;

         void display_partitions_status() const;

    public:
        Primary_Server(uint16_t port);

        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED
