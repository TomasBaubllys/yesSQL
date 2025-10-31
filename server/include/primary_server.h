#ifndef YSQL_PRIMARY_SERVER_H_INCLUDED
#define YSQL_PRIMARY_SERVER_H_INCLUDED

#include "server.h"
#include <utility>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <netdb.h>
#include <string>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include "partition_server.h"

#define PRIMARY_SERVER_PARTITION_CHECK_INTERVAL 10
#define PRIMARY_SERVER_HELLO_MSG "Hello from yesSQL server"

#define PRIMARY_SERVER_FAILED_HOSTNAME_RESOLVE "Failed to resolve: "
#define PRIMARY_SERVER_FAILED_LISTEN_ERR_MSG "Listen failed: "
#define PRIMARY_SERVER_FAILED_ACCEPT_ERR_MSG "Accept failed: "

#define PRIMARY_SERVER_DEFAULT_LISTEN_VALUE 10

// comment/uncomment this line for debug
#define PRIMARY_SERVER_DEBUG

// comment/uncomment this line for partition monitoring
#define PRIMARY_SERVER_PARTITION_MONITORING
#define PRIMARY_SERVER_PARTITION_STR_PREFIX "Partition "

#define PRIMARY_SERVER_DEFAULT_PARTITION_KEY_LEN 8192
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_1 1 
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_2 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_3 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_4 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_5 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_6 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_7 1
#define PRIMARY_SERVER_DEFAULT_RATIO_PARTITION_8 1

// SUM(RATIO_PARTITIONS) = PARTITION_COUNT
// PARTITIONS key ranges will be calculated like 8192 * i + 8192 * RATIO

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

         void start_partition_monitor_thread() const;

    public:
        Primary_Server(uint16_t port);

        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED
