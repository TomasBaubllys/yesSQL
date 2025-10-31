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
#include <cstdlib>
#include "../include/partition_entry.h"
#include <limits>

#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING "PARTITION_COUNT"
#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG "Enviromental variable PARTITION_COUNT is undefined...\n"
// #define PRIMARY_SERVER_MAX_PARTITION_COUNT 256
#define PRIMARY_SERVER_PARTITION_COUNT_ZERO_ERR_MSG "Partition count is 0!\n"

#define PRIMARY_SERVER_PARTITION_CHECK_INTERVAL 10
#define PRIMARY_SERVER_HELLO_MSG "Hello from yesSQL server"

#define PRIMARY_SERVER_FAILED_HOSTNAME_RESOLVE "Failed to resolve: "

#define PRIMARY_SERVER_DEFAULT_LISTEN_VALUE 10

// comment/uncomment this line for debug
#define PRIMARY_SERVER_DEBUG

// comment/uncomment this line for partition monitoring
#define PRIMARY_SERVER_PARTITION_MONITORING
#define PRIMARY_SERVER_PARTITION_STR_PREFIX "Partition "

#define PRIMARY_SERVER_BYTES_IN_KEY_PREFIX 4

// primary server must:
// periodically send request to all partitions to figure out if they are all alive
// rerout requests based on which partition we want to send to
// contain a table of key range [a, b] to know where to rerout to
// ONCE BOOTED GET THE IPS OF ALL THE PARTITIONS TO CHECK IF THEY ARE REACHABLE

class Primary_Server : public Server {
    private:
        uint32_t partition_count;

        uint32_t partition_range_length;

        std::vector<Partition_Entry> partitions;

        std::vector<bool> get_partitions_status() const;

        bool try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec = 1) const;

        void display_partitions_status() const;

        void start_partition_monitor_thread() const;

        uint32_t key_prefix_to_uint32(const std::string& key) const;

        Partition_Entry get_partition_for_key(const std::string& key) const;

    public:
        Primary_Server(uint16_t port);

        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED
