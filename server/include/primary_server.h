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
