#ifndef YSQL_PRIMARY_SERVER_H_INCLUDED
#define YSQL_PRIMARY_SERVER_H_INCLUDED

#include "protocol.h"
#include "server.h"
#include <utility>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <netdb.h>
#include <string>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include "partition_server.h"
#include "../include/partition_entry.h"
#include <limits>
#include <unistd.h>
#include "fd_context.h"

#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING "PARTITION_COUNT"
#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG "Enviromental variable PARTITION_COUNT is undefined...\n"
#define PRIMARY_SERVER_PARTITION_COUNT_ZERO_ERR_MSG "Partition count is 0!\n"

#define PRIMARY_SERVER_NPOS_FAILED_ERR_MSG "Could not extract the key from the message received - npos failed\n"
#define PRIMARY_SERVER_NPOS_BAD_ERR_MSG "Bad or corrupted message received, could not extract key\n"

#define PRIMARY_SERVER_FAILED_PARTITION_QUERY_ERR_MSG "Failed to query the partition: "

#define PRIMARY_SERVER_PARTITION_CHECK_INTERVAL 10
#define PRIMARY_SERVER_HELLO_MSG "Hello from yesSQL server"

#define PRIMARY_SERVER_DEFAULT_LISTEN_VALUE 1000

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
        // id -> query
        std::unordered_map<uint64_t, Query_Context> query_map;
        std::shared_mutex query_map_mutex;

        std::unordered_map<uint64_t, socket_t> id_client_map;
        std::shared_mutex id_client_map_mutex;

        std::unordered_map<socket_t, uint64_t> client_id_map;
        std::shared_mutex client_id_map_mutex;

        // parittion -> queue of client ids
        std::shared_mutex partition_queues_mutex;
        std::unordered_map<socket_t, std::queue<uint64_t>> partition_queues;

        std::atomic<uint64_t> req_id{1};

        uint32_t partition_count;

        uint32_t partition_range_length;

        std::vector<Partition_Entry> partitions;

        std::vector<bool> get_partitions_status() const;

        void display_partitions_status() const;

        void start_partition_monitor_thread() const;

        uint32_t key_prefix_to_uint32(const std::string& key) const;

        Partition_Entry& get_partition_for_key(const std::string& key);

        std::vector<Partition_Entry> get_partitions_ff(const std::string& key) const;

        std::vector<Partition_Entry> get_partitions_fb(const std::string& key) const;

        // THROWS
        std::string query_partition(Partition_Entry& partition, const std::string& raw_message);

        bool ensure_partition_connection(Partition_Entry& partition);

        // int8_t process_request(socket_t socket_fd, const  std::string& message);

        socket_t add_client_socket_to_epoll_ctx(Fd_Type fd_type);

        int8_t process_client_in(socket_t socket_fd, const Server_Message& msg);

        int8_t process_partition_in(socket_t socket_fd, const Server_Message& msg);

        // if partition has jobs in its queue it adds a new one to the partial buffers
        bool tactical_reload_partition(socket_t socket_fd);

    public:
        Primary_Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED
