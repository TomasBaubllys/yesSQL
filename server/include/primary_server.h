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
#include "cursor.h"
#include "server_message.h"

#define PRIMARY_SERVER_THREAD_POOL_SIZE_ENV_VAR "PRIMARY_SERVER_THREAD_POOL_SIZE"

#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_STRING "PARTITION_COUNT"
#define PRIMARY_SERVER_PARTITION_COUNT_ENVIROMENT_VARIABLE_UNDEF_ERR_MSG "Enviromental variable PARTITION_COUNT is undefined...\n"
#define PRIMARY_SERVER_PARTITION_COUNT_ZERO_ERR_MSG "Partition count is 0!\n"

#define PRIMARY_SERVER_NPOS_FAILED_ERR_MSG "Could not extract the key from the message received - npos failed\n"
#define PRIMARY_SERVER_NPOS_BAD_ERR_MSG "Bad or corrupted message received, could not extract key\n"
#define PRIMARY_SERVER_FAILED_PARTITION_ADD_ERR_MSG "Failed to add partitions to epoll: " 

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
        // maps client_id to client socket
        std::unordered_map<uint64_t, socket_t> id_client_map;
        std::shared_mutex id_client_map_mutex;

        // maps client socket to client_id
        std::unordered_map<socket_t, uint64_t> client_id_map;
        std::shared_mutex client_id_map_mutex;

        // id pool for new clients
        std::atomic<uint64_t> req_id{1};

        uint32_t partition_count;

        uint32_t partition_range_length;

        std::shared_mutex partitions_mutex;
        std::vector<Partition_Entry> partitions;

        // map for client cursors
        std::shared_mutex client_cursor_map_mutex;
        std::unordered_map<socket_t, std::unordered_map<std::string,Cursor>> client_cursor_map;

        // functions for partition monitoring, currently unused
        std::vector<bool> get_partitions_status() const;
        void display_partitions_status() const;
        void start_partition_monitor_thread() const;

        uint32_t key_prefix_to_uint32(const std::string& key) const;

        Partition_Entry get_partition_for_key(const std::string& key);

        std::vector<Partition_Entry> get_partitions_ff(const std::string& key) const;

        std::vector<Partition_Entry> get_partitions_fb(const std::string& key) const;

        // tries to reconnect to a partition
        bool ensure_partition_connection(Partition_Entry& partition);

        void add_client_socket_to_epoll_ctx();

        int8_t process_client_in(socket_t client_fd, Server_Message msg);

        int8_t process_partition_response(Server_Message&& msg);

        void add_partitions_to_epoll();

        // sets client_fd to epollout and adds a message to its write_buffer
        void queue_client_for_error_response(socket_t client_fd, protocol_id_t client_id);

        void queue_client_for_ok_response(socket_t client_fd, protocol_id_t client_id);

        // THROWS
        void queue_client_for_response(Server_Message &&serv_msg);

        // checks if the socket still belongs to the same client
        // returns the socket if it is still the same client
        // not recomended for threads, use with the main thread before sending a message
        socket_t is_still_same_client(protocol_id_t client_id);

        void process_remove_queue();

        // not thread safe, use only it main thread when a send / receive returns 0
        void remove_client(socket_t client_fd);

        Cursor extract_cursor_creation(const Server_Message& message);

        std::string extract_cursor_name(const Server_Message& message);

        std::pair<std::string, uint64_t> extract_cursor_name_pos(const Server_Message& message);


        std::pair<std::string, cursor_cap_t> extract_cursor_name_cap(const Server_Message& message);

    public:
        Primary_Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL,  uint32_t thread_pool_size = SERVER_DEFAULT_THREAD_POOL_VAL);

        ~Primary_Server();

        int8_t start() override;
};

#endif // YSQL_PRIMARY_SERVER_H_INCLUDED
