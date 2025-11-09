#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "server.h"
#include <cstdio>
#include "../../lsm_tree/include/lsm_tree.h"
#include <shared_mutex>

#define PARTITION_SERVER_NAME_PREFIX "yessql-partition_server-"
#define PARTITION_SERVER_PORT 9000

#define PARTITION_SERVER_FAILED_TO_EXTRACT_DATA_ERR_MSG "Failed to extract data from message - too short\n"

#define COLOR_RED     "\033[31m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_RESET   "\033[0m"

#define PARTITION_ALIVE_MSG "[" COLOR_GREEN "V" COLOR_RESET "]"
#define PARTITION_DEAD_MSG  "[" COLOR_RED   "X" COLOR_RESET "]"

class Partition_Server : public Server {
    private:
        LSM_Tree lsm_tree;
        
        // mutex used for locking operations of the lsm tree
        // all getters get shared_lock
        // remove and set gets unique_lock
        std::shared_mutex lsm_tree_mutex;

    public:
        Partition_Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        // send a response of all the entries contained in the vector
        std::string create_entries_response(const std::vector<Entry>& entry_array, protocol_id_type client_id)const;

        // THROWS
        // extracts the first value string contained in the message
        std::string extract_value(const std::string& raw_message) const;

        // Processes the clients request GET SET etc...
        int8_t process_request(socket_t socket_fd, const Server_Message& serv_msg);

        // handles SET, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_set_request(socket_t socket_fd, const Server_Message& message);

        // handles GET, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_get_request(socket_t socket_fd, const Server_Message& message);

        // handles REMOVE, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_remove_request(socket_t socket_fd, const Server_Message& message);

        void prepare_socket_for_not_found_response(socket_t socket_fd, protocol_id_type client_id);

        // @brief handles client requests
        // calls the handle int8_t process_request(socket_t, string);
        // on failure pushes the socket_fd to remove_queue 
        void handle_client(socket_t socket_fd, const Server_Message& message);

        int8_t start() override;
};

#endif // YSQL_PARTITION_SERVER_H_INCLUDED
