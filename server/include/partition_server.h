#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "commands.h"
#include "cursor.h"
#include "protocol.h"
#include "server.h"
#include <cstdio>
#include "../../lsm_tree/include/lsm_tree.h"
#include "server_message.h"
#include <shared_mutex>

#define PARTITION_SERVER_NAME_PREFIX "yessql-partition_server-"
#define PARTITION_SERVER_PORT 9000

#define PARTITION_SERVER_THREAD_POOL_SIZE_ENV_VAR "PARTITION_SERVER_THREAD_POOL_SIZE"

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

        void process_remove_queue() override;

    public:
        Partition_Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL, uint32_t thread_pool_size = SERVER_DEFAULT_THREAD_POOL_VAL);

        ~Partition_Server();

        // THROWS
        // extracts the first value string contained in the message
        std::string extract_value(const std::string& raw_message) const;

        // Processes the clients request GET SET etc...
        int8_t process_request(socket_t socket_fd, Server_Message& serv_msg);

        // handles SET, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_set_request(socket_t socket_fd, const Server_Message& message);

        // handles GET, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_get_request(socket_t socket_fd, const Server_Message& message);

        // handles REMOVE, responds to the socket_fd, upon failure returns <0 on success >= 0 
        int8_t handle_remove_request(socket_t socket_fd, const Server_Message& message);

        int8_t handle_get_keys_request(socket_t socket_fd, Server_Message& message);

        int8_t handle_get_keys_prefix_request(socket_t socket_fd, Server_Message& message);

        // handles both GET_FF and GET_FB
        int8_t handle_get_fx_request(socket_t socket_fd, Server_Message& message, Command_Code com_code);

        std::pair<std::string, Cursor_Info> extract_key_and_cursinf(Server_Message& message, std::string* prefix = nullptr);

        void queue_socket_for_not_found_response(socket_t socket_fd, protocol_id_t client_id);

        void queue_socket_for_err_response(socket_t socket_fd, protocol_id_t client_id);

        void queue_socket_for_ok_response(socket_t socket_fd, protocol_id_t client_id);

        // pass either GET_FF or GET_FB as the command_code arguments
        Server_Message create_entries_set_resp(Command_Code com_code, std::set<Entry> entries_set, std::string next_key, protocol_id_t client_id, Cursor_Info& curs_info);

        Server_Message create_keys_set_resp(Command_Code com_code, std::set<Bits> entries_set, std::string next_key, protocol_id_t client_id, Cursor_Info& curs_info);

        // used for extracting special edge case
        bool is_fb_edge_flag_set(std::string& msg);

        // @brief handles client requests
        // calls the handle int8_t process_request(socket_t, string);
        // on failure pushes the socket_fd to remove_queue 
        void handle_client(socket_t socket_fd, Server_Message message);

        int8_t start() override;
};

#endif // YSQL_PARTITION_SERVER_H_INCLUDED
