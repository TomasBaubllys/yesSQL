#ifndef YSQL_PARTITION_SERVER_H_INCLUDED
#define YSQL_PARTITION_SERVER_H_INCLUDED

#include "server.h"
#include <cstdio>
#include "../../lsm_tree/include/lsm_tree.h"

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

    public:
        Partition_Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        int8_t send_not_found_response(socket_t socket_fd) const;

        int8_t send_entries_response(const std::vector<Entry>& entry_array, socket_t socket) const;

        // THROWS
        std::string extract_value(const std::string& raw_message) const;

        int8_t start() override;

        // handles the client, passes it to 
        int8_t process_request(socket_t socket_fd, const std::string& message) override;

};

#endif // YSQL_PARTITION_SERVER_H_INCLUDED
