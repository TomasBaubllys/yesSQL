#ifndef YSQL_FD_CONTEXT_H_INCLUDED
#define YSQL_FD_CONTEXT_H_INCLUDED

#include <cstdint>
#include "protocol.h"

enum class Fd_Type : uint8_t {
    CLIENT,
    PARTITION,
    LISTENER
};

enum class Query_Direction : uint8_t {
    CLIENT_TO_PARTITION,
    PARTITION_TO_CLIENT
};

typedef struct Fd_Context {
    Fd_Type fd_type;
    socket_t socket_fd;
} Fd_Context;

typedef struct Query_Context {
    std::vector<socket_t> partition_sockets;
    socket_t client_socket;
    Server_Message message;
    Query_Direction query_direction;
    uint64_t client_id;
} Request_Context;


#endif // YSQL_FD_CONTEXT_H_INCLUDED