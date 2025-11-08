#ifndef YSQL_SERVER_REQUEST_H_INCLUDED
#define YSQL_SERVER_REQUEST_H_INCLUDED

#include "protocol.h"

typedef struct Server_Request {
    std::string message;
    protocol_message_len_type bytes_processed;
    protocol_message_len_type bytes_to_process;
} Server_Request;

#endif // YSQL_SERVER_REQUEST_H_INCLUDED