#ifndef YSQL_SERVER_MESSAGE_H_INCLUDED
#define YSQL_SERVER_MESSAGE_H_INCLUDED

#include "protocol.h"

typedef struct Server_Response {
    std::string message;
    protocol_message_len_type bytes_processed;
} Server_Response;

#endif // YSQL_SERVER_MESSAGE_H_INCLUDED