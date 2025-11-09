#ifndef YSQL_SERVER_MESSAGE_H_INCLUDED
#define YSQL_SERVER_MESSAGE_H_INCLUDED

#include "protocol.h"

typedef struct Server_Message {
    std::string message;
    protocol_message_len_type bytes_processed;
    protocol_message_len_type bytes_to_process;
    uint64_t client_id;
} Server_Message;

using Server_Request = Server_Message;
using Server_Response = Server_Message;

#endif // YSQL_SERVER_MESSAGE_H_INCLUDED