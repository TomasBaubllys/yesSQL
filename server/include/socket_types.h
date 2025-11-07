#ifndef YSQL_SOCKET_TYPE_H_INCLUDED
#define YSQL_SOCKET_TYPE_H_INCLUDED

#include <cstdint>

typedef enum Socket_Types : uint16_t {  
    MAIN_SERVER_SOCKET,
    PARTITION_SERVER_SOCKET,
    CLIENT_SOCKET
}Socket_Types;

#endif // YSQL_SOCKET_TYPE_H_INCLUDED