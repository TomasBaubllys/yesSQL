#ifndef YSQL_FD_CONTEXT_H_INCLUDED
#define YSQL_FD_CONTEXT_H_INCLUDED

#include <cstdint>
#include "protocol.h"

enum class Fd_Type : uint8_t {
    CLIENT,
    PARTITION,
    LISTENER
};

#endif // YSQL_FD_CONTEXT_H_INCLUDED