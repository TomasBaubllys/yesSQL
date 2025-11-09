#ifndef YSQL_PARTITION_ENTRY_H_INCLUDED
#define YSQL_PARTITION_ENTRY_H_INCLUDED

#include "protocol.h"
#include "range.h"
#include <string>

typedef enum Partition_Status {
    PARTITION_BLOCKED,
    PARTITION_FREE,
    PARTITION_DEAD
} Partition_Status;

// Note that partition range is [beg; end)
typedef struct Partition_Entry {
    Range range;
    std::string name;
    uint16_t port;
    socket_t socket_fd;
    int8_t status;
    int16_t id;
} Partition_Entry;

#endif // YSQL_PARTITION_ENTRY_H_INCLUDED
