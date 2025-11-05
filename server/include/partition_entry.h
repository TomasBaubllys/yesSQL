#ifndef YSQL_PARTITION_ENTRY_H_INCLUDED
#define YSQL_PARTITION_ENTRY_H_INCLUDED

#include "range.h"
#include <string>

// Note that partition range is [beg; end)
typedef struct Partition_Entry {
    Range range;
    std::string partition_name;
    uint16_t port;
} Partition_Entry;

#endif // YSQL_PARTITION_ENTRY_H_INCLUDED
