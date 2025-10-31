#ifndef YSQL_RANGE_H_INCLUDED
#define YSQL_RANGE_H_INCLUDED

#include <cstdint>

typedef struct Range {
    uint32_t beg;
    uint32_t end;
} Range;

#endif // YSQL_RANGE_H_INCLUDED