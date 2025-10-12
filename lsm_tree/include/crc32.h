#ifndef YSQL_CRC32_H_INCLUDED
#define YSQL_CRC32_H_INCLUDED

#include "bits.h"

// returns a crc32 hash of Bits converted to string
uint32_t crc32(Bits& bits_to_hash);

// returns a crc32 hash of a string
uint32_t crc32(std::string& string_to_hash);

#endif
