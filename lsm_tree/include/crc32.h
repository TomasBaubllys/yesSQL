#ifndef YSQL_CRC32_H_INCLUDED
#define YSQL_CRC32_H_INCLUDED

#include "bits.h"

// @brief uses crc32 algorithm to hash bits_to_hash Bits converted to std::string
// @returns 8 bytes of hashed bits_to_hash
uint32_t crc32(Bits& bits_to_hash);

// @brief uses crc32 algorithm to hash string_to_hash
// @returns 8 bytes of hashed string_to_hash
uint32_t crc32(std::string& string_to_hash);

#endif
