#include "../include/crc32.h"


uint32_t crc32(Bits& bits_to_hash){
    std::string data = bits_to_hash.get_string();

    uint32_t crc = 0xFFFFFFFF;
    
    for (char byte : data) {
        crc ^= byte;
        for (int i = 0; i < 8; i++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc ^ 0xFFFFFFFF;
};


uint32_t crc32(std::string& string_to_hash){

    uint32_t crc = 0xFFFFFFFF;
    
    for (char byte : string_to_hash) {
        crc ^= byte;
        for (int i = 0; i < 8; i++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc >>= 1;
            }
        }
    }
    return crc ^ 0xFFFFFFFF;
};