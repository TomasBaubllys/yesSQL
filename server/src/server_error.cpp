#include "../include/server_error.h"

Server_Error::Server_Error(uint16_t error_code, const char* str) : error_code(error_code), error_str(str) {

}

Server_Error::~Server_Error() {

}

const char* Server_Error::what() const noexcept {
    return this -> error_str.c_str();
}

const uint16_t Server_Error::code() const noexcept {
    return this -> error_code;
}