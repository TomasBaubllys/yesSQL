#include "../include/server_error.h"

Server_Error::Server_Error(Server_Error_Codes error_code, const char* str) : error_code(static_cast<uint16_t>(error_code)), error_str(str) {

}

Server_Error::~Server_Error() {

}

const char* Server_Error::what() const noexcept {
    return this -> error_str.c_str();
}

const Server_Error_Codes Server_Error::code() const noexcept {
    return static_cast<Server_Error_Codes>(this -> error_code);
}