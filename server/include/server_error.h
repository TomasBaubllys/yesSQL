#ifndef YSQL_SERVER_ERROR_H_INCLUDED
#define YSQL_SERVER_ERROR_H_INCLUDED

#include <exception>
#include <cstdint>
#include <string>

enum class Server_Error_Codes : uint16_t {
    UNKNOWN,
    PARTITION_DIED,
    CURSOR_NOT_FOUND,
    SET_FAILED,
    GET_FAILED,
    GET_FF_FAILED,
    GET_FB_FAILED,
    GET_KEYS_FAILED,
    GET_KEYS_PREFIX_FAILED
};

class Server_Error : public std::exception {
    private: 
        uint16_t error_code;
        std::string error_str;
    public:
        Server_Error(uint16_t error_code, const char* str);
        ~Server_Error();
        
        const char* what() const noexcept;
		const uint16_t code() const noexcept; 
};

#endif // YSQL_SERVER_ERROR_H_INCLUDED