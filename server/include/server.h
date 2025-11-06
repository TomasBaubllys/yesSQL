#ifndef YSQL_SERVER_H_INCLUDED
#define YSQL_SERVER_H_INCLUDED

#include <cstdint>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdexcept>
#include <cstdio>
#include <string>
#include "protocol.h"
#include <stdexcept>
#include <netdb.h>
#include <bit>
#include <fcntl.h>
#include <sys/epoll.h>
#include <unordered_map>
#include <utility>

#define SERVER_LISTENING_ON_PORT_MSG "Listening on port: "

#define SERVER_BIND_FAILED_ERR_MSG "bind() failed\n"
#define SERVER_SET_SOCK_OPT_FAILED_ERR_MSG "setsockopt() failed\n"

#define SERVER_SOCKET_FAILED_ERR_MSG "socket() failed\n"

#define SERVER_ERRNO_STR_PREFIX "errno: "

#define SERVER_FAILED_LISTEN_ERR_MSG "Listen failed: "
#define SERVER_FAILED_ACCEPT_ERR_MSG "Accept failed: "
#define SERVER_FAILED_RECV_ERR_MSG "Recv failed: "
#define SERVER_FAILED_SEND_ERR_MSG "Send failed: "

#define SERVER_MESSAGE_TOO_SHORT_ERR_MSG "The received message is too short to be valid\n"
#define SERVER_INVALID_SOCKET_ERR_MSG "The socket provided is invalid\n"

#define SERVER_FAILED_HOSTNAME_RESOLVE_ERR_MSG "Failed to resolve: "
#define SERVER_CONNECT_FAILED_ERR_MSG "Failed to connect: "

#define SERVER_DEFAULT_VERBOSE_VAL 0
#define SERVER_MESSAGE_BLOCK_SIZE 1024

#define SERVER_OK_MSG "OK"

class Server {
    protected:
        uint16_t port;
        int32_t server_fd;
        struct sockaddr_in address;

        std::unordered_map<socket_t, std::pair<protocol_message_len_type , std::string>> partial_buffers;

        // variable decides if server prints out the messages.
        uint8_t verbose;

        int8_t send_status_response(Command_Code status, socket_t socket) const;

    public:
        // THROWS
        Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        virtual int8_t start();

        // THROWS
        std::string read_message(socket_t socket);

        // THROWS
        int64_t send_message(socket_t socket, const std::string& message) const;

        Command_Code extract_command_code(const std::string& raw_message) const;

        bool try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec = 1) const;

        socket_t connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const;

        static void make_non_blocking(socket_t& socket);

        // THROWS
        std::string extract_key_str_from_msg(const std::string& raw_message) const;

        int8_t send_error_response(socket_t socket) const;

        int8_t send_ok_response(socket_t socket) const;

};

#endif // YSQL_SERVER_H_INCLUDED
