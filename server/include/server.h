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
#include "thread_pool.h"

#define SERVER_LISTENING_ON_PORT_MSG "Listening on port: "

#define SERVER_BIND_FAILED_ERR_MSG "bind() failed\n"
#define SERVER_SET_SOCK_OPT_FAILED_ERR_MSG "setsockopt() failed\n"

#define SERVER_SOCKET_FAILED_ERR_MSG "socket() failed\n"

#define SERVER_ERRNO_STR_PREFIX "errno: "

#define SERVER_FAILED_LISTEN_ERR_MSG "Listen failed: "
#define SERVER_FAILED_ACCEPT_ERR_MSG "Accept failed: "
#define SERVER_FAILED_RECV_ERR_MSG "Recv failed: "
#define SERVER_FAILED_SEND_ERR_MSG "Send failed: "
#define SERVER_SEND_0_BYTES_ERR_MSG "0 bytes send socket likely closed\n"

#define SERVER_MESSAGE_TOO_SHORT_ERR_MSG "The received message is too short to be valid\n"
#define SERVER_INVALID_SOCKET_ERR_MSG "The socket provided is invalid\n"

#define SERVER_FAILED_HOSTNAME_RESOLVE_ERR_MSG "Failed to resolve: "
#define SERVER_CONNECT_FAILED_ERR_MSG "Failed to connect: "
#define SERVER_FAILED_EPOLL_CREATE_ERR_MSG "Failed to create epoll fd\n"
#define SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG "Failed to add fd to epoll fd\n"
#define SERVER_EPOLL_WAIT_FAILED_ERR_MSG "Epoll wait failed: "
#define SERVER_FAILED_EPOLL_CREATE_ERR_MSG "Failed to create epoll fd\n"
#define SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG "Failed to add fd to epoll fd\n"
#define SERVER_EPOLL_WAIT_FAILED_ERR_MSG "Epoll wait failed: "
#define SERVER_DEFAULT_EPOLL_EVENT_VAL 255
#define SERVER_DEFAULT_THREAD_POOL_VAL 64

#define SERVER_DEFAULT_VERBOSE_VAL 0
#define SERVER_MESSAGE_BLOCK_SIZE 1024

#define SERVER_OK_MSG "OK"

class Server {
    protected:
        std::unordered_map<socket_t, Socket_Types> sockets_map;

        uint16_t port;
        int32_t server_fd;
        struct sockaddr_in address;

        std::unordered_map<socket_t, std::pair<protocol_message_len_type , std::string>> partial_buffers;

        // variable decides if server prints out the messages.
        uint8_t verbose;

        int8_t send_status_response(Command_Code status, socket_t socket) const;

        file_desc_t epoll_fd;

        std::vector<epoll_event> epoll_events;\

        Thread_Pool thread_pool;
        std::mutex remove_mutex;
        std::vector<socket_t> remove_queue;
        void request_to_remove_fd(socket_t socket);
        void process_remove_queue();
        
    public:
        // THROWS
        Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        ~Server();

        virtual int8_t start();

        // makes client_fd (the return value) non-blocking
        socket_t add_client_socket_to_epoll();
        
        file_desc_t init_epoll();

        // makes server_fd non blocking
        int32_t add_this_to_epoll();

        int32_t server_epoll_wait();

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

        // calls the handle int8_t process_request(socket_t, string);
        // on failure pushes the socket_fd to remove_queue 
        void handle_client(socket_t socket_fd, const std::string& message);

        virtual int8_t process_request(socket_t socket_fd, const std::string& message);
};

#endif // YSQL_SERVER_H_INCLUDED
