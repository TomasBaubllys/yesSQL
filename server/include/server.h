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
#include "server_message.h"
#include "fd_context.h"

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
#define SERVER_EPOLL_MOD_FAILED_ERR_MSG "Epoll mod failed\n"
#define SERVER_DEFAULT_EPOLL_EVENT_VAL 255
#define SERVER_DEFAULT_THREAD_POOL_VAL 64

#define SERVER_DEFAULT_VERBOSE_VAL 0
#define SERVER_MESSAGE_BLOCK_SIZE 1024

#define SERVER_OK_MSG "OK"

class Server {
    protected:
        std::unordered_map<socket_t, Socket_Types> sockets_type_map;
        std::unordered_map<socket_t, Fd_Context*> socket_context_map;

        uint16_t port;
        int32_t server_fd;
        struct sockaddr_in address;

        // client id - request
        std::unordered_map<socket_t, Server_Request> partial_read_buffers;
        
        std::mutex response_queue_mutex;
        // socket - response
        std::unordered_map<socket_t, Server_Response> partial_write_buffers;
        // variable decides if server prints out the messages.
        uint8_t verbose;

        std::string create_status_response(Command_Code status, protocol_id_type client_id) const;

        file_desc_t epoll_fd;

        std::vector<epoll_event> epoll_events;

        Thread_Pool thread_pool;
        std::mutex remove_mutex;
        std::vector<socket_t> remove_queue;
        void request_to_remove_fd(socket_t socket);
        void process_remove_queue();
        
    public:
        // THROWS
        Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL);

        ~Server();

        // @brief virtual method to start the server
        // overriden by other server implementations
        virtual int8_t start();

        // @brief makes client_fd (the return value) non-blocking
        // and adds it to inner epoll sockets
        socket_t add_client_socket_to_epoll(Fd_Context* context = nullptr);
        
        // @brief initializes the epoll, (create1)
        void init_epoll();

        // @brief adds the inner server_fd to the epoll fd list
        // makes server_fd non blocking
        // THROWS
        void add_this_to_epoll();

        // @brief waits for epoll to spit out some sockets
        int32_t server_epoll_wait();

        void modify_socket_for_sending_epoll(socket_t socket_fd);

        void modify_socket_for_receiving_epoll(socket_t socket_fd);

        // @brief reads a message of structure defined in protocol.h 
        // should work for both blocking and non-blocking sockets
        // THROWS
        Server_Message read_message(socket_t socket);

        void add_message_to_response_queue(socket_t socket_fd, const std::string& message);

        // THROWS
        int64_t send_message(socket_t socket, Server_Response& server_response);

        // @brief extracts the command code from the received message
        // defined in protocol.h
        Command_Code extract_command_code(const std::string& message) const;

        // @brief checks a connectivity to a certain socket
        bool try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec = 1) const;

        // @brief connects and returns a socket descriptor 
        // on success >=0 on error < 0
        socket_t connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const;

        // @brief makes a given socket non-blocking
        static void make_non_blocking(socket_t& socket);

        void modify_epoll_event(socket_t socket_fd, uint32_t new_events);

        // @brief extracts the first key that appears in the message
        // THROWS
        std::string extract_key_str_from_msg(const std::string& message) const;

        // @brief sends an ERR response to the provided socket
        std::string create_error_response(protocol_id_type client_id)const;

        // @brief sens an OK response to a given socket
        std::string create_ok_response(protocol_id_type client_id) const;

        void prepare_socket_for_response(socket_t socket_fd, const Server_Message& serv_msg);
        
        void prepare_socket_for_ok_response(socket_t socket_fd, protocol_id_type client_id);

        void prepare_socket_for_err_response(socket_t socket_fd, protocol_id_type client_id);

};

#endif // YSQL_SERVER_H_INCLUDED
