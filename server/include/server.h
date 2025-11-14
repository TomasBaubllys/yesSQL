#ifndef YSQL_SERVER_H_INCLUDED
#define YSQL_SERVER_H_INCLUDED

#include <cstdint>
#include <iostream>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
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
#include <shared_mutex>

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
#define SERVER_EPOLL_MOD_FAILED_ERR_MSG "Epoll mod failed: "
#define SERVER_WAKE_UP_FD_FAILED_ERR_MSG "Error creating wake up fd\n"
#define SERVER_EPOLL_FD_HOOKUP_ERR_MSG "Epoll reported socket dead\n"
#define SERVER_READ_BUFFER_EMPLACE_FAIL_ERR_MSG "Failed to emplace into a write buffer\n"
#define SERVER_FAILED_PARTITION_WRITE_BUFFER_ERR_MSG "Failed to move the message to partition queue\n"


#define SERVER_DEFAULT_EPOLL_EVENT_VAL 255
#define SERVER_DEFAULT_THREAD_POOL_VAL 64

#define SERVER_DEFAULT_VERBOSE_VAL 0
#define SERVER_MESSAGE_BLOCK_SIZE 1024

class Server {
    protected:
        // this servers port
        uint16_t port;
        // this servers file descriptor
        int32_t server_fd;
        // this servers address
        struct sockaddr_in address;

        // variable decides if server prints out the messages.
        uint8_t verbose;

        // used for calling epoll to update sockets 
        // use it as a dummy for send and receives on epoll wait calls 
        socket_t wakeup_fd;
        socket_t epoll_fd;

        // used for distinguishing between socket types
        std::shared_mutex fd_type_map_mutex;
        std::unordered_map<socket_t, Fd_Type> fd_type_map;

        // used for tracking server current comming in message from client or socket
        // threads cannot access it diresctly 
        std::unordered_map<socket_t, Server_Message> read_buffers;

        // used for tracking messages that are being sent out
        std::shared_mutex write_buffers_mutex;
        std::unordered_map<socket_t, Server_Message> write_buffers;

        // used for queueing messages to partitions, instead of writting to write buffer, you push it into the queue
        // can be used by threads to access
        std::shared_mutex partition_queues_mutex;
        std::unordered_map<socket_t, std::queue<Server_Message>> partition_queues;

        // used for modifying epoll socket keeps track of EPOLLOUT and EPOLLIN
        std::shared_mutex epoll_mod_mutex;
        std::unordered_map<socket_t, uint32_t> epoll_mod_map;

        Server_Message create_status_response(Command_Code status, bool contain_cid, protocol_id_t client_id) const;

        std::vector<epoll_event> epoll_events;

        Thread_Pool thread_pool;
        std::mutex remove_mutex;
        std::vector<socket_t> remove_queue;

        void request_to_remove_fd(socket_t socket);
        virtual void process_remove_queue();

        // send a response of all the entries contained in the vector
        // ADD A BOOLEAN TO TELL IF TO CONTAIN CID
        std::string create_entries_response(const std::vector<Entry>& entry_array, bool contain_cid, protocol_id_t client_id) const;
        // @brief makes client_fd (the return value) non-blocking
        // and adds it to inner epoll sockets
        std::vector<socket_t> add_client_socket_to_epoll();

        // @brief initializes the epoll, (create1)
        void init_epoll();

        // @brief adds the inner server_fd to the epoll fd list
        // makes server_fd non blocking
        // THROWS
        void add_this_to_epoll();

        // @brief waits for epoll to spit out some sockets
        int32_t server_epoll_wait();

        // @brief reads a message of structure defined in protocol.h
        // should work for both blocking and non-blocking sockets
        // THROWS
        std::vector<Server_Message> read_messages(socket_t socket);

        void queue_partition_for_response(socket_t socket_fd, Server_Message&& message);

        // THROWS
        int64_t send_message(socket_t socket, Server_Message& serv_msg);

        // @brief extracts the command code from the received message
        // defined in protocol.h
        Command_Code extract_command_code(const std::string& message, bool contains_cid) const;

        // @brief checks a connectivity to a certain socket
        bool try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec = 1) const;

        // @brief connects and returns a socket descriptor
        // on success >=0 on error < 0
        socket_t connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const;

        // @brief makes a given socket non-blocking
        static void make_non_blocking(socket_t& socket);

        // @brief extracts the first key that appears in the message
        // THROWS
        std::string extract_key_str_from_msg(const std::string& message, bool contains_cid) const;

        // @brief sends an ERR response to the provided socket
        Server_Message create_error_response(bool contain_cid, protocol_id_t client_id)const;

        // @brief sens an OK response to a given socket
        Server_Message create_ok_response(bool contain_cid, protocol_id_t client_id) const;

        bool tactical_reload_partition(socket_t socket_fd, Server_Message& out_msg);

        void request_epoll_mod(socket_t socket_fd, int32_t events);

        void apply_epoll_mod_q();

        protocol_array_len_t extract_array_size(std::string msg, bool contains_cid);

        
    public:
        // THROWS
        Server(uint16_t port, uint8_t verbose = SERVER_DEFAULT_VERBOSE_VAL, uint32_t thread_pool_size = SERVER_DEFAULT_THREAD_POOL_VAL);

        virtual ~Server();

        // @brief virtual method to start the server
        // overriden by other server implementations
        virtual int8_t start();

};

#endif // YSQL_SERVER_H_INCLUDED
