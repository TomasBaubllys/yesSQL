#include "../include/server.h"
#include <cerrno>
#include <fcntl.h>
#include <sys/eventfd.h>

Server::Server(uint16_t port, uint8_t verbose) : port(port), verbose(verbose), epoll_events(SERVER_DEFAULT_EPOLL_EVENT_VAL), thread_pool(SERVER_DEFAULT_THREAD_POOL_VAL) {
	// create a server_fd AF_INET - ipv4, SOCKET_STREAM - TCP
	this -> server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(this -> server_fd < 0) {
		throw std::runtime_error(SERVER_SOCKET_FAILED_ERR_MSG);
	}

	// set server options to reuse addresses if a crash happens
	int32_t options = 1;

	if(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &options, sizeof(options))) {
		std::string setsockopt_failed_str(SERVER_SET_SOCK_OPT_FAILED_ERR_MSG);
		setsockopt_failed_str += SERVER_ERRNO_STR_PREFIX;
		setsockopt_failed_str += std::to_string(errno);
		throw std::runtime_error(setsockopt_failed_str);
	}
  
	this -> address.sin_family = AF_INET;
	this -> address.sin_addr.s_addr = INADDR_ANY;
	this -> address.sin_port = htons(port);
  
	if(bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
		std::string bind_failed_str(SERVER_BIND_FAILED_ERR_MSG);
		bind_failed_str += SERVER_ERRNO_STR_PREFIX;
		bind_failed_str += std::to_string(errno);
    	throw std::runtime_error(SERVER_BIND_FAILED_ERR_MSG);
  }
}

Server::~Server() {
    /*
    CLOSE ALL SOCKETS
    
    */
    if (this -> epoll_fd >= 0) {
        close(this -> epoll_fd);
    }

    if(this -> server_fd > 0) {
        close(this -> server_fd);
    }
    this ->fd_type_map.clear();
}

int8_t Server::start() {

    return 0;
}

Server_Message Server::read_message(socket_t socket_fd) {
    if(socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }
    char block[SERVER_MESSAGE_BLOCK_SIZE];

    // Server_Request serv_req = this -> partial_read_buffers[socket_fd];
    std::unordered_map<socket_t, Server_Message>::iterator serv_req_iter = this -> read_buffers.find(socket_fd);
    if(serv_req_iter == this -> read_buffers.end()) {
        std::pair<std::unordered_map<socket_t, Server_Message>::iterator, bool> new_iter = this -> read_buffers.emplace(socket_fd, Server_Message());
        if(new_iter.second) {
            serv_req_iter = new_iter.first;
        }
        else {
            throw std::runtime_error(SERVER_READ_BUFFER_EMPLACE_FAIL_ERR_MSG);
        }
    }

    bool header_has_client_id = false;
    std::unordered_map<socket_t, Fd_Type>::iterator sit = this -> fd_type_map.find(socket_fd);
    if(sit != this -> fd_type_map.end() && sit -> second != Fd_Type::CLIENT) {
        header_has_client_id = true;
    }

    while (true) {
        int32_t bytes_read = recv(socket_fd, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        if (bytes_read < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            else {
                this -> request_to_remove_fd(socket_fd);
                std::string recv_failed_str(SERVER_FAILED_RECV_ERR_MSG);
                recv_failed_str += SERVER_ERRNO_STR_PREFIX;
                recv_failed_str += std::to_string(errno);
                throw std::runtime_error(recv_failed_str);
            }
        }
        // client close socket :(
        if (bytes_read == 0) {
            this -> request_to_remove_fd(socket_fd);
            // somehow indicate that client quit
            return Server_Message();
        }

        serv_req_iter -> second.get_string_data().append(block, bytes_read);
        serv_req_iter -> second.add_processed(bytes_read);

        if(serv_req_iter -> second.to_process() == 0 && serv_req_iter -> second.string().size() >= sizeof(protocol_msg_len_t) + sizeof(protocol_id_t)) {
            protocol_msg_len_t bytes_to_prcs = 0;
            memcpy(&bytes_to_prcs, serv_req_iter -> second.string().data(), sizeof(protocol_msg_len_t));
            serv_req_iter -> second.set_to_process(protocol_msg_len_ntoh(bytes_to_prcs));
            if(header_has_client_id) {
                protocol_id_t net_cid = 0;
                memcpy(&net_cid, serv_req_iter -> second.string().data() + sizeof(protocol_msg_len_t), sizeof(protocol_id_t));
                serv_req_iter -> second.set_cid(protocol_id_ntoh(net_cid));
            }
        }


        if(serv_req_iter -> second.is_fully_read()) {
            break;
        }
    }

    if (serv_req_iter -> second.is_fully_read()) {
        Server_Message msg = serv_req_iter -> second;
        serv_req_iter -> second.reset();
        return msg;
    }

    return Server_Message();

}

int64_t Server::send_message(socket_t socket_fd, Server_Message& serv_msg) {
    if (socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    size_t offset = serv_msg.processed(); // server_response.bytes_processed;
    size_t remaining = 0;

    if (serv_msg.to_process() > serv_msg.processed()) {
        remaining = serv_msg.to_process() - serv_msg.processed();
    }
    else {
        return 0;
    }

    const char* data_ptr = serv_msg.get_string_data().data() + offset;
    ssize_t bytes_sent = send(socket_fd, data_ptr, remaining, MSG_NOSIGNAL);

    if (bytes_sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return 0; // try again later
        } else {
            std::string send_fail_str(SERVER_FAILED_SEND_ERR_MSG);
            send_fail_str += SERVER_ERRNO_STR_PREFIX;
            send_fail_str += std::to_string(errno);
            throw std::runtime_error(send_fail_str);
        }
    }

    serv_msg.add_processed(bytes_sent);
    return bytes_sent;
}


Command_Code Server::extract_command_code(const std::string& message, bool contains_cid) const {
    uint64_t pos = contains_cid? PROTOCOL_COMMAND_NUMBER_POS : PROTOCOL_COMMAND_NUMBER_POS_NOCID;

    if(pos + sizeof(command_code_type) > message.size()) {
        return INVALID_COMMAND_CODE;
    }

    command_code_type com_code;
    memcpy(&com_code, &message[pos], sizeof(command_code_type));
    com_code = command_ntoh(com_code);

    return static_cast<Command_Code>(com_code);
}

bool Server::try_connect(const std::string& hostname, uint16_t port, uint32_t timeout_sec) const {
    bool success = false;

    socket_t sock = this -> connect_to(hostname, port, success);

    if(success) {
        close(sock);
    }

    return success;
}

socket_t Server::connect_to(const std::string& hostname, uint16_t port, bool& is_successful) const {
    struct addrinfo hints{}, *res = nullptr;
    hints.ai_family = AF_INET;       // IPv4
    hints.ai_socktype = SOCK_STREAM; // TCP

    std::string port_str = std::to_string(port);
    int status = getaddrinfo(hostname.c_str(), port_str.c_str(), &hints, &res);
    if (status != 0 || !res) {
        if(this -> verbose > 0) {
            std::cerr << SERVER_FAILED_HOSTNAME_RESOLVE_ERR_MSG << hostname << ": " << gai_strerror(status) << std::endl;
        }
        is_successful = false;
        return -1;
    }

    socket_t sock = socket(res -> ai_family, res -> ai_socktype, res -> ai_protocol);
    if (sock < 0) {
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    if (connect(sock, res -> ai_addr, res -> ai_addrlen) != 0) {
        if(this -> verbose) {
            std::cout << SERVER_CONNECT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
        }
        close(sock);
        freeaddrinfo(res);
        is_successful = false;
        return -1;
    }

    freeaddrinfo(res);
    is_successful = true;
    return sock;
}

std::string Server::extract_key_str_from_msg(const std::string& message, bool contains_cid) const {
    // check if the command is long enough
    uint64_t pos = contains_cid? PROTOCOL_FIRST_KEY_LEN_POS : PROTOCOL_FIRST_KEY_LEN_POS_NOCID;

    if(pos + sizeof(protocol_key_len_type)  > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_type key_len = 0;
    memcpy(&key_len, &message[pos], sizeof(key_len));
    key_len = ntohs(key_len);

    if(pos + sizeof(protocol_key_len_type) + key_len > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return message.substr(pos + sizeof(protocol_key_len_type), key_len);
}

void Server::make_non_blocking(socket_t& socket) {
    int flags = fcntl(socket, F_GETFL, 0);
    if(flags == -1) {
        flags = 0;
    }

    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

Server_Message Server::create_ok_response(bool contain_cid, protocol_id_t client_id) const {
    return this -> create_status_response(COMMAND_CODE_OK, contain_cid, client_id);
}

Server_Message Server::create_error_response(bool contain_cid, protocol_id_t client_id) const {
    return this -> create_status_response(COMMAND_CODE_ERR, contain_cid, client_id);
}

Server_Message Server::create_status_response(Command_Code status, bool contain_cid, protocol_id_t client_id) const {
    if(status != COMMAND_CODE_ERR && status != COMMAND_CODE_OK && status != COMMAND_CODE_DATA_NOT_FOUND) {
        return Server_Message();
    }

    protocol_msg_len_t message_length;
    protocol_array_len_type arr_len = 0;
    command_code_type com_code = command_hton(status);
    message_length = sizeof(message_length) + sizeof(arr_len) + sizeof(com_code);
    if(contain_cid) {
        message_length += sizeof(protocol_id_t);
    }
    std::string message(message_length, '\0');

    protocol_msg_len_t network_msg_len = protocol_msg_len_hton(message_length);

    size_t curr_pos = 0;
    memcpy(&message[0], &network_msg_len, sizeof(network_msg_len));
    curr_pos += sizeof(network_msg_len);
    if(contain_cid) {
        protocol_id_t net_cid = protocol_id_hton(client_id);
        memcpy(&message[curr_pos], &net_cid, sizeof(net_cid));
        curr_pos += sizeof(net_cid);
    }
    memcpy(&message[curr_pos], &arr_len, sizeof(arr_len));
    curr_pos += sizeof(arr_len);
    memcpy(&message[curr_pos], &com_code, sizeof(com_code));

    return Server_Message(message, client_id);
}

void Server::init_epoll() {
    this -> epoll_fd = epoll_create1(0);
    
    this -> wakeup_fd = eventfd(0, EFD_NONBLOCK);
    if(this -> wakeup_fd < 0) {
        throw std::runtime_error(SERVER_WAKE_UP_FD_FAILED_ERR_MSG);
    }

    epoll_event ev;
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = this -> wakeup_fd;
    epoll_ctl(this->epoll_fd, EPOLL_CTL_ADD, this -> wakeup_fd, &ev);

    if(this -> epoll_fd < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_CREATE_ERR_MSG);
    }
}

std::vector<socket_t> Server::add_client_socket_to_epoll() {
    std::vector<socket_t> client_fds;

    while(true) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);

        socket_t client_fd = accept(server_fd, (sockaddr*)&client_addr, &len);
        if(client_fd < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }

            return {};
        }

        epoll_event ep_ev{};
        this -> make_non_blocking(client_fd);
        
        ep_ev.events = EPOLLIN;// | EPOLLET;
        ep_ev.data.fd = client_fd;
        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, client_fd, &ep_ev) < 0) {
            throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
        };

        client_fds.push_back(client_fd);
    }

    return client_fds;
}

int32_t Server::server_epoll_wait() {
    return epoll_wait(this -> epoll_fd, this -> epoll_events.data(), this -> epoll_events.size(), -1);
}

void Server::add_this_to_epoll() {
    this -> make_non_blocking(this -> server_fd);
    this -> fd_type_map[this -> server_fd] = Fd_Type::LISTENER;
    epoll_event ep_ev{};
    ep_ev.events = EPOLLIN;
    ep_ev.data.fd = this -> server_fd;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, this -> server_fd, &ep_ev) < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_ADD_FAILED_ERR_MSG);
    }
}

void Server::request_to_remove_fd(socket_t socket) {
    std::lock_guard<std::mutex> lock(this -> remove_mutex);
    this -> remove_queue.push_back(socket);
}
        
void Server::process_remove_queue() {
   


    std::lock_guard<std::mutex> lock(this -> remove_mutex);
    for(socket_t& sock : remove_queue) {
        this -> fd_type_map.erase(sock);

        if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, sock, nullptr) < 0) {

        }
        
        close(sock);
    }
    remove_queue.clear();
}

static uint32_t added_count = 0;
void Server::queue_socket_for_response(socket_t socket_fd, const Server_Message& message) {
    std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
    this -> partition_queues[socket_fd].push(std::move(message));
    this -> request_epoll_mod(socket_fd, EPOLLIN | EPOLLOUT);
    ++added_count;
    std::cout << "ADDED ---------------------- to partition queue total: " << added_count << std::endl;
    message.print();
} 

bool Server::tactical_reload_partition(socket_t socket_fd, Server_Message& out_msg) {
       std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
       
       if (!this -> partition_queues[socket_fd].empty()) {
           out_msg = std::move(this -> partition_queues[socket_fd].front());
           this -> partition_queues[socket_fd].pop();
           return true;
       }
       
       return false;
}


void Server::request_epoll_mod(socket_t socket_fd, int32_t events) {
    std::unique_lock<std::shared_mutex> lock(this -> epoll_mod_mutex);
    this -> epoll_mod_map[socket_fd] = events;
    uint64_t one = 1;
    write(this -> wakeup_fd, &one, sizeof(one));
}

void Server::apply_epoll_mod_q() {
    std::unordered_map<socket_t, uint32_t> local_copy;
    {
        std::unique_lock<std::shared_mutex> lock(this -> epoll_mod_mutex);
        local_copy.swap(this -> epoll_mod_map);
    }

    for (const std::pair<socket_t, uint32_t> p_event : local_copy) {
        epoll_event ev{};
        ev.data.fd = p_event.first;
        ev.events = p_event.second;

        if (epoll_ctl(this -> epoll_fd, EPOLL_CTL_MOD, p_event.first, &ev) < 0) {
            if (errno == EBADF || errno == ENOENT) {
                this -> request_to_remove_fd(p_event.first);
                continue;
            }
            std::string epol_mod_fail_str(SERVER_EPOLL_MOD_FAILED_ERR_MSG);
            epol_mod_fail_str += SERVER_ERRNO_STR_PREFIX;
            epol_mod_fail_str += std::to_string(errno);
            throw std::runtime_error(epol_mod_fail_str);
        }
    }
}
