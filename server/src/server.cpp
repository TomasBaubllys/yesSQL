#include "../include/server.h"
#include <cerrno>
#include <fcntl.h>
#include <sys/eventfd.h>

Server::Server(uint16_t port, uint8_t verbose, uint32_t thread_pool_size) : port(port), verbose(verbose), epoll_events(SERVER_DEFAULT_EPOLL_EVENT_VAL), thread_pool(thread_pool_size) {
	// create a server_fd AF_INET - ipv4, SOCKET_STREAM - TCP
	this -> server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
	if(this -> server_fd < 0) {
		throw std::runtime_error(SERVER_SOCKET_FAILED_ERR_MSG);
	}

	// set server options to reuse addresses if a crash happens
	int32_t options = 1;

	if(setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &options, sizeof(options))) {
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
    // close all the sockets and clear all the maps
    for(std::unordered_map<socket_t, Fd_Type>::iterator it = this -> fd_type_map.begin(); it != this -> fd_type_map.end(); ++it) {
        epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, it -> first, nullptr);
        close(it -> first);
    }

    close(this -> epoll_fd);
    close(this -> wakeup_fd);
}

int8_t Server::start() {

    return 0;
}

std::vector<Server_Message> Server::read_messages(socket_t socket_fd) {
    if (socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    char block[SERVER_MESSAGE_BLOCK_SIZE];
    std::vector<Server_Message> messages;

    Server_Message& partial = this -> read_buffers[socket_fd];

    bool header_has_client_id = false;
    std::unordered_map<socket_t, Fd_Type>::iterator type_it = this -> fd_type_map.find(socket_fd);
    if (type_it != this -> fd_type_map.end() && type_it -> second != Fd_Type::CLIENT) {
        header_has_client_id = true;
    }

    while (true) {
        int32_t bytes_read = recv(socket_fd, block, SERVER_MESSAGE_BLOCK_SIZE, 0);

        if (bytes_read < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            this -> request_to_remove_fd(socket_fd);
            std::string fail_recv_str(SERVER_FAILED_RECV_ERR_MSG);
            fail_recv_str += SERVER_ERRNO_STR_PREFIX;
            fail_recv_str += std::to_string(errno);
            throw std::runtime_error(fail_recv_str);
        }

        if (bytes_read == 0) {
            this -> request_to_remove_fd(socket_fd);
            return {};
        }

        partial.get_string_data().append(block, bytes_read);

        while (true) {
            size_t buffer_size = partial.string().size();

            if (buffer_size < sizeof(protocol_msg_len_t))
                break;

            protocol_msg_len_t msg_len_net = 0;
            memcpy(&msg_len_net, partial.string().data(), sizeof(msg_len_net));
            protocol_msg_len_t msg_len = protocol_msg_len_ntoh(msg_len_net);

            if (buffer_size < msg_len)
                break;

            std::string one_msg_str = partial.string().substr(0, msg_len);

            Server_Message msg(one_msg_str);
            msg.set_to_process(msg_len);

            if (header_has_client_id && msg_len >= sizeof(protocol_msg_len_t) + sizeof(protocol_id_t)) {
                protocol_id_t net_cid = 0;
                memcpy(&net_cid, one_msg_str.data() + sizeof(protocol_msg_len_t), sizeof(net_cid));
                msg.set_cid(protocol_id_ntoh(net_cid));
            }

            messages.push_back(msg);

            partial.get_string_data().erase(0, msg_len);
        }
    }

    return messages;
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
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return -1; // try again later
        } 
        else {
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

    if(pos + sizeof(command_code_t) > message.size()) {
        return INVALID_COMMAND_CODE;
    }

    command_code_t com_code;
    memcpy(&com_code, &message[pos], sizeof(command_code_t));
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

    if(pos + sizeof(protocol_key_len_t)  > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_t key_len = 0;
    memcpy(&key_len, &message[pos], sizeof(key_len));
    key_len = ntohs(key_len);

    if(pos + sizeof(protocol_key_len_t) + key_len > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return message.substr(pos + sizeof(protocol_key_len_t), key_len);
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
    protocol_array_len_t arr_len = 0;
    command_code_t com_code = command_hton(status);
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

}

void Server::queue_partition_for_response(socket_t socket_fd, Server_Message&& message) {
    std::unique_lock<std::shared_mutex> lock(partition_queues_mutex);
    this -> partition_queues[socket_fd].push(std::move(message));
    this -> request_epoll_mod(socket_fd, EPOLLIN | EPOLLOUT);
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


std::string Server::create_entries_response(const std::vector<Entry>& entry_array, bool contain_cid, protocol_id_t client_id) const{
    protocol_msg_len_t msg_len = (contain_cid? sizeof(protocol_id_t) : 0) + sizeof(protocol_msg_len_t) + sizeof(protocol_array_len_t) + sizeof(command_code_t);
    for(const Entry& entry : entry_array) {
        msg_len += sizeof(protocol_key_len_t) + sizeof(protocol_value_len_type);
        msg_len += entry.get_key_length() + entry.get_value_length();
    }

    protocol_array_len_t array_len = entry_array.size();
    command_code_t com_code = COMMAND_CODE_OK;
    array_len = protocol_arr_len_hton(array_len);
    protocol_msg_len_t net_msg_len = protocol_msg_len_hton(msg_len);
    com_code = command_hton(com_code);

    size_t curr_pos = 0;
    std::string raw_message(msg_len, '\0');
    memcpy(&raw_message[0], &net_msg_len, sizeof(net_msg_len));
    curr_pos += sizeof(net_msg_len);

    if(contain_cid) {
        protocol_id_t net_cid = protocol_id_hton(client_id);
        memcpy(&raw_message[curr_pos], &net_cid, sizeof(net_cid));
        curr_pos += sizeof(net_cid);
    }

    memcpy(&raw_message[curr_pos], &array_len, sizeof(array_len));
    curr_pos += sizeof(array_len);

    memcpy(&raw_message[curr_pos], &com_code, sizeof(com_code));
    curr_pos += sizeof(com_code);

    for(const Entry& entry : entry_array) {
        protocol_key_len_t key_len = entry.get_key_length();
        protocol_value_len_type value_len = entry.get_value_length();
        protocol_key_len_t net_key_len = protocol_key_len_hton(key_len);
        protocol_value_len_type net_value_len = protocol_value_len_hton(value_len);

        memcpy(&raw_message[curr_pos], &net_key_len, sizeof(net_key_len));
        curr_pos += sizeof(net_key_len);

        std::string key_bytes = entry.get_key_string();
        memcpy(&raw_message[curr_pos], &key_bytes[0], key_len);
        curr_pos += key_len;

        memcpy(&raw_message[curr_pos], &net_value_len, sizeof(net_value_len));
        curr_pos += sizeof(net_value_len);

        std::string value_bytes = entry.get_value_string();
        // std::cout << "Sending value: " << value_bytes << std::endl;
        memcpy(&raw_message[curr_pos], &value_bytes[0], value_len);
        curr_pos += value_len;
    }

    return raw_message;
}
