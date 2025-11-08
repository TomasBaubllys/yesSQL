#include "../include/server.h"
#include <cerrno>
#include <fcntl.h>

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
    close(this -> epoll_fd);
}

int8_t Server::start() {
    if (listen(this->server_fd, 255) < 0) {   
        std::string listen_failed_str(SERVER_FAILED_LISTEN_ERR_MSG);
        listen_failed_str += SERVER_ERRNO_STR_PREFIX;
        listen_failed_str += std::to_string(errno);
        throw std::runtime_error(listen_failed_str);
    }

    init_epoll();

    add_this_to_epoll();

    while (true) {
        int32_t ready_fd_count = this -> server_epoll_wait();
        if(ready_fd_count < 0) {
            if(errno == EINTR) {
                continue;
            }
            if(this -> verbose > 0) {
                std::cerr << SERVER_EPOLL_WAIT_FAILED_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                break;
            }
        }

        for(int32_t i = 0; i < ready_fd_count; ++i) {
            socket_t socket_fd = this -> epoll_events.at(i).data.fd;
            
            if(socket_fd == this -> server_fd) {
                socket_t client_fd = this -> add_client_socket_to_epoll();
                this -> sockets_map[client_fd] = Socket_Types::MAIN_SERVER_SOCKET;
            }
            else if(this -> epoll_events[i].events & EPOLLIN) {
                std::string message = this -> read_message(socket_fd);
                if(message.empty()) {
                    continue;
                }

                this -> thread_pool.enqueue([this, socket_fd, message](){
                    this -> handle_client(socket_fd, message);
                });
            }
            else if(this -> epoll_events[i].events & EPOLLOUT) {
                std::lock_guard<std::mutex> lock(this -> response_queue_mutex);
                std::unordered_map<socket_t, Server_Response>::iterator msg = this -> partial_write_buffers.find(socket_fd);
                
                if(msg != this -> partial_write_buffers.end()) {
                    std::string& data = msg -> second.message;
                    int64_t bytes_sent = this -> send_message(socket_fd, data);
                    if(bytes_sent > 0) {
                        msg -> second.bytes_processed += bytes_sent;
                    }

                    if(msg -> second.bytes_to_process <= msg -> second.bytes_processed) {
                        this -> partial_write_buffers.erase(msg);
                        // return to listening stage
                        try {
                            this -> modify_socket_for_receiving_epoll(socket_fd);
                        }
                        catch(const std::exception& e) {
                            if(this -> verbose > 0) {
                                std::cerr << e.what() << std::endl;
                            }
                        } 
                    }
                }
            }
            else if (this -> epoll_events[i].events & (EPOLLHUP | EPOLLERR)) {
                // Client hang-up or error
                if (verbose > 0)
                    std::cerr << "Client error/hang-up: fd=" << socket_fd << std::endl;
                epoll_ctl(epoll_fd, EPOLL_CTL_DEL, socket_fd, nullptr);
                close(socket_fd);
            }
        }

        this -> process_remove_queue();
    }

    return 0;
}

std::string Server::read_message(socket_t socket) {
    if(socket < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }
    char block[SERVER_MESSAGE_BLOCK_SIZE];

    std::string &buffer = this -> partial_read_buffers[socket].second;
    protocol_message_len_type& bytes_to_read = this -> partial_read_buffers[socket].first;

    // new instance, bytes to read must be a trash value
    if(buffer.size() < sizeof(protocol_message_len_type)) {
        bytes_to_read = 0;
    }

    while (true) {
        int32_t bytes_read = recv(socket, block, SERVER_MESSAGE_BLOCK_SIZE, 0);
        if (bytes_read < 0) {
            if(errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            else {
                if(this ->verbose > 0) {
                    std::cerr << SERVER_FAILED_RECV_ERR_MSG << SERVER_ERRNO_STR_PREFIX << errno << std::endl;
                }

                partial_read_buffers.erase(socket);
                close(socket);
                return "";
            }
        }
        if (bytes_read == 0) {
            partial_read_buffers.erase(socket);
            close(socket);
            // somehow indicate that client quit
            return buffer;
        }

        buffer.append(block, bytes_read);

        if(bytes_to_read == 0 && buffer.size() >= sizeof(protocol_message_len_type)) {
            memcpy(&bytes_to_read, buffer.data(), sizeof(protocol_message_len_type));
            bytes_to_read = ntohll(bytes_to_read);
        }

        if(bytes_to_read > 0 && buffer.size() >= bytes_to_read) {
            break;
        }

        if(!(fcntl(socket, F_GETFL) & O_NONBLOCK)) {
            continue;
        }
        else {
            break;
        }
    }
    
    if (bytes_to_read > 0 && buffer.size() >= bytes_to_read) {
        std::string msg = buffer.substr(0, bytes_to_read);
        buffer.erase(0, bytes_to_read);
        return msg;
    }

    return ""; // message not complete yet

}

int64_t Server::send_message(socket_t socket_fd, const std::string& message) const {
    if(socket_fd < 0) {
        throw std::runtime_error(SERVER_INVALID_SOCKET_ERR_MSG);
    }

    return send(socket_fd, message.data(), message.size(), 0);

    /*ssize_t total_sent = 0;
    ssize_t sent_bytes = 0;

    while (total_sent < message.size()) {
        sent_bytes = send(socket, message.data() + total_sent, message.size() - total_sent, 0);
        if (sent_bytes < 0) {
            if (errno == EINTR) {
                continue; 
            }
            std::string send_failed_str(SERVER_FAILED_SEND_ERR_MSG);
            send_failed_str += SERVER_ERRNO_STR_PREFIX;
            send_failed_str += std::to_string(errno);
            throw std::runtime_error(send_failed_str);
        }

        if (sent_bytes == 0) {
            throw std::runtime_error(SERVER_SEND_0_BYTES_ERR_MSG);
        } 

        total_sent += sent_bytes;

    }

    return static_cast<int64_t>(total_sent);*/
}

Command_Code Server::extract_command_code(const std::string& message) const {
    if(PROTOCOL_COMMAND_NUMBER_POS + sizeof(command_code_type) > message.size()) {
        return INVALID_COMMAND_CODE;
    }

    command_code_type com_code;
    memcpy(&com_code, &message[PROTOCOL_COMMAND_NUMBER_POS], sizeof(command_code_type));
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
    // std::cout << port << std::endl;
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

std::string Server::extract_key_str_from_msg(const std::string& message) const {
    // check if the command is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type)  > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    protocol_key_len_type key_len = 0;
    memcpy(&key_len, &message[PROTOCOL_FIRST_KEY_LEN_POS], sizeof(key_len));
    key_len = ntohs(key_len);

    // check if msg is long enough
    if(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type) + key_len > message.size()) {
        throw std::runtime_error(SERVER_MESSAGE_TOO_SHORT_ERR_MSG);
    }

    return message.substr(PROTOCOL_FIRST_KEY_LEN_POS + sizeof(protocol_key_len_type), key_len);
}

void Server::make_non_blocking(socket_t& socket) {
    int flags = fcntl(socket, F_GETFL, 0);
    if(flags == -1) {
        flags = 0;
    }

    fcntl(socket, F_SETFL, flags | O_NONBLOCK);
}

std::string Server::create_ok_response() const {
    return this -> create_status_response(COMMAND_CODE_OK);
}

std::string Server::create_error_response() const {
    return this -> create_status_response(COMMAND_CODE_ERR);
}

std::string Server::create_status_response(Command_Code status) const {
    if(status != COMMAND_CODE_ERR && status != COMMAND_CODE_OK && status != COMMAND_CODE_DATA_NOT_FOUND) {
        return "";
    }

    protocol_message_len_type message_length;
    protocol_array_len_type arr_len = 0;
    command_code_type com_code = command_hton(status);
    message_length = sizeof(message_length) + sizeof(arr_len) + sizeof(com_code);
    std::string message(message_length, '\0');

    protocol_message_len_type network_msg_len = protocol_msg_len_hton(message_length);

    size_t curr_pos = 0;
    memcpy(&message[0], &network_msg_len, sizeof(network_msg_len));
    curr_pos += sizeof(network_msg_len);
    memcpy(&message[curr_pos], &arr_len, sizeof(arr_len));
    curr_pos += sizeof(arr_len);
    memcpy(&message[curr_pos], &com_code, sizeof(com_code));

    return message;
}

void Server::init_epoll() {
    this -> epoll_fd = epoll_create1(0);
    if(this -> epoll_fd < 0) {
        throw std::runtime_error(SERVER_FAILED_EPOLL_CREATE_ERR_MSG);
    }
}

socket_t Server::add_client_socket_to_epoll() {
    sockaddr_in client_addr{};
    socklen_t len = sizeof(client_addr);
    socket_t client_fd = accept(server_fd, (sockaddr*)&client_addr, &len);
    if(client_fd < 0) {
        return -1;
    }

    epoll_event ep_ev{};
    this -> make_non_blocking(client_fd);
    
    ep_ev.events = EPOLLIN | EPOLLET;
    ep_ev.data.fd = client_fd;
    epoll_ctl(this -> epoll_fd, EPOLL_CTL_ADD, client_fd, &ep_ev);

    return client_fd;
}

int32_t Server::server_epoll_wait() {
    return epoll_wait(this -> epoll_fd, this -> epoll_events.data(), this -> epoll_events.size(), -1);
}

void Server::add_this_to_epoll() {
    this -> make_non_blocking(this -> server_fd);
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
        epoll_ctl(this -> epoll_fd, EPOLL_CTL_DEL, sock, nullptr);
        close(sock);
    }
    remove_queue.clear();
}

void Server::handle_client(socket_t socket_fd, const std::string& message) {
    try{
        if(this -> process_request(socket_fd, message) < 0) {
            this -> request_to_remove_fd(socket_fd);
        }
    }   
    catch(const std::exception& e) {
        if(this -> verbose > 0) {
            std::cerr << e.what() << std::endl;
        }

        this -> request_to_remove_fd(socket_fd);
    }
}

int8_t Server::process_request(socket_t socket_fd, const std::string& message) {
    return -1;
}

void Server::modify_epoll_event(socket_t socket_fd, uint32_t new_events) {
    epoll_event ev{};
    ev.data.fd = socket_fd;
    ev.events = new_events;
    if(epoll_ctl(this -> epoll_fd, EPOLL_CTL_MOD, socket_fd, &ev) < 0) {
        throw std::runtime_error(SERVER_EPOLL_MOD_FAILED_ERR_MSG);
    }
}

void Server::modify_socket_for_sending_epoll(socket_t socket_fd) {
    this -> modify_epoll_event(socket_fd, EPOLLOUT);
}

void Server::modify_socket_for_receiving_epoll(socket_t socket_fd) {
    this -> modify_epoll_event(socket_fd, EPOLLIN | EPOLLET);
}

void Server::add_message_to_response_queue(socket_t socket_fd, const std::string& message) {
    std::unique_lock<std::mutex> lock(this -> response_queue_mutex);
    Server_Response s_resp;
    s_resp.message = message;
    s_resp.bytes_processed = 0;
    s_resp.bytes_to_process = message.length();
    this -> partial_write_buffers[socket_fd] = s_resp; 
}

void Server::prepare_socket_for_response(socket_t socket_fd, const std::string& message) {
    this -> modify_socket_for_sending_epoll(socket_fd);
    this -> add_message_to_response_queue(socket_fd, message);
}

void Server::prepare_socket_for_ok_response(socket_t socket_fd) {
    this -> modify_socket_for_sending_epoll(socket_fd);
    std::string ok_resp_str = this -> create_ok_response();
    this -> add_message_to_response_queue(socket_fd, ok_resp_str);
}
void Server::prepare_socket_for_err_response(socket_t socket_fd) {
    this -> modify_socket_for_sending_epoll(socket_fd);
    std::string err_resp_str = this -> create_error_response();
    this -> add_message_to_response_queue(socket_fd, err_resp_str);
}